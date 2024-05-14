from pyspark.sql import functions as f


class Bronze():
    '''
        - ingest data from kafka
        - transform value from binary to string 
        - transform string value to struct type 
        - save the stream to bronze table
    '''

    def __init__(self):
        self.base_data_dir= ""
        self.BOOTSTRAP_SERVERS = 'pkc-56d1g.eastus.azure.confluent.cloud:9092'
        self.JAAS_MODULE = 'org.apache.kafka.common.security.plain.PlainLoginModule'
        self.CLUSTER_API_KEY = 'H2OXE2ZKKZNIVY2Z'
        self.CLUSTER_API_SECRETS = 'Oo94M2e093GSZNf0pLkI2lQrOdi0kzEyh/hAmOFQcGJQ1t6yZO7TImh5JVO+FkCG'
      

    def getSchema(self):
        '''return the schema for the json '''
        return(""" <struct device_id int , sensor_type string, reading_value integer,
                        patient_id string, timestamp timestamp >
                              """
               )

    def ingestFromKafka(self, startingTime=1):
        return( spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVERS)
                     .option("kafka.security.protocol", "SASL_SSL")
                     .option("kafka.sasl.mechanism", "PLAIN")
                     .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRETS}';")
                     .option("subscribe", "topic_0")
                     .option("maxOffsetsPerTrigger", 10)
                     .option("startingTimestamp", startingTime)
                     .load()
                     )
        

    def getReading(self, kafka_df):
        return( kafka_df.select( f.col("key").cast("string"),
                                f.col("value").cast("string")
                                )
                )

    def process(self):
        print("starting Bronze Stream...")
        
        rawDF= self.ingestFromKafka()
        readingDF= self.getReading()

        return (
            readingDF.withColumn("json_value", f.from_json(f.col("value"), self.getSchema()))\
            .select(
                f.col("value_json.device_id").alias("device_id"),
                f.col("value_json.sensor_type").alias("sensor"),
                f.col("value_json.reading_value").alias("reading"),
                f.col("value_json.timestamp").alias("timestamp"),
                f.col("value_json.patient_id").alias("patient_id")
            )
        )
    
    def saveBronze(self,processDF):
        processDF= self.process()

        sQuery=(
            processDF.writeStream
                      .queryName("bronze-ingestion")
                      .option("checkpointLocation", f"{self.base_data_dir}/chekpoint/invoices_bz")
                      .outputMode("append")
                      .toTable("device_bz")
        )