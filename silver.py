class silver():
    ''' - read streaming bronze tabel 
        -read patient_details json file
        -create dataframe from patient_details json
        -find the max reading in a window of 10 with an interval of 2 min 
        -calculate new col `condition` based on the max reading 
        - sink the data to silver layer 
        '''
    def __init__(self):
        self.jPATH=""

        pass

    def getPatient(self):
       from pyspark.sql import functions as f
       return(
           spark.read.format("json")
                .option("multiline","True")
                .load(self.jPATH)\
                .select(
                    f.col('key').alias('pid'),
                    f.col('value.name'),
                    f.col('value.age'),
                    f.col('value.gender'),
                    f.col('value.demographics.marital_status'),
                    f.col('value.demographics.ethencity'),
                    f.col('value.demographics.address'),
                    f.col('value.demographics.contact_number'),
                    f.col('value.insurance_info.insurance_provider'),
                    f.col('value.insurance_info.validity'),
                    f.col('value.insurance_info.policy_number')
                )
       )

    def readBronze(self):
        return(
            spark.readStream.table(self.bzPATH)
                    )
    def getAggregate(self, sensorDF):
        from pyspark.sql.functions import window, max

        patientDF = self.getPatient()  # Assuming getPatient returns a DataFrame
        return (
            sensorDF.withWatermark("timestamp", "30 Minutes")
            .groupby(
                sensorDF.patient_id, sensorDF.device_id, sensorDF.sensor,
                window("timestamp", "10 Minutes", "2 Minutes").alias("window")
            )
            .agg(max(sensorDF.reading).alias("maxReading"))
            .join(
                patientDF,
                substring(sensorDF.patient_id, 4, 12) == patientDF.pid,
                "left"
            )
            .select(
                sensorDF.patient_id, patientDF.name, patientDF.age, sensorDF.sensor,
                sensorDF.device_id, "window.start", "window.end", "maxReading"
            )
        )
        

    def process(self):
        sensorDF = self.readBronze()
        sQuery = self.getAggregate(sensorDF)        
        
        return(display(sQuery))
