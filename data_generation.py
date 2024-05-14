import json
import time
from faker import Faker
from datetime import datetime,timedelta

f=Faker()       # Initialise the faker 

class genData():

    def patient(self):
        #variable to store the insurance provider and ethnicities
        insurance_providers = ["ABC Insurance", "XYZ Health", "Global Assurance", "Liberty Mutual",
                               "Nationwide Insurance", "Pacific Life", "Progressive Insurance",
                               "State Farm", "Travelers Insurance", "UnitedHealthcare"]
        
        ethnicities = ["White", "Black or African American", "Hispanic or Latino", "Asian", "Native American or Alaska Native", "Native Hawaiian or Other Pacific Islander", "Multiracial"]

        #varible to store the validity attributes
        start_date = datetime.strptime('2019-12-31', '%Y-%m-%d')
        end_date = datetime.strptime('2030-12-31', '%Y-%m-%d')
        
        #timestamp to run the while loop for 10 mints
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=20)  # Generate data for 10 minutes

        while datetime.now()< end_time:
            dic_pd={
                    "key":f"pid{f.random_int(min=1000, max= 999999999)}",
                    "value":{
                        "name":f.name(),
                        "age":f.random_int(min=1, max= 90),
                        "gender":f.random.choice(['male','female']),
                        "demographics":{
                            "address" :f.address(),
                            "contact_number":f.phone_number(),
                            "ethencity":f.random.choice(ethnicities),
                            "marital_status":f.random.choice(['married','unmarried'])
                                },
                    "insurance_info":{
                        "insurance_provider":f.random.choice(insurance_providers),
                        "claim_id": f"pl{f.random_int(min= 10000, max= 9999999999)}",
                        "validity":f.date_between(start_date=start_date, end_date=end_date).strftime('%Y-%m-%d'),
                        "policy_type": f.random.choice(['premium','basic','platinum'])
                            }
                        }
                    }
            yield dic_pd
            
        
