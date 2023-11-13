
from src.loadsource import DataSensor
from src.kafkalib import DataProducer
from src.objectstorage import DataBucket
import src.auxiliary as aux
import logging
import sched
import time

def repeatTask():

    specific_time = time.time() + acquisition_time  
    scheduler.enterabs(specific_time, 1, ingestionTask, ())
    scheduler.enterabs(specific_time, 1, repeatTask, ())

def ingestionTask():   
    
    log.info("Load data from ThingSpeak plataform.")    
    src_data = DataSensor(src_url)
    data_sensor = src_data.collectTSData()
    src_data.saveToFile(tmp_dir, data_sensor)   

    log.info(f"Publishing JSON data to {kafka_topic} topic.")    
    prod_data.topicProducer(data_sensor)

    log.info(f"Uploading JSON file to bucket {minio_bucket} in Minio server.")    
    bucket.sendToBucket(tmp_dir, minio_bucket)
    log.info(f"Restarting ingestion task at {acquisition_time} seconds.")

if __name__ == "__main__":

    print("#####################################################################")
    print("#                                                                   #")
    print("#                        DATA INGESTION                             #")
    print("#                                                                   #")
    print("#####################################################################\n")

    #Logs
    log = aux.createLog("dataingestion", logging.INFO)

    #Load parameters from yaml file
    config = aux.loadParams()
    
    #Kafka parameters
    
    kafka_addr = config['kafka']['addr']
    kafka_port = config['kafka']['port']    
    kafka_topic = config['kafka']['source_topic'] 

    #Minio parameters
    minio_addr = config['minio']['addr']
    minio_port = config['minio']['port']    
    minio_bucket = config['minio']['bucket'] 
 
    #Others parameters
    src_url = config['url'] 
    tmp_dir = config['raw_dir']
    acquisition_time = config["schedule_time"]

    log.info(f"Starting DataIngestion service.")    
    log.info(f"Connecting Kafka service.")
    prod_data = DataProducer(kafka_topic, kafka_addr, kafka_port)
    log.info(f"Connection to Kafka successful.")
    log.info(f"Connecting with Minio service.")
    bucket = DataBucket(minio_addr, minio_port)
    log.info(f"Connection to Minio successful.")
    log.info(f"Starting ingestion task.")
    scheduler = sched.scheduler(time.time, time.sleep)
    ingestionTask()
    repeatTask()
    scheduler.run()
   
