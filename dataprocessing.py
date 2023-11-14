
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import *
from src.streaminglib import *
import src.auxiliary as aux
import logging


def main():

    print("#####################################################################")
    print("#                                                                   #")
    print("#                        DATA PROCESSING                            #")
    print("#                                                                   #")
    print("#####################################################################\n")
    
    #Logs
    log = aux.createLog("dataprocessing", logging.DEBUG)
    
    #Load parameters from yaml file
    config = aux.loadParams()
    
    #Kafka parameters    
    kafka_addr = config['kafka']['addr']
    kafka_port = config['kafka']['port']    
    kafka_cons_topic = config['kafka']['source_topic'] 
    kafka_prod_topic = config['kafka']['transformed_topic']    
  
    log.info("Starting Data Processing service.")
    ds = DataStreaming(kafka_addr, kafka_port)

    log.info("Create SparkSession.")
    spark = ds.createSession()
    spark.sparkContext.setLogLevel("ERROR")

    ############  ----------------------------- DATA EXTRACTION ---------------------------------- ##########

    log.info(f"Connecting Spark to Kafka and subscribing to {kafka_cons_topic} topic.")        
    df_input = ds.streamingConsumer(spark, kafka_cons_topic)

    #Schema definition for received message
    file_schema = StructType(
        [
            StructField("created_at", StringType(), True),
            StructField("entry_id", LongType(), True),
            StructField("field1", StringType(), True),
            StructField("field2", StringType(), True),
            StructField("field3", StringType(), True),
            StructField("field4", StringType(), True),
            StructField("field5", StringType(), True),
            StructField("field6", StringType(), True)
        ]
    )

    #Convert binary data to string format
    df_input.selectExpr("CAST(value AS STRING)")
 
    #Load values in received menssage
    df_values = (
     df_input     
     .select(
         from_json(           
             decode(col("value"), "utf-8"),
             file_schema
            ).alias("value")
        )
        .select("value.*")
    )

    ############  ----------------------------- DATA TRANSFORMATION ---------------------------------- ##########

    #Apply basic transformations
    df_values = df_values.withColumn("created_at",from_utc_timestamp("created_at","America/Belem"))

    df_transf = (
     df_values         
         .select(  
                                
            year(df_values["created_at"]).alias('Year'), 
            date_format(df_values["created_at"],"MMM").alias('Month'),
            dayofmonth(df_values["created_at"]).alias('Day'), 
            date_format(df_values["created_at"], 'H:mm:s').alias('Time'),
            df_values["field3"].cast("float").alias('Humidity'),
            df_values["field4"].cast("float").alias("Temperature"),
            df_values["field6"].cast("float").alias("Pressure")
        )
        #Pivoting dataframe by sensor type
        .select("*")
        .melt(
            ids=["Year", "Month", "Day", "Time"], 
            values=["Humidity", "Temperature", "Pressure"], 
            variableColumnName="Sensor", 
            valueColumnName="Value"
        )
    )

    ############  ----------------------------- DATA LOADING ---------------------------------- ##############
    
    log.info(f"Publishing message to {kafka_prod_topic} topic.")
    dt_stream = ds.streamingProducer(df_transf, kafka_prod_topic)
    #dt_stream = ds.streamingProducerDebug(df_transf)    
    dt_stream.awaitTermination()   

if __name__ == "__main__":
    main()    