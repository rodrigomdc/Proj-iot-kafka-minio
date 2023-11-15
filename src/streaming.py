# -----------------------------------------------------------
# Support for communication between Spark and Kafka in streaming processing
#
# (C) 2023 Rodrigo Costa, Ananindeua, Brasil
# 
# email eng.rodrigomdc@gmail.com
# -----------------------------------------------------------

from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import sys

class DataStreaming:

    def __init__(self, server_addr, server_port):        
        self._server_addr = server_addr
        self._server_port = server_port
        self._logger = logging.getLogger(f"dataprocessing.{__name__}")
        self._logger.debug("DataStreaming")

    def createSession(self):
        """
        Create a new spark session 

        Return:
        -------
            Spark session object
        """

        spark = (
            SparkSession
            .builder
            .master("local[*]")
            .appName("StreamingProcessing")
            .config("spark.sql.session.timeZone", "UTC")  
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")  
            .getOrCreate()    
        )
        return spark

    def streamingConsumer(self, spark_session, topic):
        """
        Consume a message in a topic in Kafka

        Parameters
        ----------
            spark_session is the session created to read a message by Spark
            topic topic name that Spark must subscribe to access messages

        Return:
        -------
            Data in Pyspark Dataframe format
        """        
        
        try:
            df_input = (
                spark_session
                .readStream 
                .format("kafka") 
                .option("kafka.bootstrap.servers", f"{self._server_addr}:{self._server_port}") 
                .option("subscribe", topic) 
                .option("startingOffsets", "earliest") 
                .option("failOnDataLoss", "false")
                .option("checkpointLocation", "checkpoint")
                .load()
            )            
        except (PySparkException, TypeError) as e:
            self._logger.exception(f"Exception in streamingConsumer()")      
            sys.exit(1) 
        else:
            self._logger.debug(f"Data loaded into a streaming DataFrame.")
        return df_input
    
    def streamingProducer(self, data, topic):
        """
        Publish processed data as a message to a topic

        Parameters:
        -----------
            data is the data processed and ready to be published in a topic
            topic topic name that Spark must publish to access messages

        Return:
        -------
            DataWriteStream object
        """
        
        try:
            df_output = (
                data
                .selectExpr("to_json(struct(*)) AS value")     
                .writeStream 
                .format("kafka") 
                .option("kafka.bootstrap.servers", f"{self._server_addr}:{self._server_port}") 
                .option("topic", topic) 
                .option("checkpointLocation", "checkpoint")
                .start()        
            )            
        except (PySparkException, TypeError) as e:
            self._logger.exception(f"Exception in streamingProducer()")  
            sys.exit(1)    
            
        else:
            self._logger.debug(f"Save the content of the streaming DataFrame out into Kafka topic.")
        return df_output

    def streamingProducerDebug(self, data):
        """
        Show streaming DataFrame to debug 

        Parameters:
        -----------
            data is the data processed and ready to be published in a topic
            

        Return:
        -------
            DataWriteStream object
        """
        
        try:
            df_output = (
                data              
                #.selectExpr("to_json(struct(*)) AS value")    
                .writeStream 
                .format("console")                
                .start()        
            )            
        except (PySparkException, TypeError) as e:
            self._logger.exception(f"Exception in streamingProducerDebug()") 
            sys.exit(1)    
            
        else:
            self._logger.debug(f"Save the content of the streaming DataFrame out into Kafka topic.")
        return df_output