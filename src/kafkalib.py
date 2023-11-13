# -----------------------------------------------------------
# Interaction with the kafka-python library
#
# (C) 2023 Rodrigo Costa, Ananindeua, Brasil
# 
# email eng.rodrigomdc@gmail.com
# -----------------------------------------------------------

from kafka import KafkaProducer
import logging
import json

class DataProducer:

    def __init__(self, topic_name, srv_address, srv_port) -> None:
        self.topic_name = topic_name
        self.srv_address = srv_address
        self.srv_port = srv_port
        self.producer = KafkaProducer(
            bootstrap_servers=f"{self.srv_address}:{self.srv_port}"
        )
        self._logger = logging.getLogger(f"dataingestion.{__name__}")
        self._logger.debug("DataProducer")

    def topicProducer(self, data):
        """
        This function publish a message to a topic in JSON format

        Parameters:
            data content sent as a message to a specific topic
        """  
        try:      
            self.producer.send(self.topic_name, json.dumps(data).encode("utf-8"))
        except Exception as e:
            self._logger.exception("Exception occurred during message send.")            
        else:
            self._logger.debug(f"The message was published successfully.")



