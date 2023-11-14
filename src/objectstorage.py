# -----------------------------------------------------------
# Data ingestion to Minio bucket
#
# (C) 2023 Rodrigo Costa, Ananindeua, Brasil
# 
# email eng.rodrigomdc@gmail.com
# -----------------------------------------------------------

import config.credentials as credentials
import src.auxiliary as aux
from minio import Minio
from minio.error import *
import logging
import sys
import os 

class DataBucket:

    def __init__(self, server_addr, server_port): 
        #IP address of the Minio Server       
        self._server_addr = server_addr
        #API port of the Minio Service
        self._server_port = server_port        
        self._logger = logging.getLogger(f"dataingestion.{__name__}")
        self._logger.debug("DataBucket")
        self._cli_bucket = self._createConnection()

    def _createConnection(self):

        """
        This function initializes a new client object to access MinIO service
       
        Returns
        -------
            Minio object        
        """

        client = Minio(
            endpoint = f"{self._server_addr}:{self._server_port}",
            access_key = credentials.minio_lib['access_key'],
            secret_key = credentials.minio_lib['secret_key'],
            secure = False 
        )
        self._logger.debug("Connection with Minio successful.")
        return client 
 
    def sendToBucket(self, source_dir, bucket_name):    

        """
        This function is responsible for sending json file to specific bucket in Minio server

        Parameters
        ----------
            source_dir is the path to the local temporary directory that stores the JSON file
            bucket_name is the destination bucket that storage the json files
            status enables tracking of file sending to the destination bucket
        """

        try:
            assert os.path.isdir(source_dir)
            obj_name = aux.getLastFileName(source_dir)
            file_name = source_dir + os.listdir(source_dir)[0]
            try:            
                self._cli_bucket.fput_object(
                bucket_name, 
                obj_name,  
                file_name, 
                content_type = "application/json"
            )            
            except (Exception, S3Error) as e:
                self._logger.exception("Error in Minio service.") 
                sys.exit(1)       
        except AssertionError:
            self._logger.exception(f"Directory {source_dir} does not exist.")
            

        
        
            
            
