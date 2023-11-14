# -----------------------------------------------------------
# Manipulate data collected from an application hosted on ThingSpeak IoT platform
#
# (C) 2023 Rodrigo Costa, Ananindeua, Brasil
# 
# email eng.rodrigomdc@gmail.com
# -----------------------------------------------------------

from urllib.request import urlopen
from urllib.error import HTTPError, URLError
import logging
import json
import sys
import os

class DataSensor:

    def __init__(self, src_url): 
        self._src_url = src_url
        self._logger = logging.getLogger(f"dataingestion.{__name__}")
        self._logger.debug("DataSensor")

    def collectTSData(self):

        """
        This function collects data in JSON format based on the URL (Uniform Resource Locators) of application hosted on ThingSpeak platform
        
        Return:
        -------
            JSON object containing data extracted from the ThingSpeak platform
        """        
        try:
            with urlopen(self._src_url) as response:
                json_resp = json.loads(response.read())                       
        except ValueError as e:
            self._logger.exception("Invalid URL.")        
        except HTTPError as e:
            self._logger.exception("Error: A HTTP Error occurred.")            
        except URLError as e:
            self._logger.exception("Error: URL not found.")            
        else:
            self._logger.debug(f"Data successfully obtained from the URL {self._src_url}") 
        return json_resp   
        
    def saveToFile(self, path_tosave, data):

        """
        This function saves the collected data to a file in JSON format

        Parameters:
        ----------             
             path_tosave is the location to save JSON file
             data is the data to save in JSON format file   
        """
        
        tmsp_info = data["created_at"]
        try:            
            assert os.path.isdir(path_tosave)            
            file_name = path_tosave + f"data-sensor-{tmsp_info}.json"
            try:            
                with open(file_name, 'w') as outfile:
                    json.dump(data, outfile)
            except FileNotFoundError:
                self._logger.exception(f"The file "+ file_name + " does not exist.") 
                sys.exit(1)
        except AssertionError:
            self._logger.exception(f"Path {path_tosave} does not exist.")
            sys.exit(1)
        
            