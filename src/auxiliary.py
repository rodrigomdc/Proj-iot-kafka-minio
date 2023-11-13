# -----------------------------------------------------------
# Auxiliary functions
#
# (C) 2023 Rodrigo Costa, Ananindeua, Brasil
# 
# email eng.rodrigomdc@gmail.com
# -----------------------------------------------------------

import logging 
import yaml
import glob
import os
import sys

def createLog(log_name, log_level):
    """
    This function create and configure log service

    Parameters:
    -----------
        log_name name assigned to the logger
        log_level the logging level of this logger

    Return:
    ----------
        Object logger created
    """  
        
    try:
        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)
        
        #Set log message to auxiliary module
        global _log
        _log = logging.getLogger(f"{log_name}.{__name__}") 
        
        #Create the logging file handler and console handler
        fh = logging.FileHandler(f"logs/{log_name}.log", "a")
        sh = logging.StreamHandler()

        #Set log message format
        formFile = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        formStream = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        fh.setFormatter(formFile)
        sh.setFormatter(formStream)

        # add handler to logger object        
        logger.addHandler(sh)        
        logger.addHandler(fh)
        
    except Exception as e:
        _log.exception('Error creating log.')      
    return logger

def loadParams():
    """
    This function loads values of the main configuration parameters saved in a file in Yaml format
    """

    params = {}
    try:
        with open('config/params.yml') as file:    
            params = yaml.safe_load(file)
    except IOError as e:
        _log.exception('Error reading file.') 
        sys.exit(1)     
    except yaml.YAMLError as e:        
        _log.exception('Error parsing file.') 
        sys.exit(1)       
    except Exception as e:        
        _log.exception('General error.') 
        sys.exit(1)  
            
    return params

def getLastFileName(tmp_dir):
    """
    This function gets the latest JSON filename present in a directory

    Parameters:
    -----------
        tmp_dir is the directory that have JSON files

    Return:
    -----------
        Name of the latest file in the given directory
    """

    latest_file = ''
    try:
        files = glob.glob(f'{tmp_dir}/*')
        latest_file = max(files, key=os.path.getctime)
    except Exception as e:
       _log.exception('The path provided does not match any file or directory.')          
    return latest_file[9:]


