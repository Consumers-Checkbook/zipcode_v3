import os
import logging

#from functools import wraps
def custom_logger(name = "main", console_level="DEBUG", file_level="DEBUG"):
	logger = logging.getLogger(name)
	if not os.path.exists('./Logs'):
		os.makedirs('./Logs')
	filename = f'./Logs/{name}.log'
	console = logging.StreamHandler()
	filelog = logging.FileHandler(filename)
	formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s.py - %(message)s')
	console.setFormatter(formatter)
	filelog.setFormatter(formatter)
	console.setLevel(console_level)
	filelog.setLevel(file_level)
	logger.setLevel(logging.DEBUG)
	logger.addHandler(console)
	logger.addHandler(filelog)
	#return logger
