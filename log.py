import os
import logging
import dotenv
#from functools import wraps
dotenv.load_dotenv()
config = os.environ
def custom_logger(name):
	logger = logging.getLogger(name)
	if not os.path.exists(config["logging.root_folder"]):
		os.makedirs(config["logging.root_folder"])
	filename = f'{config["logging.root_folder"]}/JsonEngine_v2.{name}.log'
	console = logging.StreamHandler()
	filelog = logging.FileHandler(filename)
	formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s.py - %(message)s')
	console.setFormatter(formatter)
	filelog.setFormatter(formatter)
	console.setLevel(eval(config["logging.default_level"]))
	filelog.setLevel(eval(config["logging.default_level"]))
	logger.setLevel(eval(config["logging.default_level"]))
	logger.addHandler(console)
	logger.addHandler(filelog)