import os
import urllib.parse
from log import custom_logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, select,text, Column, Integer, DateTime, String, func, ForeignKey, Boolean, Index
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
import json
load_dotenv()
config = os.environ
logging = custom_logger('main')
def makeConnection(name):
	connection = config["sql.template"]
	dbs = json.loads(config["sql.databases"])[name]
	#"mssql+pyodbc://%s%s/%s?trusted_connection=%s&driver=SQL+Server+Native+Client+11.0"
	_unpw = ''
	_unpw = "" if dbs["trusted"] == "yes" else urllib.parse.quote_plus(dbs["UN"]) + ":" + urllib.parse.quote_plus(dbs["PW"]) + "@"
	CONX = connection % (_unpw, dbs["server"],dbs["database"], dbs["trusted"])
	return CONX

def makeEngine(name):
	_engine = create_engine(name, fast_executemany=True, connect_args={"check_same_thread": False}, echo = True)
	return _engine
def makeSession(engine):
	_session_factory = sessionmaker(bind=engine)
	_session = scoped_session(_session_factory)
	return _session

class SqlConnectionObject():
	def __init__(self, name):
		self.name = name
		self.connection = makeConnection(self.name)
		self.engine = makeEngine(self.connection)
		self.session  = makeSession(self.engine)

data_obj =SqlConnectionObject("main")
lib_obj =SqlConnectionObject("library")
hrg_obj =SqlConnectionObject("HRG")
hie_obj =SqlConnectionObject("HIE")
db_connections = [data_obj,lib_obj, hrg_obj, hie_obj]
