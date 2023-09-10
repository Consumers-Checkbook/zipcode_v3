import os
import urllib.parse
from log import custom_logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, select,text, Column, Integer, DateTime, String, func, ForeignKey, Boolean, Index
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
import pandas as pd
import json
load_dotenv()
config = os.environ
custom_logger(console_level=config["log.console.level"], file_level=config["log.file.level"])
# uncomment this to see SQL related debugging
# custom_logger("sqlalchemy.engine", console_level=config["log.console.level"], file_level=config["log.file.level"]) 

_DATABASES = json.loads(config["databases"])
def makeSession(engine):
	_session_factory = sessionmaker(bind=engine)
	_session = scoped_session(_session_factory)
	return _session

class PgConnectionObject():
	def __init__(self, name, server, database, **kwargs):
		_template = "postgresql+psycopg2://%s@%s/%s"
		_un = kwargs["UN"] if "UN" in kwargs.keys() else ""
		_pw = kwargs["PW"] if "PW" in kwargs.keys() else ""
		_unpw = urllib.parse.quote_plus(_un) + ":" + urllib.parse.quote_plus(_pw)
		conx = _template % (_unpw, server,database)
		self.name = name
		self.schema = ""
		self.connection = conx
		self.engine = create_engine(conx, executemany_mode='values_plus_batch', insertmanyvalues_page_size=5000, executemany_batch_page_size=500)
		self.session  = makeSession(self.engine)
	def __repr__(self):
		return f"PgConnectionObject:{self.name} = {self.engine.url.host}.{self.engine.url.database}"
	def getConnection(self):
		return self.engine.connect()
	def getStreamingConnection(self):
		return self.engine.connect().execution_options(stream_results=True)
	def executeProcedure(self, procedure_name, streaming=False):
		con = self.getConnection if streaming == False else self.getStreamingConnection
		with con() as tconn:
			try:
				tconn.execute(text(f"set nocount on; exec {procedure_name};"))	
				tconn.commit()
			except Exception as tctE:
				tconn.rollback()
				raise Exception(f"executeProcedure(): {tctE}")
	def truncateTable(self, schema, name):
		with  self.getConnection() as tconn:
			try:
				tconn.execute(text(f"truncate table {schema}.{name};"))	
				tconn.commit()
			except Exception as tctE:
				tconn.rollback()
				raise Exception(f"truncateTable(): {tctE}")
				return
	def interop_returnind_id(self, insert_command) -> int:
		con = self.engine.raw_connection()
		cur = con.cursor()
		cur.execute(insert_command)
		con.commit()
		return int(newid[0])
	def query(self, query_text):
		statement = text(query_text)
		with self.getConnection() as con:
			results = con.execute(statement).fetchall()
			con.commit()
		return results
	def execute(self, non_result_query):
		statement = text(non_result_query)
		with self.getConnection() as con:
			con.execute(statement)
			con.commit()
		#return results
	def queryStream(self, query_text):
		statement = text(query_text)
		with self.getStreamingConnection() as con:
			for item in con.execute(statement).fetchall():
				yield item
			con.commit()
	def getTable(self, query_text)->pd.DataFrame:
		statement = text(query_text)
		with self.getConnection() as con:
			df = pd.read_sql(statement,con)
		return df
	def chunkTable(self, query_text, chunksize):
		statement = text(query_text)
		with self.getStreamingConnection() as con:
			for chunk in pd.read_sql(statement, con , chunksize = chunksize):
				yield chunk
class SqlConnectionObject():
	def __init__(self, name,server, database, **kwargs):
		_template = "mssql+pyodbc://%s%s/%s?trusted_connection=%s&driver=SQL+Server+Native+Client+11.0"
		_un = kwargs["UN"] if "UN" in kwargs.keys() else ""
		_pw = kwargs["PW"] if "PW" in kwargs.keys() else ""
		_trusted = kwargs["trusted"] if "trusted" in kwargs.keys() else ""
		_unpw = "" if _trusted == "yes" else urllib.parse.quote_plus(_un) + ":" + urllib.parse.quote_plus(_pw) + "@"
		conx = _template % (_unpw, server, database, _trusted)
		self.name = name
		self.schema = ""
		self.connection = conx
		self.engine = create_engine(conx, fast_executemany=True, connect_args={"check_same_thread": False})
		self.session  = makeSession(self.engine)
	def __repr__(self):
		return f"SqlConnectionObject:{self.name} = {self.engine.url.host}.{self.engine.url.database}"
	def getConnection(self):
		return self.engine.connect()
	def getStreamingConnection(self):
		return self.engine.connect().execution_options(stream_results=True)
	def executeProcedure(self, procedure_name, streaming=False):
		con = self.getConnection if streaming == False else self.getStreamingConnection
		with con() as tconn:
			try:
				tconn.execute(text(f"set nocount on; exec {procedure_name};"))	
				tconn.commit()
			except Exception as tctE:
				tconn.rollback()
				raise Exception(f"executeProcedure(): {tctE}")
	def truncateTable(self, schema, name):
		with  self.getConnection() as tconn:
			try:
				tconn.execute(text(f"truncate table {schema}.{name};"))	
				tconn.commit()
			except Exception as tctE:
				tconn.rollback()
				raise Exception(f"executeProcedure(): {tctE}")
				return
	def interop(self, insert_command) -> int:
		con = self.engine.raw_connection()
		cur = con.cursor()
		cur.execute(insert_command)
		newid = cur.execute("select scope_identity() as new_id").fetchone()
		con.commit()
		return int(newid[0])
	def query(self, query_text):
		statement = text(query_text)
		with self.getConnection() as con:
			results = con.execute(statement).fetchall()
			con.commit()
		return results
	def execute(self, non_result_query):
		statement = text(non_result_query)
		with self.getConnection() as con:
			con.execute(statement)
			con.commit()
		#return results
	def queryStream(self, query_text):
		statement = text(query_text)
		with self.getStreamingConnection() as con:
			for item in con.execute(statement).fetchall():
				yield item
			con.commit()
	def getTable(self, query_text)->pd.DataFrame:
		statement = text(query_text)
		with self.getConnection() as con:
			df = pd.read_sql(statement,con)
		return df
	def chunkTable(self, query_text, chunksize):
		statement = text(query_text)
		with self.getStreamingConnection() as con:
			for chunk in pd.read_sql(statement, con , chunksize = chunksize):
				yield chunk

db_connections = {}
for dbs in _DATABASES.keys():
	server =  _DATABASES[dbs]["server"]
	database =  _DATABASES[dbs]["database"]
	un =  _DATABASES[dbs]["UN"]
	pw =  _DATABASES[dbs]["PW"]
	trusted =  _DATABASES[dbs]["trusted"]
	_t = _DATABASES[dbs]["type"]
	_schema = _DATABASES[dbs]["schema"]
	if _t == "mssql":
		db_connections[dbs] = SqlConnectionObject(dbs, server, database ,**{"UN":un , "PW":pw , "trusted":trusted })
		db_connections[dbs].schema = _schema
	if _t == "postgres":
		db_connections[dbs] = PgConnectionObject(dbs, server, database ,**{"UN":un , "PW":pw})
		db_connections[dbs].schema = _schema
def listDatabaseConnections():
	i = 0
	for c in db_connections.keys():
		print (f"{i}: {db_connections[c]}")
		i+=1