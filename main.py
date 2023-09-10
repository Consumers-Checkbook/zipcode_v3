import os
import sys
from alchemy_engine import *
from zipfile import ZipFile
#import requests
import urllib.request
from  urllib.parse import quote_plus
#from bs4 import BeautifulSoup
import re
from datetime import datetime
import json
import pandas as pd
from urllib import parse
from log import *
import logging
from function_library import *
config = os.environ
_LOGNAME = 'main'
expected_files = {"main":"5-digit Commercial.csv","multi":"5-digit Multi-county.csv"}
expected_tables = {"main":"master_zipcode_subscription","multi":"master_zipcode_subscription_multi"}

def prepare_download():
	now = datetime.now().strftime("%b").lower()
	year = datetime.now().strftime("%Y").lower()
	filename = f"{now}{year}_commercial_csv.zip"
	multifilename = f"{now}{year}_z5multicounty_csv.zip"
	return [{"type":"main","filename":filename, "saveto":f"{config['savedir']}/latestZipDatabase.zip"}, {"type":"multi","filename":multifilename,"saveto":f"{config['savedir']}/latestMultiZipDatabase.zip"}]
def check():
	rpt = []
	for db_name in db_connections:
		dbc = db_connections[db_name]
		cn = dbc.engine.connect()
		for table in ['[master_zipcode_subscription]','[master_zipcode_subscription_multi]']:
				rc = cn.execute(text(f"select [svn], count(1) as c from {dbc.schema}.{table} group by [svn];")).first()
				rpt.append({"server":dbc.engine.url.host, "database":dbc.engine.url.database, "table":table, "count":rc})
	df = pd.DataFrame(rpt)				
	print(df)
	return rpt
def saveToTable(l, dbc:SqlConnectionObject, df:pd.DataFrame, toschema:str, totable:str):
	l.info(f"starting {toschema}.{totable} complete")	
	cn = dbc.engine.connect()
	trans = cn.begin()
	try:
		cn.execute(text(f"truncate table [{toschema}].[{totable}];"))
		trans.commit()
	except Exception as stEe:
		trans.rollback()
		l.error(f"Had to Rollback: {stEe}")
		return
	#l.info(f"{table} truncated")
	df.to_sql(name = totable, schema = toschema,con = dbc.engine, index=False, if_exists='append')
	l.info(f"importing {toschema}.{totable} complete")			
def run():
	l = logging.getLogger(_LOGNAME)
	downloads = prepare_download()
	download_results = []
	for download in prepare_download():
		url = config["url"].replace( '{filename}', download["filename"])
		l.info(f"run(): Downloading {url}")
		result = downloadCurl(url, download["saveto"])
		download_results.append({**download, **result})
		l.info(f"run(): result: {result}")
	unzip_results = []
	for unzip in download_results:
		filename = unzip["saveto"]
		directory = f"{config['savedir']}/{unzip['type']}"
		l.info(f"run(): unzipping {filename} to {directory}")
		zf = ZipFile(filename)
		zf.extractall(path = directory )
		zf.close()
		import_file = {"unzipdir":directory,"importfrom":f"{directory}/{expected_files[unzip['type']]}"} 
		unzip_results.append({**unzip, **import_file})	
	l.info(f"run(): archiving main db")
	data_obj = db_connections["main"]
	for importfile in unzip_results:
		table = expected_tables[importfile['type']]
		l.info(f"run(): archiving current {table} data...")
		cn = data_obj.engine.connect()
		trans = cn.begin()
		try:
			cn.execute(text(f"exec [dbo].[sp_{table}];"))
			trans.commit()
		except Exception as stE:
			trans.rollback()
			l.error(f"Error archiving: {stE}")
			raise
	l.info(f"run(): importing...")
	
	for importfile in unzip_results:
		source = importfile["importfrom"]
		table = expected_tables[importfile['type']]
		df = pd.read_csv(filepath_or_buffer = source, dtype = str, keep_default_na=False)
		df["svn"] = datetime.now().strftime("%Y%m")
		for db_name in db_connections:
			dbc = db_connections[db_name]
			l.info(f"saving {source} to {dbc.engine.url.host}.{dbc.engine.url.database}.dbo.{table}")
			saveToTable(l, dbc, df, dbc.schema, table)
			#cn = dbc.engine.connect()
			#trans = cn.begin()
			#try:
			#	cn.execute(text(f"truncate table [dbo].[{table}];"))
			#	trans.commit()
			#except Exception as stEe:
			#	trans.rollback()
			#	l.error(f"Had to Rollback: {stEe}")
			#	continue
			#l.info(f"{table} truncated")
			#df.to_sql(name = table, schema = 'dbo',con = dbc.engine, index=False, if_exists='append')
			#l.info(f"importing complete")
	l.info("run() complete...")
if __name__ == "__main__":
	print("running...")
	run()