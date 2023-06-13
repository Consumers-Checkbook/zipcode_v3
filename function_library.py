from zipfile import ZipFile
import subprocess
import os
def unzipTo(zipfile, unzip_to) -> list[str]:
	zipfile_pointer = ZipFile(zipfile)
	zipfile_pointer.extractall(unzip_to)
	files = zipfile_pointer.namelist()
	zipfile_pointer.close()
	return files
def downloadCurl(hyperlink, localFile)->dict:
	#make sure to pad the hyperlink with https://
	if hyperlink.startswith("https://") == False:
		hyperlink = f"https://{hyperlink}"
	cmd = f"curl.exe -L -f -s -S --remove-on-error -o {localFile} {hyperlink}"
	p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	response, _ = p.communicate()
	downloaded = False
	size = -1
	if response == b'':
		downloaded = True
		size = os.path.getsize(localFile)
	return {"downloaded":downloaded,"size":size, "response":response, "cmd":cmd}