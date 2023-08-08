setup:  
Make sure you have python installed an up tp date as well as the virtualenv python module  
Clone to _dir_ where _dir_ is the new folder this repo was copied into 
_dir_ = serff_env  
maybe c:\development\python\virtual, we'll call this _root_.  

git clone https://github.com/Consumers-Checkbook/zipcode_v3.git zipcode_v3  

begin batch script:  
cd _root_  
git clone https://github.com/Consumers-Checkbook/zipcode_v3.git zipcode_v3  
python -m venv zipcode_v3  
cd zipcode_v3  
.\Scripts\activate  
pip install -r requirements.txt  
end batch script:  

if there is no .env file, copy template.env as .env then edit  
then, make sure to edit the .env file accordingly. One for dev, one for prod, etc.  
