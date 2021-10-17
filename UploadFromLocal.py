#!/usr/bin/env python
# coding: utf-8

# ### Upload to HDFS from local disk

# In[ ]:


import os
import sys

from hops import hdfs
import time
import utils
from pyspark import SparkContext


# #### Load Input and output paths
# 

# In[ ]:


args=utils.load_arguments(sys.argv)
if args is not None:
    args=args[utils.KEY_UPLOAD]
else :
    sys.exit(utils.NO_CONFIG_ERR)

sc = SparkContext.getOrCreate()

INPUT_ROOT=args['INPUT_ROOT']
OUTPUT_ROOT=args['OUTPUT_ROOT']
log_file='upload_log'+os.path.basename(INPUT_ROOT)+'.txt'
LOG_ROOT='Logs/Upload_FASTQ'


# #### Get local path

# In[ ]:


local_path=os.getcwd()
local_path


# #### List of all input file paths and select only FASTQ files

# In[ ]:


all_files=os.listdir(INPUT_ROOT)
all_files=list(filter(lambda x: '.fastq' in x,all_files ))


# #### Upload function

# In[ ]:



# remove any old log file
if os.path.exists(log_file):
    os.remove(log_file)


# write logs header    
with open(os.path.join(local_path,log_file),'a') as f: # open log file
    f.write('\n *** Start upload of FASTQ files at source: '+INPUT_ROOT)
    f.write('\n *** Destination at: '+ str(OUTPUT_ROOT))
    f.write('\n *** Total number of input FASTQ files: '+ str(len(all_files)))

hdfs.copy_to_hdfs(log_file,LOG_ROOT,overwrite=True)



start=time.time()

count=0 # upload file counter
skipped=0 # skipped counter

fd=hdfs.open_file(os.path.join(LOG_ROOT,log_file) ,flags='at')
for i in all_files: # upload each file
   
    fd.write('\n    - Start file: '+i)

    if not hdfs.exists(os.path.join(OUTPUT_ROOT,i)): # upload if not already present
        os.chdir(INPUT_ROOT) # change directory to input for hdfs copy.
        hdfs.copy_to_hdfs(i,OUTPUT_ROOT,overwrite=False)
        count+=1 
        
        # write in logs
        fd.write('\n    - Finished upload file: '+i)
    else :
        skipped+=1        
        fd.write('\n    - Skippping file: '+i)

# write totals and close logs
fd.write('\n *** Upload Complete. \n Total number of uploaded FASTQ files: '+str(count) + '\n Total files skipped: '+str(skipped))
fd.close()


# #### View logs

# In[ ]:


# view logs

logs = hdfs.load(os.path.join(LOG_ROOT,log_file))
print("upload logs : {}".format(logs.decode("utf-8")))

