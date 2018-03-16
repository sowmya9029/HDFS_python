
# SUFS
Simple  distributed file system like HDFS. It consists of one Master (NameNode) and multiple datanodes (DataNode). And a client for interation. It will dump metadata/namespace when given SIGINT and reload it when fired up next time. Replicate data  the way HDFS does. It will send data to one datanode and that datanode will send it to next one and so on. Reading done in similar manner. Will contact fitst minion for block, if fails then second and so on.  Uses RPyC for RPC.


### Requirements:
  - You need a Python version of 2.7.x or higher
  - rpyc 
  - The Python dependencies are listed in the file "requirements.txt"
  - Configure the AWS ACCESS KEY in .aws/credential file 
  - Configure the AWS ACCESS KEY in .aws/config file 
  

  
### How to run.
  1. Edit `dfs.conf` for setting block size, replication factor and list minions (`minionid:host:port`)
  2. Fireup master and datanodes.
  3. Run the command "pip install -r requirements.txt"
  4. Copy the files namenode.py and NameNodeService.py and dfs.conf to an EC2 instance.
  5. Run python namenode.py to start the namenode. 
  6. Copy datanode.py and DataNodeService.py to an EC2 instance.
  7. Run python datanode.py
  8. below are the commands:
```sh
$ python client.py put sourcefile.txt sometxt x.x.x.x
$ python client.py get sometxt x.x.x.x
$ python client.py delete sometxt x.x.x.x
$ python client.py adddir dir/ x.x.x.x
$ python client.py deletedir dir x.x.x.x
$ python client.py listdir x.x.x.x
$ python client.py listblocks filename x.x.x.x
$ python client.py printdir x.x.x.x
$ python client.py puts3 wordcount-satyavad input/file01 s3_file 52.40.206.13
```
##### Stop it using Ctll + C so that it will dump the namespace.


How To Run Seattle U File System



