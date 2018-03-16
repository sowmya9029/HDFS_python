import rpyc
import sys
import os
import boto3
import json


def send_to_datanode(block_uuid,data,datanodes):
  print "sending: " + str(block_uuid) + str(datanodes)
  datanode=datanodes[0]
  #datanodes=datanodes[1:]
  host,port=datanode
  con=rpyc.connect(host,port=port)
  datanode = con.root.DataNode()
  datanode.put(block_uuid,data,datanodes)

def read_from_datanode(block_uuid,datanode):
  host,port = datanode
  con=rpyc.connect(host,port=port)
  datanode = con.root.DataNode()
  return datanode.get(block_uuid)

def get(master,fname):
  file_table = master.get_file_table_entry(fname)
  if not file_table:
    print "404: file not found"
    return
  for block,dn in file_table:
    dn = master.get_datanodes_for_block(block)
    data = read_from_datanode(block,dn[0])
    if data:
      with open("Output", "a") as text_file:
        text_file.write(data)

def get_old(master,fname):
  file_table = master.get_file_table_entry(fname)
  if not file_table:
    print "404: file not found"
    return
  for block in file_table:
    for m in [master.get_datanodes()[_] for _ in block[1]]:
      data = read_from_datanode(block[0],m)
      if data:
        with open("Output", "a") as text_file:
          text_file.write(data)
        break
    else:
        print "No blocks found. Possibly a corrupt file"


def put_s3(master,bucket,key,dest):
  s3 = boto3.client("s3")
  obj = s3.get_object(Bucket=bucket, Key=key)
  size = obj['ContentLength']
  print "size of file::"+ str(size)
  blocks = master.write(dest,size)
  print len(blocks)
  #new
  add_dir(master,dest)
  #
  #with open(source) as f:
  for b in blocks:
    data = obj['Body'].read(master.get_block_size())
    block_uuid=b[0]
    datanodes = [master.get_datanodes()[_] for _ in b[1]]
    send_to_datanode(block_uuid,data,datanodes)

def put_s3_new(master,bucket,key,dest):
  s3 = boto3.client("s3")
  obj = s3.get_object(Bucket=bucket, Key=key)
  size = obj['ContentLength']
  print "size of file::"+ str(size)
  blocks = master.write(dest,size)
  print len(blocks)
  #new
  add_dir(master,dest)
  #
  #with open(source) as f:
  bdata = []
  lenb=0
  while (size - lenb > 0):
    data = obj['Body'].read(master.get_block_size())
    lenb = lenb+master.get_block_size()
    bdata.append(data)
  counter = 0
  for b in blocks:
    data = bdata[counter]
    counter = counter+1
    buid = b[0]
    dats = [master.get_datanodes()[_] for _ in b[1]]
    send_to_datanode(buid,data,dats)

def put(master,source,dest):
  size = os.path.getsize(source)
  print "size of file::"+ str(size)
  blocks = master.write(dest,size)
  print len(blocks)
  #new
  add_dir(master,dest)
  print "source" + str(source)
  with open(source) as f:
    for b in blocks:
      data = f.read(master.get_block_size())
      block_uuid=b[0]
      datanodes = [master.get_datanodes()[_] for _ in b[1]]
      send_to_datanode(block_uuid,data,datanodes)

def put_new(master,source,dest):
  size = os.path.getsize(source)
  print "size of file::"+ str(size)
  blocks = master.write(dest,size)
  print len(blocks)
  #new
  add_dir(master,dest)
  print "source" + str(source)
  bdata = {}
  bbdata = {}
  with open(source) as f:
    for b in blocks:
      data = f.read(master.get_block_size())
      bdata[b[0]] = data
      bbdata[b[0]] = b[1]
  for buid,data in bdata.items():
    dats = [master.get_datanodes()[_] for _ in bbdata[buid]]
    send_to_datanode(buid,data,dats)


def list_blocks_and_replicas_for_file(master,fname):
  file_table = master.get_file_table_entry(fname)
  #print file_table
  if not file_table:
    print "404: file not found"
    return
  for block,dn in file_table:
    sys.stdout.write("block = "+str(block))
    sys.stdout.write("data nodes with block data = "+ str(master.get_datanodes_for_block(block)))
  

#delete method
def delete_from_datanode(block_uuid,datanode):
    host,port = datanode
    con = rpyc.connect(host,port=port)
    datanode = con.root.DataNode()
    datanode.delete_block(block_uuid)

#delete helper
def delete(master,fname):
    file_table = master.get_file_table_entry(fname)
    if not file_table:
        print "404: file not found"
        return
    for block in file_table:
        for m in [master.get_datanodes()[_] for _ in block[1]]:
            delete_from_datanode(block[0],m)          
        else:
            print "Deleted file " +str(fname)
    master.delete(fname)

#alice
def add_dir(master,path):
  if path[:1] == '/':
    path = path[1:]
  master.add_directory(path)

def list_dir(master,path):
  if path[:1] != '/':
    path = '/' + path
  res = master.list_directory(path)
  if len(res) == 0:
    print "no content founded"
  else:
    print res

def delete_dir(master,path):
  if path[:1] != '/':
    path = '/' + path
  filenames = master.get_delete_filenames(path)
  for filename in filenames:
    delete(master,filename) 
  master.delete_directory(path)
  print "directory",path,"deleted!"

def print_dir(master):
  print master.print_directory()

def main(args):
  if args[0] == "put":
    con_put=rpyc.connect(args[3],port=2133)
    master_put=con_put.root.NameNode()  
    put_new(master_put,args[1],args[2])   
  elif args[0] == "puts3": 
    con=rpyc.connect(args[4],port=2133)
    master=con.root.NameNode()
    put_s3_new(master,args[1],args[2],args[3])
  else:
    con=rpyc.connect(args[2],port=2133)
    master=con.root.NameNode()
    if args[0] == "get":
      get(master,args[1])
    #delete from cmd line
    elif args[0] == 'delete':
      delete(master,args[1])
    elif args[0] == "adddir":
      add_dir(master,args[1])
    elif args[0] == "listdir":
      list_dir(master,args[1])
    elif args[0] == "deletedir":
      delete_dir(master,args[1])
    elif args[0] == "printdir":
      print_dir(master)
    elif args[0] == "listblocks":
      list_blocks_and_replicas_for_file(master,args[1])
    else:
     print "try 'put srcFile destFile OR get file'"
       
if __name__ == "__main__":
  main(sys.argv[1:])
