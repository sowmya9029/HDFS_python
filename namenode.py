import rpyc
import ConfigParser
import signal
import pickle
import sys
import os
import time
import requests
import threading
import jsonpickle
import errno
from socket import error as socket_error
from NameNodeService import NameNodeService
from rpyc.utils.server import ThreadedServer,ForkingServer, OneShotServer

#def int_handler(signal, frame):
  #pickle.dump((NameNodeService.exposed_NameNode.file_table,NameNodeService.exposed_NameNode.block_mapping),open('fs.img','wb'))
  #sys.exit(0)

def set_conf():
  conf=ConfigParser.ConfigParser()
  conf.readfp(open('dfs.conf'))
  NameNodeService.exposed_NameNode.block_size = int(conf.get('master','block_size'))
  NameNodeService.exposed_NameNode.replication_factor = int(conf.get('master','replication_factor'))
  NameNodeService.exposed_NameNode.secondary_namenode_ip,NameNodeService.exposed_NameNode.secondary_namenode_port = conf.get('master','namenode_secondary').split(":")
  NameNodeService.exposed_NameNode.isPrimary = int(conf.get('master','isPrimary'))
  print "set conf primary namenode"+str(NameNodeService.exposed_NameNode.isPrimary)

  #if os.path.isfile('fs.img'):
    #NameNodeService.exposed_NameNode.file_table,NameNodeService.exposed_NameNode.block_mapping = pickle.load(open('fs.img','rb'))

def findDeadDataNodes():
  for key,listedTime in NameNodeService.exposed_NameNode.datanode_registry.items():
    #curTime = datetime.now()
    curTime = time.time()
    timeDiff = int(curTime - listedTime)
    #if timeDiff > 150:
    if timeDiff > 45:
      print "removed the dead data node with id :" + str(key) 
      for block_id,datanodes_id in NameNodeService.exposed_NameNode.block_list.items():
        #print datanodes_id[block_id]
        if key in datanodes_id:
          print datanodes_id
          datanodes_id.remove(key)
        
        for blockuuid, datanodes in NameNodeService.exposed_NameNode.file_table.items():
          if key in datanodes:
            datanodes.remove(key)     
          #del NameNodeService.exposed_NameNode.block_list[key] 
  threading.Timer(25, findDeadDataNodes).start()             
  #threading.Timer(100, findDeadDataNodes).start()

def getMyIp():
    r = requests.get(r'http://jsonip.com')
    ip = r.json()['ip']
    return ip

def findPrimaryDeadNameNodes():
  curTime = time.time()
  #print NameNodeService.exposed_NameNode.current_heartbeat_time
  timeDiff = int(curTime - NameNodeService.exposed_NameNode.current_heartbeat_time)
  #print timeDiff
  if timeDiff > 45:
  #if timeDiff > 150:
    print "primary namenode is dead, started secondary namenode"
    #print dn
    for key,value in NameNodeService.exposed_NameNode.datanodes.items():
      ip,port=value
      con = rpyc.connect(ip, port)
      secondary_con = con.root.DataNode()
      print "sending my ip and port to datanode"
      secondary_con.update_namenode( getMyIp() , "2133" )
      #print " ip: "+str(ip) + "port "+str(port)

  threading.Timer(25, findPrimaryDeadNameNodes).start()
  #threading.Timer(100, findPrimaryDeadNameNodes).start()

def send_info_to_secondary():
  try:
    ip_namenode = NameNodeService.exposed_NameNode.secondary_namenode_ip
    port_namenode = NameNodeService.exposed_NameNode.secondary_namenode_port
    con = rpyc.connect(ip_namenode, port_namenode)
    secondary_con = con.root.NameNode()
    print "sending fite table ,blocks and directory  info to secondary namenode "  
    serialized_filetable = jsonpickle.encode(NameNodeService.exposed_NameNode.file_table)
    secondary_con.accept_ft(serialized_filetable)
    
    for key, value in NameNodeService.exposed_NameNode.datanodes.items():
      ip, port = value
      secondary_con.accept_datanodes(key, ip, port)
    #tranfering filename
    #print "sending directory information"
    if os.path.isfile('directory.txt'):
      #print "found directory file" 
      with open('directory.txt', 'r') as infile:
        json_data = infile.read()
        #print "sending data " + str(json_data)
        secondary_con.accept_directory(json_data)  
  except socket_error as serr:
      print ("Error connecting secondary NameNode. Please start secoundary NameNode")

  threading.Timer(30, send_info_to_secondary).start()
  #threading.Timer(300, send_info_to_secondary).start()

def send_replication_miss_data_datanode():
  block_list = NameNodeService.exposed_NameNode.block_list
  if not block_list: 
    print "Block reports are not yet started"
  else:
    print "sending replication miss block reports:: "+str(block_list)
  try:
    send_replication_miss(block_list)
  except socket_error as serr:
    print ("Error datanode not available!!")

  threading.Timer(30, send_replication_miss_data_datanode).start()
  #threading.Timer(180, send_replication_miss_data_datanode).start()

def send_replication_miss(block_list):
  for key,value in block_list.items():
    temp_count = len(value)  
    print "count of datanodes with block "+str(key)+" is "+str(temp_count)
    if temp_count < NameNodeService.exposed_NameNode.replication_factor:
      for dest_datanode_id in NameNodeService.exposed_NameNode.datanodes:
        if dest_datanode_id not in value:
          #print "value list = " + str(value)
          #print "source datanode is = " + str(NameNodeService.exposed_NameNode.datanodes[value[0]])
          source_datanode_ip,source_datanode_port = NameNodeService.exposed_NameNode.datanodes[value[0]]
          con=rpyc.connect(source_datanode_ip,source_datanode_port)
          source_datanode_con = con.root.DataNode()
          #print "destintion data node id " + str(dest_datanode_id)
          source_datanode_con.replication_forward(key,NameNodeService.exposed_NameNode.datanodes[dest_datanode_id]);
          break

def send_heartbeat_to_namenode():
    try:
      ip = NameNodeService.exposed_NameNode.secondary_namenode_ip
      port = NameNodeService.exposed_NameNode.secondary_namenode_port
      con = rpyc.connect(ip,port)
      master = con.root.NameNode()
      master.receive_primary_namenode_heartbeat( "I am alive from primary namenode")
    except socket_error as serr:
      print ("Error connecting secondary NameNode. Please start secoundary NameNode")
    #threading.Timer(120, send_heartbeat_to_namenode).start()
    threading.Timer(30, send_heartbeat_to_namenode).start()

#NameNodeService.exposed_NameNode.block_list={}
if __name__ == "__main__":
  set_conf()
  #signal.signal(signal.SIGINT,int_handler)
  t = ThreadedServer(NameNodeService, port = 2133)
  send_replication_miss_data_datanode()
  findDeadDataNodes()
  #print "is primary"+ str(isPrimary)
  if NameNodeService.exposed_NameNode.isPrimary == 1:
    print "primary namenode started"
    send_info_to_secondary()
    send_heartbeat_to_namenode()
  else:
    print "secondary namenode started"
    findPrimaryDeadNameNodes()
    
  
  t.start()

