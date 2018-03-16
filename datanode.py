import rpyc
import os
import requests
import threading
import ConfigParser
import errno
from socket import error as socket_error
from rpyc.utils.server import ThreadedServer
from DataNodeService import DataNodeService

DATA_DIR = "/tmp/datanode/"

def getMyIp():
    r = requests.get(r'http://jsonip.com')
    ip = r.json()['ip']
    return ip

def set_conf():
    conf = ConfigParser.ConfigParser()
    conf.readfp(open('dfs.conf'))
    host, port = conf.get('master', 'namenode_address').split(":")
    DataNodeService.exposed_DataNode.namenode_ip = host
    DataNodeService.exposed_DataNode.namenode_port = port

def registerWithMaster(ip, port):  
      con = rpyc.connect(DataNodeService.exposed_DataNode.namenode_ip, DataNodeService.exposed_DataNode.namenode_port)
      master = con.root.NameNode()
      datanode_id = master.register_datanode(ip, port)
      DataNodeService.exposed_DataNode.name = datanode_id
      print "recevied id after registration = " + str(datanode_id)
    
def send_heartbeat_to_master():
    try:      
      con = rpyc.connect(DataNodeService.exposed_DataNode.namenode_ip,DataNodeService.exposed_DataNode.namenode_port)
      master = con.root.NameNode()
      datanode_id = DataNodeService.exposed_DataNode.name
      master.receive_heartbeat(datanode_id, "I am alive")      
    except socket_error as serr:
      print ("Error Conencting to NameNode. Retrying...... ")
    
    threading.Timer(30, send_heartbeat_to_master).start()
    #threading.Timer(120, send_heartbeat_to_master).start()

def send_block_reports():
    try:
      #print "namenode ip " + str(DataNodeService.exposed_DataNode.namenode_ip)
      #print  "namenode port "+ str(DataNodeSevice.exposed_DataNode.namenode_port)
      con = rpyc.connect(DataNodeService.exposed_DataNode.namenode_ip,DataNodeService.exposed_DataNode.namenode_port)
      master = con.root.NameNode()
      datanode_id = DataNodeService.exposed_DataNode.name
      master.receive_blockreport(datanode_id, DataNodeService.exposed_DataNode.blocks)
    except socket_error as serr: 
      print ("Error Conencting to NameNode. Retrying...... ")  
    threading.Timer(55, send_block_reports).start()
    #threading.Timer(270, send_block_reports).start()

if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
    t = ThreadedServer(DataNodeService, port=8881)
    set_conf()
    registerWithMaster(getMyIp(), 8881)
    send_heartbeat_to_master()
    send_block_reports()
    t.start()
