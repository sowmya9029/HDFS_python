import rpyc
import uuid
import threading
import math
import random
import ConfigParser
import signal
import pickle
import sys
import os
import time, threading

from datetime import datetime

import treelib
import simplejson
import jsonpickle
class NameNodeService(rpyc.Service):
    class exposed_NameNode():      
        file_table = {}
        block_mapping = {}
        datanodes = {}
        datanode_registry = {}
        block_list = {}
        block_size = 0
        replication_factor = 0
        temp_count = 0
        directory = None
        secondary_namenode_ip = {}
        secondary_namenode_port = 0
        current_heartbeat_time=time.time()
        isPrimary =0

        def __init__(self):
          # if directory file exitsts, restore directory structure using the file
          # else create a new directory structure
          if os.path.isfile('directory.txt'):
            self.restore_directory()
          else:
            self.create_directory()

        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_accept_ft(self, serialized_filetable):
            #self.__class__.file_table = data
            #self.__class__.file_table[block_id] = datanodes
            self.__class__.file_table=jsonpickle.decode(serialized_filetable);
            #print self.__class__.file_table
            #print self.__class__.datanodes
        
        def exposed_accept_datanodes(self, key, ip, port):
            #print "secondary data nodes are " + str(data)
            self.__class__.datanodes[key] = (ip, port)
            #print self.__class__.datanodes

        def exposed_accept_directory(self,data):
            #rint "secondary received " + str(data)
            if os.path.isfile('directory.txt'):
              self.restore_directory_with_data(data)
            else:
              #print "creating directory"
              self.create_directory()


        def exposed_receive_primary_namenode_heartbeat(self, msg):
            self.__class__.current_heartbeat_time=time.time()
            print str(msg)+ " at "  + str(self.__class__.current_heartbeat_time)
                    #print self.__class__.datanodes[datanode_index]
                    #self.__class__.datanode_registry[datanode_index] = time.time()
                    #now = time.time()
                    #print "updated data node heart beat at :" + str(now)
        
        def exposed_receive_heartbeat(self, datanode_index, msg):
            print "Received Heartbeat from data node ("+str(datanode_index)+") at time " + str(time.time())
            #print self.__class__.datanodes[datanode_index]
            self.__class__.datanode_registry[datanode_index] = time.time()
            now = time.time()
            #print "updated data node heart beat at :" + str(now)
          
        def exposed_receive_blockreport(self, datanode_id, blockinfo):
            if not blockinfo:
              return
            for block in blockinfo:
              if block in self.__class__.block_list:
                if datanode_id not in self.__class__.block_list[block]:
                  self.__class__.block_list[block].append(datanode_id)
              else:
                self.__class__.block_list[block] = []
                self.__class__.block_list[block].append(datanode_id)           
            print "received block reports as below from datanode :: "+ str(datanode_id)
            print self.__class__.block_list 

        def exposed_get_datanodes_for_block(self,blockuuid):
          ids = self.__class__.block_list[str(blockuuid)]
          datanodes_with_block = []
          for datanode_id in ids:
            if datanode_id in self.__class__.datanodes:
              #datanodes_with_block.append(str(self.__class__.datanodes[datanode_id]))
              datanodes_with_block.append(self.__class__.datanodes[datanode_id])
          return datanodes_with_block
        
        def exposed_register_datanode(self, ip, port):
            count = 1
            while True:
              if count not in self.__class__.datanodes:
                self.__class__.datanodes[count] = (ip, port)
                self.__class__.datanode_registry[count] = time.time()
                break
              else:
                count +=1           
            now = time.time()
            print "registered data node at : " + str(now)
            return count

        def exposed_write(self, dest, size):
            if self.exists(dest):
                pass  # ignoring for now, will delete it later

            self.__class__.file_table[dest] = []
            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest, num_blocks)
            return blocks

        def exposed_get_file_table_entry(self, fname):
            if fname in self.__class__.file_table:
              return self.__class__.file_table[fname]
            else:
                return None

        def exposed_delete(self,filename):
          if filename in self.__class__.file_table:
            blocks = self.__class__.file_table[filename]
            for block,dnids in blocks:
              del self.__class__.block_list[str(block)]              
            del self.__class__.file_table[filename]

        def exposed_get_block_size(self):
            return self.__class__.block_size


        def exposed_get_datanodes(self):
            return self.__class__.datanodes

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size) / self.__class__.block_size))

        def exists(self, file):
            return file in self.__class__.file_table

        def alloc_blocks(self, dest, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                nodes_ids  = random.sample(self.__class__.datanodes.keys(), self.__class__.replication_factor)
                blocks.append((block_uuid, nodes_ids))
                self.__class__.file_table[dest].append((block_uuid, nodes_ids))
            return blocks

        # create directory
        def create_directory(self):
          self.__class__.directory = treelib.Tree()
          self.__class__.directory.create_node("/","/")
          self.save_directory()

        # restore previous directory
        def restore_directory_with_data(self,data):
          #print data
          self.__class__.directory = jsonpickle.decode(data)
          #print self.__class__.directory
          self.save_directory()

        # restore previous directory
        def restore_directory(self):
          #print self.__class__.directory
          with open('directory.txt', 'r') as infile:
            json_data = infile.read()
            self.__class__.directory = jsonpickle.decode(json_data)

        def exposed_add_directory(self,path):
          dir_name_list = path.split("/")
          cur_path = ""
          parent_path = "/"
          for i in dir_name_list:
            cur_path = cur_path+"/"+i
            if not self.__class__.directory.contains(cur_path):               
              print self.__class__.directory.create_node(i,cur_path,parent=parent_path)
            parent_path = cur_path
          self.save_directory()
          # testing
          self.__class__.directory.show()    

     # return all filenames under deleted directory
        def exposed_get_delete_filenames(self,path):
          children = self.__class__.directory.children(path)
          res = []
          for child  in children:
            if "." not in child.tag:
              continue
            res.append(name.tag)
          return res
      

        def exposed_delete_directory(self,path):
          print "path is "+str(path)
          print "path is "+str(self.__class__.directory)
          self.__class__.directory.remove_node(path)
          print "path is "+str(self.__class__.directory)
          self.save_directory()

        # list contents of a directory
        def exposed_list_directory(self,path):
          children = self.__class__.directory.children(path)
          res = []
          for child in children:
            res.append(child.tag)
          return res

        # for testing
        def exposed_print_directory(self):
          # print self.__class__.directory.create_node("text.txt","/text.txt",parent="/")
          self.__class__.directory.show()
          return self.__class__.directory.to_dict()

        # save directory to disk after each change
        def save_directory(self):
          json_data = jsonpickle.encode(self.__class__.directory)
          with open('directory.txt', 'w') as outfile:
            outfile.write(json_data)    