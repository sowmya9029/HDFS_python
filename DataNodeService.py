import rpyc
import os


DATA_DIR = "/tmp/datanode/"

class DataNodeService(rpyc.Service):
    class exposed_DataNode():
        name = {}
        blocks = []
        cur = []
        namenode_ip = {}
        namenode_port = 0

        def exposed_replication_forward(self, block_uuid, datanodes):
            print "received block to forward "+ str(block_uuid)
            print "node to send is " + str(datanodes)
            data = self.exposed_get(block_uuid)
            self.replica_forward(block_uuid, data, datanodes)

        def exposed_put(self, block_uuid, data, datanodes):
            if block_uuid in self.__class__.blocks:
                return
            print "received block to store "+ str(block_uuid)
            with open(DATA_DIR + str(block_uuid), 'w') as f:
                f.write(data)
            self.blocks.append(str(block_uuid))
            if len(datanodes) > 1:
                self.forward(block_uuid, data, datanodes[1:])

        def exposed_get(self, block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()

        def replica_forward(self, block_uuid, data, datanode):
            host, port = datanode
            print "Sending data to a different datanode "+str(datanode)
            datanodearr = []
            datanodearr.append(datanode)
            con = rpyc.connect(host, port=port)
            datanode = con.root.DataNode()
            datanode.put(block_uuid, data, datanodearr)

        def forward(self, block_uuid, data, datanodes):
            datanode = datanodes[0]
            host, port = datanode
            con = rpyc.connect(host, port=port)
            datanodeCon = con.root.DataNode()
            datanodeCon.put(block_uuid, self.exposed_get(block_uuid), datanodes[1:])

        def exposed_delete_block(self, block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if not os.path.isfile(block_addr):
                print("Sorry, I can not find %s file." % block_addr)
            try:
              os.remove(block_addr)
            except OSError, e:
              print ("Error:")
        
        #to update name node of secondary namende       
        def exposed_update_namenode(self, ip,port):
            #print "got secondary name node ip::" +str(ip) +" port ::"+str(port)
            self.__class__.namenode_ip = ip
            self.__class__.namenode_port = port

