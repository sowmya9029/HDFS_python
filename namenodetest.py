import namenode
from NameNodeService import NameNodeService

def testnamenode():
  blocklist = {}
  blocklist["b1"] = [1,2]
  namenode.send_replication_miss(blocklist)

if __name__ == "__main__":
  testnamenode()