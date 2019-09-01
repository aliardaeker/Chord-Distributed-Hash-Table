#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import glob
import sys
import hashlib
import socket
import logging

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class ChordHandler:
    files = []
    finger_table = []
    ip = socket.gethostbyname(socket.gethostname())
    port_num = sys.argv[1]
    NodeID = hashlib.sha256(ip + ":" + str(port_num)).hexdigest() 

    # void writeFile(1: RFile rFile)
    # throws (1: SystemException systemException),  
    def writeFile(self, rFile):
        key = hashlib.sha256(rFile.meta.owner + ":" + rFile.meta.filename).hexdigest() 
        node = self.findSucc(key)
        if not node.id == self.NodeID:
            raise SystemException("Write File Error - Server does not own fil`s id")
        new_file = True
        for my_file in self.files:
            if rFile.meta.filename == my_file.meta.filename:
                my_file.meta.version += 1
                my_file.meta.owner = rFile.meta.owner
                my_file.meta.contentHash = hashlib.sha256(rFile.meta.contentHash).hexdigest()
                my_file.content = rFile.content
                new_file = False
        if new_file:
            f = RFile()
            m = RFileMetadata()
            m.filename = rFile.meta.filename
            m.version = 0
            m.owner = rFile.meta.owner
            m.contentHash = hashlib.sha256(rFile.meta.contentHash).hexdigest()
            f.meta = m
            f.content = rFile.content
            self.files.append(f)

    # RFile readFile(1: string filename, 2: UserID owner)
    # throws (1: SystemException systemException),
    def readFile(self, filename, owner):
        key = hashlib.sha256(owner + ":" + filename).hexdigest() 
        node = self.findSucc(key)
        if not node.id == self.NodeID:
            raise SystemException("Write File Error - Server does not own fil`s id")
        found = False
        for my_file in self.files:
            if filename == my_file.meta.filename and owner == my_file.meta.owner:
                found = True
                f = RFile()
                m = RFileMetadata()
                m.filename = filename
                m.version = my_file.meta.version
                m.owner = owner
                m.contentHash = my_file.meta.contentHash
                f.meta = m
                f.content = my_file.content
                return f
        if not found:
            raise SystemException("Read File Error - File not found")

    # void setFingertable(1: list<NodeID> node_list),
    def setFingertable(self, node_list):  
        for i in node_list:
            n = NodeID()
            n.id = i.id
            n.ip = i.ip
            n.port = i.port
            self.finger_table.append(n)
            
    # NodeID findSucc(1: string key) 
    # throws (1: SystemException systemException),
    def findSucc(self, key):
        if not len(self.finger_table):
            raise SystemException("Find Succ Error - Finger table is missing")
        n = self.findPred(key)
        if not n.id == self.NodeID:
            t = TSocket.TSocket(n.ip, n.port)
            t = TTransport.TBufferedTransport(t)
            p = TBinaryProtocol.TBinaryProtocol(t)
            c = FileStore.Client(p)
            t.open()
            k = c.getNodeSucc()
            t.close()
            return k 
        else:
            return self.getNodeSucc()

    # NodeID findPred(1: string key) 
    # throws (1: SystemException systemException),
    def findPred(self, key):
        if not len(self.finger_table):
            raise SystemException("Find Pred Error - Finger table is missing")
        
        if self.NodeID >= self.finger_table[0].id:
            if not (key <= self.NodeID and key > self.finger_table[0].id):
                n = NodeID()
                n.id = self.NodeID
                n.ip = self.ip
                n.port = int(self.port_num)
                return n
        else:
            if key > self.NodeID and key <= self.finger_table[0].id:
                n = NodeID()
                n.id = self.NodeID
                n.ip = self.ip
                n.port = int(self.port_num)
                return n

        if self.NodeID >= key:
            for i in reversed(self.finger_table):
                if not (i.id <= self.NodeID and i.id > key):
                    t = TSocket.TSocket(i.ip, i.port)
                    t = TTransport.TBufferedTransport(t)
                    p = TBinaryProtocol.TBinaryProtocol(t)
                    c = FileStore.Client(p)
                    t.open()
                    k = c.findPred(key)
                    t.close()
                    return k
            raise SystemException("Find Pred Error - Cannot find pred 1")
        else:
            for i in reversed(self.finger_table):
                if i.id > self.NodeID and i.id <= key:
                    t = TSocket.TSocket(i.ip, i.port)
                    t = TTransport.TBufferedTransport(t)
                    p = TBinaryProtocol.TBinaryProtocol(t)
                    c = FileStore.Client(p)
                    t.open()
                    k = c.findPred(key)
                    t.close()
                    return k
            raise SystemException("Find Pred Error - Cannot find pred 2")
            
    # NodeID getNodeSucc() 
    # throws (1: SystemException systemException),
    def getNodeSucc(self):
        if not len(self.finger_table):
            raise SystemException("Get Node Succ Error - Finger table is missing")
        return self.finger_table[0]

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    handler = ChordHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=handler.port_num)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
