import os
from vocabulary import Vocabulary as voc
import utils as utl

from redis.commands.graph import Graph, Node, Edge

from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query
from redis.commands.search.document import Document

from cerberus import Validator

class Commands:
    redis_host = 'localhost'
    redis_port = 6379
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis = utl.getRedisClient(self.redis_host, self.redis_port)
        self.graph = Graph(self.redis, 'social')

    '''
        Graph support methods
    '''
    # get graph
    def getGraph(self) -> Graph|None:
        return self.graph

    # Create Node
    def createNode(self, _label: str, _props: dict) -> Node|None:
        node = Node(_label, properties=_props)
    
        return node
    
    # Create Edge
    def createEdge(self, node_1: Node, label: str, node_2: Node, props: dict) -> Edge|None:
        edge = Edge(node_1, label, node_2, properties=props)
    
        return edge

    # Add node to graph
    def addNode(self, graph: Graph, node: Node) -> Graph|None:
        graph.add_node(node)
    
        return graph
    
    # add edge to graph
    def addEdge(self, graph: Graph, edge: Edge) -> Graph|None:
        graph.add_edge(edge)
    
        return graph
    
    # Merge Node
    def mergeNode(self, graph: Graph, node: Node) -> Graph|None:
        graph.merge(node)
    
        return graph
    
    # Merge Edge
    def mergeEdge(self, graph: Graph, edge: Edge) -> Edge|None:
        graph = graph.merge(edge)        
    
        return graph
    
    #commit graph
    def commitGraph(self, graph: Graph):
        graph.commit()


    '''
        Redisearch support methods
    '''
    # Create index
    def createIndex(self, index_name: str, schema_path: str, proc: bool) -> bool:
        sch = utl.getSchemaFromFile(schema_path)        
        p_dict, n_doc = utl.getProps(sch, schema_path)  

        if p_dict == None:
            return

        try:
            self.redis.ft(index_name).create_index(utl.ft_schema(p_dict), definition=IndexDefinition(prefix=[utl.prefix(index_name)]))
        except:
            # print('Index already exists')
            return
        finally:
            ''' 
                Register index even if it already exists. Just in case 
                if it was created on Redis server manualy
            '''
            if proc:
                Commands.registerProcessor(schema_path, n_doc, sch)
            else:
                Commands.registerIndex(schema_path, n_doc, sch)
        return 

    def registerIndex(self, idx_schema_file: str, n_doc:dict, sch) -> dict|None:
        ''' Register index in idx_reg '''
        idx_reg_dict: dict = {
            voc.NAME: n_doc.get(voc.NAME),
            voc.NAMESPACE: n_doc.get(voc.NAMESPACE),
            voc.PREFIX: n_doc.get(voc.PREFIX),
            voc.LABEL: n_doc.get(voc.LABEL),
            voc.KIND: n_doc.get(voc.KIND),
            voc.SOURCE: str(sch)
        }
        # print('IDX_REG record: {}'.format(idx_reg_dict[voc.LABEL]))
        return Commands.updateRecord(self.redis, voc.IDX_REG, idx_schema_file, idx_reg_dict, True)

    def registerProcessor(self, proc_schema_file: str, n_doc:dict, sch) -> dict|None:
        ''' Register index in proc_reg '''
        # print(file)
        proc_reg_dict: dict = {
            voc.LABEL: n_doc.get(voc.LABEL),
            voc.NAME: n_doc.get(voc.NAME),
            voc.VERSION: n_doc.get(voc.VERSION),
            voc.PACKAGE: n_doc.get(voc.PACKAGE),
            voc.LANGUAGE: n_doc.get(voc.LANGUAGE),
            voc.SCHEMA_DIR: n_doc.get(voc.SCHEMA_DIR),
            voc.SCHEMA: n_doc.get(voc.SCHEMA),
            voc.SOURCE: str(sch)
        }
        # print('IDX_REG record: {}'.format(idx_reg_dict[voc.LABEL]))
        return Commands.updateRecord(self.redis, voc.PROC_REG, proc_schema_file, proc_reg_dict, True)
    
    '''
        This method creates/updates hash record in Redis. 'in_idx' (normal prefix index) 
        argument is a flag that indicates where record should be: "in" the index or outside, 
        by default we keep record outside index, by using underscore prefix - in_idx = false.
    '''
    def updateRecord(self, pref: str, schema_path: str, map:dict, in_idx: bool=False ) -> dict|None:
        '''
            Redisearch allows very simple implementation for a multistep transactional
            processing. 
            RS indecies based on hashes are linked to RS with the prefix that is
            a part of of index schema. 
            While we are working within a transaction we are using underscored prefix. This will keep
            record out of RS index.
            After commit we are renaming the record key (hash key) by removing underscore ('_')
            from the prefix. This brings record to the index. Kind of 'zero' copy solution :) 
            'in_idx' argument is a flag that indicates where record should be: in the index or outside,
            by default we keep record outside index.
        '''
        if in_idx:
            _pref = utl.prefix(pref)
        else: 
            _pref = utl.underScore(utl.prefix(pref)) 

        sch = utl.getSchemaFromFile(schema_path)     
        v = Validator()        
        k_list: dict = []
        id = ''
        if v.validate(utl.doc_0, sch):
            n_doc = v.normalized(utl.doc_0, sch)
            _map:dict = n_doc[voc.PROPS]
            _map.update(map)
            k_list = n_doc.get(voc.KEYS)
            # id = utl.sha1(k_list, _map)
            id = utl.sha1(k_list, n_doc)
            _map[voc.ID] = id
            self.redis.hset(_pref + id, mapping=_map)
            return _map
        else:
            print('updateRecord Error: ', _pref + id,  map)
            return None
        
    '''
        Managing transaction index - the list of available for processing records
        "transaction" index is a core of redis-meta. It is a list of records that are
        ready for processing. Each record is a hash that contains all the information
        about the resource including the URL reference to resource and its status.
    '''
    # This is one of the methods that populates 'transaction' index 
    def txUpdate(self, proc_id: str, proc_pref: str, item_id: str, item_prefix: str, item_type: str, status: str) -> str|None:
        tx_pref = utl.prefix(voc.TRANSACTION)
        map:dict = self.redis.hgetall(tx_pref + item_id)
        _map = {}
        _map[voc.ID] = item_id
        _map[voc.PROCESSOR_ID] = proc_id
        _map[voc.PROCESSOR_PREFIX] = proc_pref
        _map[voc.ITEM_PREFIX] = item_prefix
        _map[utl.underScore(voc.ITEM_PREFIX)] = utl.underScore(item_prefix)
        _map[voc.ITEM_TYPE] = item_type
        _map[voc.PROCESSOR_UUID] = ' '
        _map[voc.STATUS] = status
        if map == None:
            _map[voc.ITEM_ID] = item_id
            _map[voc.ITEM_PREFIX] = item_prefix
            _map[voc.DOC] = ' '

            return self.redis.hset(tx_pref + item_id, mapping=_map)
        else:
            map.update(_map)

            return self.redis.hset(tx_pref + item_id, mapping=map)
        
    # Updates status of resource with the value that reflects the processing step  
    def txStatus(self, proc_id: str, proc_pref: str, item_id: str, status: str) -> dict|None:
        
        map:dict = self.redis.hgetall(item_id)        
        if map == None:

            return None
        else:            
            map[voc.PROCESSOR_ID] = proc_id
            map[voc.PROCESSOR_PREFIX] = proc_pref
            map[voc.STATUS] = status

            return self.redis.hset(item_id, mapping=map)
    
    '''
        This method is used to lock (set status to "locked") a batch of records in transaction index 
        for processing.
    '''
    def txLock(self, query:str, limit: int, uuid: str) -> str|None:
        resources = Commands.selectBatch(voc.TRANSACTION, query, limit)
        ret = {}                    
        try:            
            for doc in resources.docs:
                map:dict = self.redis.hgetall(doc.id)
                map[voc.PROCESSOR_UUID] = uuid
                map[voc.STATUS] = voc.LOCKED
                self.redis.hset(voc.TRANSACTION + doc.id, mapping=map)
        except:
            ret = {'error', 'There is no data to process.'}

        return voc.OK
    
    def search(self, index: str, query: str|Query, limit: int = 10, query_params: dict|None = None) -> dict|None:
        _query: Query = Query(query).no_content(True).paging(0, limit)
        if query_params == None:
            result = self.redis.ft(index).search(_query)
            doc: Document = result.docs[0]
            doc.id

            return result
        else:

            return self.redis.ft(index).search(query, query_params)
    
    def selectBatch(self, idx_name: str, query:str, limit: int = 10) -> dict|None:
         _query = Query(query).no_content().paging(0, limit)

         return self.redis.ft(idx_name).search(_query)