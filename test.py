import redis
from commands import Commands
from redis.commands.graph import Graph, Node, Edge

cmd = Commands('localhost', 6379)
redis_graph = cmd.getGraph()

john: Node = cmd.createNode('john', {'name': 'John Doe', 'age': 33, 'gender': 'male', 'status': 'single'})
japan: Node = cmd.createNode('japan', {'name': 'Japan'})

cmd.addNode(redis_graph, john)

edge = cmd.mergeEdge(redis_graph, john)

cmd.commitGraph(redis_graph)

query = """MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country)
           RETURN p.name, p.age, v.purpose, c.name"""

result = redis_graph.query(query)

 # Print resultset
# result.pretty_print()

 # Use parameters
params = {'purpose':"pleasure"}
query = """MATCH (p:person)-[v:visited {purpose:$purpose}]->(c:country)
           RETURN p.name, p.age, v.purpose, c.name"""

result = redis_graph.query(query, params)

 # Print resultset
# result.pretty_print()

 # Use query timeout to raise an exception if the query takes over 10 milliseconds
result = redis_graph.query(query, params, timeout=10)

 # Iterate through resultset
for record in result.result_set:
    person_name = record[0]
    person_age = record[1]
    visit_purpose = record[2]
    country_name = record[3]

query = """MATCH p = (:person)-[:visited {purpose:"pleasure"}]->(:country) RETURN p"""

result = redis_graph.query(query)

 # Iterate through resultset
for record in result.result_set:
   path = record[0]
   print(path)

# import os
# read files from nested directories
for root, dirs, files in os.walk("C:/Users/HP/Desktop/Python"):
    for file in files:
        if file.endswith(".txt"):
             print(os.path.join(root, file))


