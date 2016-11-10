import sys
import pandas as pd
class Node(object):
    def __init__(self, name):
        self.name = str(name)
    def getName(self):
        return self.name
    def __str__(self):
        return self.name
    def __repr__(self):
        return self.name
    def __eq__(self, other):
        return self.name == other.name
    def __ne__(self, other):
        return not self.__eq__(other)
    def __hash__(self):
        return self.name.__hash__()
class Edge(object):
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest
    def getSource(self):
        return self.src
    def getDestination(self):
        return self.dest
    def __str__(self):
        return '{0}->{1}'.format(self.src, self.dest)
class Graph(object):
    """
    A graph
    """
    def __init__(self):
        self.nodes = set([])
        self.edges = {}
    def addNode(self, node):
        if node in self.nodes:
            # Even though self.nodes is a Set, we want to do this to make sure we
            # don't add a duplicate entry for the same node in the self.edges list.
            raise ValueError('Duplicate node')
        else:
            self.nodes.add(node)
            self.edges[node] = []
    def addEdge(self, edge):
        src = edge.getSource()
        dest = edge.getDestination()
        if not(src in self.nodes and dest in self.nodes):
            raise ValueError('Node not in graph')
        if dest not in self.edges[src]:
            self.edges[src].append(dest)
        if src not in self.edges[dest]:
            self.edges[dest].append(src)
        
    def childrenOf(self, node):
        return self.edges[node]
    def hasNode(self, node):
        return node in self.nodes
    def __str__(self):
        res = ''
        for k in self.edges:
            for d in self.edges[k]:
                res = '{0}{1}->{2}\n'.format(res, k, d)
        return res[:-1]
def printPath(path):
    # a path is a list of nodes
    result = ''
    for i in range(len(path)):
        if i == len(path) - 1:
            result = result + str(path[i])
        else:
            result = result + str(path[i]) + '->'
    return result
def DFSFeatureone(graph, start, end, path =[]):
    if start not in graph.nodes or end not in graph.nodes:
        return None
    if len(path) > 1:
        return None
    path = path + [start]
    #print 'Current dfs path:', printPath(path)
    if start==end:
        return True
    for node in graph.childrenOf(start):
        if node not in path:#avoid cycles
            isfriend = DFSFeatureone(graph,node,end,path)
            if isfriend!=None:
                return True
def DFSFeaturetwo(graph, start, end, path =[]):
	if start not in graph.nodes or end not in graph.nodes:
		return None
	if len(path) > 2:
		return None
	path = path + [start]
    #print 'Current dfs path:', printPath(path)
	if start==end:
		return True
	for node in graph.childrenOf(start):
		if node not in path:#avoid cycles
			isfriend = DFSFeaturetwo(graph,node,end,path)
			if isfriend!=None:
				return True
def DFSFeaturethree(graph, start, end, path =[]):
    if start not in graph.nodes or end not in graph.nodes:
        return None
    if len(path) > 4:
        return None
    path = path + [start]
    #print 'Current dfs path:', printPath(path)
    if start==end:
        return True
    for node in graph.childrenOf(start):
        if node not in path:#avoid cycles
            isfriend = DFSFeaturethree(graph,node,end,path)
            if isfriend!=None:
                return True
                
BatchFile = sys.argv[1]
StreamFile = sys.argv[2]
output1 = sys.argv[3]
output2 = sys.argv[4]
output3 = sys.argv[5]
fields = ['id1','id2']
edge_raw = pd.read_csv(BatchFile, skipinitialspace=True, usecols=fields)
df_stream = pd.read_csv(StreamFile, skipinitialspace=True, usecols=fields)

edge_clean = edge_raw.drop_duplicates()

nodes = [] # element is node
edges = [] # element is edge
for index in range(len(edge_clean)):
    src = Node(edge_clean.iloc[index][0])
    des = Node(edge_clean.iloc[index][1])
    if src not in nodes:
        nodes.append(src)
    if des not in nodes:
        nodes.append(des)
    edges.append(Edge(src,des))
    g= Graph()
for node in nodes:
    g.addNode(node)
for edge in edges:
    g.addEdge(edge)

    
final1 = pd.DataFrame()
final2 = pd.DataFrame()
final3 = pd.DataFrame()
for index in range(len(df_stream)):
    if DFSFeatureone(g,Node(df_stream.iloc[index][0]),Node(df_stream.iloc[index][1]),path=[]):
        final1 = final1.append({'correlation':'trusted'},ignore_index=True)
    else:
        final1 = final1.append({'correlation':'unverified'},ignore_index=True)
    if DFSFeaturetwo(g,Node(df_stream.iloc[index][0]),Node(df_stream.iloc[index][1]),path=[]):
        final2 = final2.append({'correlation':'trusted'},ignore_index=True)
    else:
        final2 = final2.append({'correlation':'unverified'},ignore_index=True)
    if DFSFeaturethree(g,Node(df_stream.iloc[index][0]),Node(df_stream.iloc[index][1]),path=[]):
        final3 = final3.append({'correlation':'trusted'},ignore_index=True)
    else:
        final3 = final3.append({'correlation':'unverified'},ignore_index=True)
final1.to_csv(output1, header=None, index=False)
final2.to_csv(output2, header=None, index=False)
final3.to_csv(output3, header=None, index=False)
