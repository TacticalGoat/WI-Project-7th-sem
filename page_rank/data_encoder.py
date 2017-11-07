#! ../env/bin/python
from mrjob.protocol import JSONProtocol
import json

def encode_node(node_id,links=None,score=1):
    node = {}
    if links:
        node['links'] = sorted(links.items())
    node['score'] = score
    x = JSONProtocol()
    #return x.write(node_id,node) + bytes('\n','utf-8')
    return json.dumps(node) + '\n'

if __name__ == '__main__':
    from sys import argv
    d = {}
    with open(argv[1], 'r') as fi:
        for line in fi:
            link = line.split()
            d[link[0]] = d.get(link[0], []) + [link[2]]

    with open('encoded_1.txt', 'w') as fo:
        for key in d:
            temp = {}
            for val in d[key]:
                temp[val] = (val, 1.0/len(d[key]))
            fo.write(encode_node(key,temp))
