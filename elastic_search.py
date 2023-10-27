from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Create an index
index_name = 'interactions_index'
es.indices.create(index=index_name, ignore=400)

# Bulk insert the interaction objects
interactions = parse_data_file('data.csv')

for interaction in interactions:
    es.index(index=index_name, doc_type='_doc', id=interaction['_id'], body=interaction)
