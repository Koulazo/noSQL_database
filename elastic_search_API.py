#cEFTRG80c0JCRDB3OUkwZkExYXY6c2pvaXJfU0lUSHVSOXZmclFYYVY3QQ==

from elasticsearch import Elasticsearch
from Data.data_processing import unzip_file, read_file
import pandas as pd
# es = Elasticsearch([{'host': 'localhost', 'port': '9200', "scheme": "http"}])
# es = Elasticsearch([{'host': 'localhost', 'port': '9200', "scheme": "http"}])#, http_auth==("elastic", '5lCemi6s2uVcDLpQhzqO')


# def connect_client():
client = Elasticsearch(
"http://localhost:9200",
api_key="cEFTRG80c0JCRDB3OUkwZkExYXY6c2pvaXJfU0lUSHVSOXZmclFYYVY3QQ==",
#ca_certs="/path/to/http_ca.crt",
#basic_auth=("elastic", "5lCemi6s2uVcDLpQhzqO")
)
# API key should have cluster monitor rights
client.info()
print("Connection Status is",client.ping())

    #return client
data_file = "Data/sample_interaction_data.tsv"
#def index_data(data_file):
df = (
    pd.read_csv(data_file,sep="\t",lineterminator='\n')
    .dropna()
    # .sample(5000, random_state=42)
    .reset_index()
)
#pd.set_option('display.max_colwidth', None)
print(df.head())

documents = []
for i in range(len(df)):
    documents.append(
        { "index": { "_index": "search-medications", "_id": str(i)}}
    )
    documents.append(df.iloc[i].to_dict())
#print(documents)
client.bulk(operations=documents, pipeline="ent-search-generic-ingestion")
    #return documents

key_word = "ROS1"
#def index_search(key_word):
client.search(index="search-medications", q=key_word)
print(client.search(index="search-medications", q=key_word))