#cEFTRG80c0JCRDB3OUkwZkExYXY6c2pvaXJfU0lUSHVSOXZmclFYYVY3QQ==

from elasticsearch import Elasticsearch
from Data.data_processing import unzip_file, read_file
import pandas as pd

def connect_client():
    """
    Connects to Elasticsearch client using the provided API key and returns the client object.
    
    Returns:
    Elasticsearch: Elasticsearch client object
    """
    client = Elasticsearch(
    "http://localhost:9200",
    api_key="cEFTRG80c0JCRDB3OUkwZkExYXY6c2pvaXJfU0lUSHVSOXZmclFYYVY3QQ=="
    )
    # API key should have cluster monitor rights
    client.info()
    print("Connection Status is",client.ping())

    return client

def index_data(data_file,client):
    """
    Indexes data from a given file into Elasticsearch.

    Args:
        data_file (str): The path to the file containing the data to be indexed.

    Returns:
        tuple: A tuple containing the list of documents indexed and the Elasticsearch client used for indexing.
    """
    df = (
        pd.read_csv(data_file,sep="\t",lineterminator='\n')
        .dropna()
        .reset_index()
    )

    documents = []
    for i in range(len(df)):
        documents.append(
            { "index": { "_index": "search-medications", "_id": str(i)}}
        )
        documents.append(df.iloc[i].to_dict())

    client.bulk(operations=documents, pipeline="ent-search-generic-ingestion")
    return documents, client

def index_search(key_word):
    """
    Searches for documents containing the given keyword in the index.

    Args:
        key_word (str): The keyword to be searched for.

    Returns:
        dict: The search results.
    """
    client = connect_client()
    return client.search(index="search-medications", q=key_word)

def main():
    data_file = "Data/sample_interaction_data.tsv"
    client = connect_client()
    documents, client = index_data(data_file, client)
    key_word = "ROS1"
    search_results = index_search(key_word)
    print(search_results)

if __name__ == "__main__":
    main()


