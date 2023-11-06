from elasticsearch import Elasticsearch
from Data.data_processing import unzip_file, read_file
import pandas as pd
import json

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
    print(df.head())
    documents = []
    for i in range(len(df)):
        documents.append(
            { "index": { "_index": "search-medications", "_id": str(i)}}
        )
        documents.append(df.iloc[i].to_dict())
    client.bulk(operations=documents, pipeline="ent-search-generic-ingestion")
    return documents, client


def get_interaction_by_id(id,mappings):
    """
    Returns the interaction object with the specified _id field.
    
    Args:
        id (str): The _id value to search for in the interactions index.

    Returns:
        str: A JSON string representing the interaction object that matches the specified _id.

    Raises:
        HTTPException: If no interaction object is found that matches the specified _id.
    """
    client = connect_client()
    result = client.search(index="search-medications", body={"query": {"match": {"_id": id}}})
    hits = result['hits']['hits']
    json_object = json.dumps(hits[0]['_source'], indent = 4)
    if len(hits) == 0:
        return json({"message": "Interaction not found"}), 404
    else:
        return json_object
    

def get_interaction_by_field(field_value,mappings):
    """
    Returns all interaction objects that have the specified value in the specified field.

    Args:
        field_value (str): The value to search for in the specified field.

    Returns:
        str: A JSON string representing the interaction objects that match the search criteria.

    Raises:
        HTTPException: If no interaction objects are found that match the search criteria.
    """
    client = connect_client()
    result = client.search(index="search-medications", q=field_value)
    json_object = json.dumps(mappings, indent = 4)
    hits = result['hits']['hits']
    if len(hits) == 0:
        return json({"message": "Interaction not found"}), 404
    else:
        print(json_object)
        return json_object



def main():
    mappings = {
        "properties": {
            "gene_name": {"type": "text"}, 
            "gene_claim_name": {"type": "text"},
            "entrez_id": {"type": "long "},
            "interaction_claim_source": {"type": "text"},
            "interaction_types": {"type": "text"},
            "drug_claim_primary_name": {"type": "text"},    
            "drug_name": {"type": "text"},      
            "drug_concept_id": {"type": "text"},
            "interaction_group_score": {"type": "long "},    
            "PMIDs": {"type": "long "}
        }}
    data_file = "Data/sample_interaction_data.tsv"
    client = connect_client()
    documents, client = index_data(data_file, client)
    search_method = input("Enter 1 to search by id field, 2 to search by objects within fields: ")
    if search_method == "1":
        id = input("Enter id: ")
        get_interaction_by_id(id,mappings)
    elif search_method == "2":
        field_value = input("Enter field value: ")
        get_interaction_by_field(field_value,mappings)


if __name__ == "__main__":
    main()


