
from elasticsearch import Elasticsearch
import pandas as pd
from elasticsearch.helpers import bulk
from Data.data_processing import unzip_file, read_file
#Client Password:
#5lCemi6s2uVcDLpQhzqO
#=93YXiyQq+1d9frPBoII
#APIKEY:
#cEFTRG80c0JCRDB3OUkwZkExYXY6c2pvaXJfU0lUSHVSOXZmclFYYVY3QQ==
from elasticsearch import Elasticsearch

def create_es_client():
    """
    Creates an Elasticsearch client object and returns it.

    Returns:
    es (Elasticsearch): An Elasticsearch client object.
    """
    es = Elasticsearch("http://localhost:9200")
    es.info().body
    print("Connection Status is",es.ping())
    return es



def add_data_to_es(es, file_path):
    """
    Adds data from a TSV file to an Elasticsearch index.

    Args:
        es: Elasticsearch client object
        file_path: str, path to the TSV file containing the data to be added

    Returns:
        None
    """
    # Read data from TSV file
    df = pd.read_csv(file_path, sep='\t')

    # Define mappings for Elasticsearch index
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
        }
    }

    # Create Elasticsearch index with mappings
    try:
        es.indices.create(index="medications", mappings=mappings)
    except:
        print("Index already exists")

    # Add data to index
    try:
        for i, row in df.iterrows():
            doc = {
                "index": row["index"],
                "gene_name": row["gene_name"], 
                "gene_claim_name": row["gene_claim_name"],
                "entrez_id": row["entrez_id"],
                "interaction_claim_source": row["interaction_claim_source"],
                "interaction_types": row["interaction_types"],
                "drug_claim_primary_name": row["drug_claim_primary_name"],    
                "drug_name": row["drug_name"],      
                "drug_concept_id": row["drug_concept_id"],
                "interaction_group_score": row["interaction_group_score"],    
                "PMIDs": row["PMIDs"]
            }

            es.index(index="medications", id=i, document=doc)
    except:
        print("Index already exists")               

    # Refresh index to make data available for search
    es.indices.refresh(index="medications")




def main():
    unzipped_file_path = 'Data/sample_interaction_data.tsv.zip'
    output_path = 'Data/'
    file_path = 'Data/sample_interaction_data.tsv'
    unzip_file(unzipped_file_path, output_path)
    read_file(file_path) 
    # print(interactions)
    es = create_es_client()
    add_data_to_es(es, file_path)


if __name__ == "__main__":
    main()