from elasticsearch import Elasticsearch
import pandas as pd
from elasticsearch.helpers import bulk
#docker run --rm -p 9200:9200 -p 9300:9300 -e "xpack.security.enabled=false" -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:8.7.0

#Navigate to Elasticsearch connect to local host
es = Elasticsearch("http://localhost:9200")
# es = Elasticsearch("http://http://127.0.0.1:9200/")
#export ELASTIC_PASSWORD=''
# es = Elasticsearch([{'host': 'localhost', 'port': '9200', "scheme": "http"}])
# es = Elasticsearch([{'host': 'localhost', 'port': '9200'}]), http_auth=('thesplinteredtrainer@gmail.com', '')

es.info().body
print("Connection Status is",es.ping())

#es.indices.create(index="example_index_1")

#Read data file and place into pandas dataframe
df = (
    pd.read_csv("scripps-research-test/Data/sample_interaction_data.tsv",sep="\t",lineterminator='\n')
    .dropna()
    # .sample(5000, random_state=42)
    .reset_index()
)
#pd.set_option('display.max_colwidth', None)
print(df.head())

#Create index and mappings
mappings = {
    "properties": {
        "index": {"type": "long "},
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

#es.indices.create(index="medications", mappings=mappings)



#Add data to index
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

# for i in es.get(index="medications", id=i):
#     print(i)
# print(es.get(index="medications", id=i))
# indicies = es.indices.get_alias("*")
# for i in indicies:
#     print(i)





#causes key error
#Store bulk data in list
bulk_data = []
for i,row in df.iterrows():
    bulk_data.append(
        {
            "_index": "medications",
            "_id": i,
            "_source": {        
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
                "PMIDs": row["PMIDs"],
            }
        }
    )
bulk(es, bulk_data)


es.indices.refresh(index="medications")
es.cat.count(index="medications", format="json")










resp = es.search(
    index="medications",
    query={
            "bool": {
                "must": {
                    "match_phrase": {
                        "gene_name": "ROS1",
                    }
                },
                "filter": {"bool": {"must_not": {"match_phrase": {"column name": "data field"}}}},
            },
        },            
)
resp.body