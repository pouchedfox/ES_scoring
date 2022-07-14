# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from datetime import datetime
from elasticsearch import Elasticsearch
import numpy as np
import csv

es = Elasticsearch(
    cloud_id='My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxNzZkZTlkOTlhYTQ0N2U5YTg0YmUxOWQyZjViNDYxMyRiYTQ4NTAwZGQ5ZDg0MDAwODFlNmUxOTg3M2JhNmQ3OQ==',
    basic_auth=("elastic", 'jnrRVZQKvKK0Rx7Y2gVtCT1l')
)


def read_csv(filename):
    with open(filename) as f:
        file_data = csv.reader(f)
        headers = next(file_data)
        return [dict(zip(headers, i)) for i in file_data]


if __name__ == '__main__':
    mapping = {
        "mappings": {
            "properties": {
                "text": {
                    "type": "text"
                },
                "vector": {
                    "type": "dense_vector",  # formerly "string"
                    "dims": 5
                },
            }
        }
    }
    es.options(ignore_status=[400, 404]).indices.delete(index='test-index')
    es.options(ignore_status=[400, 404]).indices.create(index="test-index", body=mapping)

    docs = read_csv('sample-movies-123.csv')
    n = 10
    for id, movie in enumerate(docs[:n]):
        doc = {
            'text': movie['description'],
            'vector': [id + 1] * 5
        }
        resp = es.index(index="test-index", id=id, document=doc)

    resp = es.get(index="test-index", id=1)

    es.indices.refresh(index="test-index")

    resp = es.search(index="test-index", body=
    {
        "query": {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    "source": "1-1/(cosineSimilarity(params.query_vector, 'vector') + 1.0)",
                    "params": {
                        "query_vector": [2, 2, 2, 2, 2]
                    }
                }
            }
        },
        "rescore": {
            "window_size": 10,
            "query": {
                "score_mode": "total",
                "rescore_query": {
                    "script_score": {
                        "query": {
                            "match": {"text": "love"}
                        },
                        "script": {
                            "source": "1-1/_score",
                        }
                    }
                },
                # "query_weight": 0.7,
                # "rescore_query_weight": 1.2,
            }
        }
    }
                     )

    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print(hit["_score"])
        print("%(text)s" % hit["_source"])
