from elasticsearch import Elasticsearch, helpers
import csv
import ast
from sentence_transformers import SentenceTransformer

es = Elasticsearch(
    cloud_id='My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxNzZkZTlkOTlhYTQ0N2U5YTg0YmUxOWQyZjViNDYxMyRiYTQ4NTAwZGQ5ZDg0MDAwODFlNmUxOTg3M2JhNmQ3OQ==',
    basic_auth=("elastic", 'jnrRVZQKvKK0Rx7Y2gVtCT1l')
)

model_id = "all-mpnet-base-v2"
st_model = SentenceTransformer(model_id)

def gen_vec(str):
    return st_model.encode(str).tolist()

def read_csv(filename):
    with open(filename) as f:
        file_data = csv.reader(f)
        headers = next(file_data)
        return [dict(zip(headers, i)) for i in file_data]


dim = 768
n = 1000
qid = 500

if __name__ == '__main__':
    mapping = {
        "mappings": {
            "properties": {
                "text": {
                    "type": "text"
                },
                "vector": {
                    "type": "dense_vector",  # formerly "string"
                    "dims": dim
                },
            }
        }
    }
    es.options(ignore_status=[400, 404]).indices.delete(index='test-index')
    es.options(ignore_status=[400, 404]).indices.create(index="test-index", body=mapping)

    docs = read_csv('sample-movies-vec.csv')

    actions = [
        {
            "_index": "test-index",
            "_id": id,
            "_source": {
                "text": movie['description'],
                'vector': ast.literal_eval(movie['desc_vec'])[:dim]}
        }
        for id, movie in enumerate(docs[:n])
    ]
    helpers.bulk(es, actions)

    es.indices.refresh(index="test-index")

    # first round, compute max_score for text search
    text_query = {"match": {"text": "betrayal"}}
    resp = es.search(index="test-index", body={"query": text_query})
    max_score = resp['hits']['hits'][0]["_score"]

    vector_query = [sum(value) for value in zip(ast.literal_eval(docs[qid]['desc_vec'])[:dim], [0.0001]*dim)]

    # second and third round, compute vector score followd by text score as rescore
    resp = es.search(index="test-index", size=100, body=
    {
        "query": {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    "source": "1/(1-(cosineSimilarity(params.query_vector, 'vector') + 1.0)/2.2)",
                    "params": {
                        "query_vector": vector_query
                    }
                }
            }
        },
        "rescore": {
            "window_size": n,
            "query": {
                "score_mode": "total",
                "rescore_query": {
                    "script_score": {
                        "query": text_query,
                        "script": {
                            "source": "1/(1-_score/params.max_score/1.1)",
                            "params": {
                                "max_score": max_score,
                            }
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
        print("id: {}\t||\tscore: {}".format(hit["_id"], hit["_score"]))
        print("text: %(text)s" % hit["_source"])
