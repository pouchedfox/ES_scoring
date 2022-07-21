from math import log

from elasticsearch import Elasticsearch, helpers
import csv
import ast
from sentence_transformers import SentenceTransformer
import time

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
query_text = "King James plays basketball with cartoon characters"

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

    start = time.time()

    term_res = es.mtermvectors(
        index="test-index",
        body=dict(
            # ids=list(range(n)),
            ids=[0, 1],
            term_statistics=True,
            field_statistics=True,
            fields=["text"]
        )
    )

    docCount = n
    k1 = 1.2
    b = 0.75
    avg_doc_len = None

    max_score = -1
    for doc in term_res['docs']:
        # print(doc['_id'])
        # print(doc)
        df_arr = []
        tf_arr = []
        doc_len = 0
        info = doc['term_vectors']['text']
        if avg_doc_len is None:
            avg_doc_len = info['field_statistics']['sum_ttf'] / info['field_statistics']['doc_count']
        for term in info['terms']:
            doc_len += info['terms'][term]['term_freq']
            tf_arr.append(info['terms'][term]['term_freq'])
            df_arr.append(info['terms'][term]['doc_freq'])
        for i, tf in enumerate(tf_arr):
            df = df_arr[i]
            score = log(1 + (docCount - df + 0.5) / (df + 0.5)) * (tf) / (tf + k1 * (1 - b + b * doc_len / avg_doc_len))
            if score > max_score:
                max_score = score

    print(max_score*len(query_text.split()))

    text_query = {"match": {"text": query_text}}
    vector_query = gen_vec(query_text)

    # first round, compute max_score for text search
    # resp = es.search(index="test-index", body={"query": text_query})
    # max_score = resp['hits']['hits'][0]["_score"]
    # print(max_score)

    # second and third round, compute vector score followd by text score as rescore
    resp = es.search(index="test-index", size=100, body=
    {
        "query": {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    "source": "1/(1-(cosineSimilarity(params.query_vector, 'vector') + 1.0)/2.01)",
                    # "source": "0",
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
                            "source": "1/(1-_score/params.max_score)",
                            "params": {
                                "max_score": max_score * len(query_text.split()),
                            }
                        }
                    }
                },
                "query_weight": 1.5,
                "rescore_query_weight": 1,
            }
        }
    }
                     )

    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print("id: {}\t||\tscore: {}".format(hit["_id"], hit["_score"]))
        print("text: %(text)s" % hit["_source"])

    end = time.time()
    print(end-start)