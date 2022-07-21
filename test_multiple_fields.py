from math import log

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
query_text = "war in the universe"

if __name__ == '__main__':
    mapping = {
        "mappings": {
            "properties": {
                "text_1": {
                    "type": "text"
                },
                "text_2": {
                    "type": "text"
                },
                "vector_1": {
                    "type": "dense_vector",  # formerly "string"
                    "dims": dim
                },
                "vector_2": {
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
                "text_1": movie['description'],
                "text_2": movie['title'],
                'vector_1': ast.literal_eval(movie['desc_vec'])[:dim],
                'vector_2': ast.literal_eval(movie['desc_vec'])[:dim]},
        }
        for id, movie in enumerate(docs[:n])
    ]
    helpers.bulk(es, actions)

    es.indices.refresh(index="test-index")
    term_res = es.mtermvectors(
        index="test-index",
        body=dict(
            # ids=list(range(n)),
            ids=[0, 1],
            term_statistics=True,
            field_statistics=True,
            fields=["text_1"]
        )
    )

    docCount = n
    k1 = 1.2
    b = 0.75
    avg_doc_len = None

    max_score_1 = -1
    for doc in term_res['docs']:
        # print(doc['_id'])
        # print(doc)
        df_arr = []
        tf_arr = []
        doc_len = 0
        info = doc['term_vectors']['text_1'] # TODO: text_2?
        if avg_doc_len is None:
            avg_doc_len = info['field_statistics']['sum_ttf'] / info['field_statistics']['doc_count']
        for term in info['terms']:
            doc_len += info['terms'][term]['term_freq']
            tf_arr.append(info['terms'][term]['term_freq'])
            df_arr.append(info['terms'][term]['doc_freq'])
        for i, tf in enumerate(tf_arr):
            df = df_arr[i]
            score = log(1 + (docCount - df + 0.5) / (df + 0.5)) * (tf) / (tf + k1 * (1 - b + b * doc_len / avg_doc_len))
            if score > max_score_1:
                max_score_1 = score

    print(max_score_1)

    term_res = es.mtermvectors(
        index="test-index",
        body=dict(
            # ids=list(range(n)),
            ids=[0, 1],
            term_statistics=True,
            field_statistics=True,
            fields=["text_2"]
        )
    )

    docCount = n
    k1 = 1.2
    b = 0.75
    avg_doc_len = None

    max_score_2 = -1
    for doc in term_res['docs']:
        # print(doc['_id'])
        # print(doc)
        df_arr = []
        tf_arr = []
        doc_len = 0
        info = doc['term_vectors']['text_2'] # TODO: text_2?
        if avg_doc_len is None:
            avg_doc_len = info['field_statistics']['sum_ttf'] / info['field_statistics']['doc_count']
        for term in info['terms']:
            doc_len += info['terms'][term]['term_freq']
            tf_arr.append(info['terms'][term]['term_freq'])
            df_arr.append(info['terms'][term]['doc_freq'])
        for i, tf in enumerate(tf_arr):
            df = df_arr[i]
            score = log(1 + (docCount - df + 0.5) / (df + 0.5)) * (tf) / (tf + k1 * (1 - b + b * doc_len / avg_doc_len))
            if score > max_score_2:
                max_score_2 = score

    print(max_score_2)

    # first round, compute max_score for text search
    text_query = {"multi_match": {
        "query": query_text,
        "fields": ["text_1", "text_2"],
    }
    }
    # {"match":
    #                   {"text": query_text}
    #               }
    # resp = es.search(index="test-index", body={"query": text_query})
    # max_score = resp['hits']['hits'][0]["_score"]

    # vector_query = [sum(value) for value in zip(ast.literal_eval(docs[qid]['desc_vec'])[:dim], [0.0001]*dim)]
    vector_query = gen_vec(query_text)
    # second and third round, compute vector score followd by text score as rescore
    resp = es.search(index="test-index", size=100, body=
    {
        "query": {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    "source": "1/(1-(cosineSimilarity(params.query_vector, 'vector_1') + 1.0)/2.2)*params.weight_1 + 1/(1-(cosineSimilarity(params.query_vector, 'vector_2') + 1.0)/2.2)*params.weight_2",
                    # "source": "0",
                    "params": {
                        "weight_1": 1,
                        "weight_2": 1,
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
                            # "source": "1",
                            "params": {
                                "max_score": (max_score_1 + max_score_2) * len(query_text.split()),
                            }
                        }
                    }
                },
                "query_weight": 5,
                "rescore_query_weight": 1,
            }
        }
    }
                     )

    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print("id: {}\t||\tscore: {}".format(hit["_id"], hit["_score"]))
        print("title: %(text_2)s" % hit["_source"])
        print("text: %(text_1)s" % hit["_source"])
