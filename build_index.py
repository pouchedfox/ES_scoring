from elasticsearch import Elasticsearch, helpers
import csv
import ast
from sentence_transformers import SentenceTransformer
import time
from relevanceai.utils.datasets import get_ecommerce_dataset
import pickle

es = Elasticsearch(
    cloud_id='My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxNzZkZTlkOTlhYTQ0N2U5YTg0YmUxOWQyZjViNDYxMyRiYTQ4NTAwZGQ5ZDg0MDAwODFlNmUxOTg3M2JhNmQ3OQ==',
    basic_auth=("elastic", 'jnrRVZQKvKK0Rx7Y2gVtCT1l')
)

model_id = "all-mpnet-base-v2"
st_model = SentenceTransformer(model_id)


def gen_vec(str):
    return st_model.encode(str).tolist()

dim = 768
# n = 10000

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
    # es.options(ignore_status=[400, 404]).indices.delete(index='ecommerce-index')
    # es.options(ignore_status=[400, 404]).indices.create(index="ecommerce-index", body=mapping)
    #
    # docs = get_ecommerce_dataset(number_of_documents=15528)
    # # docs = read_csv('sample-movies-vec.csv')
    # print(docs[0])
    #
    # actions = []
    # for id, doc in enumerate(docs):
    #     if id % 100 == 0:
    #         print(id)
    #     if 'product_description' not in doc or 'product_title' not in doc:
    #         continue
    #     actions.append(
    #         {
    #             "_index": "ecommerce-index",
    #             "_id": id,
    #             "_source": {
    #                 "text_1": doc['product_description'],
    #                 "text_2": doc['product_title'],
    #                 'vector_1': gen_vec(doc['product_description']),  # ast.literal_eval(movie['desc_vec'])[:dim]}
    #                 'vector_2': gen_vec(doc['product_title']),
    #             }
    #         }
    #     )
    #
    # print(len(actions))
    #
    # with open('ecommerce.pickle', 'wb') as handle:
    #     pickle.dump(actions, handle)

    with open('ecommerce.pickle', 'rb') as handle:
        actions = pickle.load(handle)


    helpers.bulk(es, actions)

    es.indices.refresh(index="ecommerce-index")