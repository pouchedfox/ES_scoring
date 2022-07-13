# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from datetime import datetime
from elasticsearch import Elasticsearch

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    es = Elasticsearch(
        cloud_id='My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxNzZkZTlkOTlhYTQ0N2U5YTg0YmUxOWQyZjViNDYxMyRiYTQ4NTAwZGQ5ZDg0MDAwODFlNmUxOTg3M2JhNmQ3OQ==',
        basic_auth=("elastic", 'jnrRVZQKvKK0Rx7Y2gVtCT1l')
    )
    doc = {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool.',
        'timestamp': datetime.now(),
    }
    resp = es.index(index="test-index", id=1, document=doc)
    print(resp['result'])

    resp = es.get(index="test-index", id=1)
    print(resp['_source'])

    es.indices.refresh(index="test-index")

    resp = es.search(index="test-index", query={"match_all": {}})
    print("Got %d Hits:" % resp['hits']['total']['value'])
    for hit in resp['hits']['hits']:
        print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])


