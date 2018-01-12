#!/usr/bin/env python3

from sys import argv,stdin
from requests import get,post,put,delete
from json import loads,dumps
from enqp import parse,create_aggregations,flatten_aggs
from random import randint

mapping = {
    "mappings": {
        "enqp": {
            "properties": {
                "title": { "type": "text" },
                "subTitle": { "type": "text" },
                "type": { "type": "keyword" },
                "contribution": {
                    "type": "nested",
                    "properties": {
                        "role": { "type": "keyword" },
                        "agent": {
                            "type": "nested",
                            "properties": {
                                "firstName": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
                                "lastName": { "type": "text" }
                            }
                        }
                    }
                }
            }
        }
    }
}

docs = [{
    "title": "Nested Query Parser for Elasticsearch",
    "subTitle": "You know, for nested queries",
    "type": "publication",
    "contribution": [
        {
            "role": "aut",
            "agent": {
                "firstName": "Pelle",
                "lastName": "Olsson"
            }
        },
        {
            "role": "ill",
            "agent": {
                "firstName": "Pelle",
                "lastName": "Nilsson"
            }
        }
    ]
},
{
    "title": "Something completely different",
    "subTitle": "and more things",
    "type": "publication",
    "contribution": [
        {
            "role": "aut",
            "agent": {
                "firstName": "Nils",
                "lastName": "Nilsson"
            }
        },
        {
            "role": "aut",
            "agent": {
                "firstName": "Karl",
                "lastName": "Olsson"
            }
        }
    ]
}
]


def search(q, base):
    q = parse(q)
    #q.update(create_aggregations({ 'type': 'type' }))
    #q.update(create_aggregations({ 'type': 'type', 'role': { 'contribution': 'role' } }))
    q.update(create_aggregations({ 'type': 'type', 'role': { 'contribution': 'role' }, 'first_name': { 'contribution': { 'agent': 'firstName.keyword' } } }))
    print(q)

    r = get(base + '/_search', headers={ 'Content-Type': 'application/json' }, data=dumps(q))

    return loads(r.text)


if __name__ == "__main__":
    # @todo make random string
    index_name = 'test_enqp_' + str(randint(1, 100000))
    base = argv[1] + '/' + index_name
    print(base)

    # create index / put mapping
    r = put(base, headers={ 'Content-Type': 'application/json' }, data=dumps(mapping))
    print(r.text)

    # load documents
    for i,doc in enumerate(docs):
        r = put(base + '/enqp/' + str(i), headers={ 'Content-Type': 'application/json' }, data=dumps(doc))		
        print(r.text)
    
    # run tests
    for line in stdin:
        print(dumps(flatten_aggs(search(line, base)), indent=2))
    #print(search('type:publication', base))

    # delete index
    r = delete(base)
    print(r.text)


