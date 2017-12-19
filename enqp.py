#!/usr/bin/env python3

# Elasticsearch Nested Query Parser
#
# Examples:
#
# matches documents when name and lastName match any nested objects separately
#   nested.name:pelle AND nested.lastName:Olsson
#
# matches document only when name and lastName match the same nested object
#   nested:{ name: Pelle, lastName: Olsson }
#
# match multiple levels of nested objects:
#   contributor:{ role: aut, agent: { name: Pelle, lastName: Olsson } }

from sys import argv,stdin,stderr
from lark import Lark

class ENQP():
    def __init__(self, mapping=None, nested=None, version="6"):
        self.mapping = mapping
        self.nested = nested
        self.parser = Lark('query:          query_part | boolean_query\n' +
                           'query_part:     asterisk | expr | dictionary | "(" query ")"\n' +
                           'boolean_query:  query_part operator query_part (operator query_part)*\n' +
                           'dictionary:     "{" [key_val] ("," key_val)* "}"\n' +
                           'key_val:        field ":" (string | dictionary)\n' +
                           'operator:       and | or | and_not\n' +
                           'and:            "and"i\n' +
                           'or:             "or"i\n' +
                           'and_not:        "and not"i\n' +
                           'expr:           string | fielded_expr\n' +
                           'fielded_expr:   field ":" (string | dictionary)\n' +
                           'field:          CNAME | "\\"" CNAME "\\""\n' +
                           'string:         CNAME | ESCAPED_STRING\n' +
                           'asterisk:       "*"\n' +
                           'CNAME:          /[a-zA-Z0-9\\.-]+/\n' +
                           '%import common.ESCAPED_STRING\n' +
                           '%import common.WS\n' +
                           '%ignore WS',
                           start='query')


    def _handle(self, node, field=''):
        if node.data in [ 'query', 'query_part'] :
            return self._handle(node.children[0], field)
        elif node.data == 'dictionary':
            if len(node.children) == 0:
                return { 'match_all': {} }
        elif node.data == 'boolean_query':
            return { 'bool': { 'must': {} } }
        elif node.data == 'asterisk':
            return { 'match_all': {} }
        elif node.data == 'expr':
            return self._handle(node.children[0], field)
        elif node.data == 'fielded_expr':
            return self._handle(node.children[1], '.'.join([ node.children[0].children[0].value ]))
        elif node.data == 'string':

            return { 'term': { field: node.children[0].value } }


    def _parse(self, query):
        return self.parser.parse(query)


    def parse(self, query):
        x = self.parser.parse(query)
        print(x.pretty(), file=stderr)

        return { 'query': self._handle(x) }


if __name__ == "__main__":
    e = ENQP()
    print(e.parse(argv[1]))

