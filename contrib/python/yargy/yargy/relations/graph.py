
from collections import defaultdict
from itertools import combinations

from yargy.record import Record


class Edge(Record):
    __attributes__ = ['relation', 'first', 'second']

    def __init__(self, relation, first, second):
        self.relation = relation
        self.first = first
        self.second = second


class RelationsGraph(object):
    def __init__(self):
        self.relations = {}
        self.relation_items = defaultdict(list)

    def add(self, relation, item):
        relation_id = id(relation)
        self.relations[relation_id] = relation
        self.relation_items[relation_id].append(item)

    @property
    def edges(self):
        for relation_id, items in self.relation_items.items():
            relation = self.relations[relation_id]
            for first, second in combinations(items, 2):
                yield Edge(relation, first, second)


class TokenRelationsGraph(RelationsGraph):
    def __init__(self):
        super(TokenRelationsGraph, self).__init__()
        self.tokens = {}
        self.token_forms = defaultdict(list)

    def add(self, relation, token):
        super(TokenRelationsGraph, self).add(relation, token)
        token_id = id(token)
        self.tokens[token_id] = token
        if token_id not in self.token_forms:
            for form in token.forms:
                self.token_forms[token_id].append(form)

    def validate(self):
        for relation, first, second in self.edges:
            first_id = id(first)
            second_id = id(second)
            first_forms = self.token_forms[first_id]
            second_forms = self.token_forms[second_id]
            checked_first_forms = []
            checked_second_forms = []
            for first_form in first_forms:
                for second_form in second_forms:
                    if relation(first_form, second_form):
                        if first_form not in checked_first_forms:
                            checked_first_forms.append(first_form)
                        if second_form not in checked_second_forms:
                            checked_second_forms.append(second_form)
            self.token_forms[first_id] = checked_first_forms
            self.token_forms[second_id] = checked_second_forms

        for token_id in self.tokens:
            if not self.token_forms[token_id]:
                return False
        return True

    def constrain(self, token):
        token_id = id(token)
        if token_id in self.tokens:
            token = self.tokens[token_id]
            forms = self.token_forms[token_id]
            return token.constrained(forms)
        return token
