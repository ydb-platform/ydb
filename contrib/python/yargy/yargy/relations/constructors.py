
from yargy.record import Record
from yargy.check import assert_type


class Main(Record):
    __attributes__ = ['term']

    def __init__(self, term):
        from yargy.rule import Rule
        from yargy.predicates import Predicate

        assert_type(term, (Rule, Predicate))
        self.term = term


class Relation(Record):
    def __call__(self, token, other):
        raise NotImplementedError

    @property
    def label(self):
        return repr(self)


def is_relation(item):
    return isinstance(item, Relation)


class RelationsComposition(Relation):
    __attributes__ = ['relations']

    operator = None
    name = None

    def __init__(self, relations):
        super(RelationsComposition, self).__init__()
        relations = list(relations)
        for relation in relations:
            assert_type(relation, Relation)
        self.relations = relations

    def __call__(self, form, other):
        return self.operator(_(form, other) for _ in self.relations)

    @property
    def label(self):
        return '{name}({relations})'.format(
            name=self.name,
            relations=', '.join(_.label for _ in self.relations)
        )


class AndRelation(RelationsComposition):
    operator = all
    name = 'and_'


class OrRelation(RelationsComposition):
    operator = any
    name = 'or_'


class NotRelation(Relation):
    __attributes__ = ['relation']

    def __init__(self, relation):
        super(NotRelation, self).__init__()
        assert_type(relation, Relation)
        self.relation = relation

    def __call__(self, form, other):
        return not self.relation(form, other)

    @property
    def label(self):
        return 'not_({relation})'.format(
            relation=self.relation.label
        )
