
from yargy.record import Record
from yargy.check import assert_type


class Predicate(Record):
    children = []

    def __call__(self, token):
        # return True of False
        raise NotImplementedError

    def optional(self):
        from yargy.api import rule
        return rule(self).optional()

    def repeatable(self, min=None, max=None, reverse=False):
        from yargy.api import rule
        return rule(self).repeatable(min=min, max=max, reverse=reverse)

    def named(self, name):
        from yargy.api import rule
        return rule(self).named(name)

    def interpretation(self, attribute):
        from yargy.api import rule
        from yargy.interpretation import prepare_token_interpretator
        interpretator = prepare_token_interpretator(attribute)
        return rule(self).interpretation(interpretator)

    def match(self, relation):
        from yargy.api import rule
        return rule(self).match(relation)

    def activate(self, _):
        return self

    def constrain(self, token):
        return token

    @property
    def label(self):
        return repr(self)


def is_predicate(item):
    return isinstance(item, Predicate)


class PredicateScheme(Predicate):
    def activate(self, context):
        # return Predicate not a scheme
        raise NotImplementedError


class PredicatesComposition(Predicate):
    __attributes__ = ['predicates']

    operator = None
    name = None

    def __init__(self, predicates):
        predicates = list(predicates)
        for predicate in predicates:
            assert_type(predicate, Predicate)
        self.predicates = predicates

    def __call__(self, token):
        return self.operator(_(token) for _ in self.predicates)

    def activate(self, context):
        return self.__class__(
            _.activate(context)
            for _ in self.predicates
        )

    @property
    def label(self):
        return '{name}({predicates})'.format(
            name=self.name,
            predicates=', '.join(_.label for _ in self.predicates)
        )


class AndPredicate(PredicatesComposition):
    operator = all
    name = 'and_'


class OrPredicate(PredicatesComposition):
    operator = any
    name = 'or_'


class NotPredicate(Predicate):
    __attributes__ = ['predicate']

    def __init__(self, predicate):
        assert_type(predicate, Predicate)
        self.predicate = predicate

    def __call__(self, token):
        return not self.predicate(token)

    def activate(self, context):
        return NotPredicate(self.predicate.activate(context))

    @property
    def label(self):
        return 'not_({predicate})'.format(
            predicate=self.predicate.label
        )


class ParameterPredicate(Predicate):
    __attributes__ = ['value']

    def __init__(self, value):
        self.value = value


class ParameterPredicateScheme(ParameterPredicate, PredicateScheme):
    pass
