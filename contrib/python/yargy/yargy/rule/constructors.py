
from yargy.record import Record
from yargy.check import (
    assert_type,
    assert_greater_equals
)
from yargy.visitor import TransformatorsComposition
from yargy.predicates import Predicate
from yargy.relations import Main


def prepare_terms(items):
    main = None
    terms = []
    for index, item in enumerate(items):
        assert_type(item, (Predicate, Rule, Main))
        if isinstance(item, Main):
            if main is not None:
                raise ValueError('>1 main')
            main = index
            item = item.term
        terms.append(item)
    if main is None:
        main = 0
    return terms, main


class Production(Record):
    __attributes__ = ['terms', 'main']

    def __init__(self, terms, main=0):
        self.terms, self.main = prepare_terms(terms)
        if main > 0:
            self.main = main

    @property
    def children(self):
        return self.terms

    def __str__(self):
        labels = []
        for index, term in enumerate(self.terms):
            label = term.label
            if self.main > 0 and self.main == index:
                label = '^' + label
            labels.append(label)
        return ' '.join(labels)


class EmptyProduction(Production):
    def __init__(self):
        super(EmptyProduction, self).__init__([])

    def __str__(self):
        return 'e'


class Rule(Record):
    __attributes__ = ['productions']

    def __init__(self, productions):
        productions = list(productions)
        for production in productions:
            assert_type(production, Production)
        self.productions = productions

    @property
    def children(self):
        return self.productions

    def optional(self, reverse=False):
        return OptionalRule(self, reverse)

    def repeatable(self, min=None, max=None, reverse=False):
        if min and max:
            return MinMaxBoundedRule(self, min, max, reverse)
        elif min:
            return MinBoundedRule(self, min, reverse)
        elif max:
            return MaxBoundedRule(self, max, reverse)
        else:
            return RepeatableRule(self, reverse)

    def named(self, name):
        return NamedRule(self, name)

    def interpretation(self, item):
        from yargy.interpretation import prepare_rule_interpretator
        interpretator = prepare_rule_interpretator(item)
        return InterpretationRule(self, interpretator)

    def match(self, relation):
        return RelationRule(self, relation)

    def transform(self, *transformators):
        return TransformatorsComposition(transformators)(self)

    def activate(self, context):
        from .transformators import ActivateTransformator
        return ActivateTransformator(context)(self)

    @property
    def normalized(self):
        from .transformators import (
            SquashExtendedTransformator,
            ReplaceOrTransformator,
            ReplaceEmptyTransformator,
            ReplaceExtendedTransformator,
            FlattenTransformator
        )
        return self.transform(
            SquashExtendedTransformator,
            ReplaceExtendedTransformator,
            ReplaceOrTransformator,
            ReplaceEmptyTransformator,
            FlattenTransformator,
        )

    @property
    def as_dot(self):
        from .transformators import DotRuleTransformator
        return self.transform(DotRuleTransformator)

    @property
    def as_bnf(self):
        from .bnf import (
            BNFTransformator,
            RemoveForwardTransformator,
        )
        return self.transform(
            BNFTransformator,
            RemoveForwardTransformator,
        ).as_bnf

    def walk(self, types=None):
        items = bfs_rule(self)
        if types:
            items = (_ for _ in items if isinstance(_, types))
        return items


def is_rule(item):
    return isinstance(item, Rule)


def bfs_rule(rule):
    queue = [rule]
    visited = {id(rule)}
    while queue:
        item = queue.pop(0)
        yield item
        for child in item.children:
            if id(child) not in visited:
                visited.add(id(child))
                queue.append(child)


class OrRule(Rule):
    __attributes__ = ['rules']

    def __init__(self, rules):
        rules = list(rules)
        for rule in rules:
            assert_type(rule, Rule)
        self.rules = rules

    @property
    def children(self):
        return self.rules


class WrapperRule(Rule):
    __attributes__ = ['rule']

    def __init__(self, rule):
        assert_type(rule, Rule)
        self.rule = rule

    def define(self, *args):
        return self.rule.define(*args)

    @property
    def children(self):
        yield self.rule


class ExtendedRule(WrapperRule):
    __attributes__ = ['rule', 'reverse']

    def __init__(self, rule, reverse):
        WrapperRule.__init__(self, rule)
        assert_type(reverse, bool)
        self.reverse = reverse


class OptionalRule(ExtendedRule):
    pass


class RepeatableRule(ExtendedRule):
    pass


class RepeatableOptionalRule(RepeatableRule, OptionalRule):
    __attributes__ = ['rule', 'reverse_repeatable', 'reverse_optional']

    def __init__(self, rule, reverse_repeatable, reverse_optional):
        WrapperRule.__init__(self, rule)
        assert_type(reverse_repeatable, bool)
        assert_type(reverse_optional, bool)
        self.reverse_repeatable = reverse_repeatable
        self.reverse_optional = reverse_optional


class BoundedRule(ExtendedRule):
    pass


class MinBoundedRule(BoundedRule):
    __attributes__ = ['rule', 'min', 'reverse']

    def __init__(self, rule, min, reverse):
        BoundedRule.__init__(self, rule, reverse)
        assert_greater_equals(min, 1)
        self.min = min


class MaxBoundedRule(BoundedRule):
    __attributes__ = ['rule', 'max', 'reverse']

    def __init__(self, rule, max, reverse):
        BoundedRule.__init__(self, rule, reverse)
        assert_greater_equals(max, 1)
        self.max = max


class MinMaxBoundedRule(MinBoundedRule, MaxBoundedRule):
    __attributes__ = ['rule', 'min', 'max', 'reverse']

    def __init__(self, rule, min, max, reverse):
        MinBoundedRule.__init__(self, rule, min, reverse)
        MaxBoundedRule.__init__(self, rule, max, reverse)
        assert_greater_equals(max, min)


class NamedRule(WrapperRule):
    __attributes__ = ['rule', 'name']

    def __init__(self, rule, name):
        super(NamedRule, self).__init__(rule)
        assert_type(name, str)
        self.name = name


class InterpretationRule(WrapperRule):
    __attributes__ = ['rule', 'interpretator']

    def __init__(self, rule, interpretator):
        from yargy.interpretation import Interpretator

        super(InterpretationRule, self).__init__(rule)
        assert_type(interpretator, Interpretator)
        self.interpretator = interpretator


class RelationRule(WrapperRule):
    __attributes__ = ['rule', 'relation']

    def __init__(self, rule, relation):
        from yargy.relations import Relation

        super(RelationRule, self).__init__(rule)
        assert_type(relation, Relation)
        self.relation = relation


class ForwardRule(Rule):
    __attributes__ = ['rule']

    def __init__(self):
        self.rule = None

    def define(self, item, *items):
        from yargy import rule

        if not items and is_rule(item):
            if isinstance(item, ForwardRule):
                raise ValueError('forward(forward(...)) not allowed')
            self.rule = item
        else:
            self.rule = rule(item, *items)
        return self

    @property
    def children(self):
        if self.rule:
            yield self.rule

    def __eq__(self, other):
        return id(self) == id(other)

    def __repr__(self):
        if self.rule:
            # sorry, need to prevent recursion
            return 'ForwardRule(...)'
        else:
            return 'ForwardRule()'


def is_forward_rule(item):
    return isinstance(item, ForwardRule)


class EmptyRule(Rule):
    __attributes__ = []

    children = []

    def __init__(self):
        pass


class PipelineRule(Rule):
    __attributes__ = ['pipeline']

    children = []

    def __init__(self, pipeline):
        self.pipeline = pipeline
