
from .check import assert_type
from .predicates import (
    eq,
    is_predicate,
    Predicate,
    AndPredicate,
    OrPredicate,
    NotPredicate,
)
from .relations import (
    is_relation,
    Main,
    Relation,
    AndRelation,
    OrRelation,
    NotRelation
)
from .rule import (
    is_rule,
    Production,
    Rule,
    OrRule,
    EmptyRule,
    ForwardRule,
)


__all__ = [
    'rule',
    'empty',
    'forward',

    'and_',
    'or_',
    'not_',
]


def prepare_production_item(item):
    if not isinstance(item, (Predicate, Rule, Main)):
        return eq(item)
    else:
        return item


def rule(*items):
    production = Production([prepare_production_item(_) for _ in items])
    return Rule([production])


empty = EmptyRule
forward = ForwardRule


def and_(*items):
    if all(is_predicate(_) for _ in items):
        return AndPredicate(items)
    elif all(is_relation(_) for _ in items):
        return AndRelation(items)
    else:
        types = [type(_) for _ in items]
        raise TypeError('mixed types: %r' % types)


def or_(*items):
    if all(is_predicate(_) for _ in items):
        return OrPredicate(items)
    elif all(is_relation(_) for _ in items):
        return OrRelation(items)
    elif all(is_rule(_) for _ in items):
        return OrRule(items)
    else:
        types = [type(_) for _ in items]
        raise TypeError('mixed types: %r' % types)


def not_(item):
    assert_type(item, (Predicate, Relation))
    if is_predicate(item):
        return NotPredicate(item)
    elif is_relation(item):
        return NotRelation(item)
