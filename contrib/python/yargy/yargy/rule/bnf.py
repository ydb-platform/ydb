
from yargy.record import Record

from .transformators import (
    RuleTransformator,
    InplaceRuleTransformator
)
from .constructors import (
    Production,
    Rule,
    is_forward_rule
)


def generate_names(rules):
    count = 0
    for rule in rules:
        if not rule.name:
            rule.name = 'R%d' % count
            count += 1
        yield rule


class BNF(Record):
    __attributes__ = ['rules']

    def __init__(self, rules):
        self.rules = list(generate_names(rules))

    @property
    def start(self):
        return self.rules[0]

    @property
    def source(self):
        for rule in self.rules:
            yield str(rule)

    def _repr_pretty_(self, printer, cycle):
        for line in self.source:
            printer.text(line)
            printer.break_()


class BNFRule(Rule):
    __attributes__ = ['productions', 'name', 'interpretator', 'relation']

    def __init__(self, productions, name=None, interpretator=None, relation=None):
        self.productions = productions
        self.name = name
        self.interpretator = interpretator
        self.relation = relation

    @property
    def as_bnf(self):
        return BNF(self.walk(types=BNFRule))

    def predict(self, _):
        return self.productions

    @property
    def label(self):
        name = self.name
        if self.interpretator:
            name = self.interpretator.label
        if self.relation:
            name = '{name}^{relation}'.format(
                name=name,
                relation=self.relation.label
            )
        return name

    def __str__(self):
        productions = ' | '.join(str(_) for _ in self.productions)
        return '{name} -> {productions}'.format(
            name=self.label,
            productions=productions
        )


def is_rule(item):
    return isinstance(item, BNFRule)


def lift(item):
    return BNFRule([Production([item])])


class BNFTransformator(RuleTransformator):
    def __init__(self):
        super(BNFTransformator, self).__init__()
        self.parents = {}

    def __call__(self, root):
        for item in root.walk():
            for child in item.children:
                child_id = id(child)
                count = self.parents.get(child_id, 0)
                self.parents[child_id] = count + 1
        return super(BNFTransformator, self).__call__(root)

    def is_shared(self, item):
        return self.parents[id(item)] > 1

    def raise_TypeError(self, item):
        raise TypeError(type(item))

    visit_OrRule = raise_TypeError
    visit_ExtendedRule = raise_TypeError
    visit_EmptyRule = raise_TypeError

    def visit_Rule(self, item):
        return BNFRule([self.visit(_) for _ in item.productions])

    def visit_WrapperRule(self, item):
        item = item.rule
        if is_forward_rule(item):
            return lift(item)
        shared = self.is_shared(item)
        item = self.visit(item)
        if shared:
            item = lift(item)
        return item

    def visit_NamedRule(self, item):
        name = item.name
        item = self.visit_WrapperRule(item)
        if item.name:
            item = lift(item)
        item.name = name
        return item

    def visit_InterpretationRule(self, item):
        interpretator = item.interpretator
        item = self.visit_WrapperRule(item)
        if item.interpretator:
            item = lift(item)
        item.interpretator = interpretator
        return item

    def visit_RelationRule(self, item):
        relation = item.relation
        item = self.visit_WrapperRule(item)
        if item.relation:
            item = lift(item)
        item.relation = relation
        return item

    def visit_PipelineRule(self, item):
        return item.pipeline.as_bnf


class RemoveForwardTransformator(InplaceRuleTransformator):
    def visit_term(self, item):
        if is_forward_rule(item):
            return self.visit(item)
        else:
            return item

    def visit_BNFRule(self, item):
        item.productions = [self.visit_term(_) for _ in item.productions]
        return item

    def visit_ForwardRule(self, item):
        if not item.rule:
            raise TypeError('forward not defined')
        return item.rule

    def visit_PipelineBNFRule(self, item):
        return item
