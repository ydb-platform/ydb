
from yargy.visitor import Visitor
from yargy.dot import (
    style,
    DotTransformator,
    BLUE,
    ORANGE,
    RED,
    PURPLE,
    GREEN,
    DARKGRAY
)

from yargy.predicates import is_predicate
from .constructors import (
    is_rule,
    Production,
    EmptyProduction,
    Rule,
    OrRule,
    OptionalRule,
    RepeatableRule,
    BoundedRule,
    MinBoundedRule,
    MaxBoundedRule,
    MinMaxBoundedRule,
    RepeatableOptionalRule,
    NamedRule,
    InterpretationRule,
    RelationRule,
    ForwardRule,
)


class InplaceRuleTransformator(Visitor):
    def __call__(self, root):
        for item in root.walk(types=(Rule, Production)):
            self.visit(item)
        return self.visit(root)

    def visit_term(self, item):
        return item

    def visit_Production(self, item):
        item.terms = [self.visit_term(_) for _ in item.terms]
        return item

    def visit_EmptyProduction(self, item):
        return item

    def visit_PipelineProduction(self, item):
        return item

    def visit_Rule(self, item):
        return item


class RuleTransformator(Visitor):
    def __init__(self):
        self.visited = {}

    def __call__(self, root):
        for item in root.walk(types=ForwardRule):
            if item.rule:
                item.define(self.visit(item.rule))
        return self.visit(root)

    def visit(self, item):
        item_id = id(item)
        if item_id in self.visited:
            return self.visited[item_id]
        else:
            item = self.resolve_method(item)(item)
            self.visited[item_id] = item
            return item

    def visit_term(self, item):
        if is_rule(item):
            return self.visit(item)
        else:
            return item

    def visit_Production(self, item):
        return Production(
            [self.visit_term(_) for _ in item.terms],
            item.main
        )

    def visit_EmptyProduction(self, item):
        return item

    def visit_PipelineProduction(self, item):
        return item

    def visit_Rule(self, item):
        return Rule([self.visit(_) for _ in item.productions])

    def visit_OrRule(self, item):
        return OrRule([self.visit(_) for _ in item.rules])

    def visit_OptionalRule(self, item):
        return OptionalRule(self.visit(item.rule, item.reverse))

    def visit_RepeatableRule(self, item):
        return RepeatableRule(self.visit(item.rule, item.reverse))

    def visit_RepeatableOptionalRule(self, item):
        return RepeatableOptionalRule(self.visit(
            item.rule,
            item.reverse_repeatable,
            item.reverse_optional
        ))

    def visit_MinBoundedRule(self, item):
        return MinBoundedRule(self.visit(item.rule), item.min, item.reverse)

    def visit_MaxBoundedRule(self, item):
        return MaxBoundedRule(self.visit(item.rule), item.max, item.reverse)

    def visit_MinMaxBoundedRule(self, item):
        return MinMaxBoundedRule(
            self.visit(item.rule),
            item.min, item.max, item.reverse
        )

    def visit_NamedRule(self, item):
        return NamedRule(self.visit(item.rule), item.name)

    def visit_InterpretationRule(self, item):
        return InterpretationRule(self.visit(item.rule), item.interpretator)

    def visit_RelationRule(self, item):
        return RelationRule(self.visit(item.rule), item.relation)

    def visit_ForwardRule(self, item):
        return item

    def visit_EmptyRule(self, item):
        return item

    def visit_PipelineRule(self, item):
        return item


class ActivateTransformator(InplaceRuleTransformator):
    def __init__(self, context):
        super(ActivateTransformator, self).__init__()
        self.context = context

    def visit_term(self, item):
        if is_predicate(item):
            return item.activate(self.context)
        else:
            return item

    def visit_PipelineRule(self, item):
        item.pipeline = item.pipeline.activate(self.context)
        return item


class SquashExtendedTransformator(RuleTransformator):
    def visit_RepeatableRule(self, item):
        child = item.rule
        if isinstance(child, OptionalRule):
            return self.visit(RepeatableOptionalRule(
                child.rule,
                item.reverse,
                child.reverse
            ))
        elif isinstance(child, RepeatableOptionalRule):
            return self.visit(RepeatableOptionalRule(
                child.rule,
                item.reverse,
                child.reverse_optional
            ))

        elif isinstance(child, (RepeatableRule, BoundedRule)):
            return self.visit(RepeatableRule(child.rule, item.reverse))
        else:
            return RepeatableRule(self.visit(child), item.reverse)

    def visit_OptionalRule(self, item):
        child = item.rule
        if isinstance(child, RepeatableRule):
            return self.visit(RepeatableOptionalRule(
                child.rule,
                child.reverse,
                item.reverse
            ))
        elif isinstance(child, RepeatableOptionalRule):
            return self.visit(RepeatableOptionalRule(
                child.rule,
                child.reverse_repeatable,
                item.reverse
            ))
        elif isinstance(child, OptionalRule):
            return self.visit(OptionalRule(child.rule, item.reverse))
        else:
            return OptionalRule(self.visit(child), item.reverse)

    def visit_RepeatableOptionalRule(self, item):
        child = item.rule
        if isinstance(child, (RepeatableRule, BoundedRule,
                              OptionalRule, RepeatableOptionalRule)):
            return self.visit(RepeatableOptionalRule(
                child.rule,
                item.reverse_repeatable,
                item.reverse_optional
            ))
        else:
            return RepeatableOptionalRule(
                self.visit(child),
                item.reverse_repeatable,
                item.reverse_optional
            )

    def visit_BoundedRule(self, item):
        child = item.rule
        if isinstance(child, RepeatableRule):
            return self.visit(RepeatableRule(child.rule, child.reverse))
        elif isinstance(child, RepeatableOptionalRule):
            return self.visit(RepeatableOptionalRule(
                child.rule,
                child.reverse_repeatable,
                child.reverse_optional
            ))
        raise TypeError

    def visit_MinBoundedRule(self, item):
        child = item.rule
        if isinstance(child, (RepeatableRule, RepeatableOptionalRule)):
            return self.visit_BoundedRule(item)
        elif isinstance(child, OptionalRule):
            return self.visit(OptionalRule(
                MinBoundedRule(
                    child.rule, item.min, item.reverse
                ),
                child.reverse
            ))
        else:
            return MinBoundedRule(self.visit(child), item.min, item.reverse)

    def visit_MaxBoundedRule(self, item):
        child = item.rule
        if isinstance(child, (RepeatableRule, RepeatableOptionalRule)):
            return self.visit_BoundedRule(item)
        elif isinstance(child, OptionalRule):
            return self.visit(OptionalRule(
                MaxBoundedRule(
                    child.rule, item.max, item.reverse
                ),
                child.reverse
            ))
        else:
            return MaxBoundedRule(self.visit(child), item.max, item.reverse)

    def visit_MinMaxBoundedRule(self, item):
        child = item.rule
        if isinstance(child, (RepeatableRule, RepeatableOptionalRule)):
            return self.visit_BoundedRule(item)
        elif isinstance(child, OptionalRule):
            return self.visit(OptionalRule(
                MinMaxBoundedRule(
                    child.rule, item.min, item.max, item.reverse
                ),
                child.reverse
            ))
        else:
            return MinMaxBoundedRule(
                self.visit(child),
                item.min, item.max, item.reverse
            )


class FlattenTransformator(RuleTransformator):
    def visit_term(self, item):
        if type(item) is Rule:
            productions = item.productions
            if len(productions) == 1:
                terms = productions[0].terms
                if len(terms) == 1:
                    term = terms[0]
                    return self.visit_term(term)
        return super(FlattenTransformator, self).visit_term(item)

    def visit_Production(self, item):
        terms = item.terms
        if len(terms) == 1:
            term = terms[0]
            if type(term) is Rule:
                productions = term.productions
                if len(productions) == 1:
                    production = productions[0]
                    return self.visit(production)
        return super(FlattenTransformator, self).visit_Production(item)


class ReplaceOrTransformator(RuleTransformator):
    def visit_OrRule(self, item):
        return Rule([Production([self.visit(_)]) for _ in item.rules])


class ReplaceEmptyTransformator(RuleTransformator):
    def visit_EmptyRule(self, item):
        return Rule([EmptyProduction()])


def max_bound(item, count, reverse=False):
    from yargy.api import rule, or_

    if count == 1:
        return item
    else:
        a = rule(
            item,
            max_bound(item, count - 1, reverse)
        )
        b = item
        if reverse:
            a, b = b, a
        return or_(a, b)


def repeatable(item, reverse=False):
    from yargy.api import forward, or_, rule

    temp = forward()
    a = rule(item, temp)
    b = item
    if reverse:
        a, b = b, a
    return temp.define(
        or_(
            a,
            b
        )
    )


def optional(item, reverse=False):
    from yargy.api import or_, empty

    a = empty()
    b = item
    if reverse:
        a, b = b, a
    return or_(a, b)


def repeatable_optional(item, reverse_repeatable=False, reverse_optional=False):
    from yargy.api import forward, or_, rule, empty

    temp = forward()
    a = empty()
    b = rule(item, temp)
    c = item
    if reverse_repeatable:
        b, c = c, b
    if reverse_optional:
        a, b, c = b, c, a
    return temp.define(
        or_(
            a,
            b,
            c
        )
    )


def repeat(item, count):
    return [item for _ in range(count)]


class ReplaceExtendedTransformator(RuleTransformator):
    def visit_RepeatableRule(self, item):
        child = self.visit(item.rule)
        return repeatable(child, item.reverse)

    def visit_OptionalRule(self, item):
        child = self.visit(item.rule)
        return optional(child, item.reverse)

    def visit_RepeatableOptionalRule(self, item):
        child = self.visit(item.rule)
        return repeatable_optional(
            child,
            item.reverse_repeatable,
            item.reverse_optional
        )

    def visit_MinBoundedRule(self, item):
        child = self.visit(item.rule)
        items = repeat(child, item.min - 1)
        items.append(repeatable(child, item.reverse))

        from yargy.api import rule
        return rule(*items)

    def visit_MaxBoundedRule(self, item):
        child = self.visit(item.rule)
        return max_bound(child, item.max, item.reverse)

    def visit_MinMaxBoundedRule(self, item):
        rule, min, max, reverse = item
        child = self.visit(rule)
        items = repeat(child, min - 1)
        items.append(max_bound(child, max - min + 1, reverse))

        from yargy.api import rule
        return rule(*items)


class DotRuleTransformator(DotTransformator, InplaceRuleTransformator):
    def visit_Predicate(self, item):
        self.style(
            item,
            style(label=item.label)
        )

    def visit_Production(self, item):
        self.graph.add_node(
            item,
            style(label='Production', fillcolor=BLUE)
        )
        for index, child in enumerate(item.children):
            styling = (
                style(color=DARKGRAY)
                if item.main > 0 and item.main == index
                else None
            )
            self.graph.add_edge(
                item, child,
                style=styling
            )

    def visit_EmptyProduction(self, item):
        self.style(
            item,
            style(label='EmptyProduction')
        )

    def visit_PipelineProduction(self, item):
        self.style(
            item,
            style(label='PipelineProduction', fillcolor=BLUE)
        )

    def visit_Rule(self, item):
        self.style(
            item,
            style(label='Rule', fillcolor=BLUE)
        )

    def visit_OrRule(self, item):
        self.style(
            item,
            style(label='Or', fillcolor=BLUE)
        )

    def visit_OptionalRule(self, item):
        self.style(
            item,
            style(label='Optional', fillcolor=ORANGE)
        )

    def visit_RepeatableRule(self, item):
        self.style(
            item,
            style(label='Repeatable', fillcolor=ORANGE)
        )

    def visit_RepeatableOptionalRule(self, item):
        self.style(
            item,
            style(label='RepeatableOptional', fillcolor=ORANGE)
        )

    def visit_MinBoundedRule(self, item):
        self.style(
            item,
            style(label='MinBounded >=%d' % item.min, fillcolor=ORANGE)
        )

    def visit_MaxBoundedRule(self, item):
        self.style(
            item,
            style(label='MaxBounded <=%d' % item.max, fillcolor=ORANGE)
        )

    def visit_MinMaxBoundedRule(self, item):
        self.style(
            item,
            style(
                label='MinMaxBounded [{item.min}, {item.max}]'.format(item=item),
                fillcolor=ORANGE
            )
        )

    def visit_NamedRule(self, item):
        self.style(
            item,
            style(label=item.name, fillcolor=RED)
        )

    def visit_InterpretationRule(self, item):
        self.style(
            item,
            style(label=item.interpretator.label, fillcolor=GREEN)
        )

    def visit_RelationRule(self, item):
        self.style(
            item,
            style(label=item.relation.label, fillcolor=PURPLE)
        )

    def visit_ForwardRule(self, item):
        self.style(
            item,
            style(label='Forward', fillcolor=PURPLE)
        )

    def visit_EmptyRule(self, item):
        self.style(
            item,
            style(label='Empty')
        )

    def visit_PipelineRule(self, item):
        self.style(
            item,
            style(label=item.pipeline.label, fillcolor=PURPLE)
        )

    def visit_BNFRule(self, item):
        self.style(
            item,
            style(label=item.label, fillcolor=GREEN)
        )
