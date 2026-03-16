
from yargy.record import Record
from yargy.visitor import TransformatorsComposition


class Tree(Record):
    __attributes__ = ['root', 'range']

    def __init__(self, root, range):
        self.root = root
        self.range = range

    def walk(self, types=None):
        items = dfs_tree(self.root)
        if types:
            items = (_ for _ in items if isinstance(_, types))
        return items

    def transform(self, *transformators):
        return TransformatorsComposition(transformators)(self)

    @property
    def normalized(self):
        from .transformators import PropogateEmptyTransformator
        return self.transform(PropogateEmptyTransformator)

    @property
    def relations(self):
        from .transformators import RelationsTransformator
        return self.transform(RelationsTransformator)

    def constrain(self, relations):
        from .transformators import ApplyRelationsTransformator
        transform = ApplyRelationsTransformator(relations)
        return transform(self)

    def interpret(self):
        from .transformators import (
            KeepInterpretationNodesTransformator,
            InterpretationTransformator
        )
        return self.transform(
            KeepInterpretationNodesTransformator,
            InterpretationTransformator
        )

    @property
    def as_dot(self):
        from .transformators import DotTreeTransformator
        return self.transform(DotTreeTransformator)

    def __lt__(self, other):
        if self.range == other.range:
            return self.root < other.root

        start, stop = self.range
        other_start, other_stop = other.range

        if start == other_start:
            return stop > other_stop

        return start < other_start


def bfs_tree(root):
    queue = [root]
    while queue:
        item = queue.pop(0)
        yield item
        queue.extend(item.children)


def dfs_tree(root):
    queue = [root]
    while queue:
        item = queue.pop()
        yield item
        queue.extend(reversed(item.children))


class Node(Record):
    __attributes__ = ['rule', 'production', 'rank', 'children']

    def __init__(self, rule, production, rank, children):
        self.rule = rule
        self.production = production
        self.rank = rank
        self.children = children

    def attached(self, node):
        return Node(
            self.rule,
            self.production,
            self.rank,
            self.children + [node]
        )

    @property
    def main(self):
        return self.children[self.production.main].main

    @property
    def interpretator(self):
        return self.rule.interpretator

    @property
    def relation(self):
        return self.rule.relation

    @property
    def label(self):
        return self.rule.label

    def __lt__(self, other):
        if id(self.rule) != id(other.rule):
            raise TypeError('Node() < Node() with different rules')

        if id(self) == id(other):
            return False

        if self.rank == other.rank:
            for a, b in zip(self.children, other.children):
                if is_leaf(a):  # b is also leaf
                    continue
                if id(a) == id(b):
                    continue
                if a.rank < b.rank:
                    return True
                elif a.rank > b.rank:
                    return False
                elif a.rank == b.rank:
                    return a < b
            return False
        return self.rank < other.rank


class Leaf(Node):
    __attributes__ = ['predicate', 'token']

    children = []
    interpretator = None
    relation = None

    def __init__(self, predicate, token):
        self.predicate = predicate
        self.token = token

    @property
    def main(self):
        return self.token

    @property
    def label(self):
        return self.token.value


def is_leaf(item):
    return isinstance(item, Leaf)
