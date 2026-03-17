
from yargy.visitor import Visitor
from yargy.dot import (
    style,
    DotTransformator,
    BLUE,
    GREEN
)

from yargy.pipelines import PipelineProduction
from yargy.interpretation.interpretator import InterpretatorInput

from .constructors import (
    Tree,
    Node,
    Leaf
)


class InplaceTreeTransformator(Visitor):
    def __call__(self, tree):
        for item in tree.walk():
            self.visit(item)
        return tree

    def visit_Node(self, item):
        return item


class TreeTransformator(Visitor):
    def __call__(self, tree):
        root = self.visit(tree.root)
        return Tree(root, tree.range)

    def visit_Node(self, item):
        return Node(
            item.rule,
            item.production,
            item.rank,
            [self.visit(_) for _ in item.children]
        )

    def visit_Leaf(self, item):
        return item


class PropogateEmptyTransformator(TreeTransformator):
    def visit_children(self, item):
        return list(filter(
            None,
            [self.visit(_) for _ in item.children]
        ))

    def visit_Node(self, item):
        children = self.visit_children(item)
        if children:
            return Node(
                item.rule,
                item.production,
                item.rank,
                children
            )


class KeepInterpretationNodesTransformator(TreeTransformator):
    def __call__(self, tree):
        if not tree.root.interpretator:
            raise ValueError('no .interpretation(...) for root rule')
        return super(KeepInterpretationNodesTransformator, self).__call__(tree)

    def flatten(self, item):
        for child in item.children:
            if isinstance(child, Leaf) or child.interpretator:
                yield child
            else:
                for item in self.flatten(child):
                    yield item

    def visit_Node(self, item):
        if item.interpretator:
            children = [self.visit(_) for _ in self.flatten(item)]
            return Node(
                item.rule,
                item.production,
                item.rank,
                children
            )
        else:
            super(KeepInterpretationNodesTransformator, self).visit_Node(item)


class InterpretationTransformator(TreeTransformator):
    def __call__(self, tree):
        return self.visit(tree.root)

    def visit_Node(self, item):
        input = InterpretatorInput(self.visit(_) for _ in item.children)
        if isinstance(item.production, PipelineProduction):
            input.key = item.production.value
        return item.interpretator(input)

    def visit_Leaf(self, item):
        return item.token


class RelationsTransformator(InplaceTreeTransformator):
    def __init__(self):
        from yargy.relations.graph import TokenRelationsGraph
        self.relations = TokenRelationsGraph()

    def __call__(self, tree):
        for item in tree.walk():
            self.visit(item)
        return self.relations

    def visit_Node(self, item):
        if item.relation:
            self.relations.add(item.relation, item.main)


class ApplyRelationsTransformator(InplaceTreeTransformator):
    def __init__(self, relations):
        self.relations = relations

    def visit_Leaf(self, item):
        item.token = self.relations.constrain(item.token)


class DotTreeTransformator(DotTransformator, InplaceTreeTransformator):
    def __init__(self):
        DotTransformator.__init__(self)
        TreeTransformator.__init__(self)

        from yargy.relations.graph import RelationsGraph
        self.relations = RelationsGraph()

    def __call__(self, root):
        graph = super(DotTreeTransformator, self).__call__(root)
        for edge in self.relations.edges:
            graph.add_edge(
                edge.first,
                edge.second,
                style=style(
                    label=edge.relation.label,
                    dir='none',
                    style='dashed'
                )
            )
        return graph

    def visit_Node(self, item):
        color = (
            GREEN
            if item.interpretator
            else BLUE
        )
        self.style(
            item,
            style(label=item.label, fillcolor=color)
        )
        if item.relation:
            self.relations.add(item.relation, item)

    def visit_Leaf(self, item):
        self.style(
            item,
            style(label=item.label)
        )
