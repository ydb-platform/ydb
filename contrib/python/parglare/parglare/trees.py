from functools import reduce

from parglare.common import dot_escape
from parglare.exceptions import LoopError

DOT_HEADER = """
    digraph grammar {
    rankdir=TD
    fontname = "Bitstream Vera Sans"
    fontsize = 8
    nodesep = 0.2
    edge[dir=black,arrowtail=empty, fontsize=6 arrowsize=.5 penwidth=0.7]
    node[shape=plain height=0.1 width=0.1]

"""


def tree_node_iterator(n):
    """
    Iterator for forests and trees nodes. Used in visitors.
    """
    from parglare.glr import Parent

    if isinstance(n, Parent):
        return iter(n.possibilities)
    elif n.is_term():
        return iter([])
    else:

        def _iter():
            for i in n.children:
                if isinstance(i, Parent) and len(i.possibilities) == 1:
                    yield i.possibilities[0]
                else:
                    yield i

        return _iter()


def to_str(root):
    from parglare.glr import Parent

    def visit(n, subresults, depth):
        indent = "  " * depth
        if isinstance(n, Parent):
            s = f"{indent}{n.head.symbol} - ambiguity[{n.ambiguity}]"
            for idx, p in enumerate(subresults):
                s += f"\n{indent}{idx + 1}:{p}"
        elif n.is_nonterm():
            s = f"{indent}{n.production.symbol}[{n.start_position}->{n.end_position}]"
            if subresults:
                s = "{}\n{}".format(s, "\n".join(subresults))
        else:
            s = f'{indent}{n.symbol}[{n.start_position}->{n.end_position}, "{n.value}"]'
        return s

    return visitor(root, tree_node_iterator, visit)


def to_dot(self, positions=True):
    from parglare.glr import Parent

    rendered = set()
    terminals = []

    def visit(n, subresults, _):
        sub_str = "".join(s[1] for s in subresults if id(s[1]) not in rendered)
        rendered.update(id(s[1]) for s in subresults)
        pos = f"[{n.start_position}-{n.end_position}]" if positions else ""
        if isinstance(n, Parent):
            s = '{}[label="Amb({},{})" shape=box];\n'.format(
                id(n), dot_escape(f"{n.head.symbol}{pos}"), n.ambiguity
            )
            s += sub_str
            s += "".join(f"{id(n)}->{id(s[0])};\n" for s in subresults)
        elif n.is_nonterm():
            s = '{}[label="{}"];\n'.format(id(n), dot_escape(f"{n.symbol}{pos}"))
            s += sub_str
            s += "".join(
                (
                    f'{id(n)}->{id(s[0])}[label="{idx + 1}"];\n'
                    for idx, s in enumerate(subresults)
                )
            )
        else:
            terminals.append(n)
            label = (
                f"{n.symbol}({n.value[:10]})"
                if n.symbol.name != n.value
                else n.symbol.name
            )
            s = '{} [label="{}"];\n'.format(id(n), dot_escape(f"{label}{pos}"))
        return (n, s)

    return "{}\n{}\n{}\n}}\n".format(
        DOT_HEADER,
        visitor(self, tree_node_iterator, visit)[1],
        "{{rank=same {} [style=invis]}}".format("->".join(str(id(t)) for t in terminals)),
    )


class Node:
    """A node of the parse tree."""

    __slots__ = ["context"]

    def __init__(self, context):
        self.context = context

    def __repr__(self):
        return str(self)

    def __iter__(self):
        return iter([])

    def __reversed__(self):
        return iter([])

    def __getattr__(self, name):
        return getattr(self.context, name)

    def is_nonterm(self):
        return False

    def is_term(self):
        return False

    def to_str(self):
        return to_str(self)

    def to_dot(self, positions=True):
        return to_dot(self, positions)


class NodeNonTerm(Node):
    __slots__ = ["production", "children"]

    def __init__(self, context, children, production=None):
        super().__init__(context)
        self.children = children
        self.production = production

    @property
    def solutions(self):
        "For SPPF trees"
        return reduce(lambda x, y: x * y, (c.solutions for c in self.children), 1)

    @property
    def symbol(self):
        return self.production.symbol

    def is_nonterm(self):
        return True

    def __str__(self):
        return (
            f"NonTerm({self.production.symbol}, "
            f"{self.start_position}-{self.end_position})"
        )

    def __iter__(self):
        return iter(self.children)

    def __reversed__(self):
        return reversed(self.children)


class NodeTerm(Node):
    def __init__(self, context, token=None):
        super().__init__(context)
        self.token = token

    @property
    def symbol(self):
        return self.token.symbol

    @property
    def value(self):
        return self.token.value

    @property
    def additional_data(self):
        return self.token.additional_data

    @property
    def solutions(self):
        "For SPPF trees"
        return 1

    def is_term(self):
        return True

    def __str__(self):
        return (
            f'Term({self.symbol} "{self.value[:20]}", '
            f"{self.start_position}-{self.end_position})"
        )


class Tree:
    """
    Represents a tree from the parse forest.
    """

    __slots__ = ["root", "children"]

    def __init__(self, root, counter):
        possibility = 0
        if counter > 0 and len(root.possibilities) > 1:
            # Find the right possibility bucket
            solutions = root.possibilities[possibility].solutions
            while solutions <= counter:
                counter -= solutions
                possibility += 1
                solutions = root.possibilities[possibility].solutions

        self.root = root.possibilities[possibility]
        self._init_children(counter)

    def _init_children(self, counter):
        if self.root.is_nonterm():
            self.children = self._enumerate_children(counter)
        else:
            self.children = None

    def _enumerate_children(self, counter):
        children = []
        # Calculate counter division based on weighted numbering system.
        # Basically, enumerating variations of children solutions.
        weights = [c.solutions for c in self.root.children]
        for idx, c in enumerate(self.root.children):
            factor = reduce(lambda x, y: x * y, weights[idx + 1 :], 1)
            new_counter = counter // factor
            counter %= factor
            children.append(self.__class__(c, new_counter))
        return children

    def to_str(self):
        return to_str(self)

    def to_dot(self, positions=True):
        return to_dot(self, positions)

    def __iter__(self):
        return iter(self.children or [])

    def __reversed__(self):
        return reversed(self.children or [])

    def __getitem__(self, idx):
        return self.children[idx]

    def __getattr__(self, attr):
        # Proxy to tree node
        return getattr(self.root, attr)


class LazyTree(Tree):
    """
    Represents a lazy tree from the parse forest.

    Attributes:
    root(Parent):
    counter(int):
    """

    __slots__ = ["root", "counter", "_children"]

    def __init__(self, root, counter):
        self._children = None
        super().__init__(root, counter)

    def _init_children(self, counter):
        self.counter = counter

    def __getattr__(self, attr):
        if attr == "children":
            if self._children is None and self.root.is_nonterm():
                self._children = self._enumerate_children(self.counter)
            return self._children
        # Proxy to tree node
        return getattr(self.root, attr)


class Forest:
    """
    Shared packed forest returned by the GLR parser.
    Creates lazy tree enumerators and enables iteration over trees.
    """

    def __init__(self, parser):
        self.parser = parser
        results = [p for r in parser._accepted_heads for p in r.parents.values()]
        self.result = results.pop()
        while results:
            result = results.pop()
            self.result.merge(result)

    def get_tree(self, idx=0):
        return LazyTree(self.result, idx)

    def get_nonlazy_tree(self, idx=0):
        return Tree(self.result, idx)

    def get_first_tree(self):
        """
        Gets tree 0 fully unpacked. May be used for optimization purposes where
        it doesn't matter which tree we get. The unpacked tree is faster to iterate.
        """
        from parglare.glr import Parent

        def tree_iterator(n):
            if isinstance(n, Parent):
                return iter([n.possibilities[0]])
            elif n.is_nonterm():
                return iter(n.children)
            else:
                return iter([])

        def visit(n, subresults, _):
            if isinstance(n, Parent):
                return subresults[0]
            elif n.is_nonterm():
                # Clone NodeNonTerm to preserve the forest
                return NodeNonTerm(n.context, subresults, n.production)
            else:
                return n

        return visitor(self.result.possibilities[0], tree_iterator, visit)

    @property
    def solutions(self):
        return self.result.solutions

    @property
    def ambiguities(self):
        "Number of ambiguous nodes in this forest."
        return self.result.ambiguities

    def disambiguate(self, disamfun):
        """
        Visit all Parent nodes with len(possibilities) > 1 with a given
        `disamfun` which accepts the Parent and should modify it to remove
        all invalid possibilities.
        """
        from parglare.glr import Parent

        def tree_iterator(n):
            return iter(n)

        def visit(n, _, __):
            if isinstance(n, Parent) and len(n.possibilities) > 1:
                disamfun(n)

        self.result._solutions = None
        return visitor(self.result, tree_iterator, visit)

    def __str__(self):
        return f"Forest({self.solutions})"

    def to_str(self):
        return self.result.to_str()

    def to_dot(self, positions=True):
        return self.result.to_dot(positions)

    def __len__(self):
        return self.solutions

    def __iter__(self):
        for i in range(self.solutions):
            yield self.get_tree(i)

    def __getitem__(self, idx):
        return self.get_tree(idx)

    def nonlazy_iter(self):
        for i in range(self.solutions):
            yield self.get_nonlazy_tree(i)


def visitor(root, iterator, visit, memoize=True, check_cycle=False):
    """Generic iterative depth-first visitor with memoization.

    Accepts the start of the structure to visit (root), iterator callable which
    gets called to get the next elements to visit and `visit` function which
    is called with the element and sub-results of the iterated child elements.
    Should return the result for the given node.

    Memoize parameter uses cache to store the results of already visited elements.

    """
    if memoize:
        cache = {}
    stack = [(root, iterator(root), [])]
    if check_cycle:
        visiting = set([id(root)])
    while stack:
        node, it, results = stack[-1]
        try:
            next_elem = next(it)
        except StopIteration:
            # No more sub-elements for this node
            stack.pop()
            if check_cycle:
                visiting.remove(id(node))
            result = visit(node, results, len(stack))
            if memoize:
                # Store node to preserve the reference to it.
                # Otherwise node may be freed by garbage collector.
                cache[id(node)] = result, node
            if stack:
                stack[-1][-1].append(result)
            continue
        if check_cycle and id(next_elem) in visiting:
            raise LoopError(
                f'Looping during traversal on "{next_elem}". '
                f"Last elements: {[r[0] for r in stack[-10:]]}"
            )
        if memoize and id(next_elem) in cache:
            results.append(cache[id(next_elem)][0])
        else:
            stack.append((next_elem, iterator(next_elem), []))
            if check_cycle:
                visiting.add(id(next_elem))

    return result
