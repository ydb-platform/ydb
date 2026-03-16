import codecs
import itertools
import re

from anytree import PreOrderIter

_RE_ESC = re.compile(r'["\\]')


class MermaidExporter:
    """
    Mermaid Exporter.

    Args:
        node (Node): start node.

    Keyword Args:
        graph: Mermaid graph type.

        name: Mermaid graph name.

        options: list of options added to the graph.

        indent (int): number of spaces for indent.

        nodenamefunc: Function to extract node name from `node` object.
                      The function shall accept one `node` object as
                      argument and return the name of it.
                      Returns a unique identifier by default.

        nodefunc: Function to decorate a node with attributes.
                      The function shall accept one `node` object as
                      argument and return the attributes.
                      Returns ``[{node.name}]`` and creates therefore a
                      rectangular node by default.

        edgefunc: Function to decorate a edge with attributes.
                  The function shall accept two `node` objects as
                  argument. The first the node and the second the child
                  and return edge.
                  Returns ``-->`` by default.


        filter_: Function to filter nodes to include in export.
                 The function shall accept one `node` object as
                 argument and return True if it should be included,
                 or False if it should not be included.

        stop: stop iteration at `node` if `stop` function returns `True` for `node`.

        maxlevel (int): Limit export to this number of levels.

    >>> from anytree import Node
    >>> root = Node("root")
    >>> s0 = Node("sub0", parent=root, edge=2)
    >>> s0b = Node("sub0B", parent=s0, foo=4, edge=109)
    >>> s0a = Node("sub0A", parent=s0, edge="")
    >>> s1 = Node("sub1", parent=root, edge="")
    >>> s1a = Node("sub1A", parent=s1, edge=7)
    >>> s1b = Node("sub1B", parent=s1, edge=8)
    >>> s1c = Node("sub1C", parent=s1, edge=22)
    >>> s1ca = Node("sub1Ca", parent=s1c, edge=42)

    A top-down graph:

    >>> from anytree.exporter import MermaidExporter
    >>> for line in MermaidExporter(root):
    ...     print(line)
    graph TD
    N0["root"]
    N1["sub0"]
    N2["sub0B"]
    N3["sub0A"]
    N4["sub1"]
    N5["sub1A"]
    N6["sub1B"]
    N7["sub1C"]
    N8["sub1Ca"]
    N0-->N1
    N0-->N4
    N1-->N2
    N1-->N3
    N4-->N5
    N4-->N6
    N4-->N7
    N7-->N8

    A customized graph with round boxes and named arrows:

    >>> def nodefunc(node):
    ...     return '("%s")' % (node.name)
    >>> def edgefunc(node, child):
    ...     return f"--{child.edge}-->"
    >>> options = [
    ...     "%% just an example comment",
    ...     "%% could be an option too",
    ... ]
    >>> for line in MermaidExporter(root, options=options, nodefunc=nodefunc, edgefunc=edgefunc):
    ...     print(line)
    graph TD
    %% just an example comment
    %% could be an option too
    N0("root")
    N1("sub0")
    N2("sub0B")
    N3("sub0A")
    N4("sub1")
    N5("sub1A")
    N6("sub1B")
    N7("sub1C")
    N8("sub1Ca")
    N0--2-->N1
    N0---->N4
    N1--109-->N2
    N1---->N3
    N4--7-->N5
    N4--8-->N6
    N4--22-->N7
    N7--42-->N8
    """

    def __init__(
        self,
        node,
        graph="graph",
        name="TD",
        options=None,
        indent=0,
        nodenamefunc=None,
        nodefunc=None,
        edgefunc=None,
        filter_=None,
        stop=None,
        maxlevel=None,
    ):
        self.node = node
        self.graph = graph
        self.name = name
        self.options = options
        self.indent = indent
        self.nodenamefunc = nodenamefunc
        self.nodefunc = nodefunc
        self.edgefunc = edgefunc
        self.filter_ = filter_
        self.stop = stop
        self.maxlevel = maxlevel
        self.__node_ids = {}
        self.__node_counter = itertools.count()

    def __iter__(self):
        # prepare
        indent = " " * self.indent
        nodenamefunc = self.nodenamefunc or self._default_nodenamefunc
        nodefunc = self.nodefunc or self._default_nodefunc
        edgefunc = self.edgefunc or self._default_edgefunc
        filter_ = self.filter_ or (lambda node: True)
        stop = self.stop or (lambda node: False)
        return self.__iter(indent, nodenamefunc, nodefunc, edgefunc, filter_, stop)

    # pylint: disable=arguments-differ
    def _default_nodenamefunc(self, node):
        node_id = id(node)
        try:
            num = self.__node_ids[node_id]
        except KeyError:
            num = self.__node_ids[node_id] = next(self.__node_counter)
        return f"N{num}"

    @staticmethod
    def _default_nodefunc(node):
        # pylint: disable=W0613
        return f'["{MermaidExporter.esc(node.name)}"]'

    @staticmethod
    def _default_edgefunc(node, child):
        # pylint: disable=W0613
        return "-->"

    def __iter(self, indent, nodenamefunc, nodefunc, edgefunc, filter_, stop):
        yield f"{self.graph} {self.name}"
        yield from self.__iter_options(indent)
        yield from self.__iter_nodes(indent, nodenamefunc, nodefunc, filter_, stop)
        yield from self.__iter_edges(indent, nodenamefunc, edgefunc, filter_, stop)

    def __iter_options(self, indent):
        options = self.options
        if options:
            for option in options:
                yield f"{indent}{option}"

    def __iter_nodes(self, indent, nodenamefunc, nodefunc, filter_, stop):
        for node in PreOrderIter(self.node, filter_=filter_, stop=stop, maxlevel=self.maxlevel):
            nodename = nodenamefunc(node)
            yield f"{indent}{nodename}{nodefunc(node)}"

    def __iter_edges(self, indent, nodenamefunc, edgefunc, filter_, stop):
        maxlevel = self.maxlevel - 1 if self.maxlevel else None
        for node in PreOrderIter(self.node, filter_=filter_, stop=stop, maxlevel=maxlevel):
            nodename = nodenamefunc(node)
            for child in node.children:
                if filter_(child) and not stop(child):
                    childname = nodenamefunc(child)
                    edge = edgefunc(node, child)
                    yield f"{indent}{nodename}{edge}{childname}"

    def to_file(self, filename):
        """
        Write graph to `filename`.

        >>> from anytree import Node
        >>> root = Node("root")
        >>> s0 = Node("sub0", parent=root)
        >>> s0b = Node("sub0B", parent=s0)
        >>> s0a = Node("sub0A", parent=s0)
        >>> s1 = Node("sub1", parent=root)
        >>> s1a = Node("sub1A", parent=s1)
        >>> s1b = Node("sub1B", parent=s1)
        >>> s1c = Node("sub1C", parent=s1)
        >>> s1ca = Node("sub1Ca", parent=s1c)

        >>> from anytree.exporter import MermaidExporter
        >>> MermaidExporter(root).to_file("tree.md")
        """
        with codecs.open(filename, "w", "utf-8") as file:
            file.write("```mermaid\n")
            for line in self:
                file.write(f"{line}\n")
            file.write("```")

    @staticmethod
    def esc(value):
        """Escape Strings."""
        return _RE_ESC.sub(lambda m: rf"\{m.group(0)}", str(value))
