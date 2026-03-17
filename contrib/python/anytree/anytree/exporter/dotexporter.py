import codecs
import itertools
import logging
import re
from os import path, remove
from subprocess import check_call
from tempfile import NamedTemporaryFile

from anytree import PreOrderIter

_RE_ESC = re.compile(r'["\\]')


class DotExporter:
    """
    Dot Language Exporter.

    Args:
        node (Node): start node.

    Keyword Args:
        graph: DOT graph type.

        name: DOT graph name.

        options: list of options added to the graph.

        indent (int): number of spaces for indent.

        nodenamefunc: Function to extract node name from `node` object.
                      The function shall accept one `node` object as
                      argument and return the name of it.

        nodeattrfunc: Function to decorate a node with attributes.
                      The function shall accept one `node` object as
                      argument and return the attributes.

        edgeattrfunc: Function to decorate a edge with attributes.
                      The function shall accept two `node` objects as
                      argument. The first the node and the second the child
                      and return the attributes.

        edgetypefunc: Function to which gives the edge type.
                      The function shall accept two `node` objects as
                      argument. The first the node and the second the child
                      and return the edge (i.e. '->').

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

    .. note:: If the node names are not unique, see :any:`UniqueDotExporter`.

    A directed graph:

    >>> from anytree.exporter import DotExporter
    >>> for line in DotExporter(root):
    ...     print(line)
    digraph tree {
        "root";
        "sub0";
        "sub0B";
        "sub0A";
        "sub1";
        "sub1A";
        "sub1B";
        "sub1C";
        "sub1Ca";
        "root" -> "sub0";
        "root" -> "sub1";
        "sub0" -> "sub0B";
        "sub0" -> "sub0A";
        "sub1" -> "sub1A";
        "sub1" -> "sub1B";
        "sub1" -> "sub1C";
        "sub1C" -> "sub1Ca";
    }

    The resulting graph:

    .. image:: ../static/dotexporter0.png

    An undirected graph:

    >>> def nodenamefunc(node):
    ...     return '%s:%s' % (node.name, node.depth)
    >>> def edgeattrfunc(node, child):
    ...     return 'label="%s:%s"' % (node.name, child.name)
    >>> def edgetypefunc(node, child):
    ...     return '--'
            >>> from anytree.exporter import DotExporter
    >>> for line in DotExporter(root, graph="graph",
    ...                             nodenamefunc=nodenamefunc,
    ...                             nodeattrfunc=lambda node: "shape=box",
    ...                             edgeattrfunc=edgeattrfunc,
    ...                             edgetypefunc=edgetypefunc):
    ...     print(line)
    graph tree {
        "root:0" [shape=box];
        "sub0:1" [shape=box];
        "sub0B:2" [shape=box];
        "sub0A:2" [shape=box];
        "sub1:1" [shape=box];
        "sub1A:2" [shape=box];
        "sub1B:2" [shape=box];
        "sub1C:2" [shape=box];
        "sub1Ca:3" [shape=box];
        "root:0" -- "sub0:1" [label="root:sub0"];
        "root:0" -- "sub1:1" [label="root:sub1"];
        "sub0:1" -- "sub0B:2" [label="sub0:sub0B"];
        "sub0:1" -- "sub0A:2" [label="sub0:sub0A"];
        "sub1:1" -- "sub1A:2" [label="sub1:sub1A"];
        "sub1:1" -- "sub1B:2" [label="sub1:sub1B"];
        "sub1:1" -- "sub1C:2" [label="sub1:sub1C"];
        "sub1C:2" -- "sub1Ca:3" [label="sub1C:sub1Ca"];
    }

    The resulting graph:

    .. image:: ../static/dotexporter1.png

    To export custom node implementations or :any:`AnyNode`, please provide a proper `nodenamefunc`:

    >>> from anytree import AnyNode
    >>> root = AnyNode(id="root")
    >>> s0 = AnyNode(id="sub0", parent=root)
    >>> s0b = AnyNode(id="s0b", parent=s0)
    >>> s0a = AnyNode(id="s0a", parent=s0)

    >>> from anytree.exporter import DotExporter
    >>> for line in DotExporter(root, nodenamefunc=lambda n: n.id):
    ...     print(line)
    digraph tree {
        "root";
        "sub0";
        "s0b";
        "s0a";
        "root" -> "sub0";
        "sub0" -> "s0b";
        "sub0" -> "s0a";
    }
    """

    def __init__(
        self,
        node,
        graph="digraph",
        name="tree",
        options=None,
        indent=4,
        nodenamefunc=None,
        nodeattrfunc=None,
        edgeattrfunc=None,
        edgetypefunc=None,
        filter_=None,
        maxlevel=None,
        stop=None,
    ):
        self.node = node
        self.graph = graph
        self.name = name
        self.options = options
        self.indent = indent
        self.nodenamefunc = nodenamefunc
        self.nodeattrfunc = nodeattrfunc
        self.edgeattrfunc = edgeattrfunc
        self.edgetypefunc = edgetypefunc
        self.filter_ = filter_
        self.maxlevel = maxlevel
        self.stop = stop

    def __iter__(self):
        # prepare
        indent = " " * self.indent
        nodenamefunc = self.nodenamefunc or self._default_nodenamefunc
        nodeattrfunc = self.nodeattrfunc or self._default_nodeattrfunc
        edgeattrfunc = self.edgeattrfunc or self._default_edgeattrfunc
        edgetypefunc = self.edgetypefunc or self._default_edgetypefunc
        filter_ = self.filter_ or self._default_filter
        return self.__iter(indent, nodenamefunc, nodeattrfunc, edgeattrfunc, edgetypefunc, filter_)

    @staticmethod
    def _default_nodenamefunc(node):
        return node.name

    @staticmethod
    def _default_nodeattrfunc(node):
        # pylint: disable=unused-argument
        return None

    @staticmethod
    def _default_edgeattrfunc(node, child):
        # pylint: disable=unused-argument
        return None

    @staticmethod
    def _default_edgetypefunc(node, child):
        # pylint: disable=unused-argument
        return "->"

    @staticmethod
    def _default_filter(node):
        # pylint: disable=unused-argument
        return True

    def __iter(self, indent, nodenamefunc, nodeattrfunc, edgeattrfunc, edgetypefunc, filter_):
        yield f"{self.graph} {self.name} {{"
        yield from self.__iter_options(indent)
        yield from self.__iter_nodes(indent, nodenamefunc, nodeattrfunc, filter_)
        yield from self.__iter_edges(indent, nodenamefunc, edgeattrfunc, edgetypefunc, filter_)
        yield "}"

    def __iter_options(self, indent):
        options = self.options
        if options:
            for option in options:
                yield f"{indent}{option}"

    def __iter_nodes(self, indent, nodenamefunc, nodeattrfunc, filter_):
        for node in PreOrderIter(self.node, filter_=filter_, stop=self.stop, maxlevel=self.maxlevel):
            nodename = nodenamefunc(node)
            nodeattr = nodeattrfunc(node)
            nodeattr = f" [{nodeattr}]" if nodeattr is not None else ""
            yield f'{indent}"{DotExporter.esc(nodename)}"{nodeattr};'

    def __iter_edges(self, indent, nodenamefunc, edgeattrfunc, edgetypefunc, filter_):
        maxlevel = self.maxlevel - 1 if self.maxlevel else None
        for node in PreOrderIter(self.node, filter_=filter_, stop=self.stop, maxlevel=maxlevel):
            nodename = nodenamefunc(node)
            for child in node.children:
                if not filter_(child):
                    continue
                childname = nodenamefunc(child)
                edgeattr = edgeattrfunc(node, child)
                edgetype = edgetypefunc(node, child)
                edgeattr = f" [{edgeattr}]" if edgeattr is not None else ""
                yield f'{indent}"{DotExporter.esc(nodename)}" {edgetype} "{DotExporter.esc(childname)}"{edgeattr};'

    def to_dotfile(self, filename):
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

        >>> from anytree.exporter import DotExporter
        >>> DotExporter(root).to_dotfile("tree.dot")

        The generated file should be handed over to the `dot` tool from the
        http://www.graphviz.org/ package::

            $ dot tree.dot -T png -o tree.png
        """
        with codecs.open(filename, "w", "utf-8") as file:
            for line in self:
                file.write(f"{line}\n")

    def to_picture(self, filename):
        """
        Write graph to a temporary file and invoke `dot`.

        The output file type is automatically detected from the file suffix.

        *`graphviz` needs to be installed, before usage of this method.*
        """
        fileformat = path.splitext(filename)[1][1:]
        with NamedTemporaryFile("wb", delete=False) as dotfile:
            dotfilename = dotfile.name
            for line in self:
                dotfile.write((f"{line}\n").encode())
            dotfile.flush()
            cmd = ["dot", dotfilename, "-T", fileformat, "-o", filename]
            check_call(cmd)
        try:
            remove(dotfilename)
        # pylint: disable=broad-exception-caught
        except Exception:  # pragma: no cover
            logging.getLogger(__name__).warning("Could not remove temporary file %s", dotfilename)

    @staticmethod
    def esc(value):
        """Escape Strings."""
        return _RE_ESC.sub(lambda m: rf"\{m.group(0)}", str(value))


class UniqueDotExporter(DotExporter):
    """
    Unique Dot Language Exporter.

    Handle trees with random or conflicting node names gracefully.

    Args:
        node (Node): start node.

    Keyword Args:
        graph: DOT graph type.

        name: DOT graph name.

        options: list of options added to the graph.

        indent (int): number of spaces for indent.

        nodenamefunc: Function to extract node name from `node` object.
                        The function shall accept one `node` object as
                        argument and return the name of it.

        nodeattrfunc: Function to decorate a node with attributes.
                        The function shall accept one `node` object as
                        argument and return the attributes.

        edgeattrfunc: Function to decorate a edge with attributes.
                        The function shall accept two `node` objects as
                        argument. The first the node and the second the child
                        and return the attributes.

        edgetypefunc: Function to which gives the edge type.
                        The function shall accept two `node` objects as
                        argument. The first the node and the second the child
                        and return the edge (i.e. '->').

        filter_: Function to filter nodes to include in export.
                 The function shall accept one `node` object as
                 argument and return True if it should be included,
                 or False if it should not be included.

        stop: stop iteration at `node` if `stop` function returns `True` for `node`.

        maxlevel (int): Limit export to this number of levels.

    >>> from anytree import Node
    >>> root = Node("root")
    >>> s0 = Node("sub0", parent=root)
    >>> s0b = Node("s0", parent=s0)
    >>> s0a = Node("s0", parent=s0)
    >>> s1 = Node("sub1", parent=root)
    >>> s1a = Node("s1", parent=s1)
    >>> s1b = Node("s1", parent=s1)
    >>> s1c = Node("s1", parent=s1)
    >>> s1ca = Node("sub1Ca", parent=s1c)

    >>> from anytree.exporter import UniqueDotExporter
    >>> for line in UniqueDotExporter(root):
    ...     print(line)
    digraph tree {
        "0x0" [label="root"];
        "0x1" [label="sub0"];
        "0x2" [label="s0"];
        "0x3" [label="s0"];
        "0x4" [label="sub1"];
        "0x5" [label="s1"];
        "0x6" [label="s1"];
        "0x7" [label="s1"];
        "0x8" [label="sub1Ca"];
        "0x0" -> "0x1";
        "0x0" -> "0x4";
        "0x1" -> "0x2";
        "0x1" -> "0x3";
        "0x4" -> "0x5";
        "0x4" -> "0x6";
        "0x4" -> "0x7";
        "0x7" -> "0x8";
    }

    The resulting graph:

    .. image:: ../static/uniquedotexporter2.png

    To export custom node implementations or :any:`AnyNode`, please provide a proper `nodeattrfunc`:

    >>> from anytree import AnyNode
    >>> root = AnyNode(id="root")
    >>> s0 = AnyNode(id="sub0", parent=root)
    >>> s0b = AnyNode(id="s0", parent=s0)
    >>> s0a = AnyNode(id="s0", parent=s0)

    >>> from anytree.exporter import UniqueDotExporter
    >>> for line in UniqueDotExporter(root, nodeattrfunc=lambda n: 'label="%s"' % (n.id)):
    ...     print(line)
    digraph tree {
        "0x0" [label="root"];
        "0x1" [label="sub0"];
        "0x2" [label="s0"];
        "0x3" [label="s0"];
        "0x0" -> "0x1";
        "0x1" -> "0x2";
        "0x1" -> "0x3";
    }
    """

    def __init__(
        self,
        node,
        graph="digraph",
        name="tree",
        options=None,
        indent=4,
        nodenamefunc=None,
        nodeattrfunc=None,
        edgeattrfunc=None,
        edgetypefunc=None,
        filter_=None,
        stop=None,
        maxlevel=None,
    ):
        super().__init__(
            node,
            graph=graph,
            name=name,
            options=options,
            indent=indent,
            nodenamefunc=nodenamefunc,
            nodeattrfunc=nodeattrfunc,
            edgeattrfunc=edgeattrfunc,
            edgetypefunc=edgetypefunc,
            filter_=filter_,
            stop=stop,
            maxlevel=maxlevel,
        )
        self.__node_ids = {}
        self.__node_counter = itertools.count()

    # pylint: disable=arguments-differ
    def _default_nodenamefunc(self, node):
        node_id = id(node)
        try:
            num = self.__node_ids[node_id]
        except KeyError:
            num = self.__node_ids[node_id] = next(self.__node_counter)
        return hex(num)

    @staticmethod
    def _default_nodeattrfunc(node):
        return f'label="{node.name}"'
