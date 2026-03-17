# -*- coding: utf-8 -*-

# Copyright © 2009/2021 Andrey Vlasovskikh
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
# OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""A DOT language parser using funcparserlib.

The parser is based on [the DOT grammar][1]. It is pretty complete with a few
not supported things:

* String escapes
* Ports and compass points
* XML identifiers

At the moment, the parser builds only a parse tree, not an abstract syntax tree
(AST), or an API for dealing with DOT.

  [1]: https://www.graphviz.org/doc/info/lang.html
"""

from __future__ import print_function, unicode_literals

import os
import sys
from collections import namedtuple
from re import MULTILINE
from typing import Sequence, List, TypeVar, Any, Callable, Text

from funcparserlib.lexer import TokenSpec, make_tokenizer, Token, LexerError
from funcparserlib.parser import (
    maybe,
    many,
    finished,
    oneplus,
    forward_decl,
    NoParseError,
    Parser,
    tok,
)
from funcparserlib.util import pretty_tree

ENCODING = "UTF-8"

Graph = namedtuple("Graph", "strict type id stmts")
SubGraph = namedtuple("SubGraph", "id stmts")
Node = namedtuple("Node", "id attrs")
Attr = namedtuple("Attr", "name value")
Edge = namedtuple("Edge", "nodes attrs")
DefAttrs = namedtuple("DefAttrs", "object attrs")

T = TypeVar("T")  # noqa


def tokenize(s):
    # type: (Text) -> Sequence[Token]
    specs = [
        TokenSpec("Comment", r"/\*(.|[\r\n])*?\*/", MULTILINE),
        TokenSpec("Comment", r"//.*"),
        TokenSpec("NL", r"[\r\n]+"),
        TokenSpec("Space", r"[ \t\r\n]+"),
        TokenSpec("Name", r"[A-Za-z\200-\377_][A-Za-z\200-\377_0-9]*"),
        TokenSpec("Op", r"[{};,=\[\]]|(->)|(--)"),
        TokenSpec("Number", r"-?(\.[0-9]+)|([0-9]+(\.[0-9]*)?)"),
        TokenSpec("String", r'"[^"]*"'),  # '\"' escapes are ignored
    ]
    useless = ["Comment", "NL", "Space"]
    t = make_tokenizer(specs)
    return [x for x in t(s) if x.type not in useless]


def parse(tokens):
    # type: (Sequence[Token]) -> Graph

    def un_arg(f):
        # type: (Callable[..., T]) -> Callable[[tuple], T]
        return lambda args: f(*args)

    def flatten(xs):
        # type: (List[List[Attr]]) -> List[Attr]
        return sum(xs, [])

    def n(s):
        # type: (Text) -> Parser[Token, Text]
        return tok("Name", s)

    def op(s):
        # type: (Text) -> Parser[Token, Text]
        return tok("Op", s)

    dot_id = (tok("Name") | tok("Number") | tok("String")).named("id")

    def make_graph_attr(args):
        # type: (tuple) -> DefAttrs
        return DefAttrs("graph", [Attr(*args)])

    def make_edge(node, xs, attrs):
        # type: (Node, List[Node], List[Attr]) -> Edge
        return Edge([node] + xs, attrs)

    node_id = dot_id  # + maybe(port)
    a_list = dot_id + maybe(-op("=") + dot_id) + -maybe(op(",")) >> un_arg(Attr)
    attr_list = many(-op("[") + many(a_list) + -op("]")) >> flatten
    attr_stmt = (n("graph") | n("node") | n("edge")) + attr_list >> un_arg(DefAttrs)
    graph_attr = dot_id + -op("=") + dot_id >> make_graph_attr
    node_stmt = node_id + attr_list >> un_arg(Node)
    # We use a forward_decl because of circular definitions like
    # (stmt_list -> stmt -> subgraph -> stmt_list)
    subgraph = forward_decl()  # type: Parser[Token, SubGraph]
    edge_rhs = -(op("->") | op("--")) + (subgraph | node_id)
    edge_stmt = (subgraph | node_id) + oneplus(edge_rhs) + attr_list >> un_arg(
        make_edge
    )
    stmt = attr_stmt | edge_stmt | subgraph | graph_attr | node_stmt
    stmt_list = many(stmt + -maybe(op(";")))
    graph_body = -op("{") + stmt_list + -op("}")
    subgraph.define(-n("subgraph") + maybe(dot_id) + graph_body >> un_arg(SubGraph))
    graph_modifiers = maybe(n("strict")) + maybe(n("graph") | n("digraph"))
    graph = graph_modifiers + maybe(dot_id) + graph_body >> un_arg(Graph)
    dotfile = graph + -finished

    return dotfile.parse(tokens)


def pretty_parse_tree(obj):
    # type: (object) -> Text
    Pair = namedtuple("Pair", "first second")

    def kids(x):
        # type: (Any) -> List[object]
        if isinstance(x, (Graph, SubGraph)):
            return [Pair("stmts", x.stmts)]
        elif isinstance(x, (Node, DefAttrs)):
            return [Pair("attrs", x.attrs)]
        elif isinstance(x, Edge):
            return [Pair("nodes", x.nodes), Pair("attrs", x.attrs)]
        elif isinstance(x, Pair):
            return x.second
        else:
            return []

    def show(x):
        # type: (Any) -> Text
        if isinstance(x, Pair):
            return x.first
        elif isinstance(x, Graph):
            return "Graph [id=%s, strict=%r, type=%s]" % (
                x.id,
                x.strict is not None,
                x.type,
            )
        elif isinstance(x, SubGraph):
            return "SubGraph [id=%s]" % (x.id,)
        elif isinstance(x, Edge):
            return "Edge"
        elif isinstance(x, Attr):
            return "Attr [name=%s, value=%s]" % (x.name, x.value)
        elif isinstance(x, DefAttrs):
            return "DefAttrs [object=%s]" % (x.object,)
        elif isinstance(x, Node):
            return "Node [id=%s]" % (x.id,)
        else:
            return str(x)

    return pretty_tree(obj, kids, show)


def main():
    # type: () -> None
    # import logging
    # logging.basicConfig(level=logging.DEBUG)
    # import funcparserlib
    # funcparserlib.parser.debug = True
    try:
        stdin = os.fdopen(sys.stdin.fileno(), "rb")
        text = stdin.read().decode(ENCODING)
        tree = parse(tokenize(text))
        # print(pformat(tree))
        print(pretty_parse_tree(tree).encode(ENCODING))
    except (NoParseError, LexerError) as e:
        msg = ("syntax error: %s" % e).encode(ENCODING)
        print(msg, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
