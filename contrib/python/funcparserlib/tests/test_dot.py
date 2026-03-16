# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import unittest
from typing import Text, Optional

from funcparserlib.parser import NoParseError
from funcparserlib.lexer import LexerError
from .dot import parse, tokenize, Graph, Edge, SubGraph, DefAttrs, Attr, Node


class DotTest(unittest.TestCase):
    def t(self, data, expected=None):
        # type: (Text, Optional[Graph]) -> None
        self.assertEqual(parse(tokenize(data)), expected)

    def test_comments(self):
        # type: () -> None
        self.t(
            """
            /* комм 1 */
            graph /* комм 4 */ g1 {
                // комм 2 /* комм 3 */
            }
            // комм 5
        """,
            Graph(strict=None, type="graph", id="g1", stmts=[]),
        )

    def test_connected_subgraph(self):
        # type: () -> None
        self.t(
            """
            digraph g1 {
                n1 -> n2 ->
                subgraph n3 {
                    nn1 -> nn2 -> nn3;
                    nn3 -> nn1;
                };
                subgraph n3 {} -> n1;
            }
        """,
            Graph(
                strict=None,
                type="digraph",
                id="g1",
                stmts=[
                    Edge(
                        nodes=[
                            "n1",
                            "n2",
                            SubGraph(
                                id="n3",
                                stmts=[
                                    Edge(nodes=["nn1", "nn2", "nn3"], attrs=[]),
                                    Edge(nodes=["nn3", "nn1"], attrs=[]),
                                ],
                            ),
                        ],
                        attrs=[],
                    ),
                    Edge(nodes=[SubGraph(id="n3", stmts=[]), "n1"], attrs=[]),
                ],
            ),
        )

    def test_default_attrs(self):
        # type: () -> None
        self.t(
            """
            digraph g1 {
                page="3,3";
                graph [rotate=90];
                node [shape=box, color="#0000ff"];
                edge [style=dashed];
                n1 -> n2 -> n3;
                n3 -> n1;
            }
        """,
            Graph(
                strict=None,
                type="digraph",
                id="g1",
                stmts=[
                    DefAttrs(object="graph", attrs=[Attr(name="page", value='"3,3"')]),
                    DefAttrs(object="graph", attrs=[Attr(name="rotate", value="90")]),
                    DefAttrs(
                        object="node",
                        attrs=[
                            Attr(name="shape", value="box"),
                            Attr(name="color", value='"#0000ff"'),
                        ],
                    ),
                    DefAttrs(object="edge", attrs=[Attr(name="style", value="dashed")]),
                    Edge(nodes=["n1", "n2", "n3"], attrs=[]),
                    Edge(nodes=["n3", "n1"], attrs=[]),
                ],
            ),
        )

    def test_empty_graph(self):
        # type: () -> None
        self.t(
            """
            graph g1 {}
        """,
            Graph(strict=None, type="graph", id="g1", stmts=[]),
        )

    def test_few_attrs(self):
        # type: () -> None
        self.t(
            """
            digraph g1 {
                    n1 [attr1, attr2 = value2];
            }
        """,
            Graph(
                strict=None,
                type="digraph",
                id="g1",
                stmts=[
                    Node(
                        id="n1",
                        attrs=[
                            Attr(name="attr1", value=None),
                            Attr(name="attr2", value="value2"),
                        ],
                    )
                ],
            ),
        )

    def test_few_nodes(self):
        # type: () -> None
        self.t(
            """
            graph g1 {
                n1;
                n2;
                n3
            }
        """,
            Graph(
                strict=None,
                type="graph",
                id="g1",
                stmts=[
                    Node(id="n1", attrs=[]),
                    Node(id="n2", attrs=[]),
                    Node(id="n3", attrs=[]),
                ],
            ),
        )

    def test_illegal_comma(self):
        # type: () -> None
        try:
            self.t(
                """
                graph g1 {
                    n1;
                    n2;
                    n3,
                }
            """
            )
        except NoParseError:
            pass
        else:
            self.fail("must raise NoParseError")

    def test_null(self):
        # type: () -> None
        try:
            self.t("")
        except NoParseError:
            pass
        else:
            self.fail("must raise NoParseError")

    def test_simple_cycle(self):
        # type: () -> None
        self.t(
            """
            digraph g1 {
                n1 -> n2 [w=5];
                n2 -> n3 [w=10];
                n3 -> n1 [w=7];
            }
        """,
            Graph(
                strict=None,
                type="digraph",
                id="g1",
                stmts=[
                    Edge(nodes=["n1", "n2"], attrs=[Attr(name="w", value="5")]),
                    Edge(nodes=["n2", "n3"], attrs=[Attr(name="w", value="10")]),
                    Edge(nodes=["n3", "n1"], attrs=[Attr(name="w", value="7")]),
                ],
            ),
        )

    def test_single_unicode_char(self):
        # type: () -> None
        try:
            self.t("ф")
        except LexerError:
            pass
        else:
            self.fail("must raise LexerError")

    def test_unicode_names(self):
        # type: () -> None
        self.t(
            """
            digraph g1 {
                n1 -> "Медведь" [label="Поехали!"];
                "Медведь" -> n3 [label="Добро пожаловать!"];
                n3 -> n1 ["Водка"="Селёдка"];
            }
        """,
            Graph(
                strict=None,
                type="digraph",
                id="g1",
                stmts=[
                    Edge(
                        nodes=["n1", '"Медведь"'],
                        attrs=[Attr(name="label", value='"Поехали!"')],
                    ),
                    Edge(
                        nodes=['"Медведь"', "n3"],
                        attrs=[Attr(name="label", value='"Добро пожаловать!"')],
                    ),
                    Edge(
                        nodes=["n3", "n1"],
                        attrs=[Attr(name='"Водка"', value='"Селёдка"')],
                    ),
                ],
            ),
        )
