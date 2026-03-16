# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2020 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

"""
CEL Parser.

See  https://github.com/google/cel-spec/blob/master/doc/langdef.md

https://github.com/google/cel-cpp/blob/master/parser/Cel.g4

https://github.com/google/cel-go/blob/master/parser/gen/CEL.g4

Builds a parser from the supplied cel.lark grammar.

..  todo:: Consider embedding the ``cel.lark`` file as a triple-quoted literal.

    This means fixing a LOT of \\'s. But it also eliminates a data file from the installation.

Example::

    >>> from celpy.celparser import CELParser
    >>> p = CELParser()
    >>> text2 = 'type(null)'
    >>> ast2 = p.parse(text2)
    >>> print(ast2.pretty().replace("\t","   "))  # doctest: +NORMALIZE_WHITESPACE
    expr
      conditionalor
        conditionaland
          relation
            addition
              multiplication
                unary
                  member
                    primary
                      ident_arg
                        type
                        exprlist
                          expr
                            conditionalor
                              conditionaland
                                relation
                                  addition
                                    multiplication
                                      unary
                                        member
                                          primary
                                            literal    null


"""

import re
from pathlib import Path
from typing import Any, List, Optional, cast

import importlib.resources
import lark.visitors
from lark import Lark, Token, Tree  # noqa: F401
from lark.exceptions import LexError, ParseError, UnexpectedCharacters, UnexpectedToken


class CELParseError(Exception):
    def __init__(
        self, *args: Any, line: Optional[int] = None, column: Optional[int] = None
    ) -> None:
        super().__init__(*args)
        self.line = line
        self.column = column


class CELParser:
    """Wrapper for the CEL parser and the syntax error messages."""

    CEL_PARSER: Optional[Lark] = None

    def __init__(self) -> None:
        if CELParser.CEL_PARSER is None:
            CEL_grammar = (importlib.resources.files(__package__) / "cel.lark").read_text()
            CELParser.CEL_PARSER = Lark(
                CEL_grammar,
                parser="lalr",
                start="expr",
                debug=True,
                g_regex_flags=re.M,
                lexer_callbacks={"IDENT": self.ambiguous_literals},
                propagate_positions=True,
                maybe_placeholders=False,
                priority="invert",
            )

    @staticmethod
    def ambiguous_literals(t: Token) -> Token:
        """Resolve a grammar ambiguity between identifiers and literals"""
        if t.value == "true":
            return Token("BOOL_LIT", t.value)
        elif t.value == "false":
            return Token("BOOL_LIT", t.value)
        return t

    def parse(self, text: str) -> Tree:
        if CELParser.CEL_PARSER is None:
            raise TypeError("No grammar loaded")  # pragma: no cover
        self.text = text
        try:
            return CELParser.CEL_PARSER.parse(self.text)
        except (UnexpectedToken, UnexpectedCharacters) as ex:
            message = ex.get_context(text)
            raise CELParseError(message, *ex.args, line=ex.line, column=ex.column)
        except (LexError, ParseError) as ex:  # pragma: no cover
            message = ex.args[0].splitlines()[0]
            raise CELParseError(message, *ex.args)

    def error_text(
        self, message: str, line: Optional[int] = None, column: Optional[int] = None
    ) -> str:
        source = self.text.splitlines()[line - 1] if line else self.text
        message = (
            f"ERROR: <input>:{line or '?'}:{column or '?'} {message}\n"
            f"    | {source}\n"
            f"    | {(column - 1) * '.' if column else ''}^\n"
        )
        return message


class DumpAST(lark.visitors.Visitor_Recursive):
    """Dump a CEL AST creating a close approximation to the original source."""

    @classmethod
    def display(cls_, ast: lark.Tree) -> str:
        d = cls_()
        d.visit(ast)
        return d.stack[0]

    def __init__(self) -> None:
        self.stack: List[str] = []

    def expr(self, tree: lark.Tree) -> None:
        if len(tree.children) == 1:
            return
        else:
            right = self.stack.pop()
            left = self.stack.pop()
            cond = self.stack.pop()
            self.stack.append(f"{cond} ? {left} : {right}")

    def conditionalor(self, tree: lark.Tree) -> None:
        if len(tree.children) == 1:
            return
        else:
            right = self.stack.pop()
            left = self.stack.pop()
            self.stack.append(f"{left} || {right}")

    def conditionaland(self, tree: lark.Tree) -> None:
        if len(tree.children) == 1:
            return
        else:
            right = self.stack.pop()
            left = self.stack.pop()
            self.stack.append(f"{left} && {right}")

    def relation(self, tree: lark.Tree) -> None:
        if len(tree.children) == 1:
            return
        else:
            right = self.stack.pop()
            left = self.stack.pop()
            self.stack.append(f"{left} {right}")

    def relation_lt(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} < ")

    def relation_le(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} <= ")

    def relation_gt(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} > ")

    def relation_ge(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} >= ")

    def relation_eq(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} == ")

    def relation_ne(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} != ")

    def relation_in(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} in ")

    def addition(self, tree: lark.Tree) -> None:
        if len(tree.children) == 1:
            return
        else:
            right = self.stack.pop()
            left = self.stack.pop()
            self.stack.append(f"{left} {right}")

    def addition_add(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} + ")

    def addition_sub(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} - ")

    def multiplication(self, tree: lark.Tree) -> None:
        if len(tree.children) == 1:
            return
        else:
            right = self.stack.pop()
            left = self.stack.pop()
            self.stack.append(f"{left} {right}")

    def multiplication_mul(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} * ")

    def multiplication_div(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} / ")

    def multiplication_mod(self, tree: lark.Tree) -> None:
        left = self.stack.pop()
        self.stack.append(f"{left} % ")

    def unary(self, tree: lark.Tree) -> None:
        if len(tree.children) == 1:
            return
        else:
            right = self.stack.pop()
            left = self.stack.pop()
            self.stack.append(f"{left} {right}")

    def unary_not(self, tree: lark.Tree) -> None:
        self.stack.append("!")

    def unary_neg(self, tree: lark.Tree) -> None:
        self.stack.append("-")

    def member_dot(self, tree: lark.Tree) -> None:
        right = cast(lark.Token, tree.children[1]).value
        if self.stack:
            left = self.stack.pop()
            self.stack.append(f"{left}.{right}")

    def member_dot_arg(self, tree: lark.Tree) -> None:
        if len(tree.children) == 3:
            exprlist = self.stack.pop()
        else:
            exprlist = ""
        right = cast(lark.Token, tree.children[1]).value
        left = self.stack.pop()
        self.stack.append(f"{left}.{right}({exprlist})")

    def member_index(self, tree: lark.Tree) -> None:
        right = self.stack.pop()
        left = self.stack.pop()
        self.stack.append(f"{left}[{right}]")

    def member_object(self, tree: lark.Tree) -> None:
        if len(tree.children) == 2:
            fieldinits = self.stack.pop()
        else:
            fieldinits = ""
        left = self.stack.pop()
        self.stack.append(f"{left}{{{fieldinits}}}")

    def dot_ident_arg(self, tree: lark.Tree) -> None:
        if len(tree.children) == 2:
            exprlist = self.stack.pop()
        else:
            exprlist = ""
        left = cast(lark.Token, tree.children[0]).value
        self.stack.append(f".{left}({exprlist})")

    def dot_ident(self, tree: lark.Tree) -> None:
        left = cast(lark.Token, tree.children[0]).value
        self.stack.append(f".{left}")

    def ident_arg(self, tree: lark.Tree) -> None:
        if len(tree.children) == 2:
            exprlist = self.stack.pop()
        else:
            exprlist = ""

        left = cast(lark.Token, tree.children[0]).value
        self.stack.append(f"{left}({exprlist})")

    def ident(self, tree: lark.Tree) -> None:
        self.stack.append(cast(lark.Token, tree.children[0]).value)

    def paren_expr(self, tree: lark.Tree) -> None:
        if self.stack:
            left = self.stack.pop()
            self.stack.append(f"({left})")

    def list_lit(self, tree: lark.Tree) -> None:
        if self.stack:
            left = self.stack.pop()
            self.stack.append(f"[{left}]")
        else:
            self.stack.append("")

    def map_lit(self, tree: lark.Tree) -> None:
        if self.stack:
            left = self.stack.pop()
            self.stack.append(f"{{{left}}}")
        else:
            self.stack.append("{}")

    def exprlist(self, tree: lark.Tree) -> None:
        items = ", ".join(reversed(list(self.stack.pop() for _ in tree.children)))
        self.stack.append(items)

    def fieldinits(self, tree: lark.Tree) -> None:
        names = cast(List[lark.Token], tree.children[::2])
        values = cast(List[lark.Token], tree.children[1::2])
        assert len(names) == len(values)
        pairs = reversed(
            list((n.value, self.stack.pop()) for n, v in zip(names, values))
        )
        items = ", ".join(f"{n}: {v}" for n, v in pairs)
        self.stack.append(items)

    def mapinits(self, tree: lark.Tree) -> None:
        """Note reversed pop order for values and keys."""
        keys = tree.children[::2]
        values = tree.children[1::2]
        assert len(keys) == len(values)
        pairs = reversed(
            list(
                {"value": self.stack.pop(), "key": self.stack.pop()}
                for k, v in zip(keys, values)
            )
        )
        items = ", ".join(f"{k_v['key']}: {k_v['value']}" for k_v in pairs)
        self.stack.append(items)

    def literal(self, tree: lark.Tree) -> None:
        if tree.children:
            self.stack.append(cast(lark.Token, tree.children[0]).value)


def tree_dump(ast: Tree) -> str:
    """Dumps the AST to approximate the original source"""
    d = DumpAST()
    d.visit(ast)
    return d.stack[0]


if __name__ == "__main__":  # pragma: no cover
    # A minimal sanity check.
    # This is a smoke test for the grammar to expose shift/reduce or reduce/reduce conflicts.
    # It will produce a RuntimeWarning because it's not the proper main program.
    p = CELParser()

    text = """
    account.balance >= transaction.withdrawal
    || (account.overdraftProtection
    && account.overdraftLimit >= transaction.withdrawal  - account.balance)
    """
    ast = p.parse(text)
    print(ast)

    d = DumpAST()
    d.visit(ast)
    print(d.stack)

    text2 = """type(null)"""
    ast2 = p.parse(text2)
    print(ast2.pretty())
