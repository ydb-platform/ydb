# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import unittest
from typing import Text, Optional

from funcparserlib.parser import NoParseError
from funcparserlib.lexer import LexerError
from . import json


class JsonTest(unittest.TestCase):
    def t(self, data, expected=None):
        # type: (Text, Optional[object]) -> None
        self.assertEqual(json.loads(data), expected)

    def test_1_array(self):
        # type: () -> None
        self.t("[1]", [1])

    def test_1_object(self):
        # type: () -> None
        self.t('{"foo": "bar"}', {"foo": "bar"})

    def test_bool_and_null(self):
        # type: () -> None
        self.t("[null, true, false]", [None, True, False])

    def test_empty_array(self):
        # type: () -> None
        self.t("[]", [])

    def test_empty_object(self):
        # type: () -> None
        self.t("{}", {})

    def test_many_array(self):
        # type: () -> None
        self.t("[1, 2, [3, 4, 5], 6]", [1, 2, [3, 4, 5], 6])

    def test_many_object(self):
        # type: () -> None
        # noinspection SpellCheckingInspection
        self.t(
            """
            {
                "foo": 1,
                "bar":
                {
                    "baz": 2,
                    "quux": [true, false],
                    "{}": {}
                },
                "spam": "eggs"
            }
        """,
            {
                "foo": 1,
                "bar": {
                    "baz": 2,
                    "quux": [True, False],
                    "{}": {},
                },
                "spam": "eggs",
            },
        )

    def test_null(self):
        # type: () -> None
        try:
            self.t("")
        except NoParseError:
            pass
        else:
            self.fail("must raise NoParseError")

    def test_numbers(self):
        # type: () -> None
        self.t(
            """\
            [
                0, 1, -1, 14, -14, 65536,
                0.0, 3.14, -3.14, -123.456,
                6.67428e-11, -1.602176e-19, 6.67428E-11
            ]
        """,
            [
                0,
                1,
                -1,
                14,
                -14,
                65536,
                0.0,
                3.14,
                -3.14,
                -123.456,
                6.67428e-11,
                -1.602176e-19,
                6.67428e-11,
            ],
        )

    def test_strings(self):
        # type: () -> None
        # noinspection SpellCheckingInspection
        self.t(
            r"""
            [
                ["", "hello", "hello world!"],
                ["привет, мир!", "λx.x"],
                ["\"", "\\", "\/", "\b", "\f", "\n", "\r", "\t"],
                ["\u0000", "\u03bb", "\uffff", "\uFFFF"],
                ["вот функция идентичности:\nλx.x\nили так:\n\u03bbx.x"]
            ]
        """,
            [
                ["", "hello", "hello world!"],
                ["привет, мир!", "λx.x"],
                ['"', "\\", "/", "\x08", "\x0c", "\n", "\r", "\t"],
                ["\u0000", "\u03bb", "\uffff", "\uffff"],
                ["вот функция идентичности:\nλx.x\nили так:\n\u03bbx.x"],
            ],
        )

    def test_toplevel_string(self):
        # type: () -> None
        try:
            self.t("неправильно")
        except LexerError:
            pass
        else:
            self.fail("must raise LexerError")
