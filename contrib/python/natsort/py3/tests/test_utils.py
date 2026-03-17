# -*- coding: utf-8 -*-
"""These test the utils.py functions."""

import os
import pathlib
import string
from itertools import chain
from operator import neg as op_neg
from typing import List, Pattern, Tuple, Union

import pytest
from hypothesis import given
from hypothesis.strategies import integers, lists, sampled_from, text
from natsort import utils
from natsort.ns_enum import NSType, ns


def test_do_decoding_decodes_bytes_string_to_unicode() -> None:
    assert type(utils.do_decoding(b"bytes", "ascii")) is str
    assert utils.do_decoding(b"bytes", "ascii") == "bytes"
    assert utils.do_decoding(b"bytes", "ascii") == b"bytes".decode("ascii")


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.I, utils.NumericalRegularExpressions.int_nosign()),
        (ns.I | ns.N, utils.NumericalRegularExpressions.int_nosign()),
        (ns.I | ns.S, utils.NumericalRegularExpressions.int_sign()),
        (ns.I | ns.S | ns.N, utils.NumericalRegularExpressions.int_sign()),
        (ns.F, utils.NumericalRegularExpressions.float_nosign_exp()),
        (ns.F | ns.N, utils.NumericalRegularExpressions.float_nosign_noexp()),
        (ns.F | ns.S, utils.NumericalRegularExpressions.float_sign_exp()),
        (ns.F | ns.S | ns.N, utils.NumericalRegularExpressions.float_sign_noexp()),
    ],
)
def test_regex_chooser_returns_correct_regular_expression_object(
    alg: NSType, expected: Pattern[str]
) -> None:
    assert utils.regex_chooser(alg).pattern == expected.pattern


@pytest.mark.parametrize(
    "alg, value_or_alias",
    [
        # Defaults
        (ns.DEFAULT, 0),
        (ns.INT, 0),
        (ns.UNSIGNED, 0),
        # Aliases
        (ns.INT, ns.I),
        (ns.UNSIGNED, ns.U),
        (ns.FLOAT, ns.F),
        (ns.SIGNED, ns.S),
        (ns.NOEXP, ns.N),
        (ns.PATH, ns.P),
        (ns.LOCALEALPHA, ns.LA),
        (ns.LOCALENUM, ns.LN),
        (ns.LOCALE, ns.L),
        (ns.IGNORECASE, ns.IC),
        (ns.LOWERCASEFIRST, ns.LF),
        (ns.GROUPLETTERS, ns.G),
        (ns.UNGROUPLETTERS, ns.UG),
        (ns.CAPITALFIRST, ns.C),
        (ns.UNGROUPLETTERS, ns.CAPITALFIRST),
        (ns.NANLAST, ns.NL),
        (ns.COMPATIBILITYNORMALIZE, ns.CN),
        (ns.NUMAFTER, ns.NA),
        # Convenience
        (ns.LOCALE, ns.LOCALEALPHA | ns.LOCALENUM),
        (ns.REAL, ns.FLOAT | ns.SIGNED),
    ],
)
def test_ns_enum_values_and_aliases(alg: NSType, value_or_alias: NSType) -> None:
    assert alg == value_or_alias


def test_chain_functions_is_a_no_op_if_no_functions_are_given() -> None:
    x = 2345
    assert utils.chain_functions([])(x) is x


def test_chain_functions_does_one_function_if_one_function_is_given() -> None:
    x = "2345"
    assert utils.chain_functions([len])(x) == 4


def test_chain_functions_combines_functions_in_given_order() -> None:
    x = 2345
    assert utils.chain_functions([str, len, op_neg])(x) == -len(str(x))


# Each test has an "example" version for demonstrative purposes,
# and a test that uses the hypothesis module.


def test_groupletters_gives_letters_with_lowercase_letter_transform_example() -> None:
    assert utils.groupletters("HELLO") == "hHeElLlLoO"
    assert utils.groupletters("hello") == "hheelllloo"


@given(text().filter(bool))
def test_groupletters_gives_letters_with_lowercase_letter_transform(
    x: str,
) -> None:
    assert utils.groupletters(x) == "".join(
        chain.from_iterable([y.casefold(), y] for y in x)
    )


def test_sep_inserter_does_nothing_if_no_numbers_example() -> None:
    assert list(utils.sep_inserter(iter(["a", "b", "c"]), "")) == ["a", "b", "c"]
    assert list(utils.sep_inserter(iter(["a"]), "")) == ["a"]


def test_sep_inserter_does_nothing_if_only_one_number_example() -> None:
    assert list(utils.sep_inserter(iter(["a", 5]), "")) == ["a", 5]


def test_sep_inserter_inserts_separator_string_between_two_numbers_example() -> None:
    assert list(utils.sep_inserter(iter([5, 9]), "")) == ["", 5, "", 9]


@given(lists(elements=text().filter(bool) | integers(), min_size=3))
def test_sep_inserter_inserts_separator_between_two_numbers(
    x: List[Union[str, int]]
) -> None:
    # Rather than just replicating the results in a different algorithm,
    # validate that the "shape" of the output is as expected.
    result = list(utils.sep_inserter(iter(x), ""))
    for i, pos in enumerate(result[1:-1], 1):
        if pos == "":
            assert isinstance(result[i - 1], int)
            assert isinstance(result[i + 1], int)


def test_path_splitter_splits_path_string_by_sep_example() -> None:
    given = "/this/is/a/path"
    expected = (os.sep, "this", "is", "a", "path")
    assert tuple(utils.path_splitter(given)) == tuple(expected)
    assert tuple(utils.path_splitter(pathlib.Path(given))) == tuple(expected)


@pytest.mark.parametrize("given", [".", "./", "./././", ".\\"])
def test_path_splitter_handles_dot_properly(given: str) -> None:
    # https://github.com/SethMMorton/natsort/issues/142
    expected = (os.path.normpath(given),)
    assert tuple(utils.path_splitter(given)) == expected
    assert tuple(utils.path_splitter(pathlib.Path(given))) == expected


@given(lists(sampled_from(string.ascii_letters), min_size=2).filter(all))
def test_path_splitter_splits_path_string_by_sep(x: List[str]) -> None:
    z = str(pathlib.Path(*x))
    assert tuple(utils.path_splitter(z)) == tuple(pathlib.Path(z).parts)


@pytest.mark.parametrize(
    "given, expected",
    [
        (
            "/this/is/a/path/file.x1.10.tar.gz",
            (os.sep, "this", "is", "a", "path", "file.x1.10", ".tar", ".gz"),
        ),
        (
            "/this/is/a/path/file.x1.10.tar",
            (os.sep, "this", "is", "a", "path", "file.x1.10", ".tar"),
        ),
        (
            "/this/is/a/path/file.x1.threethousand.tar",
            (os.sep, "this", "is", "a", "path", "file.x1.threethousand", ".tar"),
        ),
    ],
)
def test_path_splitter_splits_path_string_by_sep_and_removes_extension_example(
    given: str, expected: Tuple[str, ...]
) -> None:
    assert tuple(utils.path_splitter(given)) == tuple(expected)


@given(lists(sampled_from(string.ascii_letters), min_size=3).filter(all))
def test_path_splitter_splits_path_string_by_sep_and_removes_extension(
    x: List[str],
) -> None:
    z = str(pathlib.Path(*x[:-2])) + "." + x[-1]
    y = tuple(pathlib.Path(z).parts)
    assert tuple(utils.path_splitter(z)) == y[:-1] + (
        pathlib.Path(z).stem,
        pathlib.Path(z).suffix,
    )
