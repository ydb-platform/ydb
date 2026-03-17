# -*- coding: utf-8 -*-
"""\
Here are a collection of examples of how this module can be used.
See the README or the natsort homepage for more details.
"""

import math
from operator import itemgetter
from pathlib import PurePosixPath
from typing import List, Tuple, Union

import pytest
from natsort import as_utf8, natsorted, ns
from natsort.ns_enum import NSType
from pytest import raises


@pytest.fixture
def float_list() -> List[str]:
    return ["a50", "a51.", "a50.31", "a-50", "a50.4", "a5.034e1", "a50.300"]


@pytest.fixture
def fruit_list() -> List[str]:
    return ["Apple", "corn", "Corn", "Banana", "apple", "banana"]


@pytest.fixture
def mixed_list() -> List[Union[str, int, float]]:
    return ["Ä", "0", "ä", 3, "b", 1.5, "2", "Z"]


def test_natsorted_numbers_in_ascending_order() -> None:
    given = ["a2", "a5", "a9", "a1", "a4", "a10", "a6"]
    expected = ["a1", "a2", "a4", "a5", "a6", "a9", "a10"]
    assert natsorted(given) == expected


def test_natsorted_can_sort_as_signed_floats_with_exponents(
    float_list: List[str],
) -> None:
    expected = ["a-50", "a50", "a50.300", "a50.31", "a5.034e1", "a50.4", "a51."]
    assert natsorted(float_list, alg=ns.REAL) == expected


@pytest.mark.parametrize(
    # UNSIGNED is default
    "alg",
    [ns.NOEXP | ns.FLOAT | ns.UNSIGNED, ns.NOEXP | ns.FLOAT],
)
def test_natsorted_can_sort_as_unsigned_and_ignore_exponents(
    float_list: List[str], alg: NSType
) -> None:
    expected = ["a5.034e1", "a50", "a50.300", "a50.31", "a50.4", "a51.", "a-50"]
    assert natsorted(float_list, alg=alg) == expected


# DEFAULT and INT are all equivalent.
@pytest.mark.parametrize("alg", [ns.DEFAULT, ns.INT])
def test_natsorted_can_sort_as_unsigned_ints_which_is_default(
    float_list: List[str], alg: NSType
) -> None:
    expected = ["a5.034e1", "a50", "a50.4", "a50.31", "a50.300", "a51.", "a-50"]
    assert natsorted(float_list, alg=alg) == expected


def test_natsorted_can_sort_as_signed_ints(float_list: List[str]) -> None:
    expected = ["a-50", "a5.034e1", "a50", "a50.4", "a50.31", "a50.300", "a51."]
    assert natsorted(float_list, alg=ns.SIGNED) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [(ns.UNSIGNED, ["a7", "a+2", "a-5"]), (ns.SIGNED, ["a-5", "a+2", "a7"])],
)
def test_natsorted_can_sort_with_or_without_accounting_for_sign(
    alg: NSType, expected: List[str]
) -> None:
    given = ["a-5", "a7", "a+2"]
    assert natsorted(given, alg=alg) == expected


def test_natsorted_can_sort_as_version_numbers() -> None:
    given = ["1.9.9a", "1.11", "1.9.9b", "1.11.4", "1.10.1"]
    expected = ["1.9.9a", "1.9.9b", "1.10.1", "1.11", "1.11.4"]
    assert natsorted(given) == expected


def test_natsorted_can_sorts_paths_same_as_strings() -> None:
    paths = [
        PurePosixPath("a/1/something"),
        PurePosixPath("a/2/something"),
        PurePosixPath("a/10/something"),
    ]
    assert [str(p) for p in natsorted(paths)] == natsorted([str(p) for p in paths])


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, ["0", 1.5, "2", 3, "Ä", "Z", "ä", "b"]),
        (ns.NUMAFTER, ["Ä", "Z", "ä", "b", "0", 1.5, "2", 3]),
    ],
)
def test_natsorted_handles_mixed_types(
    mixed_list: List[Union[str, int, float]],
    alg: NSType,
    expected: List[Union[str, int, float]],
) -> None:
    assert natsorted(mixed_list, alg=alg) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, [float("nan"), None, float("-inf"), 5, "25", 1e40, float("inf")]),
        (ns.NANLAST, [float("-inf"), 5, "25", 1e40, float("inf"), None, float("nan")]),
    ],
)
def test_natsorted_consistent_ordering_with_nan_and_friends(
    alg: NSType, expected: List[Union[str, float, None, int]]
) -> None:
    sentinel = math.pi
    expected = [sentinel if x != x else x for x in expected]
    given: List[Union[str, float, None, int]] = [
        float("inf"),
        float("-inf"),
        "25",
        5,
        float("nan"),
        1e40,
        None,
    ]
    result = natsorted(given, alg=alg)
    result = [sentinel if x != x else x for x in result]
    assert result == expected


def test_natsorted_with_mixed_bytes_and_str_input_raises_type_error() -> None:
    with raises(TypeError, match="bytes"):
        natsorted(["ä", b"b"])

    # ...unless you use as_utf (or some other decoder).
    assert natsorted(["ä", b"b"], key=as_utf8) == ["ä", b"b"]


def test_natsorted_raises_type_error_for_non_iterable_input() -> None:
    with raises(TypeError, match="'int' object is not iterable"):
        natsorted(100)  # type: ignore


def test_natsorted_recurses_into_nested_lists() -> None:
    given = [["a1", "a5"], ["a1", "a40"], ["a10", "a1"], ["a2", "a5"]]
    expected = [["a1", "a5"], ["a1", "a40"], ["a2", "a5"], ["a10", "a1"]]
    assert natsorted(given) == expected


def test_natsorted_applies_key_to_each_list_element_before_sorting_list() -> None:
    given = [("a", "num3"), ("b", "num5"), ("c", "num2")]
    expected = [("c", "num2"), ("a", "num3"), ("b", "num5")]
    assert natsorted(given, key=itemgetter(1)) == expected


def test_natsorted_returns_list_in_reversed_order_with_reverse_option(
    float_list: List[str],
) -> None:
    expected = natsorted(float_list)[::-1]
    assert natsorted(float_list, reverse=True) == expected


def test_natsorted_handles_filesystem_paths() -> None:
    given = [
        "/p/Folder (10)/file.tar.gz",
        "/p/Folder (1)/file (1).tar.gz",
        "/p/Folder/file.x1.9.tar.gz",
        "/p/Folder (1)/file.tar.gz",
        "/p/Folder/file.x1.10.tar.gz",
    ]
    expected_correct = [
        "/p/Folder/file.x1.10.tar.gz",
        "/p/Folder/file.x1.9.tar.gz",
        "/p/Folder (1)/file.tar.gz",
        "/p/Folder (1)/file (1).tar.gz",
        "/p/Folder (10)/file.tar.gz",
    ]
    expected_incorrect = [
        "/p/Folder (1)/file (1).tar.gz",
        "/p/Folder (1)/file.tar.gz",
        "/p/Folder (10)/file.tar.gz",
        "/p/Folder/file.x1.10.tar.gz",
        "/p/Folder/file.x1.9.tar.gz",
    ]
    # Is incorrect by default.
    assert natsorted(given, alg=ns.FLOAT) == expected_incorrect
    # Need ns.PATH to make it correct.
    assert natsorted(given, alg=ns.FLOAT | ns.PATH) == expected_correct


def test_natsorted_handles_numbers_and_filesystem_paths_simultaneously() -> None:
    # You can sort paths and numbers, not that you'd want to
    given: List[Union[str, int]] = ["/Folder (9)/file.exe", 43]
    expected: List[Union[str, int]] = [43, "/Folder (9)/file.exe"]
    assert natsorted(given, alg=ns.PATH) == expected


def test_natsorted_path_extensions_heuristic() -> None:
    # https://github.com/SethMMorton/natsort/issues/145
    given = [
        "Try.Me.Bug - 09 - One.Two.Three.[text].mkv",
        "Try.Me.Bug - 07 - One.Two.5.[text].mkv",
        "Try.Me.Bug - 08 - One.Two.Three[text].mkv",
    ]
    expected = [
        "Try.Me.Bug - 07 - One.Two.5.[text].mkv",
        "Try.Me.Bug - 08 - One.Two.Three[text].mkv",
        "Try.Me.Bug - 09 - One.Two.Three.[text].mkv",
    ]
    assert natsorted(given, alg=ns.PATH) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, ["Apple", "Banana", "Corn", "apple", "banana", "corn"]),
        (ns.IGNORECASE, ["Apple", "apple", "Banana", "banana", "corn", "Corn"]),
        (ns.LOWERCASEFIRST, ["apple", "banana", "corn", "Apple", "Banana", "Corn"]),
        (ns.GROUPLETTERS, ["Apple", "apple", "Banana", "banana", "Corn", "corn"]),
        (ns.G | ns.LF, ["apple", "Apple", "banana", "Banana", "corn", "Corn"]),
    ],
)
def test_natsorted_supports_case_handling(
    alg: NSType, expected: List[str], fruit_list: List[str]
) -> None:
    assert natsorted(fruit_list, alg=alg) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, [("A5", "a6"), ("a3", "a1")]),
        (ns.LOWERCASEFIRST, [("a3", "a1"), ("A5", "a6")]),
        (ns.IGNORECASE, [("a3", "a1"), ("A5", "a6")]),
    ],
)
def test_natsorted_supports_nested_case_handling(
    alg: NSType, expected: List[Tuple[str, str]]
) -> None:
    given = [("A5", "a6"), ("a3", "a1")]
    assert natsorted(given, alg=alg) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, ["apple", "Apple", "banana", "Banana", "corn", "Corn"]),
        (ns.CAPITALFIRST, ["Apple", "Banana", "Corn", "apple", "banana", "corn"]),
        (ns.LOWERCASEFIRST, ["Apple", "apple", "Banana", "banana", "Corn", "corn"]),
        (ns.C | ns.LF, ["apple", "banana", "corn", "Apple", "Banana", "Corn"]),
    ],
)
@pytest.mark.usefixtures("with_locale_en_us")
def test_natsorted_can_sort_using_locale(
    fruit_list: List[str], alg: NSType, expected: List[str]
) -> None:
    assert natsorted(fruit_list, alg=ns.LOCALE | alg) == expected


@pytest.mark.usefixtures("with_locale_en_us")
def test_natsorted_can_sort_locale_specific_numbers_en() -> None:
    given = ["c", "a5,467.86", "ä", "b", "a5367.86", "a5,6", "a5,50"]
    expected = ["a5,6", "a5,50", "a5367.86", "a5,467.86", "ä", "b", "c"]
    assert natsorted(given, alg=ns.LOCALE | ns.F) == expected


@pytest.mark.usefixtures("with_locale_de_de")
def test_natsorted_can_sort_locale_specific_numbers_de() -> None:
    given = ["c", "a5.467,86", "ä", "b", "a5367.86", "a5,6", "a5,50"]
    expected = ["a5,50", "a5,6", "a5367.86", "a5.467,86", "ä", "b", "c"]
    assert natsorted(given, alg=ns.LOCALE | ns.F) == expected


@pytest.mark.usefixtures("with_locale_de_de")
def test_natsorted_locale_bug_regression_test_109() -> None:
    # https://github.com/SethMMorton/natsort/issues/109
    given = ["462166", "461761"]
    expected = ["461761", "462166"]
    assert natsorted(given, alg=ns.LOCALE) == expected


@pytest.mark.usefixtures("with_locale_cs_cz")
def test_natsorted_locale_bug_regression_test_140() -> None:
    # https://github.com/SethMMorton/natsort/issues/140
    given = ["Aš", "Cheb", "Česko", "Cibulov", "Znojmo", "Žilina"]
    expected = ["Aš", "Cibulov", "Česko", "Cheb", "Znojmo", "Žilina"]
    assert natsorted(given, alg=ns.LOCALE) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, ["0", 1.5, "2", 3, "ä", "Ä", "b", "Z"]),
        (ns.NUMAFTER, ["ä", "Ä", "b", "Z", "0", 1.5, "2", 3]),
        (ns.UNGROUPLETTERS, ["0", 1.5, "2", 3, "Ä", "Z", "ä", "b"]),
        (ns.UG | ns.NA, ["Ä", "Z", "ä", "b", "0", 1.5, "2", 3]),
        # Adding PATH changes nothing.
        (ns.PATH, ["0", 1.5, "2", 3, "ä", "Ä", "b", "Z"]),
        (ns.PATH | ns.NUMAFTER, ["ä", "Ä", "b", "Z", "0", 1.5, "2", 3]),
        (ns.PATH | ns.UNGROUPLETTERS, ["0", 1.5, "2", 3, "Ä", "Z", "ä", "b"]),
        (ns.PATH | ns.UG | ns.NA, ["Ä", "Z", "ä", "b", "0", 1.5, "2", 3]),
    ],
)
@pytest.mark.usefixtures("with_locale_en_us")
def test_natsorted_handles_mixed_types_with_locale(
    mixed_list: List[Union[str, int, float]],
    alg: NSType,
    expected: List[Union[str, int, float]],
) -> None:
    assert natsorted(mixed_list, alg=ns.LOCALE | alg) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, ["73", "5039", "Banana", "apple", "corn", "~~~~~~"]),
        (ns.NUMAFTER, ["Banana", "apple", "corn", "~~~~~~", "73", "5039"]),
    ],
)
def test_natsorted_sorts_an_odd_collection_of_strings(
    alg: NSType, expected: List[str]
) -> None:
    given = ["apple", "Banana", "73", "5039", "corn", "~~~~~~"]
    assert natsorted(given, alg=alg) == expected


def test_natsorted_sorts_mixed_ascii_and_non_ascii_numbers() -> None:
    given = [
        "1st street",
        "10th street",
        "2nd street",
        "2 street",
        "1 street",
        "1street",
        "11 street",
        "street 2",
        "street 1",
        "Street 11",
        "۲ street",
        "۱ street",
        "۱street",
        "۱۲street",
        "۱۱ street",
        "street ۲",
        "street ۱",
        "street ۱",
        "street ۱۲",
        "street ۱۱",
    ]
    expected = [
        "1 street",
        "۱ street",
        "1st street",
        "1street",
        "۱street",
        "2 street",
        "۲ street",
        "2nd street",
        "10th street",
        "11 street",
        "۱۱ street",
        "۱۲street",
        "street 1",
        "street ۱",
        "street ۱",
        "street 2",
        "street ۲",
        "Street 11",
        "street ۱۱",
        "street ۱۲",
    ]
    assert natsorted(given, alg=ns.IGNORECASE) == expected


def test_natsort_sorts_consistently_with_presort() -> None:
    # Demonstrate the problem:
    # Sorting is order-dependent for values that have different
    # string representations are equiavlent numerically.
    given = ["a01", "a1.4500", "a1", "a1.45"]
    expected = ["a01", "a1", "a1.4500", "a1.45"]
    result = natsorted(given, alg=ns.FLOAT)
    assert result == expected

    given = ["a1", "a1.45", "a01", "a1.4500"]
    expected = ["a1", "a01", "a1.45", "a1.4500"]
    result = natsorted(given, alg=ns.FLOAT)
    assert result == expected

    # The solution - use "presort" which will sort the
    # input by its string representation before sorting
    # with natsorted, which gives consitent results even
    # if the numeric representation is identical
    expected = ["a01", "a1", "a1.45", "a1.4500"]

    given = ["a01", "a1.4500", "a1", "a1.45"]
    result = natsorted(given, alg=ns.FLOAT | ns.PRESORT)
    assert result == expected

    given = ["a1", "a1.45", "a01", "a1.4500"]
    result = natsorted(given, alg=ns.FLOAT | ns.PRESORT)
    assert result == expected
