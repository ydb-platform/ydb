# -*- coding: utf-8 -*-
"""\
Here are a collection of examples of how this module can be used.
See the README or the natsort homepage for more details.
"""

import os
from typing import List, Tuple, Union

import pytest
from natsort import natsort_key, natsort_keygen, natsorted, ns
from natsort.compat.locale import get_strxfrm, null_string_locale
from natsort.ns_enum import NSType
from natsort.utils import BytesTransform, FinalTransform
from pytest_mock import MockerFixture


@pytest.fixture
def arbitrary_input() -> List[Union[str, float]]:
    return ["6A-5.034e+1", "/Folder (1)/Foo", 56.7]


@pytest.fixture
def bytes_input() -> bytes:
    return b"6A-5.034e+1"


def test_natsort_keygen_demonstration() -> None:
    original_list = ["a50", "a51.", "a50.31", "a50.4", "a5.034e1", "a50.300"]
    copy_of_list = original_list[:]
    original_list.sort(key=natsort_keygen(alg=ns.F))
    # natsorted uses the output of natsort_keygen under the hood.
    assert original_list == natsorted(copy_of_list, alg=ns.F)


def test_natsort_key_public() -> None:
    assert natsort_key("a-5.034e2") == ("a-", 5, ".", 34, "e", 2)


def test_natsort_keygen_with_invalid_alg_input_raises_value_error() -> None:
    # Invalid arguments give the correct response
    with pytest.raises(ValueError, match="'alg' argument"):
        natsort_keygen(None, "1")  # type: ignore


@pytest.mark.parametrize(
    "alg, expected",
    [(ns.DEFAULT, ("a-", 5, ".", 34, "e", 1)), (ns.FLOAT | ns.SIGNED, ("a", -50.34))],
)
def test_natsort_keygen_returns_natsort_key_that_parses_input(
    alg: NSType, expected: Tuple[Union[str, int, float], ...]
) -> None:
    ns_key = natsort_keygen(alg=alg)
    assert ns_key("a-5.034e1") == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (
            ns.DEFAULT,
            (("", 6, "A-", 5, ".", 34, "e+", 1), ("/Folder (", 1, ")/Foo"), ("", 56.7)),
        ),
        (
            ns.IGNORECASE,
            (("", 6, "a-", 5, ".", 34, "e+", 1), ("/folder (", 1, ")/foo"), ("", 56.7)),
        ),
        (ns.REAL, (("", 6.0, "A", -50.34), ("/Folder (", 1.0, ")/Foo"), ("", 56.7))),
        (
            ns.LOWERCASEFIRST | ns.FLOAT | ns.NOEXP,
            (
                ("", 6.0, "a-", 5.034, "E+", 1.0),
                ("/fOLDER (", 1.0, ")/fOO"),
                ("", 56.7),
            ),
        ),
        (
            ns.PATH | ns.GROUPLETTERS,
            (
                (("", 6, "aA--", 5, "..", 34, "ee++", 1),),
                ((2 * os.sep,), ("fFoollddeerr  ((", 1, "))"), ("fFoooo",)),
                (("", 56.7),),
            ),
        ),
    ],
)
def test_natsort_keygen_handles_arbitrary_input(
    arbitrary_input: List[Union[str, float]], alg: NSType, expected: FinalTransform
) -> None:
    ns_key = natsort_keygen(alg=alg)
    assert ns_key(arbitrary_input) == expected


@pytest.mark.parametrize(
    "alg, expected",
    [
        (ns.DEFAULT, (b"6A-5.034e+1",)),
        (ns.IGNORECASE, (b"6a-5.034e+1",)),
        (ns.REAL, (b"6A-5.034e+1",)),
        (ns.LOWERCASEFIRST | ns.FLOAT | ns.NOEXP, (b"6A-5.034e+1",)),
        (ns.PATH | ns.GROUPLETTERS, ((b"6A-5.034e+1",),)),
    ],
)
def test_natsort_keygen_handles_bytes_input(
    bytes_input: bytes, alg: NSType, expected: BytesTransform
) -> None:
    ns_key = natsort_keygen(alg=alg)
    assert ns_key(bytes_input) == expected


@pytest.mark.parametrize(
    "alg, expected, is_dumb",
    [
        (
            ns.LOCALE,
            (
                (null_string_locale, 6, "A-", 5, ".", 34, "e+", 1),
                ("/Folder (", 1, ")/Foo"),
                (null_string_locale, 56.7),
            ),
            False,
        ),
        (
            ns.LOCALE,
            (
                (null_string_locale, 6, "aa--", 5, "..", 34, "eE++", 1),
                ("//ffoOlLdDeErR  ((", 1, "))//ffoOoO"),
                (null_string_locale, 56.7),
            ),
            True,
        ),
        (
            ns.LOCALE | ns.CAPITALFIRST,
            (
                (("",), (null_string_locale, 6, "A-", 5, ".", 34, "e+", 1)),
                (("/",), ("/Folder (", 1, ")/Foo")),
                (("",), (null_string_locale, 56.7)),
            ),
            False,
        ),
    ],
)
@pytest.mark.usefixtures("with_locale_en_us")
def test_natsort_keygen_with_locale(
    mocker: MockerFixture,
    arbitrary_input: List[Union[str, float]],
    alg: NSType,
    expected: FinalTransform,
    is_dumb: bool,
) -> None:
    # First, apply the correct strxfrm function to the string values.
    strxfrm = get_strxfrm()
    expected_tmp = [list(sub) for sub in expected]
    try:
        for i in (2, 4, 6):
            expected_tmp[0][i] = strxfrm(expected_tmp[0][i])
        for i in (0, 2):
            expected_tmp[1][i] = strxfrm(expected_tmp[1][i])
        expected = tuple(tuple(sub) for sub in expected_tmp)
    except IndexError:  # ns.LOCALE | ns.CAPITALFIRST
        expected_tmp = [[list(subsub) for subsub in sub] for sub in expected_tmp]
        for i in (2, 4, 6):
            expected_tmp[0][1][i] = strxfrm(expected_tmp[0][1][i])
        for i in (0, 2):
            expected_tmp[1][1][i] = strxfrm(expected_tmp[1][1][i])
        expected = tuple(tuple(tuple(subsub) for subsub in sub) for sub in expected_tmp)

    mocker.patch("natsort.compat.locale.dumb_sort", return_value=is_dumb)
    ns_key = natsort_keygen(alg=alg)
    assert ns_key(arbitrary_input) == expected


@pytest.mark.parametrize(
    "alg, is_dumb",
    [(ns.LOCALE, False), (ns.LOCALE, True), (ns.LOCALE | ns.CAPITALFIRST, False)],
)
@pytest.mark.usefixtures("with_locale_en_us")
def test_natsort_keygen_with_locale_bytes(
    mocker: MockerFixture, bytes_input: bytes, alg: NSType, is_dumb: bool
) -> None:
    expected = (b"6A-5.034e+1",)
    mocker.patch("natsort.compat.locale.dumb_sort", return_value=is_dumb)
    ns_key = natsort_keygen(alg=alg)
    assert ns_key(bytes_input) == expected
