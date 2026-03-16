# -*- coding: utf-8 -*-
"""
Testing for the OS sorting
"""
import platform

import natsort
import pytest

try:
    import icu  # noqa: F401
except ImportError:
    has_icu = False
else:
    has_icu = True


def test_os_sorted_compound() -> None:
    given = [
        "/p/Folder (10)/file.tar.gz",
        "/p/Folder (1)/file (1).tar.gz",
        "/p/Folder/file.x1.9.tar.gz",
        "/p/Folder (2)/file.tar.gz",
        "/p/Folder (1)/file.tar.gz",
        "/p/Folder/file.x1.10.tar.gz",
    ]
    if platform.system() == "Windows":
        expected = [
            "/p/Folder/file.x1.9.tar.gz",
            "/p/Folder/file.x1.10.tar.gz",
            "/p/Folder (1)/file (1).tar.gz",
            "/p/Folder (1)/file.tar.gz",
            "/p/Folder (2)/file.tar.gz",
            "/p/Folder (10)/file.tar.gz",
        ]
    else:
        expected = [
            "/p/Folder/file.x1.9.tar.gz",
            "/p/Folder/file.x1.10.tar.gz",
            "/p/Folder (1)/file.tar.gz",
            "/p/Folder (1)/file (1).tar.gz",
            "/p/Folder (2)/file.tar.gz",
            "/p/Folder (10)/file.tar.gz",
        ]
    result = natsort.os_sorted(given)
    assert result == expected


def test_os_sorted_misc_no_fail() -> None:
    natsort.os_sorted([9, 4.3, None, float("nan")])


def test_os_sorted_key() -> None:
    given = ["foo0", "foo2", "goo1"]
    expected = ["foo0", "goo1", "foo2"]
    result = natsort.os_sorted(given, key=lambda x: x.replace("g", "f"))
    assert result == expected


def test_os_sorted_can_presort() -> None:
    given = ["a1", "a01"]
    expected = ["a01", "a1"]
    result = natsort.os_sorted(given, presort=True)
    assert result == expected


# The following is a master list of things that might give trouble
# when sorting like the file explorer.
given_characters = [
    "11111",
    "aaaaa",
    "foo0",
    "foo_0",
    "foo1",
    "foo2",
    "foo4",
    "foo10",
    "Foo3",
]
given_special = [
    "!",
    "#",
    "$",
    "%",
    "&",
    "'",
    "(",
    ")",
    "+",
    "+11111",
    "+aaaaa",
    ",",
    "-",
    ";",
    "=",
    "@",
    "[",
    "]",
    "^",
    "_",
    "`",
    "{",
    "}",
    "~",
    "§",
    "°",
    "´",
    "µ",
    "€",
]

# The expceted values change based on the environment
if platform.system() == "Windows":
    given = given_characters + given_special
    expected = [
        "'",
        "-",
        "!",
        "#",
        "$",
        "%",
        "&",
        "(",
        ")",
        ",",
        ";",
        "@",
        "[",
        "]",
        "^",
        "_",
        "`",
        "{",
        "}",
        "~",
        "´",
        "€",
        "+",
        "+11111",
        "+aaaaa",
        "=",
        "§",
        "°",
        "µ",
        "11111",
        "aaaaa",
        "foo_0",
        "foo0",
        "foo1",
        "foo2",
        "Foo3",
        "foo4",
        "foo10",
    ]

elif has_icu:
    given = given_characters + given_special
    expected = [
        "_",
        "-",
        ",",
        ";",
        "!",
        "'",
        "(",
        ")",
        "[",
        "]",
        "{",
        "}",
        "§",
        "@",
        "&",
        "#",
        "%",
        "`",
        "´",
        "^",
        "°",
        "+",
        "+11111",
        "+aaaaa",
        "=",
        "~",
        "$",
        "€",
        "11111",
        "aaaaa",
        "foo_0",
        "foo0",
        "foo1",
        "foo2",
        "Foo3",
        "foo4",
        "foo10",
        "µ",
    ]
else:
    # For non-ICU UNIX, the order is all over the place
    # from platform to platform, distribution to distribution.
    # It's not really possible to predict the order across all
    # the different OS. To work around this, we will exclude
    # the special characters from the sort.
    given = given_characters
    expected = [
        "11111",
        "aaaaa",
        "foo0",
        "foo1",
        "foo2",
        "Foo3",
        "foo4",
        "foo10",
        "foo_0",
    ]


@pytest.mark.usefixtures("with_locale_en_us")
def test_os_sorted_corpus() -> None:
    result = natsort.os_sorted(given)
    print(result)
    assert result == expected
