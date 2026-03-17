# coding: utf-8
import textwrap

import pytest

from deprecated.sphinx import SphinxAdapter
from deprecated.sphinx import deprecated
from deprecated.sphinx import versionadded
from deprecated.sphinx import versionchanged


@pytest.mark.parametrize(
    "line_length, expected",
    [
        (
            50,
            textwrap.dedent(
                """
                Description of foo

                :return: nothing

                .. {directive}:: 1.2.3
                   foo has changed in this version

                   bar bar bar bar bar bar bar bar bar bar bar
                   bar bar bar bar bar bar bar bar bar bar bar
                   bar
                """
            ),
        ),
        (
            0,
            textwrap.dedent(
                """
                Description of foo

                :return: nothing

                .. {directive}:: 1.2.3
                   foo has changed in this version

                   bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar bar
                """
            ),
        ),
    ],
    ids=["wrapped", "long"],
)
@pytest.mark.parametrize("directive", ["versionchanged", "versionadded", "deprecated"])
def test_sphinx_adapter(directive, line_length, expected):
    lines = [
        "foo has changed in this version",
        "",  # newline
        "bar " * 23,  # long line
        "",  # trailing newline
    ]
    reason = "\n".join(lines)
    adapter = SphinxAdapter(directive, reason=reason, version="1.2.3", line_length=line_length)

    def foo():
        """
        Description of foo

        :return: nothing
        """

    wrapped = adapter.__call__(foo)
    expected = expected.format(directive=directive)
    assert wrapped.__doc__ == expected


@pytest.mark.parametrize("directive", ["versionchanged", "versionadded", "deprecated"])
def test_sphinx_adapter__empty_docstring(directive):
    lines = [
        "foo has changed in this version",
        "",  # newline
        "bar " * 23,  # long line
        "",  # trailing newline
    ]
    reason = "\n".join(lines)
    adapter = SphinxAdapter(directive, reason=reason, version="1.2.3", line_length=50)

    def foo():
        pass

    wrapped = adapter.__call__(foo)
    expected = textwrap.dedent(
        """
        .. {directive}:: 1.2.3
           foo has changed in this version

           bar bar bar bar bar bar bar bar bar bar bar
           bar bar bar bar bar bar bar bar bar bar bar
           bar
        """
    )
    expected = expected.format(directive=directive)
    assert wrapped.__doc__ == expected


@pytest.mark.parametrize(
    "decorator_factory, directive",
    [
        (versionadded, "versionadded"),
        (versionchanged, "versionchanged"),
        (deprecated, "deprecated"),
    ],
)
def test_decorator_accept_line_length(decorator_factory, directive):
    reason = "bar " * 30
    decorator = decorator_factory(reason=reason, version="1.2.3", line_length=50)

    def foo():
        pass

    foo = decorator(foo)

    expected = textwrap.dedent(
        """
        .. {directive}:: 1.2.3
           bar bar bar bar bar bar bar bar bar bar bar
           bar bar bar bar bar bar bar bar bar bar bar
           bar bar bar bar bar bar bar bar
        """
    )
    expected = expected.format(directive=directive)
    assert foo.__doc__ == expected
