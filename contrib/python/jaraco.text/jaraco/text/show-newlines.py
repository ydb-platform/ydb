from __future__ import annotations

from typing import TYPE_CHECKING

import inflect
from more_itertools import always_iterable

import jaraco.text

if TYPE_CHECKING:
    from _typeshed import FileDescriptorOrPath


def report_newlines(filename: FileDescriptorOrPath) -> None:
    r"""
    Report the newlines in the indicated file.

    >>> tmp_path = getfixture('tmp_path')
    >>> filename = tmp_path / 'out.txt'
    >>> _ = filename.write_text('foo\nbar\n', newline='', encoding='utf-8')
    >>> report_newlines(filename)
    newline is '\n'
    >>> filename = tmp_path / 'out.txt'
    >>> _ = filename.write_text('foo\nbar\r\n', newline='', encoding='utf-8')
    >>> report_newlines(filename)
    newlines are ('\n', '\r\n')
    """
    newlines = jaraco.text.read_newlines(filename)
    count = len(tuple(always_iterable(newlines)))
    engine = inflect.engine()
    print(
        # Pyright typing issue: jaraco/inflect#210
        engine.plural_noun("newline", count),
        engine.plural_verb("is", count),
        repr(newlines),
    )
