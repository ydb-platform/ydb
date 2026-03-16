#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2025 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at https://github.com/python-babel/babel/blob/master/LICENSE.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at https://github.com/python-babel/babel/commits/master/.

from io import BytesIO, StringIO

import pytest

from babel.core import Locale
from babel.messages import pofile
from babel.messages.catalog import Catalog
from babel.messages.pofile import _enclose_filename_if_necessary, _extract_locations


def test_enclosed_filenames_in_location_comment():
    catalog = Catalog()
    catalog.add("foo", lineno=2, locations=[("main 1.py", 1)], string="")
    catalog.add("bar", lineno=6, locations=[("other.py", 2)], string="")
    catalog.add("baz", lineno=10, locations=[("main 1.py", 3), ("other.py", 4)], string="")
    buf = BytesIO()
    pofile.write_po(buf, catalog, omit_header=True, include_lineno=True)
    buf.seek(0)
    catalog2 = pofile.read_po(buf)
    assert True is catalog.is_identical(catalog2)


def test_unescape():
    escaped = '"Say:\\n  \\"hello, world!\\"\\n"'
    unescaped = 'Say:\n  "hello, world!"\n'
    assert unescaped != escaped
    assert unescaped == pofile.unescape(escaped)


def test_unescape_of_quoted_newline():
    # regression test for #198
    assert pofile.unescape(r'"\\n"') == '\\n'


def test_denormalize_on_msgstr_without_empty_first_line():
    # handle irregular multi-line msgstr (no "" as first line)
    # gracefully (#171)
    msgstr = '"multi-line\\n"\n" translation"'
    expected_denormalized = 'multi-line\n translation'

    assert expected_denormalized == pofile.denormalize(msgstr)
    assert expected_denormalized == pofile.denormalize(f'""\n{msgstr}')


@pytest.mark.parametrize(("line", "locations"), [
    ("\u2068file1.po\u2069", ["file1.po"]),
    ("file1.po \u2068file 2.po\u2069 file3.po", ["file1.po", "file 2.po", "file3.po"]),
    ("file1.po:1 \u2068file 2.po\u2069:2 file3.po:3", ["file1.po:1", "file 2.po:2", "file3.po:3"]),
    ("\u2068file1.po\u2069:1 \u2068file\t2.po\u2069:2 file3.po:3",
     ["file1.po:1", "file\t2.po:2", "file3.po:3"]),
    ("file1.po  file2.po", ["file1.po", "file2.po"]),
    ("file1.po \u2068\u2069 file2.po", ["file1.po", "file2.po"]),
])
def test_extract_locations_valid_location_comment(line, locations):
    assert locations == _extract_locations(line)


@pytest.mark.parametrize(("line",), [
    ("\u2068file 1.po",),
    ("file 1.po\u2069",),
    ("\u2069file 1.po\u2068",),
    ("\u2068file 1.po:1 \u2068file 2.po\u2069:2",),
    ("\u2068file 1.po\u2069:1 file 2.po\u2069:2",),
])
def test_extract_locations_invalid_location_comment(line):
    with pytest.raises(ValueError):
        _extract_locations(line)


@pytest.mark.parametrize(("filename",), [
    ("file.po",),
    ("file_a.po",),
    ("file-a.po",),
    ("file\n.po",),
    ("\u2068file.po\u2069",),
    ("\u2068file a.po\u2069",),
])
def test_enclose_filename_if_necessary_no_change(filename):
    assert filename == _enclose_filename_if_necessary(filename)


@pytest.mark.parametrize(("filename",), [
    ("file a.po",),
    ("file\ta.po",),
])
def test_enclose_filename_if_necessary_enclosed(filename):
    assert "\u2068" + filename + "\u2069" == _enclose_filename_if_necessary(filename)


def test_unknown_language_roundtrip():
    buf = StringIO(r'''
msgid ""
msgstr ""
"Language: sr_SP\n"''')
    catalog = pofile.read_po(buf)
    assert catalog.locale_identifier == 'sr_SP'
    assert not catalog.locale
    buf = BytesIO()
    pofile.write_po(buf, catalog)
    assert 'sr_SP' in buf.getvalue().decode()


def test_unknown_language_write():
    catalog = Catalog(locale='sr_SP')
    assert catalog.locale_identifier == 'sr_SP'
    assert not catalog.locale
    buf = BytesIO()
    pofile.write_po(buf, catalog)
    assert 'sr_SP' in buf.getvalue().decode()


def test_iterable_of_strings():
    """
    Test we can parse from an iterable of strings.
    """
    catalog = pofile.read_po(['msgid "foo"', 'msgstr "Voh"'], locale="en_US")
    assert catalog.locale == Locale("en", "US")
    assert catalog.get("foo").string == "Voh"


@pytest.mark.parametrize("order", [1, -1])
def test_iterable_of_mismatching_strings(order):
    # Mixing and matching byteses and strs in the same read_po call is not allowed.
    with pytest.raises(Exception):  # noqa: B017 (will raise either TypeError or AttributeError)
        pofile.read_po(['msgid "foo"', b'msgstr "Voh"'][::order])


def test_issue_1087():
    buf = StringIO(r'''
msgid ""
msgstr ""
"Language: \n"
''')
    assert pofile.read_po(buf).locale is None


@pytest.mark.parametrize("case", ['msgid "foo"', 'msgid "foo"\nmsgid_plural "foos"'])
@pytest.mark.parametrize("abort_invalid", [False, True])
def test_issue_1134(case: str, abort_invalid: bool):
    buf = StringIO(case)

    if abort_invalid:
        # Catalog not created, aborted with PoFileError
        with pytest.raises(pofile.PoFileError, match="missing msgstr for msgid 'foo' on 0"):
            pofile.read_po(buf, abort_invalid=True)
    else:
        # Catalog is created with warning, no abort
        output = pofile.read_po(buf)
        assert len(output) == 1
        assert output["foo"].string in ((''), ('', ''))
