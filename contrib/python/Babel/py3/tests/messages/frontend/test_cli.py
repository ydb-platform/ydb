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

from __future__ import annotations

import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from io import StringIO

import pytest
from freezegun import freeze_time

from babel import __version__ as VERSION
from babel.dates import format_datetime
from babel.messages import Catalog, frontend
from babel.messages.frontend import BaseError
from babel.messages.pofile import read_po, write_po
from babel.util import LOCALTZ
from tests.messages.consts import data_dir, get_po_file_path, i18n_dir


@pytest.fixture
def cli(monkeypatch, capsys):
    pytest.skip()
    monkeypatch.chdir(data_dir)
    monkeypatch.setattr(sys, 'argv', ['pybabel'])
    _remove_log_handlers()
    yield frontend.CommandLineInterface()
    for dirname in ['lv_LV', 'ja_JP']:
        locale_dir = os.path.join(i18n_dir, dirname)
        if os.path.isdir(locale_dir):
            shutil.rmtree(locale_dir)
    _remove_log_handlers()


def _remove_log_handlers():
    # Logging handlers will be reused if possible (#227). This breaks the
    # implicit assumption that our newly created StringIO for sys.stderr
    # contains the console output. Removing the old handler ensures that a
    # new handler with our new StringIO instance will be used.
    log = logging.getLogger('babel')
    for handler in log.handlers:
        log.removeHandler(handler)


def test_usage(cli):
    with pytest.raises(SystemExit) as ei:
        cli.run(["pybabel"])
    assert ei.value.code == 2
    assert sys.stderr.getvalue().lower() == """\
usage: pybabel command [options] [args]

pybabel: error: no valid command or option passed. try the -h/--help option for more information.
"""


def test_list_locales(cli):
    """
    Test the command with the --list-locales arg.
    """
    result = cli.run(['pybabel', '--list-locales'])
    assert not result
    output = sys.stdout.getvalue()
    assert 'fr_CH' in output
    assert 'French (Switzerland)' in output
    assert "\nb'" not in output  # No bytes repr markers in output


def _run_init_catalog(cli):
    i18n_dir = os.path.join(data_dir, 'project', 'i18n')
    pot_path = os.path.join(data_dir, 'project', 'i18n', 'messages.pot')
    cli.run(['pybabel', 'init', '--locale', 'en_US', '-d', i18n_dir, '-i', pot_path])


def test_no_duplicated_output_for_multiple_runs(cli):
    _run_init_catalog(cli)
    first_output = sys.stderr.getvalue()
    _run_init_catalog(cli)
    second_output = sys.stderr.getvalue()[len(first_output):]

    # in case the log message is not duplicated we should get the same
    # output as before
    assert first_output == second_output


def test_frontend_can_log_to_predefined_handler(cli):
    custom_stream = StringIO()
    log = logging.getLogger('babel')
    log.addHandler(logging.StreamHandler(custom_stream))

    _run_init_catalog(cli)
    assert id(sys.stderr) != id(custom_stream)
    assert not sys.stderr.getvalue()
    assert custom_stream.getvalue()


def test_help(cli):
    with pytest.raises(SystemExit) as ei:
        cli.run(['pybabel', '--help'])
    assert not ei.value.code
    content = sys.stdout.getvalue().lower()
    assert 'options:' in content
    assert all(command in content for command in ('init', 'update', 'compile', 'extract'))


@freeze_time("1994-11-11")
def test_extract_with_default_mapping(cli, pot_file):
    cli.run([
        'pybabel',
        'extract',
        '--copyright-holder', 'FooBar, Inc.',
        '--project', 'TestProject', '--version', '0.1',
        '--msgid-bugs-address', 'bugs.address@email.tld',
        '-c', 'TRANSLATOR', '-c', 'TRANSLATORS:',
        '-o', pot_file, 'project',
    ])
    date = format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ', tzinfo=LOCALTZ, locale='en')
    expected_content = fr"""# Translations template for TestProject.
# Copyright (C) {time.strftime('%Y')} FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, {time.strftime('%Y')}.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: {date}\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel {VERSION}\n"

#. TRANSLATOR: This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid "bar"
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

#: project/ignored/this_wont_normally_be_here.py:11
msgid "FooBar"
msgid_plural "FooBars"
msgstr[0] ""
msgstr[1] ""

"""
    assert expected_content == pot_file.read_text()


@freeze_time("1994-11-11")
def test_extract_with_mapping_file(cli, pot_file):
    cli.run([
        'pybabel',
        'extract',
        '--copyright-holder', 'FooBar, Inc.',
        '--project', 'TestProject', '--version', '0.1',
        '--msgid-bugs-address', 'bugs.address@email.tld',
        '--mapping', os.path.join(data_dir, 'mapping.cfg'),
        '-c', 'TRANSLATOR', '-c', 'TRANSLATORS:',
        '-o', pot_file, 'project',
    ])
    date = format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ', tzinfo=LOCALTZ, locale='en')
    expected_content = fr"""# Translations template for TestProject.
# Copyright (C) {time.strftime('%Y')} FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, {time.strftime('%Y')}.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: {date}\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel {VERSION}\n"

#. TRANSLATOR: This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid "bar"
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

"""
    assert expected_content == pot_file.read_text()


@freeze_time("1994-11-11")
def test_extract_with_exact_file(cli, pot_file):
    """Tests that we can call extract with a particular file and only
    strings from that file get extracted. (Note the absence of strings from file1.py)
    """
    file_to_extract = os.path.join(data_dir, 'project', 'file2.py')
    cli.run([
        'pybabel',
        'extract',
        '--copyright-holder', 'FooBar, Inc.',
        '--project', 'TestProject', '--version', '0.1',
        '--msgid-bugs-address', 'bugs.address@email.tld',
        '--mapping', os.path.join(data_dir, 'mapping.cfg'),
        '-c', 'TRANSLATOR', '-c', 'TRANSLATORS:',
        '-o', pot_file, file_to_extract,
    ])
    date = format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ', tzinfo=LOCALTZ, locale='en')
    expected_content = fr"""# Translations template for TestProject.
# Copyright (C) {time.strftime('%Y')} FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, {time.strftime('%Y')}.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: {date}\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel {VERSION}\n"

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

"""
    assert expected_content == pot_file.read_text()


@freeze_time("1994-11-11")
def test_init_with_output_dir(cli):
    po_file = get_po_file_path('en_US')
    cli.run([
        'pybabel',
        'init',
        '--locale', 'en_US',
        '-d', os.path.join(i18n_dir),
        '-i', os.path.join(i18n_dir, 'messages.pot'),
    ])
    date = format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ', tzinfo=LOCALTZ, locale='en')
    expected_content = fr"""# English (United States) translations for TestProject.
# Copyright (C) 2007 FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, 2007.
#
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: 2007-04-01 15:30+0200\n"
"PO-Revision-Date: {date}\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: en_US\n"
"Language-Team: en_US <LL@li.org>\n"
"Plural-Forms: nplurals=2; plural=(n != 1);\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel {VERSION}\n"

#. This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid "bar"
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

"""
    with open(po_file) as f:
        actual_content = f.read()
    assert expected_content == actual_content


@freeze_time("1994-11-11")
def test_init_singular_plural_forms(cli):
    po_file = get_po_file_path('ja_JP')
    cli.run([
        'pybabel',
        'init',
        '--locale', 'ja_JP',
        '-d', os.path.join(i18n_dir),
        '-i', os.path.join(i18n_dir, 'messages.pot'),
    ])
    date = format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ', tzinfo=LOCALTZ, locale='en')
    expected_content = fr"""# Japanese (Japan) translations for TestProject.
# Copyright (C) 2007 FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, 2007.
#
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: 2007-04-01 15:30+0200\n"
"PO-Revision-Date: {date}\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: ja_JP\n"
"Language-Team: ja_JP <LL@li.org>\n"
"Plural-Forms: nplurals=1; plural=0;\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel {VERSION}\n"

#. This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid "bar"
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""

"""
    with open(po_file) as f:
        actual_content = f.read()
    assert expected_content == actual_content


@freeze_time("1994-11-11")
def test_init_more_than_2_plural_forms(cli):
    po_file = get_po_file_path('lv_LV')
    cli.run([
        'pybabel',
        'init',
        '--locale', 'lv_LV',
        '-d', i18n_dir,
        '-i', os.path.join(i18n_dir, 'messages.pot'),
    ])
    date = format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ', tzinfo=LOCALTZ, locale='en')
    expected_content = fr"""# Latvian (Latvia) translations for TestProject.
# Copyright (C) 2007 FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, 2007.
#
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: 2007-04-01 15:30+0200\n"
"PO-Revision-Date: {date}\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: lv_LV\n"
"Language-Team: lv_LV <LL@li.org>\n"
"Plural-Forms: nplurals=3; plural=(n%10==1 && n%100!=11 ? 0 : n != 0 ? 1 :"
" 2);\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel {VERSION}\n"

#. This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid "bar"
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""
msgstr[2] ""

"""
    with open(po_file) as f:
        actual_content = f.read()
    assert expected_content == actual_content


def test_compile_catalog(cli):
    po_file = get_po_file_path('de_DE')
    mo_file = po_file.replace('.po', '.mo')
    cli.run(['pybabel', 'compile', '--locale', 'de_DE', '-d', i18n_dir])
    assert not os.path.isfile(mo_file), f'Expected no file at {mo_file!r}'
    assert sys.stderr.getvalue() == f'catalog {po_file} is marked as fuzzy, skipping\n'


def test_compile_fuzzy_catalog(cli):
    po_file = get_po_file_path('de_DE')
    mo_file = po_file.replace('.po', '.mo')
    try:
        cli.run(['pybabel', 'compile', '--locale', 'de_DE', '--use-fuzzy', '-d', i18n_dir])
        assert os.path.isfile(mo_file)
        assert sys.stderr.getvalue() == f'compiling catalog {po_file} to {mo_file}\n'
    finally:
        if os.path.isfile(mo_file):
            os.unlink(mo_file)


def test_compile_catalog_with_more_than_2_plural_forms(cli):
    po_file = get_po_file_path('ru_RU')
    mo_file = po_file.replace('.po', '.mo')
    try:
        cli.run(['pybabel', 'compile', '--locale', 'ru_RU', '--use-fuzzy', '-d', i18n_dir])
        assert os.path.isfile(mo_file)
        assert sys.stderr.getvalue() == f'compiling catalog {po_file} to {mo_file}\n'
    finally:
        if os.path.isfile(mo_file):
            os.unlink(mo_file)


def test_compile_catalog_multidomain(cli):
    po_foo = os.path.join(i18n_dir, 'de_DE', 'LC_MESSAGES', 'foo.po')
    po_bar = os.path.join(i18n_dir, 'de_DE', 'LC_MESSAGES', 'bar.po')
    mo_foo = po_foo.replace('.po', '.mo')
    mo_bar = po_bar.replace('.po', '.mo')
    try:
        cli.run([
            'pybabel', 'compile',
            '--locale', 'de_DE',
            '--domain', 'foo bar',
            '--use-fuzzy',
            '-d', i18n_dir,
        ])
        for mo_file in [mo_foo, mo_bar]:
            assert os.path.isfile(mo_file)
        assert sys.stderr.getvalue() == (
            f'compiling catalog {po_foo} to {mo_foo}\n'
            f'compiling catalog {po_bar} to {mo_bar}\n'
        )

    finally:
        for mo_file in [mo_foo, mo_bar]:
            if os.path.isfile(mo_file):
                os.unlink(mo_file)


def test_update(cli):
    template = Catalog()
    template.add("1")
    template.add("2")
    template.add("3")
    tmpl_file = os.path.join(i18n_dir, 'temp-template.pot')
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)
    po_file = os.path.join(i18n_dir, 'temp1.po')
    cli.run(['pybabel', 'init', '-l', 'fi', '-o', po_file, '-i', tmpl_file])
    with open(po_file) as infp:
        catalog = read_po(infp)
        assert len(catalog) == 3

    # Add another entry to the template

    template.add("4")

    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)

    cli.run(['pybabel', 'update', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    with open(po_file) as infp:
        catalog = read_po(infp)
        assert len(catalog) == 4  # Catalog was updated


def test_update_pot_creation_date(cli):
    template = Catalog()
    template.add("1")
    template.add("2")
    template.add("3")
    tmpl_file = os.path.join(i18n_dir, 'temp-template.pot')
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)
    po_file = os.path.join(i18n_dir, 'temp1.po')
    cli.run(['pybabel', 'init', '-l', 'fi', '-o', po_file, '-i', tmpl_file])
    with open(po_file) as infp:
        catalog = read_po(infp)
        assert len(catalog) == 3
    original_catalog_creation_date = catalog.creation_date

    # Update the template creation date
    template.creation_date -= timedelta(minutes=3)
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)

    cli.run(['pybabel', 'update', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    with open(po_file) as infp:
        catalog = read_po(infp)
        # We didn't ignore the creation date, so expect a diff
        assert catalog.creation_date != original_catalog_creation_date

    # Reset the "original"
    original_catalog_creation_date = catalog.creation_date

    # Update the template creation date again
    # This time, pass the ignore flag and expect the times are different
    template.creation_date -= timedelta(minutes=5)
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)

    cli.run(['pybabel', 'update', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file, '--ignore-pot-creation-date'])

    with open(po_file) as infp:
        catalog = read_po(infp)
        # We ignored creation date, so it should not have changed
        assert catalog.creation_date == original_catalog_creation_date


def test_check(cli):
    template = Catalog()
    template.add("1")
    template.add("2")
    template.add("3")
    tmpl_file = os.path.join(i18n_dir, 'temp-template.pot')
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)
    po_file = os.path.join(i18n_dir, 'temp1.po')
    cli.run(['pybabel', 'init', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Update the catalog file
    cli.run(['pybabel', 'update', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Run a check without introducing any changes to the template
    cli.run(['pybabel', 'update', '--check', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Add a new entry and expect the check to fail
    template.add("4")
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)

    with pytest.raises(BaseError):
        cli.run(['pybabel', 'update', '--check', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Write the latest changes to the po-file
    cli.run(['pybabel', 'update', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Update an entry and expect the check to fail
    template.add("4", locations=[("foo.py", 1)])
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)

    with pytest.raises(BaseError):
        cli.run(['pybabel', 'update', '--check', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])


def test_check_pot_creation_date(cli):
    template = Catalog()
    template.add("1")
    template.add("2")
    template.add("3")
    tmpl_file = os.path.join(i18n_dir, 'temp-template.pot')
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)
    po_file = os.path.join(i18n_dir, 'temp1.po')
    cli.run(['pybabel', 'init', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Update the catalog file
    cli.run(['pybabel', 'update', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Run a check without introducing any changes to the template
    cli.run(['pybabel', 'update', '--check', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    # Run a check after changing the template creation date
    template.creation_date = datetime.now() - timedelta(minutes=5)
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)

    # Should fail without --ignore-pot-creation-date flag
    with pytest.raises(BaseError):
        cli.run(['pybabel', 'update', '--check', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])
    # Should pass with --ignore-pot-creation-date flag
    cli.run([
        'pybabel', 'update',
        '--check',
        '-l', 'fi_FI',
        '-o', po_file,
        '-i', tmpl_file,
        '--ignore-pot-creation-date',
    ])


def test_update_init_missing(cli):
    template = Catalog()
    template.add("1")
    template.add("2")
    template.add("3")
    tmpl_file = os.path.join(i18n_dir, 'temp2-template.pot')
    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)
    po_file = os.path.join(i18n_dir, 'temp2.po')

    cli.run(['pybabel', 'update', '--init-missing', '-l', 'fi', '-o', po_file, '-i', tmpl_file])

    with open(po_file) as infp:
        catalog = read_po(infp)
        assert len(catalog) == 3

    # Add another entry to the template

    template.add("4")

    with open(tmpl_file, "wb") as outfp:
        write_po(outfp, template)

    cli.run(['pybabel', 'update', '--init-missing', '-l', 'fi_FI', '-o', po_file, '-i', tmpl_file])

    with open(po_file) as infp:
        catalog = read_po(infp)
        assert len(catalog) == 4  # Catalog was updated


def test_update_init_missing_creates_dest_dir(cli, tmp_path):
    template = Catalog()
    template.add("xyzzy")
    template.add("ferg")
    tmpl_file = tmp_path / 'temp.pot'
    with tmpl_file.open("wb") as outfp:
        write_po(outfp, template)

    dest_dir = tmp_path / 'newdir' / 'hierarchy'
    assert not dest_dir.exists()
    po_file = dest_dir / 'temp.po'

    cli.run(['pybabel', 'update', '--init-missing', '-l', 'ja', '-o', po_file, '-i', tmpl_file])
    assert dest_dir.exists()

    with po_file.open() as infp:
        assert len(read_po(infp)) == 2
