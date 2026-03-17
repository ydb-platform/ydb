# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2021 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at http://babel.edgewall.org/wiki/License.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at http://babel.edgewall.org/log/.
import shlex
from freezegun import freeze_time
from datetime import datetime
from distutils.dist import Distribution
from distutils.errors import DistutilsOptionError
from distutils.log import _global_log
import logging
import os
import shutil
import sys
import time
import unittest

import pytest

from babel import __version__ as VERSION
from babel.dates import format_datetime
from babel.messages import frontend, Catalog
from babel.messages.frontend import CommandLineInterface, extract_messages, update_catalog, po_file_read_mode
from babel.util import LOCALTZ
from babel.messages.pofile import read_po, write_po
from babel._compat import StringIO

this_dir = os.path.abspath(os.path.dirname(__file__))
data_dir = os.path.join(this_dir, 'data')
project_dir = os.path.join(data_dir, 'project')
i18n_dir = os.path.join(project_dir, 'i18n')
pot_file = os.path.join(i18n_dir, 'temp.pot')


def _po_file(locale):
    return os.path.join(i18n_dir, locale, 'LC_MESSAGES', 'messages.po')


class CompileCatalogTestCase(unittest.TestCase):

    def setUp(self):
        self.olddir = os.getcwd()
        os.chdir(data_dir)
        _global_log.threshold = 5  # shut up distutils logging

        self.dist = Distribution(dict(
            name='TestProject',
            version='0.1',
            packages=['project']
        ))
        self.cmd = frontend.compile_catalog(self.dist)
        self.cmd.initialize_options()

    def tearDown(self):
        os.chdir(self.olddir)

    def test_no_directory_or_output_file_specified(self):
        self.cmd.locale = 'en_US'
        self.cmd.input_file = 'dummy'
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    def test_no_directory_or_input_file_specified(self):
        self.cmd.locale = 'en_US'
        self.cmd.output_file = 'dummy'
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)


class ExtractMessagesTestCase(unittest.TestCase):

    def setUp(self):
        self.olddir = os.getcwd()
        os.chdir(data_dir)
        _global_log.threshold = 5  # shut up distutils logging

        self.dist = Distribution(dict(
            name='TestProject',
            version='0.1',
            packages=['project']
        ))
        self.cmd = frontend.extract_messages(self.dist)
        self.cmd.initialize_options()

    def tearDown(self):
        if os.path.isfile(pot_file):
            os.unlink(pot_file)

        os.chdir(self.olddir)

    def assert_pot_file_exists(self):
        assert os.path.isfile(pot_file)

    def test_neither_default_nor_custom_keywords(self):
        self.cmd.output_file = 'dummy'
        self.cmd.no_default_keywords = True
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    def test_no_output_file_specified(self):
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    def test_both_sort_output_and_sort_by_file(self):
        self.cmd.output_file = 'dummy'
        self.cmd.sort_output = True
        self.cmd.sort_by_file = True
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    def test_invalid_file_or_dir_input_path(self):
        self.cmd.input_paths = 'nonexistent_path'
        self.cmd.output_file = 'dummy'
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    def test_input_paths_is_treated_as_list(self):
        self.cmd.input_paths = data_dir
        self.cmd.output_file = pot_file
        self.cmd.finalize_options()
        self.cmd.run()

        with open(pot_file, po_file_read_mode) as f:
            catalog = read_po(f)
        msg = catalog.get('bar')
        self.assertEqual(1, len(msg.locations))
        self.assertTrue('file1.py' in msg.locations[0][0])

    def test_input_paths_handle_spaces_after_comma(self):
        self.cmd.input_paths = '%s,  %s' % (this_dir, data_dir)
        self.cmd.output_file = pot_file
        self.cmd.finalize_options()

        self.assertEqual([this_dir, data_dir], self.cmd.input_paths)

    def test_input_dirs_is_alias_for_input_paths(self):
        self.cmd.input_dirs = this_dir
        self.cmd.output_file = pot_file
        self.cmd.finalize_options()
        # Gets listified in `finalize_options`:
        assert self.cmd.input_paths == [self.cmd.input_dirs]

    def test_input_dirs_is_mutually_exclusive_with_input_paths(self):
        self.cmd.input_dirs = this_dir
        self.cmd.input_paths = this_dir
        self.cmd.output_file = pot_file
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    @freeze_time("1994-11-11")
    def test_extraction_with_default_mapping(self):
        self.cmd.copyright_holder = 'FooBar, Inc.'
        self.cmd.msgid_bugs_address = 'bugs.address@email.tld'
        self.cmd.output_file = 'project/i18n/temp.pot'
        self.cmd.add_comments = 'TRANSLATOR:,TRANSLATORS:'

        self.cmd.finalize_options()
        self.cmd.run()

        self.assert_pot_file_exists()

        expected_content = r"""# Translations template for TestProject.
# Copyright (C) %(year)s FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, %(year)s.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: %(date)s\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'year': time.strftime('%Y'),
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(pot_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_extraction_with_mapping_file(self):
        self.cmd.copyright_holder = 'FooBar, Inc.'
        self.cmd.msgid_bugs_address = 'bugs.address@email.tld'
        self.cmd.mapping_file = 'mapping.cfg'
        self.cmd.output_file = 'project/i18n/temp.pot'
        self.cmd.add_comments = 'TRANSLATOR:,TRANSLATORS:'

        self.cmd.finalize_options()
        self.cmd.run()

        self.assert_pot_file_exists()

        expected_content = r"""# Translations template for TestProject.
# Copyright (C) %(year)s FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, %(year)s.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: %(date)s\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'year': time.strftime('%Y'),
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(pot_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_extraction_with_mapping_dict(self):
        self.dist.message_extractors = {
            'project': [
                ('**/ignored/**.*', 'ignore', None),
                ('**.py', 'python', None),
            ]
        }
        self.cmd.copyright_holder = 'FooBar, Inc.'
        self.cmd.msgid_bugs_address = 'bugs.address@email.tld'
        self.cmd.output_file = 'project/i18n/temp.pot'
        self.cmd.add_comments = 'TRANSLATOR:,TRANSLATORS:'

        self.cmd.finalize_options()
        self.cmd.run()

        self.assert_pot_file_exists()

        expected_content = r"""# Translations template for TestProject.
# Copyright (C) %(year)s FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, %(year)s.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: %(date)s\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'year': time.strftime('%Y'),
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(pot_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    def test_extraction_add_location_file(self):
        self.dist.message_extractors = {
            'project': [
                ('**/ignored/**.*', 'ignore', None),
                ('**.py', 'python', None),
            ]
        }
        self.cmd.output_file = 'project/i18n/temp.pot'
        self.cmd.add_location = 'file'
        self.cmd.omit_header = True

        self.cmd.finalize_options()
        self.cmd.run()

        self.assert_pot_file_exists()

        expected_content = r"""#: project/file1.py
msgid "bar"
msgstr ""

#: project/file2.py
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

"""
        with open(pot_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)


class InitCatalogTestCase(unittest.TestCase):

    def setUp(self):
        self.olddir = os.getcwd()
        os.chdir(data_dir)
        _global_log.threshold = 5  # shut up distutils logging

        self.dist = Distribution(dict(
            name='TestProject',
            version='0.1',
            packages=['project']
        ))
        self.cmd = frontend.init_catalog(self.dist)
        self.cmd.initialize_options()

    def tearDown(self):
        for dirname in ['en_US', 'ja_JP', 'lv_LV']:
            locale_dir = os.path.join(i18n_dir, dirname)
            if os.path.isdir(locale_dir):
                shutil.rmtree(locale_dir)

        os.chdir(self.olddir)

    def test_no_input_file(self):
        self.cmd.locale = 'en_US'
        self.cmd.output_file = 'dummy'
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    def test_no_locale(self):
        self.cmd.input_file = 'dummy'
        self.cmd.output_file = 'dummy'
        self.assertRaises(DistutilsOptionError, self.cmd.finalize_options)

    @freeze_time("1994-11-11")
    def test_with_output_dir(self):
        self.cmd.input_file = 'project/i18n/messages.pot'
        self.cmd.locale = 'en_US'
        self.cmd.output_dir = 'project/i18n'

        self.cmd.finalize_options()
        self.cmd.run()

        po_file = _po_file('en_US')
        assert os.path.isfile(po_file)

        expected_content = r"""# English (United States) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: en_US\n"
"Language-Team: en_US <LL@li.org>\n"
"Plural-Forms: nplurals=2; plural=(n != 1)\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_keeps_catalog_non_fuzzy(self):
        self.cmd.input_file = 'project/i18n/messages_non_fuzzy.pot'
        self.cmd.locale = 'en_US'
        self.cmd.output_dir = 'project/i18n'

        self.cmd.finalize_options()
        self.cmd.run()

        po_file = _po_file('en_US')
        assert os.path.isfile(po_file)

        expected_content = r"""# English (United States) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: en_US\n"
"Language-Team: en_US <LL@li.org>\n"
"Plural-Forms: nplurals=2; plural=(n != 1)\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_correct_init_more_than_2_plurals(self):
        self.cmd.input_file = 'project/i18n/messages.pot'
        self.cmd.locale = 'lv_LV'
        self.cmd.output_dir = 'project/i18n'

        self.cmd.finalize_options()
        self.cmd.run()

        po_file = _po_file('lv_LV')
        assert os.path.isfile(po_file)

        expected_content = r"""# Latvian (Latvia) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: lv_LV\n"
"Language-Team: lv_LV <LL@li.org>\n"
"Plural-Forms: nplurals=3; plural=(n%%10==1 && n%%100!=11 ? 0 : n != 0 ? 1 :"
" 2)\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_correct_init_singular_plural_forms(self):
        self.cmd.input_file = 'project/i18n/messages.pot'
        self.cmd.locale = 'ja_JP'
        self.cmd.output_dir = 'project/i18n'

        self.cmd.finalize_options()
        self.cmd.run()

        po_file = _po_file('ja_JP')
        assert os.path.isfile(po_file)

        expected_content = r"""# Japanese (Japan) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: ja_JP\n"
"Language-Team: ja_JP <LL@li.org>\n"
"Plural-Forms: nplurals=1; plural=0\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

#. This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid "bar"
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='ja_JP')}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_supports_no_wrap(self):
        self.cmd.input_file = 'project/i18n/long_messages.pot'
        self.cmd.locale = 'en_US'
        self.cmd.output_dir = 'project/i18n'

        long_message = '"' + 'xxxxx ' * 15 + '"'

        with open('project/i18n/messages.pot', 'rb') as f:
            pot_contents = f.read().decode('latin-1')
        pot_with_very_long_line = pot_contents.replace('"bar"', long_message)
        with open(self.cmd.input_file, 'wb') as f:
            f.write(pot_with_very_long_line.encode('latin-1'))
        self.cmd.no_wrap = True

        self.cmd.finalize_options()
        self.cmd.run()

        po_file = _po_file('en_US')
        assert os.path.isfile(po_file)
        expected_content = r"""# English (United States) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: en_US\n"
"Language-Team: en_US <LL@li.org>\n"
"Plural-Forms: nplurals=2; plural=(n != 1)\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

#. This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid %(long_message)s
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en_US'),
            'long_message': long_message}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_supports_width(self):
        self.cmd.input_file = 'project/i18n/long_messages.pot'
        self.cmd.locale = 'en_US'
        self.cmd.output_dir = 'project/i18n'

        long_message = '"' + 'xxxxx ' * 15 + '"'

        with open('project/i18n/messages.pot', 'rb') as f:
            pot_contents = f.read().decode('latin-1')
        pot_with_very_long_line = pot_contents.replace('"bar"', long_message)
        with open(self.cmd.input_file, 'wb') as f:
            f.write(pot_with_very_long_line.encode('latin-1'))
        self.cmd.width = 120
        self.cmd.finalize_options()
        self.cmd.run()

        po_file = _po_file('en_US')
        assert os.path.isfile(po_file)
        expected_content = r"""# English (United States) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: en_US\n"
"Language-Team: en_US <LL@li.org>\n"
"Plural-Forms: nplurals=2; plural=(n != 1)\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

#. This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid %(long_message)s
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en_US'),
            'long_message': long_message}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)


class CommandLineInterfaceTestCase(unittest.TestCase):

    def setUp(self):
        data_dir = os.path.join(this_dir, 'data')
        self.orig_working_dir = os.getcwd()
        self.orig_argv = sys.argv
        self.orig_stdout = sys.stdout
        self.orig_stderr = sys.stderr
        sys.argv = ['pybabel']
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        os.chdir(data_dir)

        self._remove_log_handlers()
        self.cli = frontend.CommandLineInterface()

    def tearDown(self):
        os.chdir(self.orig_working_dir)
        sys.argv = self.orig_argv
        sys.stdout = self.orig_stdout
        sys.stderr = self.orig_stderr
        for dirname in ['lv_LV', 'ja_JP']:
            locale_dir = os.path.join(i18n_dir, dirname)
            if os.path.isdir(locale_dir):
                shutil.rmtree(locale_dir)
        self._remove_log_handlers()

    def _remove_log_handlers(self):
        # Logging handlers will be reused if possible (#227). This breaks the
        # implicit assumption that our newly created StringIO for sys.stderr
        # contains the console output. Removing the old handler ensures that a
        # new handler with our new StringIO instance will be used.
        log = logging.getLogger('babel')
        for handler in log.handlers:
            log.removeHandler(handler)

    def test_usage(self):
        try:
            self.cli.run(sys.argv)
            self.fail('Expected SystemExit')
        except SystemExit as e:
            self.assertEqual(2, e.code)
            self.assertEqual("""\
usage: pybabel command [options] [args]

pybabel: error: no valid command or option passed. try the -h/--help option for more information.
""", sys.stderr.getvalue().lower())

    def _run_init_catalog(self):
        i18n_dir = os.path.join(data_dir, 'project', 'i18n')
        pot_path = os.path.join(data_dir, 'project', 'i18n', 'messages.pot')
        init_argv = sys.argv + ['init', '--locale', 'en_US', '-d', i18n_dir,
                                '-i', pot_path]
        self.cli.run(init_argv)

    def test_no_duplicated_output_for_multiple_runs(self):
        self._run_init_catalog()
        first_output = sys.stderr.getvalue()
        self._run_init_catalog()
        second_output = sys.stderr.getvalue()[len(first_output):]

        # in case the log message is not duplicated we should get the same
        # output as before
        self.assertEqual(first_output, second_output)

    def test_frontend_can_log_to_predefined_handler(self):
        custom_stream = StringIO()
        log = logging.getLogger('babel')
        log.addHandler(logging.StreamHandler(custom_stream))

        self._run_init_catalog()
        self.assertNotEqual(id(sys.stderr), id(custom_stream))
        self.assertEqual('', sys.stderr.getvalue())
        assert len(custom_stream.getvalue()) > 0

    def test_help(self):
        try:
            self.cli.run(sys.argv + ['--help'])
            self.fail('Expected SystemExit')
        except SystemExit as e:
            self.assertEqual(0, e.code)
            self.assertEqual("""\
usage: pybabel command [options] [args]

options:
  --version       show program's version number and exit
  -h, --help      show this help message and exit
  --list-locales  print all known locales and exit
  -v, --verbose   print as much as possible
  -q, --quiet     print as little as possible

commands:
  compile  compile message catalogs to mo files
  extract  extract messages from source files and generate a pot file
  init     create new message catalogs from a pot file
  update   update existing message catalogs from a pot file
""", sys.stdout.getvalue().lower())

    def assert_pot_file_exists(self):
        assert os.path.isfile(pot_file)

    @freeze_time("1994-11-11")
    def test_extract_with_default_mapping(self):
        self.cli.run(sys.argv + ['extract',
                                 '--copyright-holder', 'FooBar, Inc.',
                                 '--project', 'TestProject', '--version', '0.1',
                                 '--msgid-bugs-address', 'bugs.address@email.tld',
                                 '-c', 'TRANSLATOR', '-c', 'TRANSLATORS:',
                                 '-o', pot_file, 'project'])
        self.assert_pot_file_exists()
        expected_content = r"""# Translations template for TestProject.
# Copyright (C) %(year)s FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, %(year)s.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: %(date)s\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'year': time.strftime('%Y'),
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(pot_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_extract_with_mapping_file(self):
        self.cli.run(sys.argv + ['extract',
                                 '--copyright-holder', 'FooBar, Inc.',
                                 '--project', 'TestProject', '--version', '0.1',
                                 '--msgid-bugs-address', 'bugs.address@email.tld',
                                 '--mapping', os.path.join(data_dir, 'mapping.cfg'),
                                 '-c', 'TRANSLATOR', '-c', 'TRANSLATORS:',
                                 '-o', pot_file, 'project'])
        self.assert_pot_file_exists()
        expected_content = r"""# Translations template for TestProject.
# Copyright (C) %(year)s FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, %(year)s.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: %(date)s\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'year': time.strftime('%Y'),
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(pot_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_extract_with_exact_file(self):
        """Tests that we can call extract with a particular file and only
        strings from that file get extracted. (Note the absence of strings from file1.py)
        """
        file_to_extract = os.path.join(data_dir, 'project', 'file2.py')
        self.cli.run(sys.argv + ['extract',
                                 '--copyright-holder', 'FooBar, Inc.',
                                 '--project', 'TestProject', '--version', '0.1',
                                 '--msgid-bugs-address', 'bugs.address@email.tld',
                                 '--mapping', os.path.join(data_dir, 'mapping.cfg'),
                                 '-c', 'TRANSLATOR', '-c', 'TRANSLATORS:',
                                 '-o', pot_file, file_to_extract])
        self.assert_pot_file_exists()
        expected_content = r"""# Translations template for TestProject.
# Copyright (C) %(year)s FooBar, Inc.
# This file is distributed under the same license as the TestProject
# project.
# FIRST AUTHOR <EMAIL@ADDRESS>, %(year)s.
#
#, fuzzy
msgid ""
msgstr ""
"Project-Id-Version: TestProject 0.1\n"
"Report-Msgid-Bugs-To: bugs.address@email.tld\n"
"POT-Creation-Date: %(date)s\n"
"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language-Team: LANGUAGE <LL@li.org>\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""
msgstr[1] ""

""" % {'version': VERSION,
            'year': time.strftime('%Y'),
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(pot_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_init_with_output_dir(self):
        po_file = _po_file('en_US')
        self.cli.run(sys.argv + ['init',
                                 '--locale', 'en_US',
                                 '-d', os.path.join(i18n_dir),
                                 '-i', os.path.join(i18n_dir, 'messages.pot')])
        assert os.path.isfile(po_file)
        expected_content = r"""# English (United States) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: en_US\n"
"Language-Team: en_US <LL@li.org>\n"
"Plural-Forms: nplurals=2; plural=(n != 1)\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_init_singular_plural_forms(self):
        po_file = _po_file('ja_JP')
        self.cli.run(sys.argv + ['init',
                                 '--locale', 'ja_JP',
                                 '-d', os.path.join(i18n_dir),
                                 '-i', os.path.join(i18n_dir, 'messages.pot')])
        assert os.path.isfile(po_file)
        expected_content = r"""# Japanese (Japan) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: ja_JP\n"
"Language-Team: ja_JP <LL@li.org>\n"
"Plural-Forms: nplurals=1; plural=0\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

#. This will be a translator coment,
#. that will include several lines
#: project/file1.py:8
msgid "bar"
msgstr ""

#: project/file2.py:9
msgid "foobar"
msgid_plural "foobars"
msgstr[0] ""

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    @freeze_time("1994-11-11")
    def test_init_more_than_2_plural_forms(self):
        po_file = _po_file('lv_LV')
        self.cli.run(sys.argv + ['init',
                                 '--locale', 'lv_LV',
                                 '-d', i18n_dir,
                                 '-i', os.path.join(i18n_dir, 'messages.pot')])
        assert os.path.isfile(po_file)
        expected_content = r"""# Latvian (Latvia) translations for TestProject.
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
"PO-Revision-Date: %(date)s\n"
"Last-Translator: FULL NAME <EMAIL@ADDRESS>\n"
"Language: lv_LV\n"
"Language-Team: lv_LV <LL@li.org>\n"
"Plural-Forms: nplurals=3; plural=(n%%10==1 && n%%100!=11 ? 0 : n != 0 ? 1 :"
" 2)\n"
"MIME-Version: 1.0\n"
"Content-Type: text/plain; charset=utf-8\n"
"Content-Transfer-Encoding: 8bit\n"
"Generated-By: Babel %(version)s\n"

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

""" % {'version': VERSION,
            'date': format_datetime(datetime(1994, 11, 11, 00, 00), 'yyyy-MM-dd HH:mmZ',
                                    tzinfo=LOCALTZ, locale='en')}
        with open(po_file, po_file_read_mode) as f:
            actual_content = f.read()
        self.assertEqual(expected_content, actual_content)

    def test_compile_catalog(self):
        po_file = _po_file('de_DE')
        mo_file = po_file.replace('.po', '.mo')
        self.cli.run(sys.argv + ['compile',
                                 '--locale', 'de_DE',
                                 '-d', i18n_dir])
        assert not os.path.isfile(mo_file), 'Expected no file at %r' % mo_file
        self.assertEqual("""\
catalog %s is marked as fuzzy, skipping
""" % po_file, sys.stderr.getvalue())

    def test_compile_fuzzy_catalog(self):
        po_file = _po_file('de_DE')
        mo_file = po_file.replace('.po', '.mo')
        try:
            self.cli.run(sys.argv + ['compile',
                                     '--locale', 'de_DE', '--use-fuzzy',
                                     '-d', i18n_dir])
            assert os.path.isfile(mo_file)
            self.assertEqual("""\
compiling catalog %s to %s
""" % (po_file, mo_file), sys.stderr.getvalue())
        finally:
            if os.path.isfile(mo_file):
                os.unlink(mo_file)

    def test_compile_catalog_with_more_than_2_plural_forms(self):
        po_file = _po_file('ru_RU')
        mo_file = po_file.replace('.po', '.mo')
        try:
            self.cli.run(sys.argv + ['compile',
                                     '--locale', 'ru_RU', '--use-fuzzy',
                                     '-d', i18n_dir])
            assert os.path.isfile(mo_file)
            self.assertEqual("""\
compiling catalog %s to %s
""" % (po_file, mo_file), sys.stderr.getvalue())
        finally:
            if os.path.isfile(mo_file):
                os.unlink(mo_file)

    def test_compile_catalog_multidomain(self):
        po_foo = os.path.join(i18n_dir, 'de_DE', 'LC_MESSAGES', 'foo.po')
        po_bar = os.path.join(i18n_dir, 'de_DE', 'LC_MESSAGES', 'bar.po')
        mo_foo = po_foo.replace('.po', '.mo')
        mo_bar = po_bar.replace('.po', '.mo')
        try:
            self.cli.run(sys.argv + ['compile',
                                     '--locale', 'de_DE', '--domain', 'foo bar', '--use-fuzzy',
                                     '-d', i18n_dir])
            for mo_file in [mo_foo, mo_bar]:
                assert os.path.isfile(mo_file)
            self.assertEqual("""\
compiling catalog %s to %s
compiling catalog %s to %s
""" % (po_foo, mo_foo, po_bar, mo_bar), sys.stderr.getvalue())

        finally:
            for mo_file in [mo_foo, mo_bar]:
                if os.path.isfile(mo_file):
                    os.unlink(mo_file)

    def test_update(self):
        template = Catalog()
        template.add("1")
        template.add("2")
        template.add("3")
        tmpl_file = os.path.join(i18n_dir, 'temp-template.pot')
        with open(tmpl_file, "wb") as outfp:
            write_po(outfp, template)
        po_file = os.path.join(i18n_dir, 'temp1.po')
        self.cli.run(sys.argv + ['init',
                                 '-l', 'fi',
                                 '-o', po_file,
                                 '-i', tmpl_file
                                 ])
        with open(po_file, "r") as infp:
            catalog = read_po(infp)
            assert len(catalog) == 3

        # Add another entry to the template

        template.add("4")

        with open(tmpl_file, "wb") as outfp:
            write_po(outfp, template)

        self.cli.run(sys.argv + ['update',
                                 '-l', 'fi_FI',
                                 '-o', po_file,
                                 '-i', tmpl_file])

        with open(po_file, "r") as infp:
            catalog = read_po(infp)
            assert len(catalog) == 4  # Catalog was updated


def test_parse_mapping():
    buf = StringIO(
        '[extractors]\n'
        'custom = mypackage.module:myfunc\n'
        '\n'
        '# Python source files\n'
        '[python: **.py]\n'
        '\n'
        '# Genshi templates\n'
        '[genshi: **/templates/**.html]\n'
        'include_attrs =\n'
        '[genshi: **/templates/**.txt]\n'
        'template_class = genshi.template:TextTemplate\n'
        'encoding = latin-1\n'
        '\n'
        '# Some custom extractor\n'
        '[custom: **/custom/*.*]\n')

    method_map, options_map = frontend.parse_mapping(buf)
    assert len(method_map) == 4

    assert method_map[0] == ('**.py', 'python')
    assert options_map['**.py'] == {}
    assert method_map[1] == ('**/templates/**.html', 'genshi')
    assert options_map['**/templates/**.html']['include_attrs'] == ''
    assert method_map[2] == ('**/templates/**.txt', 'genshi')
    assert (options_map['**/templates/**.txt']['template_class']
            == 'genshi.template:TextTemplate')
    assert options_map['**/templates/**.txt']['encoding'] == 'latin-1'

    assert method_map[3] == ('**/custom/*.*', 'mypackage.module:myfunc')
    assert options_map['**/custom/*.*'] == {}


def test_parse_keywords():
    kw = frontend.parse_keywords(['_', 'dgettext:2',
                                  'dngettext:2,3', 'pgettext:1c,2'])
    assert kw == {
        '_': None,
        'dgettext': (2,),
        'dngettext': (2, 3),
        'pgettext': ((1, 'c'), 2),
    }


def configure_cli_command(cmdline):
    """
    Helper to configure a command class, but not run it just yet.

    :param cmdline: The command line (sans the executable name)
    :return: Command instance
    """
    args = shlex.split(cmdline)
    cli = CommandLineInterface()
    cmdinst = cli._configure_command(cmdname=args[0], argv=args[1:])
    return cmdinst


def configure_distutils_command(cmdline):
    """
    Helper to configure a command class, but not run it just yet.

    This will have strange side effects if you pass in things
    `distutils` deals with internally.

    :param cmdline: The command line (sans the executable name)
    :return: Command instance
    """
    d = Distribution(attrs={
        "cmdclass": vars(frontend),
        "script_args": shlex.split(cmdline),
    })
    d.parse_command_line()
    assert len(d.commands) == 1
    cmdinst = d.get_command_obj(d.commands[0])
    cmdinst.ensure_finalized()
    return cmdinst


@pytest.mark.parametrize("split", (False, True))
@pytest.mark.parametrize("arg_name", ("-k", "--keyword", "--keywords"))
def test_extract_keyword_args_384(split, arg_name):
    # This is a regression test for https://github.com/python-babel/babel/issues/384
    # and it also tests that the rest of the forgotten aliases/shorthands implied by
    # https://github.com/python-babel/babel/issues/390 are re-remembered (or rather
    # that the mechanism for remembering them again works).

    kwarg_specs = [
        "gettext_noop",
        "gettext_lazy",
        "ngettext_lazy:1,2",
        "ugettext_noop",
        "ugettext_lazy",
        "ungettext_lazy:1,2",
        "pgettext_lazy:1c,2",
        "npgettext_lazy:1c,2,3",
    ]

    if split:  # Generate a command line with multiple -ks
        kwarg_text = " ".join("%s %s" % (arg_name, kwarg_spec) for kwarg_spec in kwarg_specs)
    else:  # Generate a single space-separated -k
        kwarg_text = "%s \"%s\"" % (arg_name, " ".join(kwarg_specs))

    # (Both of those invocation styles should be equivalent, so there is no parametrization from here on out)

    cmdinst = configure_cli_command(
        "extract -F babel-django.cfg --add-comments Translators: -o django232.pot %s ." % kwarg_text
    )
    assert isinstance(cmdinst, extract_messages)
    assert set(cmdinst.keywords.keys()) == {'_', 'dgettext', 'dngettext',
                                            'gettext', 'gettext_lazy',
                                            'gettext_noop', 'N_', 'ngettext',
                                            'ngettext_lazy', 'npgettext',
                                            'npgettext_lazy', 'pgettext',
                                            'pgettext_lazy', 'ugettext',
                                            'ugettext_lazy', 'ugettext_noop',
                                            'ungettext', 'ungettext_lazy'}


@pytest.mark.parametrize("kwarg,expected", [
    ("LW_", ("LW_",)),
    ("LW_ QQ Q", ("LW_", "QQ", "Q")),
    ("yiy         aia", ("yiy", "aia")),
])
def test_extract_distutils_keyword_arg_388(kwarg, expected):
    # This is a regression test for https://github.com/python-babel/babel/issues/388

    # Note that distutils-based commands only support a single repetition of the same argument;
    # hence `--keyword ignored` will actually never end up in the output.

    cmdinst = configure_distutils_command(
        "extract_messages --no-default-keywords --keyword ignored --keyword '%s' "
        "--input-dirs . --output-file django233.pot --add-comments Bar,Foo" % kwarg
    )
    assert isinstance(cmdinst, extract_messages)
    assert set(cmdinst.keywords.keys()) == set(expected)

    # Test the comma-separated comment argument while we're at it:
    assert set(cmdinst.add_comments) == {"Bar", "Foo"}


def test_update_catalog_boolean_args():
    cmdinst = configure_cli_command("update --no-wrap -N --ignore-obsolete --previous -i foo -o foo -l en")
    assert isinstance(cmdinst, update_catalog)
    assert cmdinst.no_wrap is True
    assert cmdinst.no_fuzzy_matching is True
    assert cmdinst.ignore_obsolete is True
    assert cmdinst.previous is False  # Mutually exclusive with no_fuzzy_matching


def test_extract_cli_knows_dash_s():
    # This is a regression test for https://github.com/python-babel/babel/issues/390
    cmdinst = configure_cli_command("extract -s -o foo babel")
    assert isinstance(cmdinst, extract_messages)
    assert cmdinst.strip_comments


def test_extract_add_location():
    cmdinst = configure_cli_command("extract -o foo babel --add-location full")
    assert isinstance(cmdinst, extract_messages)
    assert cmdinst.add_location == 'full'
    assert not cmdinst.no_location
    assert cmdinst.include_lineno

    cmdinst = configure_cli_command("extract -o foo babel --add-location file")
    assert isinstance(cmdinst, extract_messages)
    assert cmdinst.add_location == 'file'
    assert not cmdinst.no_location
    assert not cmdinst.include_lineno

    cmdinst = configure_cli_command("extract -o foo babel --add-location never")
    assert isinstance(cmdinst, extract_messages)
    assert cmdinst.add_location == 'never'
    assert cmdinst.no_location


def test_extract_error_code(monkeypatch, capsys):
    monkeypatch.chdir(project_dir)
    cmdinst = configure_cli_command("compile --domain=messages --directory i18n --locale fi_BUGGY")
    assert cmdinst.run() == 1
    out, err = capsys.readouterr()
    # replace hack below for py2/py3 compatibility
    assert "unknown named placeholder 'merkki'" in err.replace("u'", "'")
