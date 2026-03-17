#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2007 Michael Howitz, gocept gmbh & co. kg
#
# This is free software.  You may redistribute it under the terms
# of the Apache license and the GNU General Public License Version
# 2 or at your option any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Contributor(s):
#

import unittest
import os, sys
import os.path
import odf.userfield
import tempfile
import zipfile

from io import BytesIO

import yatest.common as yc

if sys.version_info[0]==3:
    unicode=str


def get_file_path(file_name):
    return os.path.join(os.path.dirname(yc.source_path(__file__)), "examples", file_name)


def get_user_fields(file_path):
    return odf.userfield.UserFields(file_path)


class TestUserFields(unittest.TestCase):

    userfields_odt = get_file_path(u"userfields.odt")
    userfields_ooo3_odt = get_file_path(u"userfields_ooo3.odt")
    no_userfields_odt = get_file_path(u"no_userfields.odt")

    def setUp(self):
        self.unlink_list = []

    def tearDown(self):
        # delete created destination files
        for filename in self.unlink_list:
            os.unlink(filename)

    def test_exception(self):
        # no zip-file
        no_zip = odf.userfield.UserFields(unicode(yc.source_path(__file__)))
        self.assertRaises(TypeError, no_zip.list_fields)
        self.assertRaises(TypeError, no_zip.update, {})

    def test_list_fields(self):
        """ Find the expected fields in the file """
        self.assertEqual([],
                         get_user_fields(self.no_userfields_odt).list_fields())
        self.assertEqual([u'username', u'firstname', u'lastname', u'address'],
                         get_user_fields(self.userfields_odt).list_fields())

    def test_list_fields_and_values(self):
        """ Find the expected fields and values in the file """
        no_user_fields = get_user_fields(self.no_userfields_odt)
        self.assertEqual([],
                         no_user_fields.list_fields_and_values())
        self.assertEqual([],
                         no_user_fields.list_fields_and_values([u'username']))
        user_fields = get_user_fields(self.userfields_odt)
        self.assertEqual([(u'username', u'string', u''),
                          (u'lastname', u'string', u'<none>')],
                         user_fields.list_fields_and_values([u'username',
                                                             u'lastname']))
        self.assertEqual(4, len(user_fields.list_fields_and_values()))

    def test_list_values(self):
        self.assertEqual(
            [],
            get_user_fields(self.no_userfields_odt).list_values([u'username']))
        self.assertEqual(
            [u'', u'<none>'],
            get_user_fields(self.userfields_odt).list_values(
                [u'username', u'lastname']))

    def test_get(self):
        user_fields = get_user_fields(self.userfields_odt)
        self.assertEqual(
            None,
            get_user_fields(self.no_userfields_odt).get(u'username'))
        self.assertEqual(u'', user_fields.get(u'username'))
        self.assertEqual(u'<none>', user_fields.get(u'lastname'))
        self.assertEqual(None, user_fields.get(u'street'))

    def test_get_type_and_value(self):
        self.assertEqual(
            None,
            get_user_fields(self.no_userfields_odt).get_type_and_value(
                u'username'))
        user_fields = get_user_fields(self.userfields_odt)
        self.assertEqual(
            (u'string', u''), user_fields.get_type_and_value(u'username'))
        self.assertEqual(
            (u'string', u'<none>'),
            user_fields.get_type_and_value(u'lastname'))
        self.assertEqual(None, user_fields.get_type_and_value(u'street'))

    def test_update(self):
        # test for file without user fields
        no_user_fields = get_user_fields(self.no_userfields_odt)
        no_user_fields.dest_file = self._get_dest_file_name()
        no_user_fields.update({u'username': u'mac'})
        dest = odf.userfield.UserFields(no_user_fields.dest_file)
        self.assertEqual([], dest.list_fields_and_values())

        # test for file with user field, including test of encoding
        user_fields = get_user_fields(self.userfields_odt)
        user_fields.dest_file = self._get_dest_file_name()
        user_fields.update({u'username': u'mac',
                            u'firstname': u'André',
                            u'street': u'I do not exist'})
        dest = odf.userfield.UserFields(user_fields.dest_file)
        self.assertEqual([(u'username', u'string', u'mac'),
                          (u'firstname', u'string', u'André'),
                          (u'lastname', u'string', u'<none>'),
                          (u'address', u'string', u'')],
                         dest.list_fields_and_values())

    def test_update_open_office_version_3(self):
        """Update fields in OpenOffice.org 3.x version of file."""
        user_fields = get_user_fields(self.userfields_ooo3_odt)
        user_fields.dest_file = self._get_dest_file_name()
        user_fields.update({u'username': u'mari',
                            u'firstname': u'Lukas',
                            u'street': u'I might exist.'})
        dest = odf.userfield.UserFields(user_fields.dest_file)
        self.assertEqual([(u'username', u'string', u'mari'),
                          (u'firstname', u'string', u'Lukas'),
                          (u'lastname', u'string', u'<none>'),
                          (u'address', u'string', u'')],
                         dest.list_fields_and_values())

    def test_stringio(self):
        # test wether it is possible to use a StringIO as src and dest
        infile=open(self.userfields_odt,'rb')
        src = BytesIO(infile.read())
        infile.close()
        dest = BytesIO()
        # update fields
        user_fields = odf.userfield.UserFields(src, dest)
        user_fields.update({u'username': u'mac',
                            u'firstname': u'André',
                            u'street': u'I do not exist'})
        # reread dest StringIO to get field values
        dest_user_fields = odf.userfield.UserFields(dest)
        self.assertEqual([(u'username', u'string', u'mac'),
                          (u'firstname', u'string', u'André'),
                          (u'lastname', u'string', u'<none>'),
                          (u'address', u'string', u'')],
                         dest_user_fields.list_fields_and_values())

    def test_newlines_in_values(self):
        # test that newlines in values are encoded correctly so that
        # they get read back correctly
        user_fields = get_user_fields(self.userfields_odt)
        user_fields.dest_file = self._get_dest_file_name()
        user_fields.update({'username': 'mac',
                            'firstname': 'mac',
                            'lastname': 'mac',
                            'address': 'Hall-Platz 3\n01234 Testheim'})
        dest = odf.userfield.UserFields(user_fields.dest_file)
        self.assertEqual([(u'username', u'string', u'mac'),
                          (u'firstname', u'string', u'mac'),
                          (u'lastname', u'string', u'mac'),
                          (u'address', u'string',
                           u'Hall-Platz 3\n01234 Testheim')],
                         dest.list_fields_and_values())

    def _get_dest_file_name(self):
        dummy_fh, dest_file_name = tempfile.mkstemp(u'.odt')
        os.close(dummy_fh)
        self.unlink_list.append(dest_file_name)
        return dest_file_name


if __name__ == '__main__':
    if sys.version_info[0]==3:
        unittest.main(warnings='ignore') # ignore warnings for unclosed files
    else:
        unittest.main()
