# coding: utf-8

import shutil

from filebrowser.decorators import get_path, get_file
from filebrowser.sites import site
from tests.base import FilebrowserTestCase as TestCase


class GetPathTests(TestCase):

    def test_empty(self):
        self.assertEqual(get_path('', site), '')

    def test_starts_with_period(self):
        self.assertIsNone(get_path('..', site))
        self.assertIsNone(get_path('.filter/subfolder', site))

    def test_is_absolute(self):
        self.assertIsNone(get_path('/etc/password', site))
        self.assertIsNone(get_path('/folder/subfolder', site))

    def test_does_not_exist(self):
        self.assertIsNone(get_path('folder/invalid', site))

    def test_valid(self):
        self.assertTrue(get_path('folder/subfolder', site))


class GetFileTests(TestCase):

    def test_empty(self):
        # TODO: Should this return '' or 'folder'
        self.assertEqual(get_file('folder', '', site), '')

    def test_starts_with_period(self):
        self.assertIsNone(get_file('.', 'folder/subfolder', site))

    def test_filename_starts_with_period(self):
        self.assertIsNone(get_file('folder', '../folder/', site))

    def test_is_absolute(self):
        self.assertIsNone(get_file('/etc/', 'password', site))
        self.assertIsNone(get_file('/folder/subfolder', '', site))

    def test_filename_is_absolute(self):
        self.assertIsNone(get_file('folder/subfolder', '/etc/', site))

    def test_does_not_exist(self):
        self.assertIsNone(get_file('folder', 'invalid', site))
        self.assertIsNone(get_file('folder', 'invalid.jpg', site))

    def test_valid_folder(self):
        self.assertTrue(get_file('folder', 'subfolder', site))

    def test_valid_file(self):
        self.assertIsNone(get_file('folder/subfolder', 'testimage.jpg', site))
        shutil.copy(self.STATIC_IMG_PATH, self.SUBFOLDER_PATH)
        self.assertTrue(get_file('folder/subfolder', 'testimage.jpg', site))
