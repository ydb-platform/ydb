# coding: utf-8

import os
import sys
import shutil

from django.conf import settings
from django.core.management import call_command
from six import StringIO

from filebrowser.settings import DIRECTORY
from tests.base import FilebrowserTestCase as TestCase


class VersionGenerateCommandTests(TestCase):

    def setUp(self):
        super(VersionGenerateCommandTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)
        self.version_file = os.path.join(settings.MEDIA_ROOT, "_test/_versions/folder/testimage_large.jpg")
        # self._stdout = sys.stdout

    def tearDown(self):
        super(VersionGenerateCommandTests, self).tearDown()
        # sys.stdout = self._stdout

    def test_fb_version_generate(self):
        self.assertFalse(os.path.exists(self.version_file))

        # sys.stdout = open(os.devnull, 'wb')
        sys.stdin = StringIO("large")

        call_command('fb_version_generate', DIRECTORY)

        self.assertTrue(os.path.exists(self.version_file))
