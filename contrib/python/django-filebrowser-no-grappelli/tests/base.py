import os
import random
import shutil

import yatest.common
from django.conf import settings
from django.contrib.auth.models import User
from django.test import TestCase

from filebrowser.settings import DIRECTORY, VERSIONS_BASEDIR
from filebrowser.base import FileObject
from filebrowser.sites import site


class FilebrowserTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        super(FilebrowserTestCase, cls).setUpClass()

        if not User.objects.filter(username='testuser').count():
            cls.user = User.objects.create_user('testuser', 'test@domain.com', 'password')
            cls.user.is_staff = True
            cls.user.save()
        else:
            cls.user = User.objects.get(username='testuser')

    def setUp(self):
        self.DIRECTORY = DIRECTORY
        self.TEST_PATH = os.path.join(site.storage.location, '_test')
        self.DIRECTORY_PATH = os.path.join(site.storage.location, DIRECTORY)
        self.VERSIONS_PATH = os.path.join(site.storage.location, VERSIONS_BASEDIR)

        if os.path.exists(self.TEST_PATH):
            raise Exception('TEST_PATH Already Exists.' + self.TEST_PATH)

        self.TEMP_PATH = os.path.join(self.TEST_PATH, 'tempfolder')
        self.FOLDER_PATH = os.path.join(self.DIRECTORY_PATH, 'folder')
        self.SUBFOLDER_PATH = os.path.join(self.FOLDER_PATH, 'subfolder')
        self.CREATEFOLDER_PATH = os.path.join(self.DIRECTORY_PATH, 'create')
        self.PLACEHOLDER_PATH = os.path.join(self.DIRECTORY_PATH, 'placeholders')

        # Yandex specific
        base_dir = yatest.common.source_path(
            'contrib/python/django-filebrowser-no-grappelli/filebrowser/static/filebrowser/img'
        )
        # End yandex specific
        self.STATIC_IMG_PATH = os.path.join(base_dir, "testimage.jpg")
        self.STATIC_IMG_BAD_NAME_PATH = os.path.join(base_dir, "TEST_IMAGE_000.jpg")
        self.STATIC_IMG_BAD_PATH = os.path.join(base_dir, "test_not_an_image.jpg")

        self.F_IMAGE = FileObject(os.path.join(DIRECTORY, 'folder', "testimage.jpg"), site=site)
        self.F_IMAGE_BAD = FileObject(os.path.join(DIRECTORY, 'folder', "test_not_an_image.jpg"), site=site)
        self.F_MISSING = FileObject(os.path.join(DIRECTORY, 'folder', "missing.jpg"), site=site)
        self.F_FOLDER = FileObject(os.path.join(DIRECTORY, 'folder'), site=site)
        self.F_SUBFOLDER = FileObject(os.path.join(DIRECTORY, 'folder', 'subfolder'), site=site)
        self.F_CREATEFOLDER = FileObject(os.path.join(DIRECTORY, 'create'), site=site)
        self.F_TEMPFOLDER = FileObject(os.path.join('_test', 'tempfolder'), site=site)

        os.makedirs(self.FOLDER_PATH)
        os.makedirs(self.SUBFOLDER_PATH)

    def tearDown(self):
        shutil.rmtree(self.TEST_PATH)
