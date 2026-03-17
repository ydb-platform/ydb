# coding: utf-8
from __future__ import with_statement
import os
import json
import shutil

try:
    from django.urls import reverse
except ImportError:
    from django.core.urlresolvers import reverse
from django.utils.http import urlencode
from mock import patch

from filebrowser.settings import VERSIONS, DEFAULT_PERMISSIONS
from filebrowser.base import FileObject
from filebrowser.sites import site
from tests.base import FilebrowserTestCase as TestCase


class BrowseViewTests(TestCase):
    def setUp(self):
        super(BrowseViewTests, self).setUp()
        self.url = reverse('filebrowser:fb_browse')
        self.client.login(username=self.user.username, password='password')

    def test_get(self):
        response = self.client.get(self.url)
        self.assertTrue(response.status_code == 200)
        self.assertTrue('filebrowser/index.html' in [t.name for t in response.templates])

        # Check directory was set correctly in the context. If this fails, it may indicate
        # that two sites were instantiated with the same name.
        self.assertTrue(site.directory == response.context['filebrowser_site'].directory)

    def test_ckeditor_params_in_search_form(self):
        """
        The CKEditor GET params must be included in the search form as hidden
        inputs so they persist after searching.
        """
        response = self.client.get(self.url, {
            'pop': '3',
            'type': 'image',
            'CKEditor': 'id_body',
            'CKEditorFuncNum': '1',
        })

        self.assertTrue(response.status_code == 200)
        self.assertContains(response, '<input type="hidden" name="pop" value="3" />')
        self.assertContains(response, '<input type="hidden" name="type" value="image" />')
        self.assertContains(response, '<input type="hidden" name="CKEditor" value="id_body" />')
        self.assertContains(response, '<input type="hidden" name="CKEditorFuncNum" value="1" />')


class CreateDirViewTests(TestCase):
    def setUp(self):
        super(CreateDirViewTests, self).setUp()
        self.url = reverse('filebrowser:fb_createdir')
        self.client.login(username=self.user.username, password='password')

    def test_post(self):
        self.assertFalse(site.storage.exists(self.CREATEFOLDER_PATH))
        response = self.client.post(self.url, {'name': self.F_CREATEFOLDER.path_relative_directory})
        self.assertTrue(response.status_code == 302)
        self.assertTrue(site.storage.exists(self.CREATEFOLDER_PATH))


class UploadViewTests(TestCase):
    def setUp(self):
        super(UploadViewTests, self).setUp()
        self.url = reverse('filebrowser:fb_upload')
        self.client.login(username=self.user.username, password='password')

    def test_get(self):
        response = self.client.get(self.url, {'name': self.F_CREATEFOLDER.path_relative_directory})
        self.assertTrue(response.status_code == 200)
        self.assertTrue('filebrowser/upload.html' in [t.name for t in response.templates])


class UploadFileViewTests(TestCase):
    def setUp(self):
        super(UploadFileViewTests, self).setUp()
        self.url = reverse('filebrowser:fb_do_upload')
        self.url_bad_name = '?'.join([self.url, urlencode({'folder': self.F_SUBFOLDER.path_relative_directory, 'qqfile': 'TEST_IMAGE_000.jpg'})])

        self.client.login(username=self.user.username, password='password')

    def test_post(self):

        uploaded_path = os.path.join(self.F_SUBFOLDER.path, 'testimage.jpg')
        self.assertFalse(site.storage.exists(uploaded_path))

        url = '?'.join([self.url, urlencode({'folder': self.F_SUBFOLDER.path_relative_directory})])

        with open(self.STATIC_IMG_PATH, "rb") as f:
            file_size = os.path.getsize(f.name)
            response = self.client.post(url, data={'qqfile': 'testimage.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')

        # Check we get OK response
        self.assertTrue(response.status_code == 200)
        data = json.loads(response.content.decode('utf-8'))
        self.assertEqual(data["filename"], "testimage.jpg")
        self.assertEqual(data["temp_filename"], None)

        # Check the file now exists
        self.testfile = FileObject(uploaded_path, site=site)
        self.assertTrue(site.storage.exists(uploaded_path))

        # Check the file has the correct size
        self.assertTrue(file_size == site.storage.size(uploaded_path))

        # Check permissions
        # TODO: break out into separate test
        if DEFAULT_PERMISSIONS is not None:
            permissions_default = oct(DEFAULT_PERMISSIONS)
            permissions_file = oct(os.stat(self.testfile.path_full).st_mode & 0o777)
            self.assertTrue(permissions_default == permissions_file)

    @patch('filebrowser.sites.UPLOAD_TEMPDIR', '_test/tempfolder')
    def test_do_temp_upload(self):
        """
        Test the temporary upload (used with the FileBrowseUploadField)

        TODO: This is undocumented.
        """

        uploaded_path = os.path.join(self.F_TEMPFOLDER.path, 'testimage.jpg')
        self.assertFalse(site.storage.exists(uploaded_path))

        # TODO: Why is folder required to be temp? Shouldn't it use tempfolder
        # regardless of what is specified?
        url = reverse('filebrowser:fb_do_upload')
        url = '?'.join([url, urlencode({'folder': self.F_TEMPFOLDER.path_relative_directory, 'qqfile': 'testimage.jpg', 'temporary': 'true'})])

        with open(self.STATIC_IMG_PATH, "rb") as f:
            file_size = os.path.getsize(f.name)
            response = self.client.post(url, data={'qqfile': 'testimage.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')

        # Check we get OK response
        self.assertTrue(response.status_code == 200)
        data = json.loads(response.content.decode('utf-8'))
        self.assertEqual(data["filename"], "testimage.jpg")
        self.assertEqual(data["temp_filename"], os.path.join(self.F_TEMPFOLDER.path_relative_directory, "testimage.jpg"))

        # Check the file now exists
        self.testfile = FileObject(uploaded_path, site=site)
        self.assertTrue(site.storage.exists(uploaded_path))

        # Check the file has the correct size
        self.assertTrue(file_size == site.storage.size(uploaded_path))

        # Check permissions
        if DEFAULT_PERMISSIONS is not None:
            permissions_default = oct(DEFAULT_PERMISSIONS)
            permissions_file = oct(os.stat(self.testfile.path_full).st_mode & 0o777)
            self.assertTrue(permissions_default == permissions_file)

    @patch('filebrowser.sites.OVERWRITE_EXISTING', True)
    def test_overwrite_existing_true(self):
        shutil.copy(self.STATIC_IMG_PATH, self.SUBFOLDER_PATH)
        self.assertEqual(site.storage.listdir(self.F_SUBFOLDER.path), ([], [u'testimage.jpg']))

        url = '?'.join([self.url, urlencode({'folder': self.F_SUBFOLDER.path_relative_directory})])

        with open(self.STATIC_IMG_PATH, "rb") as f:
            self.client.post(url, data={'qqfile': 'testimage.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')

        self.assertEqual(site.storage.listdir(self.F_SUBFOLDER.path), ([], [u'testimage.jpg']))

    @patch('filebrowser.sites.OVERWRITE_EXISTING', False)
    def test_overwrite_existing_false(self):
        shutil.copy(self.STATIC_IMG_PATH, self.SUBFOLDER_PATH)
        self.assertEqual(site.storage.listdir(self.F_SUBFOLDER.path), ([], [u'testimage.jpg']))

        url = '?'.join([self.url, urlencode({'folder': self.F_SUBFOLDER.path_relative_directory})])

        with open(self.STATIC_IMG_PATH, "rb") as f:
            self.client.post(url, data={'qqfile': 'testimage.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')

        self.assertEqual(len(site.storage.listdir(self.F_SUBFOLDER.path)[1]), 2)

    @patch('filebrowser.utils.CONVERT_FILENAME', False)
    @patch('filebrowser.utils.NORMALIZE_FILENAME', False)
    def test_convert_false_normalize_false(self):
        with open(self.STATIC_IMG_BAD_NAME_PATH, "rb") as f:
            self.client.post(self.url_bad_name, data={'qqfile': 'TEST_IMAGE_000.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')
            self.assertEqual(site.storage.listdir(self.F_SUBFOLDER.path), ([], [u'TEST_IMAGE_000.jpg']))

    @patch('filebrowser.utils.CONVERT_FILENAME', True)
    @patch('filebrowser.utils.NORMALIZE_FILENAME', False)
    def test_convert_true_normalize_false(self):
        with open(self.STATIC_IMG_BAD_NAME_PATH, "rb") as f:
            self.client.post(self.url_bad_name, data={'qqfile': 'TEST_IMAGE_000.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')
            self.assertEqual(site.storage.listdir(self.F_SUBFOLDER.path), ([], [u'test_image_000.jpg']))

    @patch('filebrowser.utils.CONVERT_FILENAME', False)
    @patch('filebrowser.utils.NORMALIZE_FILENAME', True)
    def test_convert_false_normalize_true(self):
        with open(self.STATIC_IMG_BAD_NAME_PATH, "rb") as f:
            self.client.post(self.url_bad_name, data={'qqfile': 'TEST_IMAGE_000.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')
            self.assertEqual(site.storage.listdir(self.F_SUBFOLDER.path), ([], [u'TEST_IMAGE_000.jpg']))

    @patch('filebrowser.utils.CONVERT_FILENAME', True)
    @patch('filebrowser.utils.NORMALIZE_FILENAME', True)
    def test_convert_true_normalize_true(self):
        with open(self.STATIC_IMG_BAD_NAME_PATH, "rb") as f:
            self.client.post(self.url_bad_name, data={'qqfile': 'TEST_IMAGE_000.jpg', 'file': f}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')
            self.assertEqual(site.storage.listdir(self.F_SUBFOLDER.path), ([], [u'test_image_000.jpg']))


class DetailViewTests(TestCase):
    def setUp(self):
        super(DetailViewTests, self).setUp()
        self.url = reverse('filebrowser:fb_detail')
        self.client.login(username=self.user.username, password='password')
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    def test_get(self):
        """ Check the detail view and version generation. Check also renaming of files. """
        response = self.client.get(self.url, {'dir': self.F_IMAGE.dirname, 'filename': self.F_IMAGE.filename})

        self.assertTrue(response.status_code == 200)

        # At this moment all versions should be generated. Check that.
        pre_rename_versions = []
        for version_suffix in VERSIONS:
            path = self.F_IMAGE.version_path(version_suffix)
            pre_rename_versions.append(path)
            self.assertTrue(site.storage.exists(path))

        # Attemp renaming the file
        url = '?'.join([self.url, urlencode({'dir': self.F_IMAGE.dirname, 'filename': self.F_IMAGE.filename})])
        response = self.client.post(url, {'name': 'testpic.jpg'})

        # Check we get 302 response for renaming
        self.assertTrue(response.status_code == 302)

        # Check the file was renamed correctly:
        self.assertTrue(site.storage.exists(os.path.join(self.F_IMAGE.head, 'testpic.jpg')))

        # Store the renamed file
        self.F_IMAGE = FileObject(os.path.join(self.F_IMAGE.head, 'testpic.jpg'), site=site)

        # Check if all pre-rename versions were deleted:
        for path in pre_rename_versions:
            self.assertFalse(site.storage.exists(path))

        # Check if all post–rename versions were deleted (resp. not being generated):
        for version_suffix in VERSIONS:
            path = self.F_IMAGE.version_path(version_suffix)
            self.assertFalse(site.storage.exists(path))


class DeleteConfirmViewTests(TestCase):
    def setUp(self):
        super(DeleteConfirmViewTests, self).setUp()
        self.url = reverse('filebrowser:fb_delete_confirm')
        self.client.login(username=self.user.username, password='password')
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    def test_get(self):
        """ Check that the delete view functions as expected. Does not check the deletion itself, that happens in test_delete(). """
        response = self.client.get(self.url, {'dir': self.F_IMAGE.dirname, 'filename': self.F_IMAGE.filename})
        self.assertTrue(response.status_code == 200)
        self.assertTrue('filebrowser/delete_confirm.html' in [t.name for t in response.templates])


class DeleteViewTests(TestCase):
    def setUp(self):
        super(DeleteViewTests, self).setUp()
        self.url = reverse('filebrowser:fb_delete')
        self.client.login(username=self.user.username, password='password')
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    def test_get(self):
        """
        Generate all versions for the uploaded file and attempt a deletion of that file.
        Finally, attempt a deletion of the tmp dir.
        """
        versions = []
        for version_suffix in VERSIONS:
            versions.append(self.F_IMAGE.version_generate(version_suffix))

        # Request the delete view
        response = self.client.get(self.url, {'dir': self.F_IMAGE.dirname, 'filename': self.F_IMAGE.filename})

        # Check we get 302 response for delete
        self.assertTrue(response.status_code == 302)

        # Check the file and its versions do not exist anymore
        self.assertFalse(site.storage.exists(self.F_IMAGE.path))
        for version in versions:
            self.assertFalse(site.storage.exists(version.path))
