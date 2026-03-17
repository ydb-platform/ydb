# coding: utf-8

import os
import ntpath
import posixpath
import shutil

from mock import patch

from filebrowser.base import FileObject, FileListing
from filebrowser.sites import site
from filebrowser.settings import VERSIONS
from tests.base import FilebrowserTestCase as TestCase


class FileObjectPathTests(TestCase):

    def setUp(self):
        super(FileObjectPathTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    @patch('filebrowser.base.os.path', ntpath)
    def test_windows_paths(self):
        """
        Use ntpath to test windows paths independently from current os
        """
        f = FileObject('_test\\uploads\\folder\\testfile.jpg', site=site)

        self.assertEqual(f.path_relative_directory, 'folder\\testfile.jpg')
        self.assertEqual(f.dirname, r'folder')

    @patch('filebrowser.base.os.path', posixpath)
    def test_posix_paths(self):
        """
        Use posixpath to test posix paths independently from current os
        """
        f = FileObject('_test/uploads/folder/testfile.jpg', site=site)

        self.assertEqual(f.path_relative_directory, 'folder/testfile.jpg')
        self.assertEqual(f.dirname, r'folder')


class FileObjectUnicodeTests(TestCase):

    def setUp(self):
        super(FileObjectUnicodeTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    @patch('filebrowser.base.os.path', ntpath)
    def test_windows_paths(self):
        """
        Use ntpath to test windows paths independently from current os
        """
        f = FileObject('_test\\uploads\\$%^&*\\測試文件.jpg', site=site)

        self.assertEqual(f.path_relative_directory, '$%^&*\\測試文件.jpg')
        self.assertEqual(f.dirname, r'$%^&*')

    @patch('filebrowser.base.os.path', posixpath)
    def test_posix_paths(self):
        """
        Use posixpath to test posix paths independently from current os
        """
        f = FileObject('_test/uploads/$%^&*/測試文件.jpg', site=site)

        self.assertEqual(f.path_relative_directory, '$%^&*/測試文件.jpg')
        self.assertEqual(f.dirname, r'$%^&*')

    @patch('filebrowser.base.os.path', posixpath)
    @patch('filebrowser.namers.VERSION_NAMER', 'filebrowser.namers.OptionsNamer')
    def test_unicode_options_namer_version(self):
        path_unicode = os.path.join(self.FOLDER_PATH, '測試文件.jpg')
        expected = u'測試文件_large--680x0.jpg'

        shutil.copy(self.STATIC_IMG_PATH, path_unicode)
        f = FileObject(path_unicode, site=site)
        version = f.version_generate('large')
        self.assertEqual(version.filename, expected)


class FileObjectAttributeTests(TestCase):

    def setUp(self):
        super(FileObjectAttributeTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    def test_init_attributes(self):
        """
        FileObject init attributes

        # path
        # head
        # filename
        # filename_lower
        # filename_root
        # extension
        # mimetype
        """
        self.assertEqual(self.F_IMAGE.path, "_test/uploads/folder/testimage.jpg")
        self.assertEqual(self.F_IMAGE.head, '_test/uploads/folder')
        self.assertEqual(self.F_IMAGE.filename, 'testimage.jpg')
        self.assertEqual(self.F_IMAGE.filename_lower, 'testimage.jpg')
        self.assertEqual(self.F_IMAGE.filename_root, 'testimage')
        self.assertEqual(self.F_IMAGE.extension, '.jpg')
        self.assertEqual(self.F_IMAGE.mimetype, ('image/jpeg', None))

    def test_general_attributes(self):
        """
        FileObject general attributes

        # filetype
        # filesize
        # date
        # datetime
        # exists
        """
        self.assertEqual(self.F_IMAGE.filetype, 'Image')

        self.assertEqual(self.F_IMAGE.filetype, 'Image')
        self.assertEqual(self.F_IMAGE.filesize, 870037)
        # FIXME: test date/datetime
        self.assertEqual(self.F_IMAGE.exists, True)

    def test_path_url_attributes(self):
        """
        FileObject path and url attributes

        # path (see init)
        # path_relative_directory
        # path_full
        # dirname
        # url
        """
        # test with image
        self.assertEqual(self.F_IMAGE.path, "_test/uploads/folder/testimage.jpg")
        self.assertEqual(self.F_IMAGE.path_relative_directory, "folder/testimage.jpg")
        self.assertEqual(self.F_IMAGE.path_full, os.path.join(site.storage.location, site.directory, "folder/testimage.jpg"))
        self.assertEqual(self.F_IMAGE.dirname, "folder")
        self.assertEqual(self.F_IMAGE.url, site.storage.url(self.F_IMAGE.path))

        # test with folder
        self.assertEqual(self.F_FOLDER.path, "_test/uploads/folder")
        self.assertEqual(self.F_FOLDER.path_relative_directory, "folder")
        self.assertEqual(self.F_FOLDER.path_full, os.path.join(site.storage.location, site.directory, "folder"))
        self.assertEqual(self.F_FOLDER.dirname, "")
        self.assertEqual(self.F_FOLDER.url, site.storage.url(self.F_FOLDER.path))

        # test with alternative folder
        self.assertEqual(self.F_SUBFOLDER.path, "_test/uploads/folder/subfolder")
        self.assertEqual(self.F_SUBFOLDER.path_relative_directory, "folder/subfolder")
        self.assertEqual(self.F_SUBFOLDER.path_full, os.path.join(site.storage.location, site.directory, "folder/subfolder"))
        self.assertEqual(self.F_SUBFOLDER.dirname, "folder")
        self.assertEqual(self.F_SUBFOLDER.url, site.storage.url(self.F_SUBFOLDER.path))

    def test_image_attributes(self):
        """
        FileObject image attributes

        # dimensions
        # width
        # height
        # aspectratio
        # orientation
        """
        self.assertEqual(self.F_IMAGE.dimensions, (1000, 750))
        self.assertEqual(self.F_IMAGE.width, 1000)
        self.assertEqual(self.F_IMAGE.height, 750)
        self.assertEqual(self.F_IMAGE.aspectratio, 1.3333333333333333)
        self.assertEqual(self.F_IMAGE.orientation, 'Landscape')

    def test_folder_attributes(self):
        """
        FileObject folder attributes

        # directory (deprecated) > path_relative_directory
        # folder (deprecated) > dirname
        # is_folder
        # is_empty
        """
        # test with image
        self.assertEqual(self.F_IMAGE.path_relative_directory, "folder/testimage.jpg")  # equals path_relative_directory
        self.assertEqual(self.F_IMAGE.dirname, "folder")  # equals dirname
        self.assertEqual(self.F_IMAGE.is_folder, False)
        self.assertEqual(self.F_IMAGE.is_empty, False)

        # test with folder
        self.assertEqual(self.F_FOLDER.path_relative_directory, "folder")  # equals path_relative_directory
        self.assertEqual(self.F_FOLDER.dirname, "")  # equals dirname
        self.assertEqual(self.F_FOLDER.is_folder, True)
        self.assertEqual(self.F_FOLDER.is_empty, False)

        # test with alternative folder
        self.assertEqual(self.F_SUBFOLDER.path_relative_directory, "folder/subfolder")  # equals path_relative_directory
        self.assertEqual(self.F_SUBFOLDER.dirname, "folder")  # equals dirname
        self.assertEqual(self.F_SUBFOLDER.is_folder, True)
        self.assertEqual(self.F_SUBFOLDER.is_empty, True)

    @patch('filebrowser.base.ADMIN_VERSIONS', ['large'])
    def test_version_attributes_1(self):
        """
        FileObject version attributes/methods
        without versions_basedir

        # is_version
        # original
        # original_filename
        # versions_basedir
        # versions
        # admin_versions
        # version_name(suffix)
        # version_path(suffix)
        # version_generate(suffix)
        """
        # new settings
        version_list = sorted(['_test/_versions/folder/testimage_{}.jpg'.format(name) for name in VERSIONS.keys()])
        admin_version_list = ['_test/_versions/folder/testimage_large.jpg']

        self.assertEqual(self.F_IMAGE.is_version, False)
        self.assertEqual(self.F_IMAGE.original.path, self.F_IMAGE.path)
        self.assertEqual(self.F_IMAGE.versions_basedir, "_test/_versions/")
        self.assertEqual(self.F_IMAGE.versions(), version_list)
        self.assertEqual(self.F_IMAGE.admin_versions(), admin_version_list)
        self.assertEqual(self.F_IMAGE.version_name("large"), "testimage_large.jpg")
        self.assertEqual(self.F_IMAGE.version_path("large"), "_test/_versions/folder/testimage_large.jpg")

        # version does not exist yet
        f_version = FileObject(os.path.join(site.directory, 'folder', "testimage_large.jpg"), site=site)
        self.assertEqual(f_version.exists, False)
        # generate version
        f_version = self.F_IMAGE.version_generate("large")
        self.assertEqual(f_version.path, "_test/_versions/folder/testimage_large.jpg")
        self.assertEqual(f_version.exists, True)
        self.assertEqual(f_version.is_version, True)
        self.assertEqual(f_version.original_filename, "testimage.jpg")
        self.assertEqual(f_version.original.path, self.F_IMAGE.path)
        # FIXME: versions should not have versions or admin_versions

    @patch('filebrowser.base.ADMIN_VERSIONS', ['large'])
    def test_version_attributes_2(self):
        """
        FileObject version attributes/methods
        with versions_basedir

        # is_version
        # original
        # original_filename
        # versions_basedir
        # versions
        # admin_versions
        # version_name(suffix)
        # version_generate(suffix)
        """

        version_list = sorted(['_test/_versions/folder/testimage_{}.jpg'.format(name) for name in VERSIONS.keys()])
        admin_version_list = ['_test/_versions/folder/testimage_large.jpg']

        self.assertEqual(self.F_IMAGE.is_version, False)
        self.assertEqual(self.F_IMAGE.original.path, self.F_IMAGE.path)
        self.assertEqual(self.F_IMAGE.versions_basedir, "_test/_versions/")
        self.assertEqual(self.F_IMAGE.versions(), version_list)
        self.assertEqual(self.F_IMAGE.admin_versions(), admin_version_list)
        self.assertEqual(self.F_IMAGE.version_name("large"), "testimage_large.jpg")
        self.assertEqual(self.F_IMAGE.version_path("large"), "_test/_versions/folder/testimage_large.jpg")

        # version does not exist yet
        f_version = FileObject(os.path.join(site.directory, 'folder', "testimage_large.jpg"), site=site)
        self.assertEqual(f_version.exists, False)
        # generate version
        f_version = self.F_IMAGE.version_generate("large")
        self.assertEqual(f_version.path, "_test/_versions/folder/testimage_large.jpg")
        self.assertEqual(f_version.exists, True)
        self.assertEqual(f_version.is_version, True)
        self.assertEqual(f_version.original_filename, "testimage.jpg")
        self.assertEqual(f_version.original.path, self.F_IMAGE.path)
        self.assertEqual(f_version.versions(), [])
        self.assertEqual(f_version.admin_versions(), [])

    @patch('filebrowser.base.ADMIN_VERSIONS', ['large'])
    def test_version_attributes_3(self):
        """
        FileObject version attributes/methods
        with alternative versions_basedir

        # is_version
        # original
        # original_filename
        # versions_basedir
        # versions
        # admin_versions
        # version_name(suffix)
        # version_generate(suffix)
        """

        # new settings
        version_list = sorted(['_test/_versions/folder/testimage_{}.jpg'.format(name) for name in VERSIONS.keys()])
        admin_version_list = ['_test/_versions/folder/testimage_large.jpg']

        self.assertEqual(self.F_IMAGE.is_version, False)
        self.assertEqual(self.F_IMAGE.original.path, self.F_IMAGE.path)
        self.assertEqual(self.F_IMAGE.versions_basedir, "_test/_versions/")
        self.assertEqual(self.F_IMAGE.versions(), version_list)
        self.assertEqual(self.F_IMAGE.admin_versions(), admin_version_list)
        self.assertEqual(self.F_IMAGE.version_name("large"), "testimage_large.jpg")
        self.assertEqual(self.F_IMAGE.version_path("large"), "_test/_versions/folder/testimage_large.jpg")

        # version does not exist yet
        f_version = FileObject(os.path.join(site.directory, 'folder', "testimage_large.jpg"), site=site)
        self.assertEqual(f_version.exists, False)
        # generate version
        f_version = self.F_IMAGE.version_generate("large")
        self.assertEqual(f_version.path, "_test/_versions/folder/testimage_large.jpg")
        self.assertEqual(f_version.exists, True)
        self.assertEqual(f_version.is_version, True)
        self.assertEqual(f_version.original_filename, "testimage.jpg")
        self.assertEqual(f_version.original.path, self.F_IMAGE.path)
        self.assertEqual(f_version.versions(), [])
        self.assertEqual(f_version.admin_versions(), [])

    def test_delete(self):
        """
        FileObject delete methods

        # delete
        # delete_versions
        # delete_admin_versions
        """

        # version does not exist yet
        f_version = FileObject(os.path.join(site.directory, 'folder', "testimage_large.jpg"), site=site)
        self.assertEqual(f_version.exists, False)
        # generate version
        f_version = self.F_IMAGE.version_generate("large")
        f_version_thumb = self.F_IMAGE.version_generate("admin_thumbnail")
        self.assertEqual(f_version.exists, True)
        self.assertEqual(f_version_thumb.exists, True)
        self.assertEqual(f_version.path, "_test/_versions/folder/testimage_large.jpg")
        self.assertEqual(f_version_thumb.path, "_test/_versions/folder/testimage_admin_thumbnail.jpg")

        # delete admin versions (large)
        self.F_IMAGE.delete_admin_versions()
        self.assertEqual(site.storage.exists(f_version.path), False)

        # delete versions (admin_thumbnail)
        self.F_IMAGE.delete_versions()
        self.assertEqual(site.storage.exists(f_version_thumb.path), False)


class FileListingTests(TestCase):
    """
    /_test/uploads/testimage.jpg
    /_test/uploads/folder/
    /_test/uploads/folder/subfolder/
    /_test/uploads/folder/subfolder/testimage.jpg
    """

    def setUp(self):
        super(FileListingTests, self).setUp()

        self.F_LISTING_FOLDER = FileListing(self.DIRECTORY, sorting_by='date', sorting_order='desc')
        self.F_LISTING_IMAGE = FileListing(os.path.join(self.DIRECTORY, 'folder', 'subfolder', "testimage.jpg"))

        shutil.copy(self.STATIC_IMG_PATH, self.SUBFOLDER_PATH)
        shutil.copy(self.STATIC_IMG_PATH, self.DIRECTORY_PATH)

    def test_init_attributes(self):

        """
        FileListing init attributes

        # path
        # filter_func
        # sorting_by
        # sorting_order
        """
        self.assertEqual(self.F_LISTING_FOLDER.path, '_test/uploads/')
        self.assertEqual(self.F_LISTING_FOLDER.filter_func, None)
        self.assertEqual(self.F_LISTING_FOLDER.sorting_by, 'date')
        self.assertEqual(self.F_LISTING_FOLDER.sorting_order, 'desc')

    def test_listing(self):
        """
        FileObject listing

        # listing
        # files_listing_total
        # files_listing_filtered
        # results_listing_total
        # results_listing_filtered
        """

        self.assertEqual(self.F_LISTING_IMAGE.listing(), [])
        self.assertEqual(list(self.F_LISTING_FOLDER.listing()), [u'folder', u'testimage.jpg'])
        self.assertEqual(list(f.path for f in self.F_LISTING_FOLDER.files_listing_total()), [u'_test/uploads/testimage.jpg', u'_test/uploads/folder'])
        self.assertEqual(list(f.path for f in self.F_LISTING_FOLDER.files_listing_filtered()), [u'_test/uploads/testimage.jpg', u'_test/uploads/folder'])
        self.assertEqual(self.F_LISTING_FOLDER.results_listing_total(), 2)
        self.assertEqual(self.F_LISTING_FOLDER.results_listing_filtered(), 2)

    def test_listing_filtered(self):
        """
        FileObject listing

        # listing
        # files_listing_total
        # files_listing_filtered
        # results_listing_total
        # results_listing_filtered
        """

        self.assertEqual(self.F_LISTING_IMAGE.listing(), [])
        self.assertEqual(list(self.F_LISTING_FOLDER.listing()), [u'folder', u'testimage.jpg'])
        self.assertEqual(list(f.path for f in self.F_LISTING_FOLDER.files_listing_total()), [u'_test/uploads/testimage.jpg', u'_test/uploads/folder'])
        self.assertEqual(list(f.path for f in self.F_LISTING_FOLDER.files_listing_filtered()), [u'_test/uploads/testimage.jpg', u'_test/uploads/folder'])
        self.assertEqual(self.F_LISTING_FOLDER.results_listing_total(), 2)
        self.assertEqual(self.F_LISTING_FOLDER.results_listing_filtered(), 2)

    def test_walk(self):
        """
        FileObject walk

        # walk
        # files_walk_total
        # files_walk_filtered
        # results_walk_total
        # results_walk_filtered
        """

        self.assertEqual(self.F_LISTING_IMAGE.walk(), [])
        self.assertEqual(list(self.F_LISTING_FOLDER.walk()), [u'folder/subfolder/testimage.jpg', u'folder/subfolder', u'folder', u'testimage.jpg'])
        self.assertEqual(list(f.path for f in self.F_LISTING_FOLDER.files_walk_total()), [u'_test/uploads/testimage.jpg', u'_test/uploads/folder', u'_test/uploads/folder/subfolder', u'_test/uploads/folder/subfolder/testimage.jpg'])
        self.assertEqual(list(f.path for f in self.F_LISTING_FOLDER.files_walk_filtered()), [u'_test/uploads/testimage.jpg', u'_test/uploads/folder', u'_test/uploads/folder/subfolder', u'_test/uploads/folder/subfolder/testimage.jpg'])
        self.assertEqual(self.F_LISTING_FOLDER.results_walk_total(), 4)
        self.assertEqual(self.F_LISTING_FOLDER.results_walk_filtered(), 4)


class FileObjecNamerTests(TestCase):

    PATCH_VERSIONS = {
        'thumbnail': {'verbose_name': 'Thumbnail (1 col)', 'width': 60, 'height': 60, 'opts': 'crop'},
        'small': {'verbose_name': 'Small (2 col)', 'width': 140, 'height': '', 'opts': ''},
        'large': {'verbose_name': 'Large (8 col)', 'width': 680, 'height': '', 'opts': ''},
    }
    PATCH_ADMIN_VERSIONS = ['large']

    def setUp(self):
        super(FileObjecNamerTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    @patch('filebrowser.namers.VERSION_NAMER', 'filebrowser.namers.OptionsNamer')
    def test_init_attributes(self):
        """
        FileObject init attributes

        # path
        # head
        # filename
        # filename_lower
        # filename_root
        # extension
        # mimetype
        """
        self.assertEqual(self.F_IMAGE.path, "_test/uploads/folder/testimage.jpg")
        self.assertEqual(self.F_IMAGE.head, '_test/uploads/folder')
        self.assertEqual(self.F_IMAGE.filename, 'testimage.jpg')
        self.assertEqual(self.F_IMAGE.filename_lower, 'testimage.jpg')
        self.assertEqual(self.F_IMAGE.filename_root, 'testimage')
        self.assertEqual(self.F_IMAGE.extension, '.jpg')
        self.assertEqual(self.F_IMAGE.mimetype, ('image/jpeg', None))

    @patch('filebrowser.namers.VERSION_NAMER', 'filebrowser.namers.OptionsNamer')
    @patch('filebrowser.base.VERSIONS', PATCH_VERSIONS)
    @patch('filebrowser.base.ADMIN_VERSIONS', PATCH_ADMIN_VERSIONS)
    def test_version_attributes_with_options_namer(self):
        """
        FileObject version attributes/methods
        without versions_basedir

        # is_version
        # original
        # original_filename
        # versions_basedir
        # versions
        # admin_versions
        # version_name(suffix)
        # version_path(suffix)
        # version_generate(suffix)
        """
        # new settings
        version_list = sorted([
            '_test/_versions/folder/testimage_large--680x0.jpg',
            '_test/_versions/folder/testimage_small--140x0.jpg',
            '_test/_versions/folder/testimage_thumbnail--60x60--opts-crop.jpg'
        ])
        admin_version_list = ['_test/_versions/folder/testimage_large--680x0.jpg']

        self.assertEqual(self.F_IMAGE.is_version, False)
        self.assertEqual(self.F_IMAGE.original.path, self.F_IMAGE.path)
        self.assertEqual(self.F_IMAGE.versions_basedir, "_test/_versions/")
        self.assertEqual(self.F_IMAGE.versions(), version_list)
        self.assertEqual(self.F_IMAGE.admin_versions(), admin_version_list)
        self.assertEqual(self.F_IMAGE.version_name("large"), "testimage_large--680x0.jpg")
        self.assertEqual(self.F_IMAGE.version_path("large"), "_test/_versions/folder/testimage_large--680x0.jpg")

        # version does not exist yet
        f_version = FileObject(os.path.join(site.directory, 'folder', "testimage_large--680x0.jpg"), site=site)
        self.assertEqual(f_version.exists, False)
        # generate version
        f_version = self.F_IMAGE.version_generate("large")
        self.assertEqual(f_version.path, "_test/_versions/folder/testimage_large--680x0.jpg")
        self.assertEqual(f_version.exists, True)
        self.assertEqual(f_version.is_version, True)
        self.assertEqual(f_version.original_filename, "testimage.jpg")
        self.assertEqual(f_version.original.path, self.F_IMAGE.path)
