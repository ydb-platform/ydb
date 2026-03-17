# coding: utf-8

import shutil

from mock import patch

from filebrowser.settings import VERSIONS
from tests.base import FilebrowserTestCase as TestCase

from filebrowser.namers import OptionsNamer


class BaseNamerTests(TestCase):
    NAMER_CLASS = OptionsNamer

    def setUp(self):
        super(BaseNamerTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

    def _get_namer(self, version_suffix, file_object=None, **extra_options):
        if not file_object:
            file_object = self.F_IMAGE

        extra_options.update(VERSIONS.get(version_suffix, {}))

        return self.NAMER_CLASS(
            file_object=file_object,
            version_suffix=version_suffix,
            filename_root=file_object.filename_root,
            extension=file_object.extension,
            options=extra_options,
        )


class OptionsNamerTests(BaseNamerTests):

    def test_should_return_correct_version_name_using_predefined_versions(self):
        expected = [
            ("admin_thumbnail", "testimage_admin-thumbnail--60x60--opts-crop.jpg", ),
            ("thumbnail", "testimage_thumbnail--60x60--opts-crop.jpg", ),
            ("small", "testimage_small--140x0.jpg", ),
            ("medium", "testimage_medium--300x0.jpg", ),
            ("big", "testimage_big--460x0.jpg", ),
            ("large", "testimage_large--680x0.jpg", ),
        ]

        for version_suffix, expected_name in expected:
            namer = self._get_namer(version_suffix)
            self.assertEqual(namer.get_version_name(), expected_name)

    @patch('filebrowser.namers.VERSION_NAMER', 'filebrowser.namers.OptionsNamer')
    def test_should_return_correct_original_name_using_predefined_versions(self):
        expected = 'testimage.jpg'
        for version_suffix in VERSIONS.keys():
            file_object = self.F_IMAGE.version_generate(version_suffix)
            namer = self._get_namer(version_suffix, file_object)
            self.assertNotEqual(file_object.filename, expected)

            self.assertEqual(namer.get_original_name(), expected)
            self.assertEqual(file_object.original_filename, expected)

    def test_should_append_extra_options(self):
        expected = [
            (
                "thumbnail",
                "testimage_thumbnail--60x60--opts-crop--sepia.jpg",
                {'sepia': True}
            ), (
                "small",
                "testimage_small--140x0--thumb--transparency-08.jpg",
                {'transparency': 0.8, 'thumb': True}
            ), (
                "large",
                "testimage_large--680x0--nested-xpto-ops--thumb.jpg",
                {'thumb': True, 'nested': {'xpto': 'ops'}}
            ),
        ]

        for version_suffix, expected_name, extra_options in expected:
            namer = self._get_namer(version_suffix, **extra_options)
            self.assertEqual(namer.get_version_name(), expected_name)

    def test_generated_version_name_options_list_should_be_ordered(self):
        "the order is important to always generate the same name"
        expected = [
            (
                "small",
                "testimage_small--140x0--a--x-crop--z-4.jpg",
                {'z': 4, 'a': True, 'x': 'crop'}
            ),
        ]

        for version_suffix, expected_name, extra_options in expected:
            namer = self._get_namer(version_suffix, **extra_options)
            self.assertEqual(namer.get_version_name(), expected_name)
