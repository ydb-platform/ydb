# coding: utf-8
import os
import shutil

import pytest
from django.conf import settings
from django.template import Context, Template, TemplateSyntaxError
from mock import patch

from tests.base import FilebrowserTestCase as TestCase
from filebrowser.settings import STRICT_PIL
from filebrowser import utils
from filebrowser.utils import scale_and_crop, process_image

if STRICT_PIL:
    from PIL import Image
else:
    try:
        from PIL import Image
    except ImportError:
        import Image


def processor_mark_1(im, **kwargs):
    im.mark_1 = True
    return im


def processor_mark_2(im, **kwargs):
    im.mark_2 = True
    return im


class ImageProcessorsTests(TestCase):
    def setUp(self):
        super(ImageProcessorsTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

        utils._default_processors = None
        self.im = Image.open(self.F_IMAGE.path_full)

    def tearDown(self):
        super(ImageProcessorsTests, self).tearDown()
        utils._default_processors = None

    def test_process_image_calls_scale_and_crop(self):
        version = process_image(self.im, {'width': 500, 'height': ""})
        self.assertEqual(version.size, (500, 375))

    @patch('filebrowser.utils.VERSION_PROCESSORS', [
        '__tests__.test_versions.processor_mark_1',
        '__tests__.test_versions.processor_mark_2',
    ])
    def test_process_image_calls_the_stack_of_processors_in_settings(self):
        version = process_image(self.im, {})
        self.assertTrue(hasattr(version, 'mark_1'))
        self.assertTrue(hasattr(version, 'mark_2'))

    @patch('filebrowser.utils.VERSION_PROCESSORS', [
        '__tests__.test_versions.processor_mark_1',
    ])
    def test_process_image_calls_only_explicit_provided_processors(self):
        version = process_image(self.im, {}, processors=[processor_mark_2])
        self.assertFalse(hasattr(version, 'mark_1'))
        self.assertTrue(hasattr(version, 'mark_2'))


class ScaleAndCropTests(TestCase):
    def setUp(self):
        super(ScaleAndCropTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

        self.im = Image.open(self.F_IMAGE.path_full)

    def test_do_not_scale(self):
        version = scale_and_crop(self.im, "", "", "")
        self.assertEqual(version.size, self.im.size)

    def test_do_not_scale_if_desired_size_is_equal_to_original(self):
        width, height = self.im.size
        version = scale_and_crop(self.im, width, height, "")
        self.assertIs(version, self.im)
        self.assertEqual(version.size, (width, height))

    def test_scale_width(self):
        version = scale_and_crop(self.im, 500, "", "")
        self.assertEqual(version.size, (500, 375))

    def test_scale_height(self):
        # new height 375 > 500/375
        version = scale_and_crop(self.im, "", 375, "")
        self.assertEqual(version.size, (500, 375))

    def test_scale_no_upscale_too_wide(self):
        version = scale_and_crop(self.im, 1500, "", "")
        self.assertIs(version, self.im)

    def test_scale_no_upscale_too_tall(self):
        version = scale_and_crop(self.im, "", 1125, "")
        self.assertIs(version, self.im)

    def test_scale_no_upscale_too_wide_and_tall(self):
        version = scale_and_crop(self.im, 1500, 1125, "")
        self.assertIs(version, self.im)

    def test_scale_with_upscale_width(self):
        version = scale_and_crop(self.im, 1500, "", "upscale")
        self.assertEqual(version.size, (1500, 1125))

    def test_scale_with_upscale_height(self):
        version = scale_and_crop(self.im, "", 1125, "upscale")
        self.assertEqual(version.size, (1500, 1125))

    def test_scale_with_upscale_width_and_height(self):
        version = scale_and_crop(self.im, 1500, 1125, "upscale")
        self.assertEqual(version.size, (1500, 1125))

    def test_scale_with_upscale_width_and_zero_height(self):
        version = scale_and_crop(self.im, 1500, 0, "upscale")
        self.assertEqual(version.size, (1500, 1125))

    def test_scale_with_upscale_zero_width_and_height(self):
        version = scale_and_crop(self.im, 0, 1125, "upscale")
        self.assertEqual(version.size, (1500, 1125))

    def test_scale_with_upscale_width_too_small_for_upscale(self):
        version = scale_and_crop(self.im, 500, "", "upscale")
        self.assertEqual(version.size, (500, 375))

    def test_scale_with_upscale_height_too_small_for_upscale(self):
        version = scale_and_crop(self.im, "", 375, "upscale")
        self.assertEqual(version.size, (500, 375))

    def test_crop_width_and_height(self):
        version = scale_and_crop(self.im, 500, 500, "crop")
        self.assertEqual(version.size, (500, 500))

    def test_crop_width_and_height_too_large_no_upscale(self):
        # new width 1500 and height 1500 w. crop > false (upscale missing)
        version = scale_and_crop(self.im, 1500, 1500, "crop")
        self.assertIs(version, self.im)

    def test_crop_width_and_height_too_large_with_upscale(self):
        version = scale_and_crop(self.im, 1500, 1500, "crop,upscale")
        self.assertEqual(version.size, (1500, 1500))

    def test_width_smaller_but_height_bigger_no_upscale(self):
        # new width 500 and height 1125
        # new width is smaller than original, but new height is bigger
        # width has higher priority
        version = scale_and_crop(self.im, 500, 1125, "")
        self.assertEqual(version.size, (500, 375))

    def test_width_smaller_but_height_bigger_with_upscale(self):
        # same with upscale
        version = scale_and_crop(self.im, 500, 1125, "upscale")
        self.assertEqual(version.size, (500, 375))

    def test_width_bigger_but_height_smaller_no_upscale(self):
        # new width 1500 and height 375
        # new width is bigger than original, but new height is smaller
        # height has higher priority
        version = scale_and_crop(self.im, 1500, 375, "")
        self.assertEqual(version.size, (500, 375))

    def test_width_bigger_but_height_smaller_with_upscale(self):
        # same with upscale
        version = scale_and_crop(self.im, 1500, 375, "upscale")
        self.assertEqual(version.size, (500, 375))


class VersionTemplateTagTests(TestCase):
    """Test basic version uses

    Eg:
    {% version obj "large" %}
    {% version path "large" %}

    """

    def setUp(self):
        super(VersionTemplateTagTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

        os.makedirs(self.PLACEHOLDER_PATH)
        shutil.copy(self.STATIC_IMG_PATH, self.PLACEHOLDER_PATH)

    def test_wrong_token(self):
        self.assertRaises(TemplateSyntaxError, lambda: Template('{% load fb_versions %}{% version obj.path %}'))
        self.assertRaises(TemplateSyntaxError, lambda: Template('{% load fb_versions %}{% version %}'))

    def test_invalid_version(self):
        # FIXME: should this throw an error?
        t = Template('{% load fb_versions %}{% version obj "invalid" %}')
        c = Context({"obj": self.F_IMAGE})
        r = t.render(c)
        self.assertEqual(r, "")

    def test_hardcoded_path(self):
        t = Template('{% load fb_versions %}{% version path "large" %}')
        c = Context({"obj": self.F_IMAGE, "path": "_test/uploads/folder/testimage.jpg"})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_with_obj(self):
        t = Template('{% load fb_versions %}{% version obj "large" %}')
        c = Context({"obj": self.F_IMAGE})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_with_obj_path(self):
        t = Template('{% load fb_versions %}{% version obj.path "large" %}')
        c = Context({"obj": self.F_IMAGE})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    @patch.dict('filebrowser.templatetags.fb_versions.VERSIONS', {'fixedheight': {'verbose_name': 'Fixed height', 'width': '', 'height': 100, 'opts': ''}})
    def test_size_fixedheight(self):
        t = Template('{% load fb_versions %}{% version path "fixedheight" %}')
        c = Context({"obj": self.F_IMAGE, "path": "_test/uploads/folder/testimage.jpg"})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_fixedheight.jpg"))

    def test_non_existing_path(self):
        # FIXME: templatetag version with non-existing path
        t = Template('{% load fb_versions %}{% version path "large" %}')
        c = Context({"obj": self.F_IMAGE, "path": "_test/uploads/folder/testimagexxx.jpg"})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, ""))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    @patch('filebrowser.templatetags.fb_versions.FORCE_PLACEHOLDER', True)
    def test_force_placeholder_with_existing_image(self, ):
        t = Template('{% load fb_versions %}{% version obj.path suffix %}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    @patch('filebrowser.templatetags.fb_versions.FORCE_PLACEHOLDER', True)
    def test_force_placeholder_without_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj.path suffix %}')
        c = Context({"obj": self.F_MISSING, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    def test_no_force_placeholder_with_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj.path suffix %}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    def test_no_force_placeholder_without_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj.path suffix %}')
        c = Context({"obj": self.F_MISSING, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))

    # def test_permissions(self):
    # FIXME: Test permissions by creating file AFTER we patch DEFAULT_PERMISSIONS
    #     permissions_file = oct(os.stat(os.path.join(settings.MEDIA_ROOT, "_test/_versions/folder/testimage_large.jpg")).st_mode & 0o777)
    #     self.assertEqual(oct(0o755), permissions_file)


class VersionAsTemplateTagTests(TestCase):
    """Test variable version uses

    Eg:
    {% version obj "large" as version_large %}
    {% version path "large" as version_large %}

    """

    def setUp(self):
        super(VersionAsTemplateTagTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)

        os.makedirs(self.PLACEHOLDER_PATH)
        shutil.copy(self.STATIC_IMG_PATH, self.PLACEHOLDER_PATH)

    def test_hardcoded_path(self):
        t = Template('{% load fb_versions %}{% version path "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "path": "_test/uploads/folder/testimage.jpg"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_obj_path(self):
        t = Template('{% load fb_versions %}{% version obj.path "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_with_obj(self):
        t = Template('{% load fb_versions %}{% version obj "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_with_suffix_as_variable(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_non_existing_path(self):
        # FIXME: templatetag version with non-existing path
        t = Template('{% load fb_versions %}{% version path "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "path": "_test/uploads/folder/testimagexxx.jpg"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, ""))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, ""))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    @patch('filebrowser.templatetags.fb_versions.FORCE_PLACEHOLDER', True)
    def test_force_placeholder_with_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    def test_no_force_placeholder_with_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    @patch('filebrowser.templatetags.fb_versions.FORCE_PLACEHOLDER', True)
    def test_force_placeholder_with_non_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_MISSING, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    def test_no_force_placeholder_with_non_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_MISSING, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))


class VersionObjectTemplateTagTests(TestCase):
    """Test version uses

    Eg:
    {% version obj "large" as version_large %}
    {% version path "large" as version_large %}

    """
    def setUp(self):
        super(VersionObjectTemplateTagTests, self).setUp()
        shutil.copy(self.STATIC_IMG_PATH, self.FOLDER_PATH)
        shutil.copy(self.STATIC_IMG_BAD_PATH, self.FOLDER_PATH)
        os.makedirs(self.PLACEHOLDER_PATH)
        shutil.copy(self.STATIC_IMG_PATH, self.PLACEHOLDER_PATH)

    def test_wrong_token(self):
        self.assertRaises(TemplateSyntaxError, lambda: Template('{% load fb_versions %}{% version obj.path %}'))
        self.assertRaises(TemplateSyntaxError, lambda: Template('{% load fb_versions %}{% version %}'))
        # next one does not raise an error anymore, because we use version instead of version_object
        # leave here for reference and delete later
        # self.assertRaises(TemplateSyntaxError, lambda: Template('{% load fb_versions %}{% version obj.path "medium" %}'))

    def test_hardcoded_path(self):
        t = Template('{% load fb_versions %}{% version path "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "path": "_test/uploads/folder/testimage.jpg"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_obj_path(self):
        t = Template('{% load fb_versions %}{% version obj.path "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_with_obj(self):
        t = Template('{% load fb_versions %}{% version obj "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_suffix_as_variable(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    def test_non_existing_path(self):
        # FIXME: templatetag version with non-existing path
        t = Template('{% load fb_versions %}{% version path "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "path": "_test/uploads/folder/testimagexxx.jpg"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, ""))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, ""))

    def test_jpg_not_an_image(self):
        t = Template('{% load fb_versions %}{% version obj "large" as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE_BAD})
        r = t.render(c)
        self.assertEqual(c["version_large"], "")
        self.assertEqual(r, "")

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    @patch('filebrowser.templatetags.fb_versions.FORCE_PLACEHOLDER', True)
    def test_force_with_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    def test_no_force_with_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_IMAGE, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/folder/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    @patch('filebrowser.templatetags.fb_versions.FORCE_PLACEHOLDER', True)
    def test_force_with_non_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_MISSING, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))

    @patch('filebrowser.templatetags.fb_versions.SHOW_PLACEHOLDER', True)
    @patch('filebrowser.templatetags.fb_versions.FORCE_PLACEHOLDER', False)
    def test_no_force_with_non_existing_image(self):
        t = Template('{% load fb_versions %}{% version obj suffix as version_large %}{{ version_large.url }}')
        c = Context({"obj": self.F_MISSING, "suffix": "large"})
        r = t.render(c)
        self.assertEqual(c["version_large"].url, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))
        self.assertEqual(r, os.path.join(settings.MEDIA_URL, "_test/_versions/placeholders/testimage_large.jpg"))
