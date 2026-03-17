# coding: utf-8
from django.test import TestCase
from django.http import QueryDict

from filebrowser.templatetags.fb_tags import get_file_extensions


class GetFileExtensionsTemplateTagTests(TestCase):
    def test_get_all(self):
        self.assertEqual(
            sorted(eval(get_file_extensions(''))),
            sorted([
                '.pdf', '.doc', '.rtf', '.txt', '.xls', '.csv', '.docx', '.mov',
                '.wmv', '.mpeg', '.mpg', '.avi', '.rm', '.jpg', '.jpeg', '.gif', '.png',
                '.tif', '.tiff', '.mp3', '.mp4', '.wav', '.aiff', '.midi', '.m4p', '.m4v', '.webm'
            ]))

    def test_get_filtered(self):
        self.assertEqual(
            get_file_extensions(QueryDict('type=image')),
            "['.jpg', '.jpeg', '.gif', '.png', '.tif', '.tiff']"
        )
