# coding: utf-8

from tests.base import FilebrowserTestCase as TestCase

from six import string_types

from filebrowser.base import FileObject
from filebrowser.fields import FileBrowseField


class FileBrowseFieldTests(TestCase):
    path_to_file = "/path/to/file.jpg"

    def test_to_python(self):
        field = FileBrowseField()

        # should deal gracefully with any of the following arguments:
        # An instance of the correct type, a string, and None (if the field allows null=True)
        values = [FileObject(self.path_to_file), self.path_to_file]
        for v in values:
            actual = field.to_python(v)
            self.assertIsInstance(actual, FileObject)
            self.assertEqual(actual.path, self.path_to_file)
        self.assertEqual(field.to_python(None), None)

    def test_get_prep_value(self):
        field = FileBrowseField()

        # similar to to_python() should handle all 3 cases
        values = [FileObject(self.path_to_file), self.path_to_file]
        for v in values:
            actual = field.get_prep_value(v)
            self.assertIsInstance(actual, string_types)
            self.assertEqual(actual, self.path_to_file)
        self.assertEqual(field.to_python(None), None)
