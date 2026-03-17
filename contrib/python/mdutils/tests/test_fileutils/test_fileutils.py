# Python
#
# This module implements tests for MdUtils class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2020 Dídac Coll

from unittest import TestCase
from mdutils.fileutils.fileutils import MarkDownFile
import tempfile


class TestMarkdownFile(TestCase):

    def test_create_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = MarkDownFile('Test_file', tmp)
            self.assertEqual(file.file_name, f'{tmp}/Test_file.md')

    def test_create_file_case_0(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = MarkDownFile('Test_filemd', tmp)
            self.assertEqual(file.file_name, f'{tmp}/Test_filemd.md')

    def test_create_file_case_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = MarkDownFile('Test_file.md', tmp)
            self.assertEqual(file.file_name, f'{tmp}/Test_file.md')
    
    def test_create_file_case_3(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = MarkDownFile(f'{tmp}/Test_file.md')
            self.assertEqual(file.file_name, f'{tmp}/Test_file.md')

    def test_rewrite_all_file(self):
        expected_content = "Write some content"
        file_name = 'Test_file.md'
        with tempfile.TemporaryDirectory() as tmp:
            file = MarkDownFile(file_name, tmp)
            file.rewrite_all_file(expected_content)
            with open(f'{tmp}/{file_name}', 'r') as actual_md:
                self.assertEqual(actual_md.read(), expected_content)

    def test_append_end(self):
        expected_content = "Write some content and some data"
        file_name = 'Test_file.md'
        with tempfile.TemporaryDirectory() as tmp:
            file = MarkDownFile(file_name, tmp)
            file.rewrite_all_file("Write some content")
            file.append_end(" and some data")
            with open(f'{tmp}/{file_name}', 'r') as actual_md:
                self.assertEqual(actual_md.read(), expected_content)

    def test_append_second_line(self):
        expected_content = "This is the 1st line\nThis is the 2nd line\nThis is the 3th line\nThis is the 4th line"
        file_name = 'Test_file.md'
        with tempfile.TemporaryDirectory() as tmp:
            file = MarkDownFile(file_name, tmp)
            file.rewrite_all_file("This is the 1st line\nThis is the 2nd line\nThis is the 4th line")
            file.append_after_second_line("This is the 3th line")
            with open(f'{tmp}/{file_name}', 'r') as actual_md:
                self.assertEqual(actual_md.read(), expected_content)

    def test_read(self):
        expected_content = "This is the expected content after reading the file"
        file_name = 'Test_file.md'
        with tempfile.TemporaryDirectory() as tmp:
            with open(f'{tmp}/{file_name}', 'w') as file:
                file.write(expected_content)

            self.assertEqual(MarkDownFile.read_file(f'{tmp}/{file_name}'), expected_content)
