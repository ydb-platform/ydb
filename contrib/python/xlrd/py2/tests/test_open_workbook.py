import os
import shutil
import tempfile
import unittest
from unittest import TestCase

from xlrd import open_workbook

from .base import from_this_dir


class TestOpen(TestCase):
    # test different uses of open_workbook

    def test_names_demo(self):
        # For now, we just check this doesn't raise an error.
        open_workbook(
            from_this_dir('namesdemo.xls'),
        )

    @unittest.skip("No access to write in user's home dir in TestEnv")
    def test_tilde_path_expansion(self):
        with tempfile.NamedTemporaryFile(suffix='.xlsx', dir=os.path.expanduser('~')) as fp:
            shutil.copyfile(from_this_dir('text_bar.xlsx'), fp.name)
            # For now, we just check this doesn't raise an error.
            open_workbook(os.path.join('~', os.path.basename(fp.name)))

    def test_ragged_rows_tidied_with_formatting(self):
        # For now, we just check this doesn't raise an error.
        open_workbook(from_this_dir('issue20.xls'),
                      formatting_info=True)

    def test_BYTES_X00(self):
        # For now, we just check this doesn't raise an error.
        open_workbook(from_this_dir('picture_in_cell.xls'),
                      formatting_info=True)

    def test_xlsx_simple(self):
        # For now, we just check this doesn't raise an error.
        open_workbook(from_this_dir('text_bar.xlsx'))
        # we should make assertions here that data has been
        # correctly processed.

    def test_xlsx(self):
        # For now, we just check this doesn't raise an error.
        open_workbook(from_this_dir('reveng1.xlsx'))
        # we should make assertions here that data has been
        # correctly processed.


    def test_err_cell_empty(self):
        # For cell with type "e" (error) but without inner 'val' tags
        open_workbook(from_this_dir('err_cell_empty.xlsx'))
