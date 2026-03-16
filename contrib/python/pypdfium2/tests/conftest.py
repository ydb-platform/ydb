# SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com>
# SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause

import sys
import pytest
from pathlib import Path
from argparse import Namespace
import pypdfium2.__main__ as pdfium_cli

import yatest.common

PyVersion = (sys.version_info.major, sys.version_info.minor)


pdfium_cli.setup_logging()

TestDir         = Path(yatest.common.source_path("contrib/python/pypdfium2/tests"))
ResourceDir     = TestDir / "resources"
OutputDir       = Path(yatest.common.output_path())
ExpectationsDir = TestDir / "expectations"


def _gather_resources(dir, skip_exts=[".in"]):
    test_files = Namespace()
    for path in dir.iterdir():
        if not path.is_file() or path.suffix in skip_exts:
            continue
        setattr(test_files, path.stem, (dir / path.name))
    return test_files

TestFiles = _gather_resources(ResourceDir)
TestExpectations = _gather_resources(ExpectationsDir)


def get_members(cls):
    members = []
    for attr in dir(cls):
        if attr.startswith("_"):
            continue
        members.append( getattr(cls, attr) )
    return members


def compare_n2(data, exp_data, approx_abs=1):
    assert len(data) == len(exp_data)
    for d, exp_d in zip(data, exp_data):
        assert pytest.approx(d, abs=approx_abs) == exp_d


ExpRenderPixels = (
    ( (0,   0  ), (255, 255, 255) ),
    ( (150, 180), (129, 212, 26 ) ),
    ( (150, 390), (42,  96,  153) ),
    ( (150, 570), (128, 0,   128) ),
)


# def iterate_testfiles(skip_encrypted=True):
#     encrypted = (TestFiles.encrypted, )
#     for attr_name in dir(TestFiles):
#         if attr_name.startswith("_"):
#             continue
#         member = getattr(TestFiles, attr_name)
#         if skip_encrypted and member in encrypted:
#             continue
#         yield member
#
#
# def test_testpaths():
#     for dirpath in (TestDir, ProjectDir, ResourceDir, OutputDir):
#         assert dirpath.is_dir()
#     for filepath in iterate_testfiles(False):
#         assert filepath.is_file()
