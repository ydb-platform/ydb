import os

from devtools.yamaker.fileutil import files
from devtools.yamaker.modules import Py3Program, Module, py_srcs
from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    # Remove CFFI implementation, keeping the CPython default.
    os.unlink(self.dstdir + "/lmdb/cffi.py")
    # Pythonize.
    self.yamakes["."].to_py_library(
        PY_SRCS=py_srcs(self.dstdir + "/lmdb", rel=self.dstdir),
        RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
        RECURSE=["bin"],
        RECURSE_FOR_TESTS=["tests"],
    )
    # Add py-lmdb tool.
    self.yamakes["bin"] = Py3Program(
        SUBSCRIBER=self.owners,
        LICENSE=["OLDAP-2.8"],
        PEERDIR=[self.arcdir],
        PY_MAIN="lmdb.tool",
    )
    # Add tests.
    self.yamakes["tests"] = Module(
        module="PY3TEST",
        SUBSCRIBER=self.owners,
        PEERDIR=[self.arcdir],
        NO_LINT=True,
        PY_SRCS=["TOP_LEVEL", "testlib.py"],
        TEST_SRCS=files(self.dstdir + "/tests", rel=True, test=lambda x: x != "testlib.py"),
    )


python_lmdb = NixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/lmdb/py3",
    nixattr=python.make_nixattr("lmdb"),
    copy_sources=["lmdb/", "tests/"],
    disable_includes=["memsink.h"],
    post_install=post_install,
)
