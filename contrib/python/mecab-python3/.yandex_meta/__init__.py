from devtools.yamaker.modules import py_srcs
from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    self.yamakes["."].to_py_library(
        PY_SRCS=py_srcs(self.dstdir),
        RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
    )


mecab_python3 = NixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/mecab-python3",
    nixattr=python.make_nixattr("mecab-python3"),
    install_subdir="src",
    copy_sources=["MeCab/*.py"],
    post_install=post_install,
)
