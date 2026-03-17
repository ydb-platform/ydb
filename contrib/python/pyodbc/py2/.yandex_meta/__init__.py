from devtools.yamaker.project import NixProject
from devtools.yamaker.modules import py_srcs
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    self.yamakes["."].to_py_library(
        module="PY2_LIBRARY",
        SRCDIR=[f"{self.arcdir}/src"],
        PY_SRCS=py_srcs(f"{self.dstdir}/src"),
        NO_UTIL=True,
        RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
    )


pyodbc = NixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/pyodbc/py2",
    nixattr=python.make_nixattr("pyodbc"),
    cflags=[
        # rename common symbol that goes into conflict with other libraries
        "-DConnectionType=_pyodbc_ConnectionType",
    ],
    copy_sources=["src/*.pyi"],
    post_install=post_install,
)
