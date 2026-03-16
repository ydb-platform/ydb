from devtools.yamaker.modules import py_srcs
from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    self.yamakes["."].to_py_library(
        PEERDIR=["contrib/python/pyasn1", "contrib/python/pyasn1-modules"],
        SRCDIR=[f"{self.arcdir}/Lib"],
        PY_SRCS=py_srcs(f"{self.dstdir}/Lib"),
        NO_CHECK_IMPORTS=["ldap.extop.disconnection"],
        RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
    )


python_ldap = NixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/python-ldap",
    nixattr=python.make_nixattr("ldap"),
    disable_includes=["config.h"],
    copy_sources=["Lib/**/*.py"],
    post_install=post_install,
)
