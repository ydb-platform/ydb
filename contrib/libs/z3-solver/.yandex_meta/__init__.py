from devtools.yamaker.modules import DLL, Py3Library, Words, py_srcs
from devtools.yamaker.project import GNUMakeNixProject

import os
import shutil

LIB_DIR_CPP = "contrib/libs/z3-solver"
LIB_DIR_PY = "contrib/python/z3-solver"


def append_dir(source):
    return 'z3/' + source if source != 'TOP_LEVEL' else source


def post_install(self):
    # Create dynamic lib
    dll_lib = DLL(
        SUBSCRIBER=["g:python-contrib"],
        VERSION=self.version,
        PEERDIR=[LIB_DIR_CPP],
        WHOLE_ARCHIVE=[LIB_DIR_CPP],
        LICENSE=[self.license],
    )
    abs_dir = os.path.join(self.ctx.arc, LIB_DIR_PY, "dll")
    os.makedirs(abs_dir, exist_ok=True)
    with open(f"{abs_dir}/ya.make", "wt") as ymake:
        ymake.write(str(dll_lib))

    # Create py library
    py_sources = sorted(py_srcs(self.srcdir + "/src/api/python/z3"))
    py_lib = Py3Library(
        SUBSCRIBER=["g:python-contrib"],
        VERSION=self.version,
        LICENSE=[self.license],
        NO_LINT=True,
        PY_SRCS=map(append_dir, py_sources),
        PEERDIR=["library/python/resource", "contrib/python/setuptools"],
        RESOURCE_FILES=[Words("PREFIX", "lib/z3/"), "libz3.so"],
        BUNDLE=[Words("contrib/python/z3-solver/dll", "NAME", "libz3.so")],
        RECURSE=["dll"],
    )
    # Copy .py files
    abs_dir = os.path.join(self.ctx.arc, LIB_DIR_PY)
    for src in map(append_dir, py_sources):
        if src == 'TOP_LEVEL':
            continue
        shutil.copyfile(self.srcdir + f"/src/api/python/{src}", f"{abs_dir}/{src}")
    # Emit ya.make for python3 library
    with open(f"{abs_dir}/ya.make", "wt") as ymake:
        ymake.write(str(py_lib))

    for mod in self.yamakes.values():
        mod.LICENSE = [self.license]
        mod.LICENSE_TEXTS = [f"{self.arcdir}/LICENSE.txt"]

    return


z3_solver = GNUMakeNixProject(
    nixattr="z3",
    license="MIT",
    owners=[],
    arcdir=LIB_DIR_CPP,
    build_subdir="build",
    post_install=post_install,
)
