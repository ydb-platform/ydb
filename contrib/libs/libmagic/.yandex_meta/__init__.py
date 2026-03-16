import os

from devtools.yamaker.fileutil import files
from devtools.yamaker.modules import GLOBAL, indented, Library, Program, Words
from devtools.yamaker.project import NixProject

SRCS_FROM_LIBC_COMPAT = ["strlcat.c", "strlcpy.c"]


def post_install(self):
    def a(s):
        return f"{self.arcdir}/{s}"

    with self.yamakes["src"] as m:
        # Take strl{cat,cpy} functions from libc_compat library
        for src_file in SRCS_FROM_LIBC_COMPAT:
            m.SRCS.remove(src_file)
            os.remove(os.path.join(self.dstdir, "src", src_file))
        m.PEERDIR.add("contrib/libs/libc_compat")
        # Use magic.mgc from resource.
        m.PEERDIR += ["library/cpp/resource"]
        m.SRCS.add("res.cpp")
        m.NO_UTIL = False
        m.NO_RUNTIME = False
    self.yamakes["src/file"].module = "LIBRARY"
    self.yamakes["file"] = Program(PEERDIR=[self.arcdir, a("src/file")])
    self.yamakes["file/0"] = Program(PROGRAM=["file0"], PEERDIR=[a("src"), a("src/file")])
    self.yamakes["magic"] = Library(
        SRCDIR=[a("magic/Magdir")],
        RESOURCE=[Words("Magdir.mgc", "/magic/magic.mgc")],
        RUN_PROGRAM=[
            indented(
                [
                    a("file/0") + " -C -m $CURDIR/Magdir",
                    "CWD $BINDIR",
                    "OUT Magdir.mgc",
                    " ".join(["IN"] + files(self.dstdir + "/magic/Magdir", rel=True)),
                ]
            )
        ],
    )
    self.yamakes["."] = self.module(
        Library,
        PEERDIR=[a("magic"), a("src")],
        ADDINCL=[GLOBAL(a("include"))],
        RECURSE=set(self.yamakes) - {"."},
    )
    for m in self.yamakes.values():
        m.SUBSCRIBER = self.owners
    self.yamakes.make_recursive(recurse_from_modules=True)


libmagic = NixProject(
    arcdir="contrib/libs/libmagic",
    nixattr="file",
    license="BSD-2-Clause",
    put={"magic": "src", "file": "src/file"},
    cflags=["-DHAVE_CONFIG_H", '-DMAGIC="res@/magic/magic.mgc"'],
    ignore_commands=["bash", "sed"],
    disable_includes=[
        "bzlib.h",
        "seccomp.h",
        "lzma.h",
        "lzlib.h",
        "Lrzip.h",
        "os2.h",
        "mygetopt.h",
        "sys/bswap.h",
        "zstd.h",
        "zstd_errors.h",
    ],
    keep_paths=["src/res.cpp"],
    add_defines=False,
    copy_sources=["magic/Magdir/"],
    inclink={"include": ["src/magic.h"]},
    platform_dispatchers=["config.h"],
    post_install=post_install,
)
