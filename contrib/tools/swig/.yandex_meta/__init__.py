import os

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import Library
from devtools.yamaker.project import GNUMakeNixProject


SWIG_LANGS = {
    "go",
    "java",
    "perl5",
    "python",
}


def post_install(self):
    with self.yamakes["."] as swig:
        swig.SRCS.add("swig_lib.cpp")
        # replace bison-generated code with its original
        os.remove(f"{self.dstdir}/Source/CParse/parser.c")
        os.remove(f"{self.dstdir}/Source/CParse/parser.h")
        swig.SRCS.remove("Source/CParse/parser.c")
        swig.SRCS.add("Source/CParse/parser.y")
        swig.BISON_GEN_C = True

    for lang in SWIG_LANGS:
        self.yamakes[f"Lib/{lang}"] = self.module(
            Library,
            ADDINCL=[
                ArcPath(f"{self.arcdir}/Lib/{lang}", GLOBAL=True, FOR="swig"),
                ArcPath(f"{self.arcdir}/Lib", GLOBAL=True, FOR="swig"),
            ],
        )
        swig.RECURSE.add(f"Lib/{lang}")


swig = GNUMakeNixProject(
    arcdir="contrib/tools/swig",
    nixattr="swig4",
    flags=["--disable-ccache"],
    build_subdir="build",
    cflags=lambda self: [f"-DSWIG_LIB_ARCPATH={self.arcdir}/Lib"],
    ignore_commands=["bash", "sed", "bison"],
    copy_sources=[
        "Lib/*.i",
        "Lib/*.swg",
        "Lib/std",
        "Lib/typemaps",
        "Source/CParse/parser.y",
    ]
    + [
        # fmt: off
        f"Lib/{lang}"
        for lang in SWIG_LANGS
        # fmt: on
    ],
    keep_paths=["swig_lib.cpp"],
    disable_includes=[
        "cgocall.h",
        "runtime.h",
        "structmember.h",
    ],
    post_install=post_install,
)
