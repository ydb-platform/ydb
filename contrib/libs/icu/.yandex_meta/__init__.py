import os.path as P
import shutil

from devtools.yamaker.fileutil import copy, subcopy
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixProject


def post_build(self):
    # copying icudt.dat file from original repository
    icu_dat_path = f"{self.srcdir}/data/in/icudt75l.dat"
    rodata_path = f"{self.dstdir}/icudt75_dat.rodata"
    shutil.copy(icu_dat_path, rodata_path)


def post_install(self):
    result_target = self.yamakes["."]

    result_target.SRCS.add("icudt75_dat.rodata")

    result_target.CFLAGS = [
        "-DU_COMMON_IMPLEMENTATION",
        "-DU_I18N_IMPLEMENTATION",
        "-DU_IO_IMPLEMENTATION",
    ]

    # Requires that U_STATIC_IMPLEMENTATION be defined in user code that links against ICU's static libraries
    # See https://htmlpreview.github.io/?https://github.com/unicode-org/icu/blob/master/icu4c/readme.html#RecBuild
    windows_cflags = Linkable()
    windows_cflags.CFLAGS = [
        GLOBAL("-DU_STATIC_IMPLEMENTATION"),
    ]

    default_cflags = Linkable()
    default_cflags.CFLAGS = [
        "-DU_STATIC_IMPLEMENTATION",
    ]

    result_target.after(
        "CFLAGS",
        Switch(
            {
                "OS_WINDOWS": windows_cflags,
                "default": default_cflags,
            }
        ),
    )

    # Win
    # TODO add CYGWINMSVC ?
    # TODO add _CRT_SECURE_NO_DEPRECATE ?

    # stubdata is there because it is linked in shared library during build
    # And even though it's target is missing in Project and no ya.make references this sources, they are copied
    # In arcadia full icudata is always statically linked in
    # So we should not need it
    shutil.rmtree(P.join(self.dstdir, "stubdata"))

    # copy_top_sources does not work due to nixsrcdir
    subcopy(P.join(self.tmpdir, "icu"), self.dstdir, ["LICENSE", "*.html", "*.css"])

    # Usual icu4c includes look like this `#include "unicode/brkiter.h"`
    # And all headers is installed in ${PREFIX}/include/unicode
    # But in sources layout is different - headers for each sublibrary is separate, and inside sublib sources
    # So all headers are inside source/common/unicode, source/i18n/unicode and source/io/unicode
    # With original layout one need to use ADDINCL to contrib/libs/icu/common to be able to `#include "unicode/brkiter.h"`
    # But that will leak headers from contrib/libs/icu, i.e. contrib/libs/icu/common/util.h
    # So we move public headers to separate dirs, to avoid unnecessary headers
    for sublib in [
        "common",
        "i18n",
        "io",
    ]:
        src = P.join(self.dstdir, sublib, "unicode")
        copy(
            [src],
            P.join(self.dstdir, "include"),
        )
        shutil.rmtree(src)


icu = NixProject(
    owners=[
        "g:cpp-contrib",
    ],
    arcdir="contrib/libs/icu",
    nixattr="icu67",
    # https://github.com/NixOS/nixpkgs/blob/b2f6aa631edc025cb6748133864317983bf5c6d1/pkgs/development/libraries/icu/base.nix#L17-L20
    nixsrcdir="icu/source",
    put_with={
        "icuio": [
            "icuuc",
            "icui18n",
        ],
    },
    install_targets=[
        "icuio",
    ],
    copy_sources=[
        "common/unicode/*.h",
        "common/*.h",
        "i18n/*.h",
    ],
    disable_includes=[
        "sys/isa_defs.h",
    ],
    # We setup ADDINCL GLOBAL, so we have sinlge "usual" way to include ICU headers by default
    addincl_global={".": {"./include"}},
    ignore_commands=[
        "bash",
    ],
    post_build=post_build,
    post_install=post_install,
)
