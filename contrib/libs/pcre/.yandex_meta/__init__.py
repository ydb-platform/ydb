import os

from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixProject


def post_install(self):
    fileutil.convert_to_utf8(f"{self.dstdir}/ChangeLog", from_="latin-1")

    fileutil.re_sub_file(f"{self.dstdir}/config.h", "#define [^ ]*_EXP_DE", r"// \g<0>")
    fileutil.re_sub_dir(self.dstdir, r'(# *include) "config\.h"', r'\1 "pcre_config.h"')
    fileutil.re_sub_dir(self.dstdir, r"(# *include) <(pcre.*)>", r'\1 "\2"')
    os.rename(f"{self.dstdir}/config.h", f"{self.dstdir}/pcre_config.h")

    with self.yamakes["."] as pcre:
        pcre.CFLAGS.insert(0, GLOBAL("-DPCRE_STATIC"))
        pcre.after(
            "CFLAGS",
            """
# JIT adds ≈108KB to binary size which may be critical for mobile and embedded devices binary distributions
DEFAULT(ARCADIA_PCRE_ENABLE_JIT yes)
            """.strip(),
        )
        pcre.after(
            "CFLAGS",
            Switch(ARCADIA_PCRE_ENABLE_JIT=Linkable(CFLAGS=["-DARCADIA_PCRE_ENABLE_JIT"])),
        )


pcre = NixProject(
    arcdir="contrib/libs/pcre",
    nixattr="pcre",
    license="BSD-3-Clause",
    disable_includes=[
        "bits/type_traits.h",
        "sys/cache.h",
        "sljitNativeSPARC_64.c",
        "sljitProtExecAllocator.c",
    ],
    put_with={"pcre": ["pcreposix"]},
    install_targets=["pcre", "pcre16", "pcre32", "pcrecpp"],
    put={"pcre": "."},
    copy_sources=[
        "sljit/**",
    ],
    post_install=post_install,
)
