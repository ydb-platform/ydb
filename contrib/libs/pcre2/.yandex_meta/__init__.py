import os

from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import NixProject


def post_build(self):
    fileutil.re_sub_dir(self.dstdir, r'(# *include) "config\.h"', r"\1 <pcre2_config.h>")
    os.rename(f"{self.dstdir}/src/config.h", f"{self.dstdir}/src/pcre2_config.h")
    os.rename(f"{self.dstdir}/src/pcre2.h", f"{self.dstdir}/pcre2.h")


def post_install(self):
    with self.yamakes["."] as m:
        m.CFLAGS.append(GLOBAL("-DPCRE2_STATIC"))


pcre2 = NixProject(
    arcdir="contrib/libs/pcre2",
    nixattr="pcre2",
    license="BSD-3-Clause",
    disable_includes=[
        "sys/cache.h",
        "sljitNativeSPARC_64.c",
        "pcre2_printint.c",
        "pcre2_jit_neon_inc.h",
        "sljitConfigPost.h",
        "sljitConfigPre.h",
    ],
    install_targets=["pcre2-8", "pcre2-16", "pcre2-32"],
    put={"pcre2-8": "."},
    copy_sources=[
        "src/sljit/**",
    ],
    platform_dispatchers=[
        "src/pcre2_config.h",
    ],
    post_build=post_build,
    post_install=post_install,
)
