import os
import os.path as P

from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def libwebp_post_install(self):
    # Update includes to supported stripped src.
    re_sub_dir(self.dstdir, '#include "src/', '#include "../')
    # Match current style of relative includes.
    for d in os.listdir(self.dstdir):
        absd = P.join(self.dstdir, d)
        if P.isdir(absd):
            re_sub_dir(absd, '#include "../' + d + "/", '#include "./')
    # Deduplicate SRCS.
    for s in "dsp/webpdsp", "utils/webputils":
        with self.yamakes[s] as m:
            m.PEERDIR.add(self.arcdir + "/" + s + "decode")
            m.SRCS -= self.yamakes[s + "decode"].SRCS
    # Support NEON on 32-bit Androids.
    self.yamakes["dsp/webpdspdecode"].after(
        "PEERDIR",
        Switch(
            OS_ANDROID=Linkable(
                PEERDIR=["contrib/libs/android_cpufeatures"],
                ADDINCL=["contrib/libs/android_cpufeatures"],
            )
        ),
    )


libwebp = GNUMakeNixProject(
    arcdir="contrib/libs/libwebp",
    nixattr="libwebp",
    license="BSD-3-Clause",
    makeflags=["-C", "src"],
    install_subdir="src",
    copy_sources=["dsp/mips_macro.h", "dsp/msa_macro.h", "dsp/neon.h"],
    platform_dispatchers=["webp/config.h"],
    put={"webp": "."},
    post_install=libwebp_post_install,
)
