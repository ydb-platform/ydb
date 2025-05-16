import os
import shutil

from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    # These directories contain generated includes which
    # are copied from source in include/freetype/config
    shutil.rmtree(os.path.join(self.dstdir, "builds"))
    shutil.rmtree(os.path.join(self.dstdir, "objs"))
    with self.yamakes["."] as m:
        m.ADDINCL.remove(os.path.join(self.arcdir, "builds/unix"))
        m.ADDINCL.remove(os.path.join(self.arcdir, "objs"))
        m.ADDINCL.add(os.path.join(self.arcdir, "include/freetype/config"))
        m.CFLAGS.remove("-DFT_DEBUG_LEVEL_TRACE")
        m.CFLAGS.remove("-DFT_DEBUG_LOGGING")
        m.CFLAGS.remove("-DFT_CONFIG_CONFIG_H=<ftconfig.h>")
        m.CFLAGS.remove("-DFT_CONFIG_MODULES_H=<ftmodule.h>")
        m.CFLAGS.remove("-DFT_CONFIG_OPTIONS_H=<ftoption.h>")
        m.SRCS.remove("builds/unix/ftsystem.c")
        m.SRCS.add("src/base/ftsystem.c")
        m.after("SRCS", Switch(OS_DARWIN=Linkable(SRCS=["src/base/ftmac.c"])))


freetype = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/freetype",
    nixattr="freetype",
    flags=[
        "--enable-biarch-config",
        "--without-bzip2",
    ],
    disable_includes=[
        "adler32.c",
        "brotli/decode.h",
        "bzlib.h",
        "crc32.c",
        "ft-hb.h",
        "ftimage.h",
        "ftmisc.h",
        "inffast.c",
        "inflate.c",
        "inftrees.c",
        "hb.h",
        "hb-ft.h",
        "hb-ot.h",
        "openssl/md5.h",
        "png.h",
        "zutil.c",
    ],
    addincl_global={
        ".": {"./include"},
    },
    copy_sources=[
        "docs/*",
        "include/freetype/ftmac.h",
        "include/freetype/config/ftconfig.h",
        "include/freetype/config/ftoption.h",
        "include/freetype/config/ftmodule.h",
        "src/base/ftmac.c",
        "src/base/ftsystem.c",
    ],
    ignore_commands=["bash"],
    install_targets=["freetype"],
    post_install=post_install,
)
