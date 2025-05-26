import os

from devtools.yamaker.fileutil import files
from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as libpng:
        # libpng generates export script, but we are going to link statically
        os.remove(f"{self.dstdir}/libpng.vers")

        # libpng generates config.h but does not use any of its defines.
        os.remove(f"{self.dstdir}/config.h")
        libpng.CFLAGS.remove("-DHAVE_CONFIG_H")

        # Support ARM.
        arm_srcs = files(f"{self.dstdir}/arm", rel=self.dstdir)
        self.yamakes["."].after(
            "SRCS",
            Switch(
                {
                    "NOT MSVC": Switch(
                        {
                            "ARCH_AARCH64 OR ARCH_ARM": Linkable(SRCS=arm_srcs),
                        }
                    )
                }
            ),
        )

        libpng.RECURSE.add("include")


libpng = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libpng",
    nixattr="libpng",
    makeflags=["libpng16.la"],
    copy_sources=[
        "arm/*",
        "pngprefix.h",
    ],
    disable_includes=[
        "config.h",
        "pngusr.h",
        "mem.h",
        "PNG_MIPS_MMI_FILE",
        "PNG_MIPS_MSA_FILE",
        "PNG_POWERPC_VSX_FILE",
        "PNG_ARM_NEON_FILE",
    ],
    # Original libpng layout provides unnecessary ADDINCL(include/libpng16)
    keep_paths=["include/ya.make"],
    ignore_commands=["gawk"],
    inclink={"include": ["png.h", "pngconf.h", "pnglibconf.h"]},
    post_install=post_install,
)
