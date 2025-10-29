import os

from devtools.yamaker import fileutil
from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as libzstd:
        # Unbundle xxhash.
        fileutil.re_sub_dir(self.dstdir, '"([^"]*/)?xxhash.h"', "<contrib/libs/xxhash/xxhash.h>")
        libzstd.CFLAGS.remove("-DXXH_NAMESPACE=ZSTD_")
        libzstd.SRCS.remove("lib/common/xxhash.c")
        libzstd.PEERDIR.add("contrib/libs/xxhash")

        os.remove(f"{self.dstdir}/lib/common/xxhash.h")
        os.remove(f"{self.dstdir}/lib/common/xxhash.c")

        # Enable runtime-dispatching for bmi2
        libzstd.SRCS.remove("lib/decompress/huf_decompress_amd64.S")
        libzstd.after(
            "CFLAGS",
            Switch(
                {
                    "ARCH_X86_64 AND NOT MSVC": Linkable(
                        CFLAGS=["-DDYNAMIC_BMI2"],
                        SRCS=["lib/decompress/huf_decompress_amd64.S"],
                    )
                }
            ),
        )

    with self.yamakes["programs/zstd"] as zstd:
        zstd.CFLAGS.remove("-DXXH_NAMESPACE=ZSTD_")


zstd = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/zstd",
    nixattr="zstd",
    build_subdir="build_",
    install_targets=["zstd", "programs/zstd"],
    put={
        "zstd": ".",
        "Program zstd": "programs/zstd",
    },
    disable_includes=[
        # our zstd is built without other codecs support
        "lz4.h",
        "lz4frame.h",
        "lzma.h",
        "zlib.h",
    ],
    inclink={
        # list of public headers, obtained from `dpkg -L libzstd-dev`
        "include": [
            "lib/zstd_errors.h",
            "lib/zdict.h",
            "lib/zstd.h",
        ],
    },
    post_install=post_install,
)
