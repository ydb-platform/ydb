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

    # Update version stored in python binding
    # (we have to update two different files for py2 and py3 correspondingly)
    version_as_number = self.version.replace(".", "0")
    fileutil.re_sub_file(
        f"{self.ctx.arc}/contrib/python/zstandard/py2/zstd.c",
        r"(ZSTD_VERSION_NUMBER != )[0-9]+( \|\| ZSTD_versionNumber\(\) != )[0-9]+",
        r"\g<1>{v}\g<2>{v}".format(v=version_as_number),
    )
    fileutil.re_sub_file(
        f"{self.ctx.arc}/contrib/python/zstandard/py3/c-ext/backend_c.c",
        r"unsigned our_hardcoded_version = [0-9]+;",
        rf"unsigned our_hardcoded_version = {version_as_number};",
    )


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
