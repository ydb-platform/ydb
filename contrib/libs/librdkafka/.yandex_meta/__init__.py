import os
import shutil

from devtools.yamaker.project import NixProject

UNVENDOR_SRCS = ("src/rdxxhash.c",)


def librdkafka_post_install(self):
    with self.yamakes["."] as librdkafka:
        librdkafka.PEERDIR.add("contrib/libs/lz4")
        librdkafka.ADDINCL.add("contrib/libs/lz4")
        librdkafka.PEERDIR.add("contrib/libs/nanopb")
        librdkafka.PEERDIR.add("contrib/libs/curl")
        librdkafka.ADDINCL.add("contrib/libs/xxhash")
        librdkafka.PEERDIR.add("contrib/libs/xxhash")
        # unbundle xxhash
        librdkafka.SRCS.remove("src/rdxxhash.c")
        os.remove(f"{self.dstdir}/src/rdxxhash.h")
        os.remove(f"{self.dstdir}/src/rdxxhash.c")
        # unbundle nanopb
        librdkafka.SRCS.remove("src/nanopb/pb_common.c")
        librdkafka.SRCS.remove("src/nanopb/pb_decode.c")
        librdkafka.SRCS.remove("src/nanopb/pb_encode.c")
        shutil.rmtree(f"{self.dstdir}/src/nanopb")
        # lz4frame.h use from ADDINCL
        os.remove(f"{self.dstdir}/src/lz4frame.h")
        # unused
        if os.path.exists(f"{self.dstdir}/lds-gen.py"):
            os.remove(f"{self.dstdir}/lds-gen.py")


librdkafka = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/librdkafka",
    nixattr="rdkafka",
    put_with={"rdkafka-dbg": ["rdkafka-static"]},
    disable_includes=[
        "asm/unaligned.h",
        "linux/kernel.h",
        "linux/uio.h",
        "linux/module.h",
        "linux/string.h",
        "linux/slab.h",
        "linux/snappy.h",
        "linux/vmalloc.h",
        "openssl/provider.h",
        "rdhttp.h",
        "rdkafka_sasl_oauthbearer_oidc.h",
        "sys/byteorder.h",
        "sys/isa_defs.h",
        "sys/null.h",
    ],
    copy_sources=[
        "src/rdwin32.h",
        "src/win32_config.h",
    ],
    platform_dispatchers=["config.h"],
    keep_paths=["include"],
    post_install=librdkafka_post_install,
)
