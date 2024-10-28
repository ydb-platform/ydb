import os

from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as lz4:
        # Unbundle xxhash.
        lz4.PEERDIR.add("contrib/libs/xxhash")
        lz4.ADDINCL.add("contrib/libs/xxhash")
        lz4.SRCS.remove("xxhash.c")
        lz4.CFLAGS = [x for x in lz4.CFLAGS if x != "-DXXH_NAMESPACE=LZ4_"]
        os.remove(f"{self.dstdir}/xxhash.h")
        os.remove(f"{self.dstdir}/xxhash.c")

        # Ensure no warnings.
        lz4.NO_COMPILER_WARNINGS = False


lz4 = GNUMakeNixProject(
    arcdir="contrib/libs/lz4",
    nixattr="lz4",
    nixsrcdir="source/lib",
    makeflags=["liblz4.a"],
    post_install=post_install,
)
