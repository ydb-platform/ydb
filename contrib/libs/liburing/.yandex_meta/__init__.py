import os

from devtools.yamaker.project import NixProject


def post_install(self):
    # FIXME: src/liburing.map gets copied sometimes
    liburing_map = f"{self.dstdir}/src/liburing.map"
    if os.path.isfile(liburing_map):
        os.remove(liburing_map)


liburing = NixProject(
    owners=["g:cpp-contrib", "g:yt"],
    arcdir="contrib/libs/liburing",
    nixattr="liburing",
    license="MIT",
    addincl_global={".": ["./src/include"]},
    copy_sources=[
        "src/arch/aarch64/lib.h",
        "src/arch/aarch64/syscall.h",
    ],
    disable_includes=[
        "arch/generic/lib.h",
        "arch/generic/syscall.h",
        "arch/riscv64/",
        "../generic/syscall.h",
    ],
    ignore_targets=[
        "uring-ffi",
        # depends on statx(), which was added in glibc 2.28 / Ubuntu 20.04
        "statx.t",
        # depends on gettid(), which was added in glibc 2.30 / Ubuntu 20.04
        "uring_cmd_ublk.t",
    ],
    put={
        "uring": ".",
    },
    post_install=post_install,
)
