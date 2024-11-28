from devtools.yamaker.project import NixProject


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
        # statx.t depends on struct statx / statx call availability, which were added in glibc 2.28
        "statx.t",
        # fstnotify.t depends on <sys/fanotify> availablity, which is unavailable in current OS_SDK
        "fsnotify.t",
        "uring-ffi",
    ],
    put={
        "uring": ".",
    },
)
