import os.path as P

from devtools.yamaker.fileutil import files
from devtools.yamaker.modules import GLOBAL, Switch, Linkable
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    def a(s):
        return self.arcdir + "/" + s

    def ap(s):
        return f"ADDINCL({self.arcdir}/platform/{s})"

    m = self.yamakes["."]
    m.ADDINCL.sorted = False  # Include order determines platform headers.

    # Upstream compiles C as C++ to reuse nsync-C sources for nsync_cpp, but
    # our COMPILE_C_AS_CXX is incomplete and fails with --target-platform=gcc82.
    for s in files(self.dstdir, test=lambda s: s.endswith(".c")):
        with open(s + "pp", "w") as f:
            f.write(f'#include "{P.basename(s)}"\n')
    m.SRCS = [s + "pp" if s.endswith(".c") else s for s in m.SRCS]

    # Linux platform.h (includes C++11 platform.h).
    m.ADDINCL.remove(a("platform/c++11.futex"))
    m.before("ADDINCL", Switch(OS_LINUX=ap("c++11.futex")))
    # C++11 atomic.h and platform.h (before compiler and OS specific ones).
    m.ADDINCL.remove(a("platform/c++11"))
    m.before("ADDINCL", Linkable(ADDINCL=[a("platform/c++11")]))
    # compiler.h.
    m.ADDINCL.remove(a("platform/gcc"))
    m.before("ADDINCL", Switch(CLANG=ap("clang"), GCC=ap("gcc"), MSVC=ap("msvc")))
    # platform_c++11_os.h.
    m.before("ADDINCL", Switch(OS_DARWIN=ap("macos"), OS_WINDOWS=ap("win32")))

    # cputype.h.
    m.before(
        "ADDINCL",
        Switch(
            ARCH_X86_64=ap("x86_64"),
            ARCH_I386=ap("x86_32"),
            ARCH_ARM7=ap("arm"),
            ARCH_ARM64=ap("aarch64"),
            ARCH_PPC64LE=ap("ppc64"),
        ),
    )

    m.ADDINCL.remove(a("public"))
    m.ADDINCL.insert(0, GLOBAL(a("public")))

    m.SRCS.remove("platform/linux/src/nsync_semaphore_futex.cpp")
    m.after(
        "SRCS",
        Switch(
            OS_LINUX=Linkable(SRCS={"platform/linux/src/nsync_semaphore_futex.cpp"}),
            default=Linkable(SRCS={"platform/c++11/src/nsync_semaphore_mutex.cc"}),
        ),
    )

    m.after(
        "SRCS",
        Switch(
            # Nothing for OS_DARWIN:
            # - No clock_gettime.c because relevant versions of macOS implement it.
            # - No nsync_semaphore_mutex.c because .cc implements the same API.
            OS_WINDOWS=Linkable(
                SRCS={
                    "platform/win32/src/clock_gettime.cpp",
                    "platform/win32/src/pthread_key_win32.cc",
                }
            ),
        ),
    )


nsync = CMakeNinjaNixProject(
    arcdir="contrib/libs/nsync",
    nixattr="nsync",
    flags=["-DNSYNC_ENABLE_TESTS=OFF"],
    build_install_subdir="cpp",
    ignore_targets=["nsync"],  # Keep nsync_cpp.
    copy_sources=list(
        map(
            "platform/".__add__,
            [
                # compiler.h
                "clang/compiler.h",
                "gcc/compiler.h",
                "msvc/compiler.h",
                # platform_c++11_os.h
                "macos/platform_c++11_os.h",
                "win32/platform_c++11_os.h",
                # cputype.h
                "aarch64/cputype.h",
                "arm/cputype.h",
                "ppc64/cputype.h",
                "x86_32/cputype.h",
                "x86_64/cputype.h",
                # SRCS
                "c++11/src/",
                "win32/src/",
            ],
        )
    ),
    disable_includes=["grpc/support/time.h"],
    post_install=post_install,
)
