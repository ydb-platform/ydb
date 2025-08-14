import os
from collections import OrderedDict
from glob import glob
from os.path import basename, dirname

from devtools.yamaker.modules import GLOBAL, Linkable, Program, Recursable, Switch
from devtools.yamaker.project import GNUMakeNixProject


def libffi_post_build(self):
    configs_dir = self.dstdir + "/configs"
    os.mkdir(configs_dir)
    os.rename(self.dstdir + "/x86_64-pc-linux-gnu", configs_dir + "/x86_64-pc-linux-gnu")

    os.unlink(self.dstdir + "/libffi.map.in")


def libffi_post_install(self):
    configs_dir = self.arcdir + "/configs"

    with self.yamakes["."] as m:
        # Disable dllimport/dllexport on windows
        # We know that the library is always linked statically
        m.CFLAGS.append(GLOBAL("-DFFI_BUILDING"))

        m.ADDINCL = [path for path in m.ADDINCL if "/x86_64-pc-linux-gnu" not in path] + [
            GLOBAL(self.arcdir + "/include")
        ]

        m.SRCS = [path for path in m.SRCS if "/x86/" not in path]

        # See configure.host script in libffi distribution for original host->srcs mapping
        m.after(
            "SRCS",
            Switch(
                OrderedDict(
                    sorted(
                        [
                            (
                                "ARCH_I386 AND OS_WINDOWS",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/i386-microsoft-windows",
                                        GLOBAL(configs_dir + "/i386-microsoft-windows/include"),
                                    ],
                                    SRCS=[
                                        "src/x86/ffi.c",
                                        "configs/i386-microsoft-windows/sysv_intel.masm",
                                    ],
                                    LDFLAGS=[
                                        "/safeseh:no",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_I386 AND OS_ANDROID",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/i686-pc-linux-android16",
                                        GLOBAL(configs_dir + "/i686-pc-linux-android16/include"),
                                    ],
                                    SRCS=[
                                        "src/x86/ffi.c",
                                        "src/x86/sysv.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_X86_64 AND OS_LINUX",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/x86_64-pc-linux-gnu",
                                        GLOBAL(configs_dir + "/x86_64-pc-linux-gnu/include"),
                                    ],
                                    SRCS=[
                                        "src/x86/ffi64.c",
                                        "src/x86/ffiw64.c",
                                        "src/x86/unix64.S",
                                        "src/x86/win64.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_X86_64 AND OS_ANDROID",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/x86_64-pc-linux-android21",
                                        GLOBAL(configs_dir + "/x86_64-pc-linux-android21/include"),
                                    ],
                                    SRCS=[
                                        "src/x86/ffi64.c",
                                        "src/x86/ffiw64.c",
                                        "src/x86/unix64.S",
                                        "src/x86/win64.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_X86_64 AND OS_DARWIN",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/x86_64-apple-macosx",
                                        GLOBAL(configs_dir + "/x86_64-apple-macosx/include"),
                                    ],
                                    SRCS=[
                                        "src/x86/ffi64.c",
                                        "src/x86/ffiw64.c",
                                        "src/x86/unix64.S",
                                        "src/x86/win64.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_ARM64 AND OS_DARWIN",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/aarch64-apple-macos",
                                        GLOBAL(configs_dir + "/aarch64-apple-macos/include"),
                                    ],
                                    SRCS=[
                                        "src/aarch64/ffi.c",
                                        "src/aarch64/sysv.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_X86_64 AND OS_IOS",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/x86_64-apple-iphonesimulator",
                                        GLOBAL(configs_dir + "/x86_64-apple-iphonesimulator/include"),
                                    ],
                                    SRCS=[
                                        "src/x86/ffi64.c",
                                        "src/x86/ffiw64.c",
                                        "src/x86/unix64.S",
                                        "src/x86/win64.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_X86_64 AND OS_WINDOWS",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/x86_64-microsoft-windows",
                                        GLOBAL(configs_dir + "/x86_64-microsoft-windows/include"),
                                    ],
                                    SRCS=[
                                        "src/x86/ffiw64.c",
                                        "configs/x86_64-microsoft-windows/win64_intel.masm",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_ARM7 AND OS_LINUX",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/armv7a-unknown-linux-gnueabihf",
                                        GLOBAL(configs_dir + "/armv7a-unknown-linux-gnueabihf/include"),
                                    ],
                                    SRCS=[
                                        "src/arm/ffi.c",
                                        "src/arm/sysv.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_ARM7 AND OS_ANDROID",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/armv7a-unknown-linux-androideabi16",
                                        GLOBAL(configs_dir + "/armv7a-unknown-linux-androideabi16/include"),
                                    ],
                                    SRCS=[
                                        "src/arm/ffi.c",
                                        "src/arm/sysv.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_ARM64 AND OS_LINUX",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/aarch64-unknown-linux-gnu",
                                        GLOBAL(configs_dir + "/aarch64-unknown-linux-gnu/include"),
                                    ],
                                    SRCS=[
                                        "src/aarch64/ffi.c",
                                        "src/aarch64/sysv.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_ARM64 AND OS_ANDROID",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/aarch64-unknown-linux-android21",
                                        GLOBAL(configs_dir + "/aarch64-unknown-linux-android21/include"),
                                    ],
                                    SRCS=[
                                        "src/aarch64/ffi.c",
                                        "src/aarch64/sysv.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_ARM64 AND OS_IOS",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/aarch64-apple-iphoneos",
                                        GLOBAL(configs_dir + "/aarch64-apple-iphoneos/include"),
                                    ],
                                    SRCS=[
                                        "src/aarch64/ffi.c",
                                        "src/aarch64/sysv.S",
                                    ],
                                ),
                            ),
                            (
                                "ARCH_PPC64LE AND OS_LINUX",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/powerpc64le-unknown-linux-gnu",
                                        GLOBAL(configs_dir + "/powerpc64le-unknown-linux-gnu/include"),
                                    ],
                                    SRCS=[
                                        "src/powerpc/ffi.c",
                                        "src/powerpc/ffi_linux64.c",
                                        "src/powerpc/ffi_sysv.c",
                                        "src/powerpc/linux64.S",
                                        "src/powerpc/linux64_closure.S",
                                        "src/powerpc/ppc_closure.S",
                                        "src/powerpc/sysv.S",
                                    ],
                                ),
                            ),
                            # fix only configure-stage for OS_NONE, see YMAKE-218, DEVTOOLSSUPPORT-46190
                            (
                                "OS_NONE",
                                Linkable(
                                    ADDINCL=[
                                        configs_dir + "/x86_64-pc-linux-gnu",
                                        GLOBAL(configs_dir + "/x86_64-pc-linux-gnu/include"),
                                    ],
                                ),
                            ),
                        ]
                        + [
                            (
                                "default",
                                "MESSAGE(FATAL_ERROR Unsupported libffi platform: ${TARGET_PLATFORM} / ${HARDWARE_TYPE})",
                            ),
                        ]
                    )
                )
            ),
        )

    def _add_test(testdir, src):
        modpath = "testsuite/" + testdir + "/" + src
        self.yamakes[modpath] = Program(
            SUBSCRIBER=self.owners,
            NO_RUNTIME=True,
            NO_COMPILER_WARNINGS=True,
            PEERDIR=[self.arcdir],
            SRCDIR=[self.arcdir + "/testsuite/" + testdir],
            SRCS=[src + ".c"],
            LICENSE=["GPL-2.0-only"],  # See LICENSE-BUILDTOOLS file
        )
        self.yamakes["."].RECURSE.add(modpath)

    for testpath in glob(self.dstdir + "/testsuite/*/*.c"):
        if basename(testpath) == "testcases.c":
            continue
        _add_test(basename(dirname(testpath)), basename(testpath)[:-2])

    self.yamakes.make_recursive()

    with self.yamakes["testsuite"] as m:
        m.RECURSE -= {"libffi.go", "libffi.complex"}
        # Fails to build.
        m.after("RECURSE", Switch({"NOT OS_IOS": Recursable(RECURSE={"libffi.go"})}))
        # MSVC does not support 'T _Complex' C syntax
        # powerpc64le is configured without complex types
        m.after(
            "RECURSE",
            Switch({"NOT OS_WINDOWS AND NOT ARCH_PPC64LE": Recursable(RECURSE={"libffi.complex"})}),
        )
    with self.yamakes["testsuite/libffi.closures"] as m:
        recs = {
            "cls_align_longdouble_split",
            "cls_align_longdouble_split2",
            "cls_many_mixed_float_double",
        }
        m.RECURSE -= recs
        m.after("RECURSE", Switch({"NOT OS_WINDOWS": Recursable(RECURSE=recs)}))


libffi = GNUMakeNixProject(
    nixattr="libffi",
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/libffi",
    makeflags=["libffi.la", "libffi.map"],
    copy_sources=[
        "src/aarch64/*",
        "src/arm/*",
        "src/powerpc/*",
        "src/x86/*",
        "testsuite/*/*.c",
        "testsuite/*/*.h",
        "testsuite/*/*.inc",
    ],
    disable_includes=[
        "os2.h",
        "sunmedia_types.h",
        "/usr/include/malloc.h",
    ],
    keep_paths=[
        "configs",
    ],
    post_build=libffi_post_build,
    post_install=libffi_post_install,
)
