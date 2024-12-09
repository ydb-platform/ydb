import shutil

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.modules import Program, Linkable, Recursable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_build(self):
    # neon-compat.h is replaced during CMake build upon native / cross-compilation under arm.
    # As we do not run such compilation, we have to generate corresponding source manually.
    shutil.copy(
        f"{self.srcdir}/simd/arm/neon-compat.h.in",
        f"{self.dstdir}/simd/arm/neon-compat.h",
    )
    fileutil.re_sub_file(f"{self.dstdir}/simd/arm/neon-compat.h", r"\#cmakedefine", "// #cmakedefine")


def post_install(self):
    for prj, m in self.yamakes.items():
        if isinstance(m, Program):
            m.PEERDIR.add(self.arcdir)

    with self.yamakes["."] as m:
        # These flags break build of libjpeg-turbo with local xcode toolchain.
        # (`ya make --maps-mobile --target-platform=local-iossim-arm64' at the time)
        m.CFLAGS.remove("-DELF")
        m.CFLAGS.remove("-D__x86_64__")

        m.ADDINCL.remove(self.arcdir + "/simd/x86_64")
        m.ADDINCL.remove(self.arcdir + "/simd/nasm")
        m.ADDINCL.add(ArcPath(f"{self.arcdir}/simd/nasm", FOR="asm"))
        m.after("CFLAGS", Switch({"SANITIZER_TYPE": Linkable(CFLAGS=["-DWITH_SANITIZER"])}))
        m.after("ADDINCL", Switch({"OS_DARWIN OR OS_IOS": "SET(ASM_PREFIX '_')"}))

        # Sources from simd/arm/aarch64/*-neon.c are included into other sources.
        # This heuristics is used to detect whether the source should be compiled directly.
        def is_direct_source(src):
            return pathutil.is_source(src) and "ext" not in src

        amd64 = {s for s in m.SRCS if s.startswith("simd/")}
        i386 = fileutil.files(self.dstdir + "/simd/i386", rel=self.dstdir, test=is_direct_source)
        arm = fileutil.files(self.dstdir + "/simd/arm/aarch32", rel=self.dstdir, test=is_direct_source)
        arm64 = [
            src for src in fileutil.listdir(f"{self.dstdir}/simd/arm", rel=self.dstdir) if is_direct_source(src)
        ] + [
            src for src in fileutil.listdir(f"{self.dstdir}/simd/arm/aarch64", rel=self.dstdir) if is_direct_source(src)
        ]
        simd_none = ["jsimd_none.c"]

        # This file contains the older GNU Assembler implementation of the Neon SIMD
        # extensions for certain algorithms.
        # We are using clang 12+ which has a full set of Neon intrinsics
        arm64.remove("simd/arm/aarch64/jsimd_neon.S")

        m.SRCS -= amd64
        m.before(
            "SRCS",
            Switch(
                [
                    ("OS_ANDROID", Linkable(SRCS=simd_none)),
                    ("ARCH_I386", Linkable(SRCS=i386)),
                    ("ARCH_X86_64", Linkable(SRCS=amd64)),
                    ("ARCH_ARM7 AND NOT MSVC", Linkable(SRCS=arm)),
                    (
                        "ARCH_ARM64 AND NOT MSVC",
                        Linkable(SRCS=arm64, ADDINCL=[f"{self.arcdir}/simd/arm"]),
                    ),
                    ("default", Linkable(SRCS=simd_none)),
                ]
            ),
        )

        m.after(
            "RECURSE",
            Switch(
                [
                    (
                        "NOT OS_ANDROID AND NOT OS_IOS",
                        Recursable(RECURSE_FOR_TESTS=["ut"]),
                    ),
                ]
            ),
        )


libjpeg_turbo = CMakeNinjaNixProject(
    arcdir="contrib/libs/libjpeg-turbo",
    nixattr="libjpeg",
    owners=["g:cpp-contrib", "g:avatars"],
    ignore_commands={"bash", "sed"},
    install_targets={"turbojpeg", "cjpeg", "djpeg", "tjunittest", "jpegtran"},
    put={
        "turbojpeg": ".",
        "cjpeg": "cjpeg",
        "djpeg": "djpeg",
        "jpegtran": "jpegtran",
        "tjunittest": "tjunittest",
    },
    platform_dispatchers=["jconfigint.h"],
    copy_sources=[
        "simd/arm/",
        "simd/i386/",
        "simd/nasm/",
        "simd/x86_64/",
        "jsimd_none.c",
    ],
    keep_paths={
        "ut/*.py",
        "ut/canondata/",
        "ut/ya.make",
        "testimages/",
    },
    post_build=post_build,
    post_install=post_install,
)
