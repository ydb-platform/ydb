import shutil

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.modules import Program, Linkable, Switch
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
        m.after(
            "CFLAGS",
            """
            CHECK_CONFIG_H(jconfigint.h)
            """,
        )

        m.ADDINCL.remove(self.arcdir + "/simd/x86_64")
        m.ADDINCL.remove(self.arcdir + "/simd/nasm")
        m.ADDINCL.add(ArcPath(f"{self.arcdir}/simd/nasm", FOR="asm"))
        m.after("CFLAGS", Switch({"SANITIZER_TYPE": Linkable(CFLAGS=["-DWITH_SANITIZER"])}))
        m.after("ADDINCL", Switch({"OS_DARWIN OR OS_IOS": "SET(ASM_PREFIX '_')"}))

        # Sources from simd/arm/aarch64/*-neon.c are included into other sources.
        # This heuristics is used to detect whether the source should be compiled directly.
        def is_direct_source(src):
            return pathutil.is_source(src) and "ext" not in src

        def list_simd_sources(arch):
            # fmt: off
            return [
                src
                for src in fileutil.listdir(f"{self.dstdir}/simd/{arch}", rel=self.dstdir)
                if is_direct_source(src)
            ]
            # fmt: on

        amd64 = {s for s in m.SRCS if s.startswith("simd/")}
        i386 = list_simd_sources("i386")
        arm32 = list_simd_sources("arm/aarch32") + list_simd_sources("arm")
        arm64 = list_simd_sources("arm/aarch64") + list_simd_sources("arm")

        # This file contains the older GNU Assembler implementation of the Neon SIMD
        # extensions for certain algorithms.
        # We are using clang 12+ which has a full set of Neon intrinsics
        arm64.remove("simd/arm/aarch64/jsimd_neon.S")

        m.SRCS -= amd64
        m.before(
            "SRCS",
            Switch(
                [
                    ("ARCH_I386 AND NOT OS_ANDROID", Linkable(SRCS=i386)),
                    ("ARCH_X86_64", Linkable(SRCS=amd64)),
                    (
                        "ARCH_ARM7",
                        Linkable(
                            SRCS=arm32,
                            ADDINCL=[f"{self.arcdir}/simd/arm"],
                        ),
                    ),
                    (
                        "ARCH_ARM64",
                        Linkable(
                            SRCS=arm64,
                            ADDINCL=[f"{self.arcdir}/simd/arm"],
                        ),
                    ),
                ]
            ),
        )


libjpeg_turbo = CMakeNinjaNixProject(
    arcdir="contrib/libs/libjpeg-turbo",
    nixattr="libjpeg",
    owners=["g:cpp-contrib", "g:avatars"],
    ignore_commands={"bash", "sed"},
    use_full_libnames=True,
    install_targets={
        "libturbojpeg",
        "cjpeg",
        "djpeg",
        "jpegtran",
    },
    put={
        "libturbojpeg": ".",
        "cjpeg": "cjpeg",
        "djpeg": "djpeg",
        "jpegtran": "jpegtran",
    },
    platform_dispatchers=["jconfigint.h"],
    copy_sources=[
        "simd/arm/",
        "simd/i386/",
        "simd/nasm/",
        "simd/x86_64/",
    ],
    inclink={
        ".": [
            "src/jpeglib.h",
            "src/jmorecfg.h",
            "src/jpegint.h",
            "src/jerror.h",
            "src/transupp.h",
            "src/turbojpeg.h",
        ],
        "src": [
            "jconfig.h",
        ],
    },
    post_build=post_build,
    post_install=post_install,
)
