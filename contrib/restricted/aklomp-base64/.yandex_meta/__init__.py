from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as lib:
        lib.SRCS -= set(["lib/arch/avx512/codec.c", "lib/arch/avx2/codec.c"])
        lib.before(
            "SRCS",
            """
            IF (ARCH_X86_64)
                SRC_C_AVX512(lib/arch/avx512/codec.c -mavx512vbmi)
            ELSE()
                SRC_C_AVX512(lib/arch/avx512/codec.c)
            ENDIF()
            """,
        )
        lib.before("SRCS", "SRC_C_AVX2(lib/arch/avx2/codec.c)")


aklomp_base64 = CMakeNinjaNixProject(
    nixattr="aklomp_base64",
    arcdir="contrib/restricted/aklomp-base64",
    addincl_global={
        ".": {"./include"},
    },
    post_install=post_install,
    # All <platform>/codec.c sources use relative paths for includes,
    # therefore, it will be difficult to disable all unnecessary sources without patches.
    copy_sources=[
        "lib/arch/generic/32/*.c",
        "lib/arch/ssse3/*.c",
        "lib/arch/avx2/*.c",
        "lib/arch/neon32/*.c",
        "lib/arch/neon64/*.c",
    ],
    disable_includes=["lib_openmp.c"],
    use_full_libnames=True,
    install_targets=[
        "libbase64",
    ],
    put={
        "libbase64": ".",
    },
    platform_dispatchers=[
        "./config.h",
    ],
)
