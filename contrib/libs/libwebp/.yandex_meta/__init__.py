from devtools.yamaker.project import GNUMakeNixProject


SHARPYUV_SUBLIBS = [
    "libsharpyuv_neon",
    "libsharpyuv_sse2",
]


WEBP_SUBLIBS = [
    # "libexample_util",
    # "libimageio_util",
    "libwebp",
    "libwebpdecode",
    "libwebpdecoder",
    "libwebpdemux",
    "libwebpdsp",
    "libwebpdsp_mips32",
    "libwebpdsp_mips_dsp_r2",
    "libwebpdsp_msa",
    "libwebpdsp_neon",
    "libwebpdsp_sse2",
    "libwebpdsp_sse41",
    "libwebpdspdecode",
    "libwebpdspdecode_mips32",
    "libwebpdspdecode_mips_dsp_r2",
    "libwebpdspdecode_msa",
    "libwebpdspdecode_neon",
    "libwebpdspdecode_sse2",
    "libwebpdspdecode_sse41",
    "libwebpencode",
    "libwebpmux",
    "libwebputils",
    "libwebputilsdecode",
]


def post_install(self):
    with self.yamakes["."] as libwebp:
        libwebp.ADDINCL.add(self.arcdir)

    for path in (".", "sharpyuv"):
        # Support Android build
        self.yamakes[path].after(
            "PEERDIR",
            """
            IF (OS_ANDROID)
                PEERDIR(
                    contrib/libs/android_cpufeatures
                )
                ADDINCL(
                    contrib/libs/android_cpufeatures
                )
            ENDIF()
            """,
        )


libwebp = GNUMakeNixProject(
    arcdir="contrib/libs/libwebp",
    nixattr="libwebp",
    copy_sources=[
        "src/dsp/mips_macro.h",
        "src/dsp/msa_macro.h",
        "src/dsp/neon.h",
    ],
    platform_dispatchers=[
        "src/webp/config.h",
    ],
    use_full_libnames=True,
    # fmt: off
    install_targets=[
        "libwebp",
        "libsharpyuv"
    ] + SHARPYUV_SUBLIBS + WEBP_SUBLIBS,
    # fmt: on
    put={
        "libwebp": ".",
        "libsharpyuv": "sharpyuv",
    },
    put_with={
        "libwebp": WEBP_SUBLIBS,
        "libsharpyuv": SHARPYUV_SUBLIBS,
    },
    inclink={
        # put public includes to top-level
        "webp": ["src/webp/*.h"],
    },
    post_install=post_install,
)
