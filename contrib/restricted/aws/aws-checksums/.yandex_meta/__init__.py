from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    # Otherwise debug build fails with gcc, as expected by the authors.
    with self.yamakes["."] as m:
        m.SRCS.remove("source/intel/intrin/crc64nvme_clmul.c")
        m.SRCS.remove("source/intel/intrin/crc32c_sse42_avx512.c")
        m.SRCS.remove("source/intel/intrin/crc64nvme_avx512.c")
        m.after(
            "SRCS",
            """
            IF (ARCH_X86_64)
                SRC_C_AVX(source/intel/intrin/crc64nvme_clmul.c)
                SRC_C_AVX512(source/intel/intrin/crc32c_sse42_avx512.c -mvpclmulqdq)
                SRC_C_AVX512(source/intel/intrin/crc64nvme_avx512.c -mvpclmulqdq)
            ELSEIF (ARCH_ARM64)
                SRC(source/arm/crc32c_arm.c -mcrc)
            ENDIF()
            """,
        )


aws_checksums = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-checksums",
    nixattr="aws-checksums",
    owners=["g:cpp-contrib"],
    copy_sources=[
        "source/arm/crc32c_arm.c",
    ],
    flags=["-DBUILD_JNI_BINDINGS=OFF"],
    disable_includes=["aws/checksums/crc_jni.h"],
    post_install=post_install,
)
