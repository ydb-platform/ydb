from devtools.yamaker.modules import Linkable, Switch, Words
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    m = self.yamakes["."]

    m.CFLAGS.remove("-DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_PTHREAD_ATTR")
    m.after(
        "CFLAGS",
        Switch(
            # musl does not provide pthread_attr_setaffinity_np,
            # use pthread_setaffinity_np after thread creation
            MUSL=Linkable(
                CFLAGS=["-DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_PTHREAD"],
            ),
            OS_DARWIN=Linkable(
                # MacOS does not provide cpu_set_t structure
                CFLAGS=["-DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_NONE"],
                # aws_wrapped_cf_allocator uses CFAllocator.
                LDFLAGS=[Words("-framework", "CoreFoundation")],
            ),
            OS_WINDOWS=Linkable(
                CFLAGS=["-DAWS_COMMON_EXPORTS"],
            ),
            default=Linkable(
                CFLAGS=["-DAWS_AFFINITY_METHOD=AWS_AFFINITY_METHOD_PTHREAD_ATTR"],
            ),
        ),
    )

    # handle arch-specific SRCS
    m.SRCS.remove("source/arch/intel/asm/cpuid.c")
    m.SRCS.remove("source/arch/intel/cpuid.c")
    m.SRCS.remove("source/arch/intel/encoding_avx2.c")
    if_intel = Linkable(
        SRCS=[
            "source/arch/intel/asm/cpuid.c",
            "source/arch/intel/cpuid.c",
        ],
        SRC_C_AVX2=[
            [
                "source/arch/intel/encoding_avx2.c",
            ]
        ],
    )
    if_arm = Linkable(
        SRCS=[
            "source/arch/arm/auxv/cpuid.c",
        ]
    )
    m.after(
        "SRCS",
        Switch(
            ARCH_X86_64=if_intel,
            ARCH_ARM=if_arm,
        ),
    )

    posix_srcs = []
    for src in sorted(m.SRCS):
        if "/posix/" in src:
            m.SRCS.remove(src)
            posix_srcs.append(src)

    m.after(
        "SRCS",
        Switch(
            {
                "NOT OS_WINDOWS": Linkable(SRCS=posix_srcs),
            }
        ),
    )

    # handle arch-specific CFLAGS
    m.CFLAGS.remove("-DUSE_SIMD_ENCODING")
    m.after(
        "CFLAGS",
        Switch(
            ARCH_X86_64=Linkable(
                CFLAGS=[
                    "-DUSE_SIMD_ENCODING",
                ],
            )
        ),
    )


aws_c_common = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-common",
    nixattr="aws-c-common",
    owners=["g:cpp-contrib"],
    flags=["-DAWS_HAVE_EXECINFO=OFF"],
    copy_sources=[
        "include/aws/common/*.inl",
        "include/aws/common/external/ittnotify.h",
        "source/arch/arm/auxv/cpuid.c",
    ],
    disable_includes=[
        "legacy/ittnotify.h",
    ],
    platform_dispatchers=[
        "generated/include/aws/common/config.h",
    ],
    install_targets=["aws-c-common"],
    post_install=post_install,
)
