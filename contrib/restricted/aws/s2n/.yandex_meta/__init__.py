from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        avx2_srcs = []
        avx2_asm_srcs = []
        avx512_srcs = []
        for src in sorted(m.SRCS):
            if src.endswith("_avx2.c"):
                m.SRCS.remove(src)
                avx2_srcs.append(src)
            elif src.endswith("_avx2.S"):
                m.SRCS.remove(src)
                avx2_asm_srcs.append(src)
            elif src.endswith("_avx512.c"):
                m.SRCS.remove(src)
                avx512_srcs.append(src)

        x86_flags = [
            "-DS2N_CPUID_AVAILABLE",
            "-DS2N_KYBER512R3_AVX2_BMI2",
        ]
        for flag in x86_flags:
            m.CFLAGS.remove(flag)

        x86_only_section = Linkable(
            CFLAGS=x86_flags,
            SRCS=avx2_asm_srcs,
        )

        for src in avx2_srcs:
            x86_only_section.after("SRCS", f"SRC_C_AVX2({src})")

        for src in avx512_srcs:
            x86_only_section.after("SRCS", f"SRC_C_AVX512({src})")

        # Support Darwin.
        m.CFLAGS.remove("-DS2N_FEATURES_AVAILABLE")
        m.CFLAGS.remove("-DS2N_LINUX_SENDFILE")
        m.CFLAGS.remove("-DS2N_KTLS_SUPPORTED")
        m.after(
            "CFLAGS",
            Switch(
                OS_LINUX=Linkable(
                    CFLAGS=[
                        "-DS2N_FEATURES_AVAILABLE",
                        "-DS2N_LINUX_SENDFILE",
                        "-DS2N_KTLS_SUPPORTED",
                    ],
                )
            ),
        )

        # Support musl.
        m.CFLAGS.remove("-DS2N_STACKTRACE")
        m.after("CFLAGS", Switch({"NOT MUSL": Linkable(CFLAGS=["-DS2N_STACKTRACE"])}))

        m.after(
            "CFLAGS",
            Switch({"ARCH_X86_64": x86_only_section}),
        )


s2n = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/s2n",
    nixattr="s2n",
    disable_includes=[
        "openssl/mem.h",
        "openssl/provider.h",
        "openssl/hkdf.h",
        "sys/isa_defs.h",
    ],
    post_install=post_install,
)
