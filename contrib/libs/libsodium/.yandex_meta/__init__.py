import os

from devtools.yamaker.modules import GLOBAL, Switch, Linkable
from devtools.yamaker.project import GNUMakeNixProject

libsodium_cpu_features = dict(
    aesni="-maes -mpclmul",
    avx2="",
    avx512f="",
    rdrand="-mrdrnd",
    sse2="",
    sse41="-msse4.1",
    ssse3="",
)


def libsodium_post_install(self):
    # Move runtime-cpu-dispatched sources into the main ya.make.
    srcs = []
    for s, cflags in libsodium_cpu_features.items():
        name = "m/" + s
        prefix = os.path.relpath(self.yamakes[name].SRCDIR[0], self.arcdir) + "/"
        if prefix == "./":
            prefix = ""
        for src in self.yamakes[name].SRCS:
            srcs.append([prefix + src] + (cflags or "-m" + s).split())
        del self.yamakes[name]

    with self.yamakes["."] as m:
        m.PEERDIR = []
        m.RECURSE = []
        m.after("SRCS", Switch(ARCH_X86_64=Linkable(SRC=srcs)))
        # Fix build with the default glibc.
        m.CFLAGS.remove("-DHAVE_EXPLICIT_BZERO=1")
        m.CFLAGS.remove("-DHAVE_GETRANDOM=1")
        m.CFLAGS.remove("-DHAVE_SYS_RANDOM_H=1")

        # Support Darwin.
        linux_cflags = [
            "-DASM_HIDE_SYMBOL=.hidden",
            "-DHAVE_GETENTROPY=1",
        ]
        darwin_cflags = [
            "-DASM_HIDE_SYMBOL=.private_extern",
            "-DHAVE_ARC4RANDOM=1",
            "-DHAVE_ARC4RANDOM_BUF=1",
            "-DHAVE_MEMSET_S=1",
            "-DHAVE_SYS_RANDOM_H=1",
        ]
        x86_64_cflags = [
            "-DHAVE_AMD64_ASM=1",
            "-DHAVE_AVX2INTRIN_H=1",
            "-DHAVE_AVX512FINTRIN_H=1",
            "-DHAVE_AVXINTRIN_H=1",
            "-DHAVE_AVX_ASM=1",
            "-DHAVE_CATCHABLE_ABRT=1",
            "-DHAVE_CATCHABLE_SEGV=1",
            "-DHAVE_CPUID=1",
            "-DHAVE_EMMINTRIN_H=1",
            "-DHAVE_MMINTRIN_H=1",
            "-DHAVE_PMMINTRIN_H=1",
            "-DHAVE_RDRAND=1",
            "-DHAVE_SMMINTRIN_H=1",
            "-DHAVE_TMMINTRIN_H=1",
            "-DHAVE_WMMINTRIN_H=1",
        ]
        for cflag in linux_cflags + x86_64_cflags:
            print(f"Removing {cflag}")
            m.CFLAGS.remove(cflag)
        # Support Windows.
        m.after("CFLAGS", Switch({"NOT OS_WINDOWS": Linkable(CFLAGS=m.CFLAGS)}))
        m.after(
            "CFLAGS",
            Switch({"NOT OS_WINDOWS AND ARCH_X86_64": Linkable(CFLAGS=x86_64_cflags)}),
        )
        m.CFLAGS = [GLOBAL("-DSODIUM_STATIC")]
        m.after(
            "CFLAGS",
            Switch(
                OS_LINUX=Linkable(CFLAGS=linux_cflags),
                OS_DARWIN=Linkable(CFLAGS=darwin_cflags),
            ),
        )
        asms = {s for s in m.SRCS if s.endswith(".S")}
        m.SRCS -= asms
        m.after("SRCS", Switch({"NOT OS_WINDOWS AND ARCH_X86_64": Linkable(SRCS=asms)}))


libsodium = GNUMakeNixProject(
    arcdir="contrib/libs/libsodium",
    nixattr="libsodium",
    disable_includes=["emscripten.h", "randombytes_internal.h"],
    copy_sources=[
        "crypto_core/ed25519/ref10/fe_25_5/",
        "crypto_onetimeauth/poly1305/donna/poly1305_donna32.h",
        "include/sodium.h",
        "include/sodium/private/ed25519_ref10_fe_25_5.h",
    ],
    makeflags=["-C", "src/libsodium", "libsodium.la"],
    install_subdir="src/libsodium",
    put={s: "m/" + s for s in libsodium_cpu_features},
    post_install=libsodium_post_install,
)
