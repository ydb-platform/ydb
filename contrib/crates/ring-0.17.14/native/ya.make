# The source selection follows RING_SRCS and ASM_TARGETS in ../build.rs.
# Use the regular C toolchain for target SDKs and sanitizer flags.
LIBRARY(arcadia_ring_core_0_17_14)

LICENSE(Apache-2.0 AND ISC)
VERSION(0.17.14)

NO_COMPILER_WARNINGS()

ADDINCL(
    contrib/crates/ring-0.17.14/include
    contrib/crates/ring-0.17.14/pregenerated
)

SRCDIR(contrib/crates/ring-0.17.14)

CFLAGS(-DNDEBUG)
IF (NOT OS_WINDOWS)
    CFLAGS(-fvisibility=hidden -std=c11)
ENDIF()

SRCS(
    crypto/curve25519/curve25519.c
    crypto/fipsmodule/aes/aes_nohw.c
    crypto/fipsmodule/bn/montgomery.c
    crypto/fipsmodule/bn/montgomery_inv.c
    crypto/fipsmodule/ec/ecp_nistz.c
    crypto/fipsmodule/ec/gfp_p256.c
    crypto/fipsmodule/ec/gfp_p384.c
    crypto/fipsmodule/ec/p256.c
    crypto/fipsmodule/ec/p256-nistz.c
    crypto/limbs/limbs.c
    crypto/mem.c
    crypto/poly1305/poly1305.c
)

IF (ARCH_X86_64)
    SRCS(
        crypto/crypto.c
        crypto/cpu_intel.c
        crypto/curve25519/curve25519_64_adx.c
    )
    IF (OS_WINDOWS)
        USE_NASM()
        ADDINCL(FOR asm contrib/crates/ring-0.17.14/pregenerated)
        SRCS(
            pregenerated/aes-gcm-avx2-x86_64-nasm.asm
            pregenerated/aesni-gcm-x86_64-nasm.asm
            pregenerated/aesni-x86_64-nasm.asm
            pregenerated/chacha-x86_64-nasm.asm
            pregenerated/chacha20_poly1305_x86_64-nasm.asm
            pregenerated/ghash-x86_64-nasm.asm
            pregenerated/p256-x86_64-asm-nasm.asm
            pregenerated/sha256-x86_64-nasm.asm
            pregenerated/sha512-x86_64-nasm.asm
            pregenerated/vpaes-x86_64-nasm.asm
            pregenerated/x86_64-mont-nasm.asm
            pregenerated/x86_64-mont5-nasm.asm
        )
    ELSE()
        SRCS(
            third_party/fiat/asm/fiat_curve25519_adx_mul.S
            third_party/fiat/asm/fiat_curve25519_adx_square.S
        )
        IF (OS_DARWIN)
            SRCS(
                pregenerated/aes-gcm-avx2-x86_64-macosx.S
                pregenerated/aesni-gcm-x86_64-macosx.S
                pregenerated/aesni-x86_64-macosx.S
                pregenerated/chacha-x86_64-macosx.S
                pregenerated/chacha20_poly1305_x86_64-macosx.S
                pregenerated/ghash-x86_64-macosx.S
                pregenerated/p256-x86_64-asm-macosx.S
                pregenerated/sha256-x86_64-macosx.S
                pregenerated/sha512-x86_64-macosx.S
                pregenerated/vpaes-x86_64-macosx.S
                pregenerated/x86_64-mont-macosx.S
                pregenerated/x86_64-mont5-macosx.S
            )
        ELSE()
            SRCS(
                pregenerated/aes-gcm-avx2-x86_64-elf.S
                pregenerated/aesni-gcm-x86_64-elf.S
                pregenerated/aesni-x86_64-elf.S
                pregenerated/chacha-x86_64-elf.S
                pregenerated/chacha20_poly1305_x86_64-elf.S
                pregenerated/ghash-x86_64-elf.S
                pregenerated/p256-x86_64-asm-elf.S
                pregenerated/sha256-x86_64-elf.S
                pregenerated/sha512-x86_64-elf.S
                pregenerated/vpaes-x86_64-elf.S
                pregenerated/x86_64-mont-elf.S
                pregenerated/x86_64-mont5-elf.S
            )
        ENDIF()
    ENDIF()
ELSEIF (ARCH_AARCH64 OR ARCH_ARM64)
    IF (OS_DARWIN)
        SRCS(
            pregenerated/aesv8-armx-ios64.S
            pregenerated/aesv8-gcm-armv8-ios64.S
            pregenerated/armv8-mont-ios64.S
            pregenerated/chacha-armv8-ios64.S
            pregenerated/chacha20_poly1305_armv8-ios64.S
            pregenerated/ghash-neon-armv8-ios64.S
            pregenerated/ghashv8-armx-ios64.S
            pregenerated/p256-armv8-asm-ios64.S
            pregenerated/sha256-armv8-ios64.S
            pregenerated/sha512-armv8-ios64.S
            pregenerated/vpaes-armv8-ios64.S
        )
    ELSE()
        SRCS(
            pregenerated/aesv8-armx-linux64.S
            pregenerated/aesv8-gcm-armv8-linux64.S
            pregenerated/armv8-mont-linux64.S
            pregenerated/chacha-armv8-linux64.S
            pregenerated/chacha20_poly1305_armv8-linux64.S
            pregenerated/ghash-neon-armv8-linux64.S
            pregenerated/ghashv8-armx-linux64.S
            pregenerated/p256-armv8-asm-linux64.S
            pregenerated/sha256-armv8-linux64.S
            pregenerated/sha512-armv8-linux64.S
            pregenerated/vpaes-armv8-linux64.S
        )
    ENDIF()
ENDIF()

END()
