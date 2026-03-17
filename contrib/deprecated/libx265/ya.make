LIBRARY()

LICENSE(
    GPL-2.0-only AND
    GPL-2.0-or-later AND
    ISC AND
    Public-Domain
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.6)

NO_COMPILER_WARNINGS()

NO_UTIL()

ADDINCL(
    GLOBAL contrib/deprecated/libx265
    contrib/deprecated/libx265/common
    contrib/deprecated/libx265/encoder
)

CFLAGS(
    -DEXPORT_C_API=1
    -DHAVE_INT_TYPES_H=1
    -DHIGH_BIT_DEPTH=0
    -DX265_DEPTH=8
    -DX265_NS=x265
    -DX265_VERSION=2.6
)

IF (ARCH_X86_64)
    CFLAGS(
        -DENABLE_ASSEMBLY=1
        -DX86_64=1
    )
ENDIF()

IF (OS_LINUX AND ARCH_X86_64 OR OS_DARWIN AND ARCH_X86_64 OR OS_DARWIN AND ARCH_ARM64)
    CFLAGS(
        -DHAVE_STRTOK_R=1
        -D__STDC_LIMIT_MACROS=1
    )
ENDIF()

IF (OS_LINUX AND ARCH_AARCH64)
    CFLAGS(
        -DHAVE_STRTOK_R=0
        -DX265_ARCH_ARM=1
    )
ELSE()
    CFLAGS(
        -DX265_ARCH_X86=1
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    CFLAGS(
        -DMACOS=1
    )
ENDIF()

SET_APPEND(YASM_FLAGS
    -DARCH_X86_64=1
    -DBIT_DEPTH=8
    -DHIGH_BIT_DEPTH=0
    -DX265_NS=x265
)

IF (OS_LINUX AND ARCH_X86_64 OR OS_DARWIN AND ARCH_X86_64)
    SET_APPEND(YASM_FLAGS
        -DHAVE_ALIGNED_STACK=1
        -DPIC
    )
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    SET_APPEND(YASM_FLAGS -DPREFIX)
ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    SET_APPEND(YASM_FLAGS -DHAVE_ALIGNED_STACK=0)
ENDIF()

SRCS(
    common/bitstream.cpp
    common/common.cpp
    common/constants.cpp
    common/cpu.cpp
    common/cudata.cpp
    common/dct.cpp
    common/deblock.cpp
    common/frame.cpp
    common/framedata.cpp
    common/intrapred.cpp
    common/ipfilter.cpp
    common/loopfilter.cpp
    common/lowpassdct.cpp
    common/lowres.cpp
    common/md5.cpp
    common/param.cpp
    common/piclist.cpp
    common/picyuv.cpp
    common/pixel.cpp
    common/predict.cpp
    common/primitives.cpp
    common/quant.cpp
    common/scalinglist.cpp
    common/shortyuv.cpp
    common/slice.cpp
    common/threading.cpp
    common/threadpool.cpp
    common/vec/vec-primitives.cpp
    common/version.cpp
    common/wavefront.cpp
    common/yuv.cpp
    encoder/analysis.cpp
    encoder/api.cpp
    encoder/bitcost.cpp
    encoder/dpb.cpp
    encoder/encoder.cpp
    encoder/entropy.cpp
    encoder/frameencoder.cpp
    encoder/framefilter.cpp
    encoder/level.cpp
    encoder/motion.cpp
    encoder/nal.cpp
    encoder/ratecontrol.cpp
    encoder/reference.cpp
    encoder/sao.cpp
    encoder/search.cpp
    encoder/sei.cpp
    encoder/slicetype.cpp
    encoder/weightPrediction.cpp
)

IF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
        common/winxp.cpp
    )
ENDIF()

IF (NOT ARCH_AARCH64)
    SRCS(
        common/x86/asm-primitives.cpp
        common/x86/blockcopy8.asm
        common/x86/const-a.asm
        common/x86/cpu-a.asm
        common/x86/dct8.asm
        common/x86/intrapred8.asm
        common/x86/intrapred8_allangs.asm
        common/x86/ipfilter8.asm
        common/x86/loopfilter.asm
        common/x86/mc-a.asm
        common/x86/mc-a2.asm
        common/x86/pixel-a.asm
        common/x86/pixel-util8.asm
        common/x86/pixeladd8.asm
        common/x86/sad-a.asm
        common/x86/seaintegral.asm
        common/x86/ssd-a.asm
    )
    SRC_C_SSE3(common/vec/dct-sse3.cpp)
    SRC_C_SSE41(common/vec/dct-sse41.cpp)
    SRC_C_SSSE3(common/vec/dct-ssse3.cpp)
ENDIF()

END()
