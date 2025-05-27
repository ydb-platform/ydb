LIBRARY(isa-l_ec)

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.31)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    contrib/libs/isa-l/include
    FOR
    asm
    contrib/libs/isa-l/include
)

IF (ARCH_X86_64)
IF (OS_DARWIN)
    SRCS(
        ec_multibinary_darwin.asm
    )
ELSE()
    SRCS(
        ec_multibinary.asm
    )
ENDIF()

SRCS(
    ec_base.c
    ec_highlevel_func.c
    gf_2vect_dot_prod_avx2.asm
    gf_2vect_dot_prod_avx2_gfni.asm
    gf_2vect_dot_prod_avx512.asm
    gf_2vect_dot_prod_avx512_gfni.asm
    gf_2vect_dot_prod_avx.asm
    gf_2vect_dot_prod_sse.asm
    gf_2vect_mad_avx2.asm
    gf_2vect_mad_avx2_gfni.asm
    gf_2vect_mad_avx512.asm
    gf_2vect_mad_avx512_gfni.asm
    gf_2vect_mad_avx.asm
    gf_2vect_mad_sse.asm
    gf_3vect_dot_prod_avx2.asm
    gf_3vect_dot_prod_avx2_gfni.asm
    gf_3vect_dot_prod_avx512.asm
    gf_3vect_dot_prod_avx512_gfni.asm
    gf_3vect_dot_prod_avx.asm
    gf_3vect_dot_prod_sse.asm
    gf_3vect_mad_avx2.asm
    gf_3vect_mad_avx2_gfni.asm
    gf_3vect_mad_avx512.asm
    gf_3vect_mad_avx512_gfni.asm
    gf_3vect_mad_avx.asm
    gf_3vect_mad_sse.asm
    gf_4vect_dot_prod_avx2.asm
    gf_4vect_dot_prod_avx512.asm
    gf_4vect_dot_prod_avx512_gfni.asm
    gf_4vect_dot_prod_avx.asm
    gf_4vect_dot_prod_sse.asm
    gf_4vect_mad_avx2.asm
    gf_4vect_mad_avx2_gfni.asm
    gf_4vect_mad_avx512.asm
    gf_4vect_mad_avx512_gfni.asm
    gf_4vect_mad_avx.asm
    gf_4vect_mad_sse.asm
    gf_5vect_dot_prod_avx2.asm
    gf_5vect_dot_prod_avx512.asm
    gf_5vect_dot_prod_avx512_gfni.asm
    gf_5vect_dot_prod_avx.asm
    gf_5vect_dot_prod_sse.asm
    gf_5vect_mad_avx2.asm
    gf_5vect_mad_avx2_gfni.asm
    gf_5vect_mad_avx512.asm
    gf_5vect_mad_avx512_gfni.asm
    gf_5vect_mad_avx.asm
    gf_5vect_mad_sse.asm
    gf_6vect_dot_prod_avx2.asm
    gf_6vect_dot_prod_avx512.asm
    gf_6vect_dot_prod_avx512_gfni.asm
    gf_6vect_dot_prod_avx.asm
    gf_6vect_dot_prod_sse.asm
    gf_6vect_mad_avx2.asm
    gf_6vect_mad_avx512.asm
    gf_6vect_mad_avx512_gfni.asm
    gf_6vect_mad_avx.asm
    gf_6vect_mad_sse.asm
    gf_vect_dot_prod_avx2.asm
    gf_vect_dot_prod_avx2_gfni.asm
    gf_vect_dot_prod_avx512.asm
    gf_vect_dot_prod_avx512_gfni.asm
    gf_vect_dot_prod_avx.asm
    gf_vect_dot_prod_sse.asm
    gf_vect_mad_avx2.asm
    gf_vect_mad_avx2_gfni.asm
    gf_vect_mad_avx512.asm
    gf_vect_mad_avx512_gfni.asm
    gf_vect_mad_avx.asm
    gf_vect_mad_sse.asm
    gf_vect_mul_avx.asm
    gf_vect_mul_sse.asm
)
ELSEIF(ARCH_AARCH64)
SRCS(
    ec_base.c
    aarch64/ec_aarch64_dispatcher.c
    aarch64/ec_aarch64_highlevel_func.c
)

PEERDIR(
    contrib/libs/isa-l/erasure_code/aarch64
)
ENDIF()

END()
