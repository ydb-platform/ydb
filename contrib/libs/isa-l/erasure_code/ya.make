LIBRARY(isa-l_ec)

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.28)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    contrib/libs/isa-l/include
    FOR
    asm
    contrib/libs/isa-l/include
)

SRCS(
    ec_base.c
    ec_highlevel_func.c
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
    gf_vect_mul_sse.asm
    gf_vect_mul_avx.asm
    gf_vect_dot_prod_sse.asm
    gf_vect_dot_prod_avx.asm
    gf_vect_dot_prod_avx2.asm
    gf_vect_dot_prod_avx512.asm
    gf_2vect_dot_prod_sse.asm
    gf_2vect_dot_prod_avx.asm
    gf_2vect_dot_prod_avx2.asm
    gf_2vect_dot_prod_avx512.asm
    gf_3vect_dot_prod_sse.asm
    gf_3vect_dot_prod_avx.asm
    gf_3vect_dot_prod_avx2.asm
    gf_3vect_dot_prod_avx512.asm
    gf_4vect_dot_prod_sse.asm
    gf_4vect_dot_prod_avx.asm
    gf_4vect_dot_prod_avx2.asm
    gf_4vect_dot_prod_avx512.asm
    gf_5vect_dot_prod_sse.asm
    gf_5vect_dot_prod_avx.asm
    gf_5vect_dot_prod_avx2.asm
    gf_6vect_dot_prod_sse.asm
    gf_6vect_dot_prod_avx.asm
    gf_6vect_dot_prod_avx2.asm
    gf_vect_mad_sse.asm
    gf_vect_mad_avx.asm
    gf_vect_mad_avx2.asm
    gf_vect_mad_avx512.asm
    gf_2vect_mad_sse.asm
    gf_2vect_mad_avx.asm
    gf_2vect_mad_avx2.asm
    gf_2vect_mad_avx512.asm
    gf_3vect_mad_sse.asm
    gf_3vect_mad_avx.asm
    gf_3vect_mad_avx2.asm
    gf_3vect_mad_avx512.asm
    gf_4vect_mad_sse.asm
    gf_4vect_mad_avx.asm
    gf_4vect_mad_avx2.asm
    gf_4vect_mad_avx512.asm
    gf_5vect_mad_sse.asm
    gf_5vect_mad_avx.asm
    gf_5vect_mad_avx2.asm
    gf_6vect_mad_sse.asm
    gf_6vect_mad_avx.asm
    gf_6vect_mad_avx2.asm
)
ENDIF()

END()
