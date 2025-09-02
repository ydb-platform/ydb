LIBRARY()

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.31)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    contrib/libs/isa-l/include
)

IF(ARCH_AARCH64)
CFLAGS(-D__ASSEMBLY__)
SRCS(
    ec_multibinary_arm.S
    gf_2vect_dot_prod_neon.S
    gf_2vect_dot_prod_sve.S
    gf_2vect_mad_neon.S
    gf_2vect_mad_sve.S
    gf_3vect_dot_prod_neon.S
    gf_3vect_dot_prod_sve.S
    gf_3vect_mad_neon.S
    gf_3vect_mad_sve.S
    gf_4vect_dot_prod_neon.S
    gf_4vect_dot_prod_sve.S
    gf_4vect_mad_neon.S
    gf_4vect_mad_sve.S
    gf_5vect_dot_prod_neon.S
    gf_5vect_dot_prod_sve.S
    gf_5vect_mad_neon.S
    gf_5vect_mad_sve.S
    gf_6vect_dot_prod_sve.S
    gf_6vect_mad_neon.S
    gf_6vect_mad_sve.S
    gf_7vect_dot_prod_sve.S
    gf_8vect_dot_prod_sve.S
    gf_vect_dot_prod_neon.S
    gf_vect_dot_prod_sve.S
    gf_vect_mad_neon.S
    gf_vect_mad_sve.S
    gf_vect_mul_neon.S
    gf_vect_mul_sve.S
)
ENDIF()

END()
