SUBSCRIBER(
    akozhikhov
    g:yt
)

EXECTEST()

VERSION(2.31)

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

SIZE(MEDIUM)

RUN(erasure_code_test)

RUN(erasure_code_base_test)

RUN(erasure_code_update_test)

RUN(gf_inverse_test)

RUN(gf_vect_dot_prod_base_test)

RUN(gf_vect_dot_prod_test)

RUN(gf_vect_mad_test)

RUN(gf_vect_mul_test)

RUN(gf_vect_mul_base_test)

DEPENDS(
    contrib/libs/isa-l/erasure_code/ut/erasure_code_test
    contrib/libs/isa-l/erasure_code/ut/erasure_code_base_test
    contrib/libs/isa-l/erasure_code/ut/erasure_code_update_test
    contrib/libs/isa-l/erasure_code/ut/gf_inverse_test
    contrib/libs/isa-l/erasure_code/ut/gf_vect_dot_prod_base_test
    contrib/libs/isa-l/erasure_code/ut/gf_vect_dot_prod_test
    contrib/libs/isa-l/erasure_code/ut/gf_vect_mad_test
    contrib/libs/isa-l/erasure_code/ut/gf_vect_mul_test
    contrib/libs/isa-l/erasure_code/ut/gf_vect_mul_base_test
)

END()

RECURSE_FOR_TESTS(
    erasure_code_test
    erasure_code_base_test
    erasure_code_update_test
    gf_inverse_test
    gf_vect_dot_prod_base_test
    gf_vect_dot_prod_test
    gf_vect_mad_test
    gf_vect_mul_test
    gf_vect_mul_base_test
)
