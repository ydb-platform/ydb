PROGRAM()

VERSION(2.31)

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

SUBSCRIBER(
    akozhikhov
    g:base
    g:yt
)

ADDINCL(contrib/libs/isa-l/include)

NO_COMPILER_WARNINGS()

SRCS(
    ../../gf_vect_mul_base_test.c
)

PEERDIR(
    contrib/libs/isa-l/erasure_code
)

END()
