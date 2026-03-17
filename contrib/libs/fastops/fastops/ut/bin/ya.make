PROGRAM(fastops_test)

VERSION(1.0.0)

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

SRCDIR(contrib/libs/fastops/fastops/ut)

SRCS(
    fastops_ut.cpp
)

PEERDIR(
    contrib/libs/fastops/fastops/avx
    contrib/libs/fastops/fastops/avx2
    contrib/libs/fastops/fastops/plain
)

END()
