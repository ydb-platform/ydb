LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

SRCS(
    fastops.h
    fastops.cpp
)

PEERDIR(
    contrib/libs/fastops/fastops/avx
    contrib/libs/fastops/fastops/avx2
    contrib/libs/fastops/fastops/plain
)

END()
