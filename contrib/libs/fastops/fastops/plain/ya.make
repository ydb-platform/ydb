LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

SRCS(
    ops_plain.h
    ops_plain.cpp
)

PEERDIR(
    contrib/libs/fmath
)

END()
