LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.1.3)

ADDINCL(GLOBAL contrib/libs/metrohash/src)

NO_UTIL()

CFLAGS(-msse4.2)

SRCS(
    src/metrohash128.cpp
    src/metrohash128crc.cpp
    src/metrohash64.cpp
)

END()
