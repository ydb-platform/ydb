LIBRARY()

LICENSE(BSL-1.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

SRCS(
    src/random_device.cpp
)

PEERDIR(
    contrib/restricted/boost/libs/system
)

END()
