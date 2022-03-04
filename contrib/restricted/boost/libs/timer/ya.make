LIBRARY()

LICENSE(BSL-1.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)
PEERDIR(
    ${BOOST_ROOT}
)

SRCS(
    src/auto_timers_construction.cpp
    src/cpu_timer.cpp
)

END()
