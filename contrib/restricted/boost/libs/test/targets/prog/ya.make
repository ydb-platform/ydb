LIBRARY(prg_exec_mon)

WITHOUT_LICENSE_TEXTS()

LICENSE(BSL-1.0)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)
PEERDIR(
    ${BOOST_ROOT}
)

SRCDIR(${BOOST_ROOT}/libs/test/src)

SRCS(
    execution_monitor.cpp
    debug.cpp
    cpp_main.cpp
)

END()
