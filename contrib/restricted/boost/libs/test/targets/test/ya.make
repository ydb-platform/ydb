LIBRARY(test_exec_mon)

WITHOUT_LICENSE_TEXTS()

LICENSE(BSL-1.0)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc) 

SRCDIR(${BOOST_ROOT}/libs/test/src)

PEERDIR(
    ${BOOST_ROOT}/libs/test/targets/lib
)

SRCS(
    test_main.cpp
)

END()
