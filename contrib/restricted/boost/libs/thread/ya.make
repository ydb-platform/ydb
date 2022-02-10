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
    ${BOOST_ROOT}/libs/chrono 
    ${BOOST_ROOT}/libs/system 
)

IF (OS_WINDOWS)
    SRCS(
        src/win32/thread.cpp
        src/win32/tss_dll.cpp
        src/win32/tss_pe.cpp
    )
ELSE()
    SRCS(
        src/pthread/once.cpp
        src/pthread/thread.cpp
    )
ENDIF()

SRCS(
    src/future.cpp
)

END()
