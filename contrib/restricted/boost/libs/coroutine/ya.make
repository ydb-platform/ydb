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
    ${BOOST_ROOT}/libs/context
) 
 
IF (OS_WINDOWS)
    SRCS(
        src/windows/stack_traits.cpp
    )
ELSE()
    SRCS(
        src/posix/stack_traits.cpp
    )
ENDIF()

SRCS(
    src/detail/coroutine_context.cpp
    src/exceptions.cpp
)

END()
