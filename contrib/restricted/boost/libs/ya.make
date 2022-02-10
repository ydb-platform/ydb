LIBRARY()

LICENSE(BSL-1.0) 
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.67)

OWNER(
    antoshkka
    g:cpp-committee
    g:cpp-contrib
)

INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

# This is an "all batteries included" version of boost with a full set of components
# It should be avoided in common libraries or projects that care for binary size

PEERDIR(
    ${BOOST_ROOT}/libs/asio
    ${BOOST_ROOT}/libs/atomic
    ${BOOST_ROOT}/libs/chrono
    ${BOOST_ROOT}/libs/context
    ${BOOST_ROOT}/libs/container
    ${BOOST_ROOT}/libs/coroutine
    ${BOOST_ROOT}/libs/date_time
    ${BOOST_ROOT}/libs/exception
    ${BOOST_ROOT}/libs/filesystem
    ${BOOST_ROOT}/libs/iostreams
    ${BOOST_ROOT}/libs/locale
    ${BOOST_ROOT}/libs/program_options
    ${BOOST_ROOT}/libs/random
    ${BOOST_ROOT}/libs/regex
    ${BOOST_ROOT}/libs/timer
    ${BOOST_ROOT}/libs/log
    ${BOOST_ROOT}/libs/serialization
    ${BOOST_ROOT}/libs/system
    ${BOOST_ROOT}/libs/thread
)

END()

RECURSE(
    asio
    atomic
    chrono
    container
    context
    coroutine
    date_time
    exception
    filesystem
    iostreams
    locale
    log
    program_options
    random
    regex
    serialization
    system
    test
    thread
    timer
)

IF (NOT OS_ANDROID)
    RECURSE(
        python
    )
ENDIF()
