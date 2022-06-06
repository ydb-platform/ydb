UNITTEST_FOR(ydb/core/persqueue)

OWNER(
    alexnick
    g:kikimr
    g:logbroker
)

FORK_SUBTESTS()

SPLIT_FACTOR(40)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(3000)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    internals_ut.cpp
    metering_sink_ut.cpp
    mirrorer_ut.cpp
    pq_ut.cpp
    pq_ut.h
    sourceid_ut.cpp
    type_codecs_ut.cpp
    user_info_ut.cpp
)

END()
