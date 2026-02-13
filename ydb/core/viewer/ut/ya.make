UNITTEST_FOR(ydb/core/viewer)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    viewer_ut.cpp
    topic_data_ut.cpp
    ut/ut_utils.cpp
)

PEERDIR(
    ydb/core/mon
    ydb/core/mon/ut_utils
    library/cpp/http/misc
    library/cpp/http/simple
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/core/tx/schemeshard/ut_helpers
)

END()
