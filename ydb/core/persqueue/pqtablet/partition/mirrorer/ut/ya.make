UNITTEST_FOR(ydb/core/persqueue/pqtablet/partition/mirrorer)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()
SPLIT_FACTOR(20)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    mirrorer_actor_ut.cpp
    mirrorer_autoscaling_ut.cpp
    mirrorer_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/string_utils/base64
    library/cpp/svnversion
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/persqueue/common
    ydb/core/persqueue/common/proxy
    ydb/core/persqueue/events
    ydb/core/persqueue/pqtablet/partition/mirrorer
    ydb/core/persqueue/ut/common
    ydb/core/protos
    ydb/core/tablet
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/persqueue/topic_parser
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/library/kafka
)

END()
