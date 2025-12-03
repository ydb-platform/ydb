UNITTEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/string_utils/url
    ydb/library/actors/http
    ydb/library/grpc/server
    ydb/library/grpc/server/actors
    ydb/core/base
    ydb/core/http_proxy
    ydb/core/http_proxy/ut/datastreams_fixture
    ydb/core/testlib/default
    ydb/library/aclib
    ydb/library/persqueue/tests
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/types
    ydb/services/ydb
)

SRCS(
    ../sqs_topic_ut.cpp
    inside_ydb_ut.cpp
)

ENV(INSIDE_YDB="1")

YQL_LAST_ABI_VERSION()

END()
