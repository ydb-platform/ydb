UNITTEST_FOR(ydb/core/local_proxy/local_pq_client)

FORK_SUBTESTS()

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    common.cpp
    local_deferred_publish_client_ut.cpp
    local_federated_topic_client_ut.cpp
    local_topic_client_factory_ut.cpp
    local_topic_client_ut.cpp
    local_topic_read_session_ut.cpp
    local_topic_write_session_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/library/aclib
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
