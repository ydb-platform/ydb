UNITTEST_FOR(ydb/apps/etcd_proxy/service)

SIZE(MEDIUM)

SRCS(
    etcd_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/protos
    ydb/core/testlib/default
    ydb/apps/etcd_proxy/service
    ydb/services/keyvalue
)

YQL_LAST_ABI_VERSION()

END()
