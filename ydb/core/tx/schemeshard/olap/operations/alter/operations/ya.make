LIBRARY()

SRCS(
    start.cpp
    continue.cpp
    publish.cpp
    finish.cpp
)

PEERDIR(
    ydb/library/login/protos
    ydb/core/protos
    contrib/libs/apache/arrow
    library/cpp/lwtrace/protos
    ydb/library/aclib/protos
    contrib/libs/opentelemetry-proto
)

YQL_LAST_ABI_VERSION()

END()
