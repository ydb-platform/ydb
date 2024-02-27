LIBRARY()

PEERDIR(
    library/cpp/string_utils/parse_size
    ydb/library/yql/minikql
    ydb/library/yql/sql
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/integration
)

GENERATE_ENUM_SERIALIZATION(yql_dq_settings.h)

SRCS(
    attrs.cpp
    yql_dq_common.cpp
    yql_dq_settings.cpp
)

YQL_LAST_ABI_VERSION()

END()
