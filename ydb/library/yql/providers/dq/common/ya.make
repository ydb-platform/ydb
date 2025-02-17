LIBRARY()

PEERDIR(
    library/cpp/string_utils/parse_size
    yql/essentials/minikql
    yql/essentials/utils
    yql/essentials/utils/log
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/proto
    yql/essentials/core/dq_integration
)

GENERATE_ENUM_SERIALIZATION(yql_dq_settings.h)

SRCS(
    attrs.cpp
    yql_dq_common.cpp
    yql_dq_settings.cpp
)

YQL_LAST_ABI_VERSION()

END()
