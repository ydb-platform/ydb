LIBRARY()

SRCS(
    yql_data_provider_impl.cpp
    yql_data_provider_impl.h
    yql_provider.cpp
    yql_provider.h
    yql_provider_names.h
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/sql # fixme
    ydb/library/yql/core
)

YQL_LAST_ABI_VERSION()

END()
