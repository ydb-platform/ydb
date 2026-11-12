YQL_LIBRARY()

SRCS(
    row_dispatcher_settings.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/ydb
    ydb/core/protos
    ydb/library/accessor
    ydb/library/actors/core
)

GENERATE_ENUM_SERIALIZATION(row_dispatcher_settings.h)

END()
