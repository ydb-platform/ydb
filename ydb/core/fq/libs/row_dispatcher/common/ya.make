LIBRARY()

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

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(row_dispatcher_settings.h)

END()
