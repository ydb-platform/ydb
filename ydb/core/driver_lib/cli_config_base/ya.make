LIBRARY()

SRCS(
    config_base.h
    config_base.cpp
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    ydb/core/util
    ydb/public/lib/deprecated/client
    ydb/library/yql/minikql
)

YQL_LAST_ABI_VERSION()

END()
