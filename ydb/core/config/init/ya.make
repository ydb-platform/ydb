LIBRARY()

SRCS(
    init.h
    init.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/base
    ydb/library/yaml_config
)

YQL_LAST_ABI_VERSION()

END()
