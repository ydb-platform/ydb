LIBRARY()

SRCS(
    interactive_config.cpp
    interactive_log.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/base
    ydb/public/lib/ydb_cli/common
)

GENERATE_ENUM_SERIALIZATION(interactive_config.h)

END()
