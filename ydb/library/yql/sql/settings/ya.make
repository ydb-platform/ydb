LIBRARY()

SRCS(
    partitioning.cpp
    translation_settings.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/json
    ydb/library/yql/core/issue
    ydb/library/yql/core/pg_settings
    ydb/library/yql/core/issue/protos
    ydb/library/yql/utils
)

END()
