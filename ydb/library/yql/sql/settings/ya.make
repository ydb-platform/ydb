LIBRARY()

OWNER(
    g:yql g:yql_ydb_core
)

SRCS(
    translation_settings.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    ydb/library/yql/core/issue
    ydb/library/yql/core/issue/protos
    ydb/library/yql/utils
)

END()
