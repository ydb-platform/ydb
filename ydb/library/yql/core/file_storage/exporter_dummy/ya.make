LIBRARY()

OWNER(g:yql)

SRCS(
    file_exporter.cpp
)

PEERDIR(
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/utils
    ydb/library/yql/utils/fetch
    ydb/library/yql/utils/log
)

END()
