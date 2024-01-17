LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/grpc/client
    yt/cpp/mapreduce/interface
    ydb/library/yql/providers/dq/config
    ydb/library/yql/core/issue
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/yt/lib/log
    ydb/library/yql/providers/dq/actors/events
)

IF (NOT OS_WINDOWS)
    PEERDIR(
        yt/yt/client
    )
ENDIF()

SET(
    SOURCE
    nodeid_assigner.cpp
    nodeid_assigner.h
    resource_manager.cpp
    resource_manager.h
)

IF (NOT OS_WINDOWS)
    SET(
        SOURCE
        ${SOURCE}
        nodeid_cleaner.cpp
        nodeid_cleaner.h
        worker_registrator.cpp
        worker_registrator.h
        lock.cpp
        lock.h
        resource_uploader.cpp
        resource_downloader.cpp
        resource_cleaner.cpp
        yt_wrapper.cpp
        yt_wrapper.h
        yt_resource_manager.cpp
    )
ENDIF()

SRCS(
    ${SOURCE}
)

YQL_LAST_ABI_VERSION()

END()
