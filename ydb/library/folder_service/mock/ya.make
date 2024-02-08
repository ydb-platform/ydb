LIBRARY()

SRCS(
    mock_folder_service_adapter.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/folder_service
    ydb/library/folder_service/proto
)

YQL_LAST_ABI_VERSION()

END()
