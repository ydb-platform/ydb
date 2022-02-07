OWNER(g:yq)

LIBRARY()

SRCS(
    mock_folder_service.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/folder_service
    ydb/library/folder_service/proto
)

YQL_LAST_ABI_VERSION()

END()
