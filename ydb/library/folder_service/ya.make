LIBRARY()

SRCS(
    events.h
    folder_service.cpp
    folder_service.h
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/library/folder_service/proto
)

END()

RECURSE(
    mock
    proto
)
