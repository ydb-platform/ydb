LIBRARY()

PEERDIR(
    ydb/core/protos
    ydb/library/yql/core/issue/protos
    ydb/public/api/protos
)

SRCS(
    modification_type.cpp
    error_codes.cpp
)

END()
