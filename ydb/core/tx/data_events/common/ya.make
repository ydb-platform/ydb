LIBRARY()

PEERDIR(
    ydb/core/protos
    ydb/library/conclusion
    yql/essentials/core/issue/protos
    ydb/public/api/protos
)

SRCS(
    modification_type.cpp
    error_codes.cpp
)

END()
