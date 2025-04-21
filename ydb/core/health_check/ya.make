LIBRARY()

SRCS(
    group_checker.h
    health_check_structs.h
    health_check_utils.h
    health_check.cpp
    health_check.h
    merge_issues.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/mon
    ydb/library/aclib
    ydb/public/api/protos
    ydb/public/api/grpc
    yql/essentials/public/issue/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
