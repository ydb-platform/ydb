LIBRARY()

SRCS(
    retry_queue.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/dq/actors/protos
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ydb")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
