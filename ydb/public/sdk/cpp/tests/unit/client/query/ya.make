UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
   client_session_ut.cpp
   deferred_session_creation_ut.cpp
   query_stats_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/impl/session
    ydb/public/sdk/cpp/src/client/query/impl 
    ydb/public/sdk/cpp/src/client/query
)

END()
