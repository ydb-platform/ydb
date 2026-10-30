UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/common
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/impl/internal/scheme_helpers
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/tests/unit/client/draft/helpers
)

SRCS(
    ydb_keyvalue_ut.cpp
    ydb_replication_ut.cpp
    ydb_scripting_response_headers_ut.cpp
    ydb_view_ut.cpp
    ydb_udf_ut.cpp
)

END()
