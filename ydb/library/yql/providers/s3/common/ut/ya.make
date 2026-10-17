UNITTEST_FOR(ydb/library/yql/providers/s3/common)

SRCS(
    util_ut.cpp
)

IF (CLANG AND NOT WITH_VALGRIND)

    SRCS(
        source_context_ut.cpp
    )

ENDIF()

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
