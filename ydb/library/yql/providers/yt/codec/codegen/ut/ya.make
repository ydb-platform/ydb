UNITTEST_FOR(ydb/library/yql/providers/yt/codec/codegen)

SRCS(
    yt_codec_cg_ut.cpp
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ELSE()
    SIZE(SMALL)
ENDIF()


PEERDIR(
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/yt/codec
)

YQL_LAST_ABI_VERSION()

IF (MKQL_DISABLE_CODEGEN)
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

END()
