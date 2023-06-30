UNITTEST_FOR(ydb/library/yql/providers/yt/codec/codegen)

SRCS(
    yt_codec_cg_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation/llvm
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
