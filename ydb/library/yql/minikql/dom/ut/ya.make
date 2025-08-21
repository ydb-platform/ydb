IF (NOT WINDOWS)
    UNITTEST_FOR(ydb/library/yql/minikql/dom)

    TAG(ya:manual)

    SRCS(
        yson_ut.cpp
        json_ut.cpp
    )

    SIZE(MEDIUM)

    PEERDIR(
        ydb/library/yql/minikql/computation/llvm14
        ydb/library/yql/public/udf/service/exception_policy
        ydb/library/yql/sql/pg_dummy
    )

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()
