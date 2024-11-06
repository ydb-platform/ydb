IF (NOT WINDOWS)
    UNITTEST_FOR(yql/essentials/minikql/dom)

    SRCS(
        yson_ut.cpp
        json_ut.cpp
    )

    SIZE(MEDIUM)

    PEERDIR(
        contrib/ydb/library/yql/minikql/computation/llvm14
        yql/essentials/public/udf/service/exception_policy
        contrib/ydb/library/yql/sql/pg_dummy
    )

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()
