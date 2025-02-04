IF (NOT WINDOWS)
    UNITTEST_FOR(yql/essentials/minikql/dom)

    SRCS(
        yson_ut.cpp
        json_ut.cpp
    )

    SIZE(MEDIUM)

    PEERDIR(
        yql/essentials/minikql/computation/llvm16
        yql/essentials/public/udf/service/exception_policy
        yql/essentials/sql/pg_dummy
    )

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()
