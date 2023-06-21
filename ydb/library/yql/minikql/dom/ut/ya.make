IF (NOT WINDOWS)
    UNITTEST_FOR(yql/library/dom)

    SRCS(
        yson_ut.cpp
        json_ut.cpp
    )

    IF (SANITIZER_TYPE)
        SIZE(MEDIUM)
        TIMEOUT(600)
    ENDIF()

    PEERDIR(
        ydb/library/yql/minikql/computation/llvm
        ydb/library/yql/public/udf/service/exception_policy
    )

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()
