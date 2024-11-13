IF (NOT SANITIZER_TYPE)

UNITTEST()

PEERDIR(
    library/cpp/protobuf/util
    yql/essentials/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc
    ydb/library/yql/public/purecalc/io_specs/protobuf
    ydb/library/yql/public/purecalc/ut/protos
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    test_spec.cpp
)

END()

ENDIF()
