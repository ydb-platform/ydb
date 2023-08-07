IF (NOT SANITIZER_TYPE AND NOT OPENSOURCE)

UNITTEST()

PEERDIR(
    library/cpp/protobuf/util
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc/common
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
