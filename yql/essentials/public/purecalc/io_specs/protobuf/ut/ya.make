IF (NOT SANITIZER_TYPE)

UNITTEST()

PEERDIR(
    library/cpp/protobuf/util
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/public/purecalc
    yql/essentials/public/purecalc/io_specs/protobuf
    yql/essentials/public/purecalc/ut/protos
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    test_spec.cpp
)

END()

ENDIF()
