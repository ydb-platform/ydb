GTEST(unittester-core-bus)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bus_ut.cpp
    ssl_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
    library/cpp/testing/common
    library/cpp/resource
)

EXPLICIT_DATA()

SET(TEST_DATA_DIR ${ARCADIA_ROOT}/yt/yt/core/bus/unittests/testdata)

RESOURCE(
    ${TEST_DATA_DIR}/ca.pem /testdata/ca.pem
    ${TEST_DATA_DIR}/ca_ec.pem /testdata/ca_ec.pem
    ${TEST_DATA_DIR}/ca_with_ip_in_san.pem /testdata/ca_with_ip_in_san.pem
    ${TEST_DATA_DIR}/cert.pem  /testdata/cert.pem
    ${TEST_DATA_DIR}/cert_ec.pem /testdata/cert_ec.pem
    ${TEST_DATA_DIR}/cert_with_ip_in_san.pem /testdata/cert_with_ip_in_san.pem
    ${TEST_DATA_DIR}/key.pem /testdata/key.pem
    ${TEST_DATA_DIR}/key_ec.pem /testdata/key_ec.pem
    ${TEST_DATA_DIR}/key_with_ip_in_san.pem /testdata/key_with_ip_in_san.pem
)

REQUIREMENTS(
    cpu:4
    ram:4
    ram_disk:1
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(MEDIUM)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(
        ya:fat
        ya:force_sandbox
        ya:exotic_platform
        ya:large_tests_on_single_slots
    )
ENDIF()

END()
