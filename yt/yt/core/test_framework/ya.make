LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    fixed_growth_string_output.cpp
    test_memory_tracker.cpp
    test_proxy_service.cpp
    test_server_host.cpp
    test_key.cpp
    GLOBAL framework.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/testing/hook
    yt/yt/build
    yt/yt/core
    yt/yt/core/http
    yt/yt/library/profiling/solomon
)

EXPLICIT_DATA()

SET(TEST_DATA_DIR ${ARCADIA_ROOT}/yt/yt/core/test_framework/testdata)

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

END()
