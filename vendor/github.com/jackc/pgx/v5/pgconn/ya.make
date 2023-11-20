GO_LIBRARY()

LICENSE(MIT)

SRCS(
    auth_scram.go
    config.go
    doc.go
    errors.go
    krb5.go
    pgconn.go
)

GO_TEST_SRCS(
    benchmark_private_test.go
    export_test.go
    pgconn_private_test.go
)

GO_XTEST_SRCS(
    # benchmark_test.go # st/YMAKE-102
    # config_test.go
    # errors_test.go # st/YMAKE-102
    # helper_test.go # st/YMAKE-102
    # pgconn_stress_test.go # st/YMAKE-102
    # pgconn_test.go
)

IF (OS_LINUX)
    SRCS(defaults.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(defaults.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(defaults_windows.go)
ENDIF()

END()

RECURSE(
    gotest
    internal
)
