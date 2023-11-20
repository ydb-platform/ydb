GO_LIBRARY()

SRCS(
    cert_pool.go
    notboring.go
    parser.go
    pem_decrypt.go
    pkcs1.go
    pkcs8.go
    root.go
    sec1.go
    verify.go
    x509.go
)

GO_TEST_SRCS(
    cert_pool_test.go
    name_constraints_test.go
    parser_test.go
    pem_decrypt_test.go
    pkcs8_test.go
    platform_test.go
    root_test.go
    sec1_test.go
    verify_test.go
    x509_test.go
)

GO_XTEST_SRCS(
    example_test.go
    hybrid_pool_test.go
)

IF (OS_LINUX)
    SRCS(
        root_linux.go
        root_unix.go
    )

    GO_TEST_SRCS(root_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        root_darwin.go
    )

    GO_XTEST_SRCS(root_darwin_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        root_windows.go
    )

    GO_XTEST_SRCS(root_windows_test.go)
ENDIF()

END()

RECURSE(
    internal
    pkix
)
