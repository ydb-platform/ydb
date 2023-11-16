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

IF (OS_DARWIN)
    SRCS(
        root_darwin.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        root_linux.go
        root_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        root_windows.go
    )
ENDIF()

END()

RECURSE(
    internal
    pkix
)
