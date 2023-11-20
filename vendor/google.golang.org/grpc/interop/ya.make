GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    orcalb.go
    test_utils.go
)

END()

RECURSE(
    alts
    client
    fake_grpclb
    grpc_testing
    http2
    server
    xds
    xds_federation
    # yo
)

IF (OS_LINUX)
    RECURSE(grpclb_fallback)
ENDIF()
