GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    backoff.go
    balancer_conn_wrappers.go
    call.go
    clientconn.go
    codec.go
    dialoptions.go
    doc.go
    idle.go
    interceptor.go
    picker_wrapper.go
    pickfirst.go
    preloader.go
    resolver_conn_wrapper.go
    rpc_util.go
    server.go
    service_config.go
    stream.go
    trace.go
    version.go
)

GO_TEST_SRCS(
    clientconn_authority_test.go
    clientconn_parsed_target_test.go
    clientconn_test.go
    codec_test.go
    default_dial_option_server_option_test.go
    grpc_test.go
    idle_test.go
    picker_wrapper_test.go
    resolver_test.go
    rpc_util_test.go
    server_test.go
    service_config_test.go
    trace_test.go
)

END()

RECURSE(
    admin
    attributes
    authz
    backoff
    balancer
    benchmark
    binarylog
    channelz
    codes
    connectivity
    credentials
    encoding
    # gotest
    grpclog
    health
    internal
    interop
    keepalive
    metadata
    orca
    peer
    profiling
    reflection
    resolver
    serviceconfig
    stats
    status
    stress
    tap
    # test
    testdata
    xds
)
