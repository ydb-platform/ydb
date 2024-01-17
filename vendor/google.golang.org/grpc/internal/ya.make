GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    internal.go
    xds_handshake_cluster.go
)

END()

RECURSE(
    admin
    backoff
    balancer
    balancergroup
    balancerload
    binarylog
    buffer
    cache
    channelz
    credentials
    envconfig
    googlecloud
    grpclog
    grpcrand
    grpcsync
    grpctest
    grpcutil
    hierarchy
    leakcheck
    metadata
    pretty
    profiling
    proto
    resolver
    serviceconfig
    status
    stubserver
    testutils
    transport
    wrr
    xds
    # yo
)

IF (OS_LINUX)
    RECURSE(syscall)
ENDIF()

IF (OS_DARWIN)
    RECURSE(syscall)
ENDIF()

IF (OS_WINDOWS)
    RECURSE(syscall)
ENDIF()
