GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cds.pb.go
    cds.pb.validate.go
    cluster.pb.go
    cluster.pb.validate.go
    discovery.pb.go
    discovery.pb.validate.go
    eds.pb.go
    eds.pb.validate.go
    endpoint.pb.go
    endpoint.pb.validate.go
    lds.pb.go
    lds.pb.validate.go
    listener.pb.go
    listener.pb.validate.go
    rds.pb.go
    rds.pb.validate.go
    route.pb.go
    route.pb.validate.go
    scoped_route.pb.go
    scoped_route.pb.validate.go
    srds.pb.go
    srds.pb.validate.go
)

END()

RECURSE(
    auth
    cluster
    core
    endpoint
    listener
    ratelimit
    route
)
