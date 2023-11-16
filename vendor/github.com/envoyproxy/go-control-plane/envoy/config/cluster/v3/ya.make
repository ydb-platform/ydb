GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    circuit_breaker.pb.go
    circuit_breaker.pb.validate.go
    cluster.pb.go
    cluster.pb.validate.go
    filter.pb.go
    filter.pb.validate.go
    outlier_detection.pb.go
    outlier_detection.pb.validate.go
)

END()
