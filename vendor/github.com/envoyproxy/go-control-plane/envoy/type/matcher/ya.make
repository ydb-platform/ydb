GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    metadata.pb.go
    metadata.pb.validate.go
    node.pb.go
    node.pb.validate.go
    number.pb.go
    number.pb.validate.go
    path.pb.go
    path.pb.validate.go
    regex.pb.go
    regex.pb.validate.go
    string.pb.go
    string.pb.validate.go
    struct.pb.go
    struct.pb.validate.go
    value.pb.go
    value.pb.validate.go
)

END()

RECURSE(v3)
