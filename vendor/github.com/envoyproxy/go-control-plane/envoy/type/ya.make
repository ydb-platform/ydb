GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    hash_policy.pb.go
    hash_policy.pb.validate.go
    http.pb.go
    http.pb.validate.go
    http_status.pb.go
    http_status.pb.validate.go
    percent.pb.go
    percent.pb.validate.go
    range.pb.go
    range.pb.validate.go
    semantic_version.pb.go
    semantic_version.pb.validate.go
    token_bucket.pb.go
    token_bucket.pb.validate.go
)

END()

RECURSE(
    http
    matcher
    metadata
    tracing
    v3
)
