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
    ratelimit_strategy.pb.go
    ratelimit_strategy.pb.validate.go
    ratelimit_unit.pb.go
    ratelimit_unit.pb.validate.go
    semantic_version.pb.go
    semantic_version.pb.validate.go
    token_bucket.pb.go
    token_bucket.pb.validate.go
)

END()
