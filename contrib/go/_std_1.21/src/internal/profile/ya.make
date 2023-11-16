GO_LIBRARY()

SRCS(
    encode.go
    filter.go
    legacy_profile.go
    merge.go
    profile.go
    proto.go
    prune.go
)

GO_TEST_SRCS(
    profile_test.go
    proto_test.go
)

END()

RECURSE(
)
