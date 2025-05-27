GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.0.2)

SRCS(
    location.go
    location_string.go
    reflectwalk.go
)

GO_TEST_SRCS(reflectwalk_test.go)

END()

RECURSE(
    gotest
)
