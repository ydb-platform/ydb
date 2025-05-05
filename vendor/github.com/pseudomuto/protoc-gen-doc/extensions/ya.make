GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.5.1)

SRCS(
    extensions.go
)

END()

RECURSE(
    envoyproxy_validate
    google_api_http
    lyft_validate
    validator_field
)
