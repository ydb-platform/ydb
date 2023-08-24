GO_LIBRARY()

SRCS(
    deploy.go
    qloud.go
    zap.go
    zapify.go
)

GO_TEST_SRCS(
    benchmark_test.go
    zap_test.go
    zapify_test.go
)

END()

RECURSE(
    asynczap
    encoders
    gotest
    logrotate
)
