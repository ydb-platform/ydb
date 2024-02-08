GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    glog.go
    glog_file.go
    glog_flags.go
)

GO_TEST_SRCS(
    glog_bench_test.go
    glog_test.go
    glog_vmodule_test.go
)

END()

RECURSE(
    gotest
    internal
)
