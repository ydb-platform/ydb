GO_LIBRARY()

SRCS(
    elf.go
    label.go
    map.go
    pe.go
    pprof.go
    proto.go
    protobuf.go
    protomem.go
    runtime.go
)

GO_TEST_SRCS(
    label_test.go
    mprof_test.go
    pprof_test.go
    proto_test.go
    protomem_test.go
    runtime_test.go
)

IF (OS_LINUX)
    SRCS(
        pprof_rusage.go
        proto_other.go
    )

    GO_TEST_SRCS(rusage_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        pprof_rusage.go
        proto_other.go
    )

    GO_TEST_SRCS(rusage_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        pprof_windows.go
        proto_windows.go
    )
ENDIF()

END()

RECURSE(
)
