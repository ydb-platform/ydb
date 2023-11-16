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

IF (OS_DARWIN)
    SRCS(
        pprof_rusage.go
        proto_other.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        pprof_rusage.go
        proto_other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        pprof_windows.go
        proto_windows.go
    )
ENDIF()

END()
