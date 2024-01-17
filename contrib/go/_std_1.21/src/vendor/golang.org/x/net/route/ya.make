GO_LIBRARY()

BUILD_ONLY_IF(
    WARNING
    OS_DARWIN
)

IF (OS_DARWIN)
    SRCS(
        address.go
        binary.go
        empty.s
        interface.go
        interface_classic.go
        interface_multicast.go
        message.go
        route.go
        route_classic.go
        sys.go
        sys_darwin.go
        syscall.go
        zsys_darwin.go
    )
ENDIF()

END()
