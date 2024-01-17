GO_LIBRARY()

BUILD_ONLY_IF(
    WARNING
    OS_DARWIN
)

IF (OS_DARWIN)
    SRCS(
        corefoundation.go
        corefoundation.s
        security.go
        security.s
    )
ENDIF()

END()
