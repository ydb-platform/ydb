GO_LIBRARY()

SRCS(
    common.go
    format.go
    reader.go
    strconv.go
    writer.go
)

IF (OS_DARWIN)
    SRCS(
        stat_actime2.go
        stat_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        stat_actime1.go
        stat_unix.go
    )
ENDIF()

END()
