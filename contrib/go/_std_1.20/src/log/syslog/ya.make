GO_LIBRARY()

SRCS(
    doc.go
)

IF (OS_DARWIN)
    SRCS(
        syslog.go
        syslog_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        syslog.go
        syslog_unix.go
    )
ENDIF()

END()
