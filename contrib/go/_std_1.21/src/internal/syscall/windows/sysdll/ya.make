GO_LIBRARY()

BUILD_ONLY_IF(
    WARNING
    OS_WINDOWS
)

IF (OS_WINDOWS)
    SRCS(
        sysdll.go
    )
ENDIF()

END()
