GO_LIBRARY()

SRCS(
    rand.go
    util.go
)

IF (OS_DARWIN)
    SRCS(
        rand_getentropy.go
        rand_unix.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        rand_getrandom.go
        rand_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        rand_windows.go
    )
ENDIF()

END()
