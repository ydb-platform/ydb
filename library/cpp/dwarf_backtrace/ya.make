LIBRARY()

NO_WSHADOW()

SRCS(
    backtrace.cpp
)

PEERDIR(
    contrib/libs/backtrace
)

END()

IF (NOT OS_WINDOWS)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
