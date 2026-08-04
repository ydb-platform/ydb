LIBRARY()

LICENSE(Service-Sourceless-Library)

VERSION(Service-proxy-version)

NO_UTIL()

NO_COMPILER_WARNINGS()

CFLAGS(
    -fsanitize-memory-track-origins=2
)

SRCS(
    msan.cpp
)

END()
