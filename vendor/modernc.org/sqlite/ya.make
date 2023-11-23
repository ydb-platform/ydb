GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    mutex.go
    sqlite.go
    sqlite_go18.go
)

GO_TEST_SRCS(
    # all_test.go
    # null_test.go
    # sqlite_go18_test.go
    # tcl_test.go
)

IF (OS_LINUX)
    SRCS(rulimit.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(rulimit.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(norlimit.go)
ENDIF()

GO_TEST_EMBED_PATTERN(embed.db)

GO_TEST_EMBED_PATTERN(embed2.db)

END()

RECURSE(
    gotest
    lib
    vfs
)
