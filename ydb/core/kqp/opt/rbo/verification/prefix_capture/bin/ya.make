PROGRAM(kqp_rbo_prefix_capture)

# Match the embedded-server test targets: reuse compiled objects while avoiding
# resident debug sections in the large host link on constrained machines.
IF (OS_LINUX AND RBO_LOW_MEMORY_LINK)
    LDFLAGS(-Wl,--no-gdb-index -Wl,--strip-debug)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    library/cpp/resource
    ydb/core/kqp
    ydb/core/kqp/opt/rbo/verification/prefix_capture
    ydb/core/kqp/ut/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
