PROGRAM(kqp_rbo_prefix_capture)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/resource
    ydb/core/kqp
    ydb/core/kqp/opt/rbo/verification/prefix_capture
    ydb/core/kqp/ut/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
