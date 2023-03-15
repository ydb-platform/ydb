LIBRARY()

SRCS(
    kqp_opt_peephole_wide_read.cpp
    kqp_opt_peephole.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/library/naming_conventions
    ydb/library/yql/dq/opt
)

YQL_LAST_ABI_VERSION()

END()
