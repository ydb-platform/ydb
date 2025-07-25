LIBRARY()

PEERDIR(
    ydb/core/tx/replication/service
    ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper
)

SRCS(
    purecalc.cpp
    scheme.cpp
    transfer_writer.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    ut/common
)

RECURSE_FOR_TESTS(
    ut/functional
    ut/large
)

