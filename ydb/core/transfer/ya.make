LIBRARY()

PEERDIR(
    ydb/core/tx/replication/service
    ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper
)

SRCS(
    column_table.cpp
    purecalc.cpp
    purecalc_input.cpp
    purecalc_output.cpp
    row_table.cpp
    scheme.cpp
    transfer_writer.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    ut/common
)

RECURSE_FOR_TESTS(
    ut/column_table
    ut/functional
    ut/large
    ut/row_table
)

