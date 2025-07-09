LIBRARY()

SRCS(
    purecalc.cpp
)

PEERDIR(
    ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
