LIBRARY()

OWNER(
    vvvv
    g:yql_ydb_core
)

SRCS(
    udf_support.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
)

YQL_LAST_ABI_VERSION()

END()
