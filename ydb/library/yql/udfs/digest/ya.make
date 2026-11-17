YQL_UDF_YDB(digest_aggregate_udf)

YQL_ABI_VERSION(
    2
    43
    0
)

SRCS(
    canonicalize.h
    udf.cpp
)

PEERDIR(
    ydb/library/table_digest
    yql/essentials/types/binary_json
)

END()

RECURSE_FOR_TESTS(
    test
)
