YQL_UDF_YDB(uuid_udf)

YQL_ABI_VERSION(
    2
    46
    0
)

SRCS(
    uuid.cpp
)

PEERDIR(
    yql/essentials/types/uuid
)

END()

RECURSE_FOR_TESTS(
    test
    ut
)
