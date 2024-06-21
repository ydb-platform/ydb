YQL_UDF_YDB(roaring)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    roaring.cpp
)

PEERDIR(
    contrib/libs/croaring
)


END()

RECURSE_FOR_TESTS(
    test
)