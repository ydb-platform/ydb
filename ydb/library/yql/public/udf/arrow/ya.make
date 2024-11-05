LIBRARY()

SRCS(
    args_dechunker.cpp
    udf_arrow_helpers.cpp
    bit_util.cpp
    util.cpp
    block_reader.cpp
    block_item.cpp
    block_item_hasher.cpp
    block_item_comparator.cpp
    block_type_helper.cpp
    memory_pool.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
    contrib/libs/apache/arrow
)

PROVIDES(YqlUdfSdkArrow)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
