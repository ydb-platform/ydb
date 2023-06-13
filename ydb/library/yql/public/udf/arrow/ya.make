LIBRARY()

SRCS(
    args_dechunker.cpp
    udf_arrow_helpers.cpp
    bit_util.cpp
    util.cpp
    block_reader.cpp
    block_item.cpp
    block_item_comparator.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
    contrib/libs/apache/arrow
)

YQL_LAST_ABI_VERSION()

END()
