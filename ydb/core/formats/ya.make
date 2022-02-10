RECURSE_FOR_TESTS(
    ut
)

LIBRARY() 
 
OWNER(
    davenger
    g:kikimr
)
 
PEERDIR( 
    contrib/libs/apache/arrow
    ydb/core/scheme
) 
 
SRCS( 
    arrow_batch_builder.cpp
    arrow_batch_builder.h
    arrow_helpers.cpp
    arrow_helpers.h
    clickhouse_block.h 
    clickhouse_block.cpp 
    input_stream.h
    merging_sorted_input_stream.cpp
    merging_sorted_input_stream.h
    one_batch_input_stream.h
    sharding.h
    sort_cursor.h
    factory.h 
    program.cpp
    program.h
) 
 
END() 
