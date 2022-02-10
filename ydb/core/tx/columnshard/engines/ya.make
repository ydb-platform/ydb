RECURSE_FOR_TESTS(
    ut
)
 
LIBRARY() 
 
OWNER(
    chertus
    g:kikimr
)
 
SRCS( 
    column_engine_logs.cpp 
    db_wrapper.cpp 
    insert_table.cpp 
    index_info.cpp 
    indexed_read_data.cpp 
    filter.cpp 
    portion_info.cpp 
) 
 
PEERDIR( 
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/formats
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
) 
 
YQL_LAST_ABI_VERSION()
 
END() 
