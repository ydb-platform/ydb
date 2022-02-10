LIBRARY()
 
YQL_ABI_VERSION( 
    2 
    9
    0 
) 
 
OWNER(
    xenoxeno
    g:yql
    g:yql_ydb_core
)

SRCS(
    tdigest.proto
    static_udf.cpp
    stat_udf.h
    tdigest.h
    tdigest.cpp
)

PEERDIR(
    contrib/libs/protobuf
    util/draft
    ydb/library/yql/public/udf
)

END()
