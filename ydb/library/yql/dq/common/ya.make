LIBRARY()

OWNER(
    g:yql 
    g:yql_ydb_core 
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/mkql_proto/protos 
)

SRCS(
    dq_common.cpp
    dq_resource_quoter.h
    dq_value.cpp
)

END()
