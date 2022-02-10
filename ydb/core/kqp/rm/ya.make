LIBRARY()

OWNER(g:kikimr) 

SRCS(
    kqp_resource_estimation.cpp
    kqp_snapshot_manager.cpp
    kqp_rm.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/actorlib_impl 
    ydb/core/base 
    ydb/core/cms/console 
    ydb/core/kqp/common 
    ydb/core/kqp/counters 
    ydb/core/mind 
    ydb/core/mon 
    ydb/core/protos 
    ydb/core/tablet 
)

YQL_LAST_ABI_VERSION() 

END()

RECURSE_FOR_TESTS(
    ut
)
