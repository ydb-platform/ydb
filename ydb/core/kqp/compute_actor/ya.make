LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    kqp_compute_actor.cpp
    kqp_pure_compute_actor.cpp
    kqp_scan_compute_actor.cpp
)

PEERDIR(
    ydb/core/actorlib_impl 
    ydb/core/base 
    ydb/core/kqp/runtime 
    ydb/core/tx/datashard 
    ydb/core/tx/scheme_cache 
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
