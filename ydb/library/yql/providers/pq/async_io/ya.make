LIBRARY()

OWNER(
    d-mokhnatkin
    g:yq
    g:yql
)

SRCS(
    dq_pq_read_actor.cpp
    dq_pq_write_actor.cpp
    probes.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation 
    ydb/library/yql/providers/common/token_accessor/client 
    ydb/library/yql/public/types 
    ydb/library/yql/utils/log 
    ydb/public/sdk/cpp/client/ydb_driver 
    ydb/public/sdk/cpp/client/ydb_persqueue_core 
    ydb/public/sdk/cpp/client/ydb_types/credentials 
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/pq/proto
)

YQL_LAST_ABI_VERSION()

END()

IF (NOT OPENSOURCE) 
    IF (OS_LINUX) 
        # Logbroker recipe is supported only for linux. 
        RECURSE_FOR_TESTS( 
            ut 
        ) 
    ENDIF() 
ENDIF () 
