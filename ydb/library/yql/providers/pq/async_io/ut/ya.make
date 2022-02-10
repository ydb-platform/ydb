UNITTEST_FOR(ydb/library/yql/providers/pq/async_io)

OWNER(
    d-mokhnatkin
    g:yq
    g:yql
)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
 
ENV(LOGBROKER_CREATE_TOPICS=ReadFromTopic,ReadWithFreeSpace,SaveLoadPqRead,WriteToTopic,WriteToTopicMultiBatch,DeferredWriteToTopic,SaveLoadPqWrite,Checkpoints,LoadFromSeveralStates)
 
ENV(LOGBROKER_TOPICS_PARTITIONS=1)
 
INCLUDE(${ARCADIA_ROOT}/kikimr/yq/tools/lbk_recipe_with_dummy_cm/recipe.inc)

SRCS(
    dq_pq_read_actor_ut.cpp
    dq_pq_write_actor_ut.cpp
    ut_helpers.cpp
)

PEERDIR(
    ydb/core/testlib/basics 
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf/service/exception_policy 
    ydb/library/yql/sql 
    ydb/public/sdk/cpp/client/ydb_persqueue_public 
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/ut_helpers
)

YQL_LAST_ABI_VERSION()

END()
