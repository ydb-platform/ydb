LIBRARY()

SRCS(
    kikimr_lookup_actor.cpp
    kikimr_lookup_factories.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/base
    ydb/core/control/lib
    ydb/library/aclib/protos
    ydb/library/yql/dq/actors/compute
    ydb/public/api/protos
    yql/essentials/minikql/computation
    yql/essentials/public/types
)

YQL_LAST_ABI_VERSION()

END()
