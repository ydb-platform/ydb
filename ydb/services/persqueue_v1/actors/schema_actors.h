#pragma once

#include "events.h"
#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

class TPQDescribeTopicActor : public TPQGrpcSchemaBase<TPQDescribeTopicActor, NKikimr::NGRpcService::TEvPQDescribeTopicRequest>
                            , public TCdcStreamCompatible
{
using TBase = TPQGrpcSchemaBase<TPQDescribeTopicActor, TEvPQDescribeTopicRequest>;

public:
     TPQDescribeTopicActor(NKikimr::NGRpcService::TEvPQDescribeTopicRequest* request);
    ~TPQDescribeTopicActor() = default;

    void StateWork(TAutoPtr<IEventHandle>& ev);

    void Bootstrap(const NActors::TActorContext& ctx);

    void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
};

} // namespace NKikimr::NGRpcProxy::V1
