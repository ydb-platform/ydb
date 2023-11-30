#include "db_async_resolver_impl.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/fq/libs/events/events.h>

namespace NFq {
using namespace NThreading;

TDatabaseAsyncResolverImpl::TDatabaseAsyncResolverImpl(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& recipient,
    const TString& ydbMvpEndpoint,
    const TString& mdbGateway,
    const NYql::IMdbEndpointGenerator::TPtr& mdbEndpointGenerator, 
    const TString& traceId)
    : ActorSystem(actorSystem)
    , Recipient(recipient)
    , YdbMvpEndpoint(ydbMvpEndpoint)
    , MdbGateway(mdbGateway)
    , MdbEndpointGenerator(mdbEndpointGenerator)
    , TraceId(traceId)
{
}

TFuture<NYql::TDatabaseResolverResponse> TDatabaseAsyncResolverImpl::ResolveIds(const TDatabaseAuthMap& ids) const
{
    // Cloud database ids validataion
    for (const auto& kv: ids) {
        // empty cluster name is not good
        YQL_ENSURE(kv.first.first, "empty cluster name");
    }

    auto promise = NewPromise<NYql::TDatabaseResolverResponse>();
    TDuration timeout = TDuration::Seconds(40);
    auto callback = MakeHolder<NYql::TRichActorFutureCallback<TEvents::TEvEndpointResponse>>(
        [promise] (TAutoPtr<NActors::TEventHandle<TEvents::TEvEndpointResponse>>& event) mutable {
            promise.SetValue(std::move(event->Get()->DbResolverResponse));
        },
        [promise, timeout] () mutable {
            promise.SetException("Couldn't resolve database ids for " + timeout.ToString());
        },
        timeout
    );

    NActors::TActorId callbackId = ActorSystem->Register(callback.Release());

    ActorSystem->Send(new NActors::IEventHandle(Recipient, callbackId,
        new TEvents::TEvEndpointRequest(ids, YdbMvpEndpoint, MdbGateway,
            TraceId, MdbEndpointGenerator)));

    return promise.GetFuture();
}

} // NFq
