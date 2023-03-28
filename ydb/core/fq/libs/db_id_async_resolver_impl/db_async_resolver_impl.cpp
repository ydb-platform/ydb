#include "db_async_resolver_impl.h"

#include <library/cpp/actors/core/actorsystem.h>

namespace NFq {
using namespace NThreading;

TDatabaseAsyncResolverImpl::TDatabaseAsyncResolverImpl(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& recipient,
    const TString& ydbMvpEndpoint,
    const TString& mdbGateway,
    bool mdbTransformHost,
    const TString& traceId)
    : ActorSystem(actorSystem)
    , Recipient(recipient)
    , YdbMvpEndpoint(ydbMvpEndpoint)
    , MdbGateway(mdbGateway)
    , MdbTransformHost(mdbTransformHost)
    , TraceId(traceId)
{}

TFuture<NYql::TDbResolverResponse> TDatabaseAsyncResolverImpl::ResolveIds(
    const THashMap<std::pair<TString, NYql::DatabaseType>, NYql::TDatabaseAuth>& ids) const
{
    auto promise = NewPromise<NYql::TDbResolverResponse>();
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
            TraceId, MdbTransformHost)));
    return promise.GetFuture();
}

} // NFq
