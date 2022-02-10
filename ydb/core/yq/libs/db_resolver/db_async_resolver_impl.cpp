#include "db_async_resolver_impl.h"

namespace NYq {

TDatabaseAsyncResolver::TDatabaseAsyncResolver(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& recipient,
    const TString& ydbMvpEndpoint,
    const TString& mdbGateway,
    const bool mdbTransformHost)
    : ActorSystem(actorSystem)
    , Recipient(recipient)
    , YdbMvpEndpoint(ydbMvpEndpoint)
    , MdbGateway(mdbGateway)
    , MdbTransformHost(mdbTransformHost)
{}

NThreading::TFuture<TEvents::TDbResolverResponse> TDatabaseAsyncResolver::ResolveIds(const TResolveParams& params) const {
    auto promise = NThreading::NewPromise<TEvents::TDbResolverResponse>();
    auto callback = MakeHolder<NYql::TRichActorFutureCallback<TEvents::TEvEndpointResponse>>(
        [promise] (TAutoPtr<NActors::TEventHandle<TEvents::TEvEndpointResponse>>& event) mutable {
            promise.SetValue(std::move(event->Get()->DbResolverResponse));
        },
        [promise] () mutable {
            //TODO add logs
            promise.SetException("Error occurred on resolving ids. Message was undelivered.");
        },
        TDuration::Seconds(10)
    );

    NActors::TActorId callbackId = ActorSystem->Register(callback.Release());

    ActorSystem->Send(new NActors::IEventHandle(Recipient, callbackId,
        new TEvents::TEvEndpointRequest(params.Ids, YdbMvpEndpoint, MdbGateway,
            params.TraceId, MdbTransformHost)));
    return promise.GetFuture();
}

} // NYq
