#include "query.h"

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NKqp::NPrivateEvents {

void TEvQueryRequest::SetClientLostAction(TActorId actorId, NActors::TActorSystem* as) {
    if (RequestCtx) {
        RequestCtx->SetFinishAction([actorId, as]() {
            as->Send(actorId, new NGRpcService::TEvClientLost());
            });
    } else if (Record.HasCancelationActor()) {
        auto cancelationActor = ActorIdFromProto(Record.GetCancelationActor());
        NGRpcService::SubscribeRemoteCancel(cancelationActor, actorId, as);
    }
}

TEvQueryResponse::TEvQueryResponse() = default;

TEvQueryResponse::TEvQueryResponse(TIntrusivePtr<NActors::TProtoArenaHolder> arena)
    : TEventPBBase(arena ? std::move(arena) : MakeIntrusive<NActors::TProtoArenaHolder>())
{}

TEvQueryResponse::~TEvQueryResponse() = default;

} // namespace NKikimr::NKqp::NPrivateEvents
