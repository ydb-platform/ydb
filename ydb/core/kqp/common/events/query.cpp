#include "query.h"

#include <ydb/core/kqp/common/simple/mvcc_version.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NKqp::NPrivateEvents {

TMaybe<TString> TEvQueryRequest::GetMvccVersion() const {
    if (RequestCtx) {
        return RequestCtx->GetPeerMetaValues(YDB_MVCC_VERSION_HEADER);
    }
    const auto& request = Record.GetRequest();
    return request.HasMvccVersion() ? TMaybe<TString>(request.GetMvccVersion()) : Nothing();
}

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
