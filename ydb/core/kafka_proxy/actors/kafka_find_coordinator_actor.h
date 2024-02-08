#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/client/server/ic_nodes_cache_service.h>

namespace NKafka {

class TKafkaFindCoordinatorActor: public NActors::TActorBootstrapped<TKafkaFindCoordinatorActor> {
public:
    TKafkaFindCoordinatorActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFindCoordinatorRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);
    
private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse, Handle);
        }
    }

    void Handle(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void SendResponseOkAndDie(const TString& host, i32 port, ui64 nodeId, const NActors::TActorContext& ctx);
    void SendResponseFailAndDie(EKafkaErrors error, const TString& message, const NActors::TActorContext& ctx);

    TString LogPrefix();

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TFindCoordinatorRequestData> Message;
};

} // NKafka
