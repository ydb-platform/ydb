#include "kafka_find_coordinator_actor.h"

#include <ydb/core/kafka_proxy/kafka_events.h>


namespace NKafka {

static constexpr ui8 SUPPORTED_KEY_TYPE = 0; // consumer

NActors::IActor* CreateKafkaFindCoordinatorActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFindCoordinatorRequestData>& message) {
    return new TKafkaFindCoordinatorActor(context, correlationId, message);
}

TString TKafkaFindCoordinatorActor::LogPrefix() {
    TStringBuilder sb;
    sb << "TKafkaFindCoordinatorActor " << SelfId().ToString() << ": ";
    return sb;
}

void TKafkaFindCoordinatorActor::Bootstrap(const NActors::TActorContext& ctx) {
    if (Message->KeyType != SUPPORTED_KEY_TYPE) {
        SendResponseFailAndDie(EKafkaErrors::INVALID_REQUEST, TStringBuilder() << "Unsupported coordinator KeyType: " << Message->KeyType, ctx);
        return;
    }

    bool withProxy = Context->Config.HasProxy() && !Context->Config.GetProxy().GetHostname().Empty();
    if (withProxy) {
        SendResponseOkAndDie(Context->Config.GetProxy().GetHostname(), Context->Config.GetProxy().GetPort(), NKafka::ProxyNodeId, ctx);
        return;
    }

    Send(NKikimr::NIcNodeCache::CreateICNodesInfoCacheServiceId(), new NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoRequest());
    Become(&TKafkaFindCoordinatorActor::StateWork); 
}

void TKafkaFindCoordinatorActor::SendResponseOkAndDie(const TString& host, i32 port, ui64 nodeId, const NActors::TActorContext& ctx) {
    TFindCoordinatorResponseData::TPtr response = std::make_shared<TFindCoordinatorResponseData>();

    for (auto coordinatorKey: Message->CoordinatorKeys) {
        KAFKA_LOG_I("FIND_COORDINATOR incoming request for group# " << coordinatorKey);

        TFindCoordinatorResponseData::TCoordinator coordinator;
        coordinator.ErrorCode = NONE_ERROR;
        coordinator.Host = host;
        coordinator.Port = port;
        coordinator.NodeId = nodeId;
        coordinator.Key = coordinatorKey;

        response->Coordinators.push_back(coordinator);
    }

    response->ErrorCode = NONE_ERROR;
    response->Host = host;
    response->Port = port;
    response->NodeId = nodeId;

    KAFKA_LOG_D("FIND_COORDINATOR response. Host#: " << host << ", Port#: " << port << ", NodeId# " << nodeId);

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
    Die(ctx);
}

void TKafkaFindCoordinatorActor::SendResponseFailAndDie(EKafkaErrors error, const TString& message, const NActors::TActorContext& ctx) {
    TFindCoordinatorResponseData::TPtr response = std::make_shared<TFindCoordinatorResponseData>();

    for (auto coordinatorKey: Message->CoordinatorKeys) {
        KAFKA_LOG_CRIT("FIND_COORDINATOR request failed. Reason# " << message);

        TFindCoordinatorResponseData::TCoordinator coordinator;
        coordinator.ErrorCode = error;
        coordinator.Key = coordinatorKey;
        coordinator.ErrorMessage = message;

        response->Coordinators.push_back(coordinator);
    }

    response->ErrorCode = error;
 
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(CorrelationId, response, static_cast<EKafkaErrors>(response->ErrorCode)));
    Die(ctx);
}

void TKafkaFindCoordinatorActor::Handle(NKikimr::NIcNodeCache::TEvICNodesInfoCache::TEvGetAllNodesInfoResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto iter = ev->Get()->NodeIdsMapping->find(ctx.SelfID.NodeId());
    Y_ABORT_UNLESS(!iter.IsEnd());
    
	auto host = (*ev->Get()->Nodes)[iter->second].Host;
    if (host.StartsWith(UnderlayPrefix)) {
        host = host.substr(sizeof(UnderlayPrefix) - 1);
    }
    KAFKA_LOG_D("FIND_COORDINATOR incoming TEvGetAllNodesInfoResponse. Host#: " << host);
    SendResponseOkAndDie(host, Context->Config.GetListeningPort(), ctx.SelfID.NodeId(), ctx);
}

} // NKafka
