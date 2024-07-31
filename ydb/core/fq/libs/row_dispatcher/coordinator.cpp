#include "coordinator.h"

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_election.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NFq {

using namespace NActors;
using namespace NThreading;

using NYql::TIssues;

namespace {

struct TEvCreateSemaphoreResult : NActors::TEventLocal<TEvCreateSemaphoreResult, TEventIds::EvDeleteRateLimiterResourceResponse> {
    NYdb::NCoordination::TResult<void> Result;

    explicit TEvCreateSemaphoreResult(NYdb::NCoordination::TResult<void> result)
        : Result(std::move(result))
    {}
};

struct TEvCreateSessionResult : NActors::TEventLocal<TEvCreateSessionResult, TEvRowDispatcher::EvCreateSemaphoreResult> {
    NYdb::NCoordination::TSessionResult Result;

    explicit TEvCreateSessionResult(NYdb::NCoordination::TSessionResult result)
        : Result(std::move(result))
    {}
};

struct TEvAcquireSemaphoreResult : NActors::TEventLocal<TEvAcquireSemaphoreResult, TEventIds::EvSchemaUpdated> { // TODO
    NYdb::NCoordination::TResult<bool> Result;

    explicit TEvAcquireSemaphoreResult(NYdb::NCoordination::TResult<bool> result)
        : Result(std::move(result))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TActorCoordinator : public TActorBootstrapped<TActorCoordinator> {

    NConfig::TRowDispatcherCoordinatorConfig Config;
    const NKikimr::TYdbCredentialsProviderFactory& CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TMaybe<TActorId> LeaderActorId;
    const TString LogPrefix;
    const TString Tenant;

    struct NodeInfo {
        bool Connected = false;
        TActorId ActorId;
    };
    std::map<ui32, NodeInfo> RowDispatchersByNode; 
    bool IsLeader = false;

public:
    TActorCoordinator(
        NActors::TActorId rowDispatcherId,
        const NConfig::TRowDispatcherCoordinatorConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        const TString& tenant);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_RD_COORDINATOR";

    void Handle(NActors::TEvents::TEvPing::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NActors::TEvents::TEvPing, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorRequest, Handle);
    })

private:
    void DebugPrint();
};

TActorCoordinator::TActorCoordinator(
    NActors::TActorId /*rowDispatcherId*/,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant)
    : Config(config)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , LogPrefix("Coordinator: ")
    , Tenant(tenant) {
}

void TActorCoordinator::Bootstrap() {
    Become(&TActorCoordinator::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped coordinator, id " << SelfId());
    Register(NewLeaderElection(SelfId(), Config, CredentialsProviderFactory, YqSharedResources, Tenant).release());
}

void TActorCoordinator::Handle(NActors::TEvents::TEvPing::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("StartSession received, " << ev->Sender);

    ui32 nodeId = ev->Sender.NodeId();
    auto& nodeInfo = RowDispatchersByNode[nodeId];
    nodeInfo.Connected = true;
    nodeInfo.ActorId = ev->Sender;

    DebugPrint();
    Send(ev->Sender, new NActors::TEvents::TEvPong(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TActorCoordinator::DebugPrint() {
    LOG_ROW_DISPATCHER_DEBUG("RowDispatchers: ");

    for (const auto& [nodeId, nodeInfo] : RowDispatchersByNode) {
            LOG_ROW_DISPATCHER_DEBUG("   node " << nodeId << ", connected " << nodeInfo.Connected);
    }
}

void TActorCoordinator::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected " << ev->Get()->NodeId);
}

void TActorCoordinator::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected " << ev->Get()->NodeId);
    auto& nodeInfo = RowDispatchersByNode[ev->Get()->NodeId];
    nodeInfo.Connected = false;
}

void TActorCoordinator::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString());
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, Reason: " << ev->Get()->Reason);
    auto& nodeInfo = RowDispatchersByNode[ev->Sender.NodeId()];
    nodeInfo.Connected = false;
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("new leader " << ev->Get()->CoordinatorActorId);
    LOG_ROW_DISPATCHER_DEBUG("SelfId " << SelfId());

    IsLeader = SelfId() == ev->Get()->CoordinatorActorId;
    LOG_ROW_DISPATCHER_DEBUG("IsLeader " << IsLeader);
}

void TActorCoordinator::Handle(NFq::TEvRowDispatcher::TEvCoordinatorRequest::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorRequest: ");
    LOG_ROW_DISPATCHER_DEBUG("  TopicPath " << ev->Get()->Record.GetSource().GetTopicPath());
    
    for (auto& partitionId : ev->Get()->Record.GetPartitionId()) {
        LOG_ROW_DISPATCHER_DEBUG("  partitionId " << partitionId);
    }
    DebugPrint();

    if (RowDispatchersByNode.empty()) {
        LOG_ROW_DISPATCHER_DEBUG("empty  RowDispatchersByNode"); // TODO
        return;
    }

    const auto& nodeInfo = RowDispatchersByNode.begin()->second;
    LOG_ROW_DISPATCHER_DEBUG("Send  TEvCoordinatorResult " << nodeInfo.ActorId);
    auto response = std::make_unique<TEvRowDispatcher::TEvCoordinatorResult>();
    auto* partitions = response->Record.AddPartitions();
    for (auto& partitionId : ev->Get()->Record.GetPartitionId()) {
        partitions->AddPartitionId(partitionId);
    }
    
    ActorIdToProto(nodeInfo.ActorId, partitions->MutableActorId());

    Send(ev->Sender, response.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}


} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewCoordinator(
    NActors::TActorId rowDispatcherId,
    const NConfig::TRowDispatcherCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const TString& tenant)
{
    return std::unique_ptr<NActors::IActor>(new TActorCoordinator(rowDispatcherId, config, credentialsProviderFactory, yqSharedResources, tenant));
}

} // namespace NFq
