#include "row_dispatcher.h"
#include "coordinator.h"
#include "leader_detector.h"

#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/strip.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRowDispatcher : public TActorBootstrapped<TRowDispatcher> {
    NConfig::TRowDispatcherConfig Config;
    NConfig::TCommonConfig CommonConfig;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TActorId CoordinatorActorId;
    TMaybe<TActorId> LeaderActorId;

public:
    explicit TRowDispatcher(
        const NConfig::TRowDispatcherConfig& config,
        const NConfig::TCommonConfig& commonConfig,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER";

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);

    void Handle(TEvents::TEvUndelivered::TPtr &ev) ;
    void Handle(NActors::TEvents::TEvWakeup::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorInfo::TPtr &ev) ;

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle)
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorInfo, Handle);
    
    })

private:
    // STRICT_STFUNC(StateFunc,
    //     hFunc(NYql::NDq::TEvDqCompute::TEvGetTaskState, Handle);
    // )

    // void Handle(NYql::NDq::TEvDqCompute::TEvGetTaskState::TPtr& ev);
};

TRowDispatcher::TRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : Config(config)
    , CommonConfig(commonConfig)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources) {
    //LOG_YQ_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id  111 1 " << SelfId());
}

void TRowDispatcher::Bootstrap() {
    Become(&TRowDispatcher::StateFunc);
    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: Successfully bootstrapped row dispatcher, id " << SelfId());

    if (Config.GetCoordinator().GetEnabled()) {
        const auto& config = Config.GetCoordinator();
        Register(NewLeaderDetector(SelfId(), config, CredentialsProviderFactory, YqSharedResources).release());
        Register(NewRDCoordinator(SelfId(), config, CredentialsProviderFactory, YqSharedResources).release());
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: Coordinator changed, new leader " << ev->Get()->LeaderActorId);

    LeaderActorId = ev->Get()->LeaderActorId;
    Send(*LeaderActorId, new NFq::TEvRowDispatcher::TEvStartSession(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TRowDispatcher::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
        LOG_YQ_ROW_DISPATCHER_DEBUG("RD: EvNodeConnected " << ev->Get()->NodeId);

}


void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: TEvNodeDisconnected " << ev->Get()->NodeId);

}

void TRowDispatcher::Handle(TEvents::TEvUndelivered::TPtr &ev) {

    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: TEvUndelivered, ev: " << ev->Get()->ToString());
    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: TEvUndelivered, Reason: " << ev->Get()->Reason);
    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: TEvUndelivered, Data: " << ev->Get()->Data);
    Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());

}

void TRowDispatcher::Handle(NActors::TEvents::TEvWakeup::TPtr&) {

    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: TEvWakeup, send start session to " << *LeaderActorId);

    Send(*LeaderActorId, new NFq::TEvRowDispatcher::TEvStartSession(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorInfo::TPtr &) {
    LOG_YQ_ROW_DISPATCHER_DEBUG("RD: TEvCoordinatorInfo ");

}


} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(config, commonConfig, credentialsProviderFactory, yqSharedResources));
}

} // namespace NFq
