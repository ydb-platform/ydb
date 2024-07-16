#include "row_dispatcher.h"
#include "coordinator.h"
#include "leader_detector.h"

#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>

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
    //TActorId CoordinatorActorId;
    TMaybe<TActorId> CoordinatorActorId;
    TSet<TActorId> CoordinatorChangedSubscribers;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const TString LogPrefix;

    using SessionKey = std::pair<TString, ui32>; // TopicPath / PartitionId

    struct TopicSessionInfo {
        TSet<TActorId> Consumers;
    };


    struct PartitionInfo {
        TMap<TActorId, TopicSessionInfo> TopicSessions;
    };

    TMap<SessionKey, PartitionInfo> Sessions;

public:
    explicit TRowDispatcher(
        const NConfig::TRowDispatcherConfig& config,
        const NConfig::TCommonConfig& commonConfig,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER";

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);

    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) ;
    void Handle(NActors::TEvents::TEvWakeup::TPtr &ev);
    void Handle(NActors::TEvents::TEvPong::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvRowDispatcherRequest::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle)
        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvRowDispatcherRequest, Handle);

        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);
    })

private:

};

TRowDispatcher::TRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
    : Config(config)
    , CommonConfig(commonConfig)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , CredentialsFactory(credentialsFactory)
    , LogPrefix("RowDispatcher: ")  {
}

void TRowDispatcher::Bootstrap() {
    Become(&TRowDispatcher::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id " << SelfId());

    if (Config.GetCoordinator().GetEnabled()) {
        const auto& config = Config.GetCoordinator();
        Register(NewLeaderDetector(SelfId(), config, CredentialsProviderFactory, YqSharedResources->UserSpaceYdbDriver).release());
        Register(NewCoordinator(SelfId(), config, CredentialsProviderFactory, YqSharedResources).release());
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("Coordinator changed, new leader " << ev->Get()->CoordinatorActorId);

    CoordinatorActorId = ev->Get()->CoordinatorActorId;
    if (!CoordinatorActorId) {

    }
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    for (auto actorId : CoordinatorChangedSubscribers) {
        Send(
            actorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorChanged(ev->Get()->CoordinatorActorId),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    }
}

void TRowDispatcher::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected " << ev->Get()->NodeId);
}

void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected " << ev->Get()->NodeId);
}

void TRowDispatcher::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString());
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, Reason: " << ev->Get()->Reason);
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, Data: " << ev->Get()->Data);
    Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
}

void TRowDispatcher::Handle(NActors::TEvents::TEvWakeup::TPtr&) {
    if (!CoordinatorActorId) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("TEvWakeup, send start session to " << *CoordinatorActorId);
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TRowDispatcher::Handle(NActors::TEvents::TEvPong::TPtr &) {
    LOG_ROW_DISPATCHER_DEBUG("NActors::TEvents::TEvPong ");
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvRowDispatcherRequest::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvRowDispatcherRequest ");
    if (!CoordinatorActorId) {
        return;
    }
    Send(ev->Sender, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(*CoordinatorActorId), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    CoordinatorChangedSubscribers.insert(ev->Sender);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStartSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    TMaybe<ui64> readOffset;
    if (ev->Get()->Record.HasOffset()) {
        readOffset = ev->Get()->Record.GetOffset();
    }
    Send(ev->Sender, new NFq::TEvRowDispatcher::TEvAck(ev->Get()->Record.GetTransportMeta()));

    SessionKey key{ev->Get()->Record.GetSource().GetTopicPath(), ev->Get()->Record.GetPartitionId()};
    LOG_ROW_DISPATCHER_DEBUG("Sessions count " << Sessions.size());
    auto& sessions = Sessions[key].TopicSessions;

    TActorId sessionActorId;

    if (sessions.empty() || readOffset) {
        LOG_ROW_DISPATCHER_DEBUG("Create new session " << readOffset);
        auto actorId = Register(NewTopicSession(
            ev->Get()->Record.GetSource(),
            ev->Get()->Record.GetPartitionId(),
            YqSharedResources->UserSpaceYdbDriver,
            CreateCredentialsProviderFactoryForStructuredToken(
                CredentialsFactory,
                ev->Get()->Record.GetToken(),
                ev->Get()->Record.GetAddBearerToToken())).release());
        sessions[actorId];
        sessionActorId = actorId;
    } else {
        sessionActorId = sessions.begin()->first;
    }

    //auto sessionActorId = Sessions[key];

    sessions[sessionActorId].Consumers.insert(ev->Sender);
    auto event = std::make_unique<TEvRowDispatcher::TEvSessionAddConsumer>();
    event->ConsumerActorId = ev->Sender;
    event->Offset = readOffset;
    event->StartingMessageTimestampMs = ev->Get()->Record.GetStartingMessageTimestampMs(); 
    event->SourceParams = ev->Get()->Record.GetSource();
    Send(sessionActorId, event.release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());
    SessionKey key{ev->Get()->Record.GetSource().GetTopicPath(), ev->Get()->Record.GetPartitionId()};
    LOG_ROW_DISPATCHER_DEBUG("Sessions count " << Sessions.size());
    auto& sessionsMap = Sessions[key].TopicSessions;
    LOG_ROW_DISPATCHER_DEBUG("Sessions count by key " << Sessions.size());
    if (sessionsMap.empty()) {
        return;
    }

    for (auto& [topicSessionActorId, info]: sessionsMap) {
        for (auto consumerActorId: info.Consumers) {
            if (consumerActorId != ev->Sender) {
                continue;
            }

            auto event = std::make_unique<TEvRowDispatcher::TEvSessionDeleteConsumer>();
            event->ConsumerActorId = ev->Sender;
            Send(topicSessionActorId, event.release());

            info.Consumers.erase(ev->Sender);
            if (info.Consumers.empty()) {
                Send(topicSessionActorId, new NActors::TEvents::TEvPoisonPill());
                sessionsMap.erase(topicSessionActorId);
                if (sessionsMap.empty()) {
                    Sessions.erase(key);
                }
            }
            return;
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(config, commonConfig, credentialsProviderFactory, yqSharedResources, credentialsFactory));
}

} // namespace NFq
