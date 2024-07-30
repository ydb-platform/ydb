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
#include <ydb/core/fq/libs/row_dispatcher/consumer.h>

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

class TRowDispatcher : public TActorBootstrapped<TRowDispatcher> , public NYql::NDq::TRetryEventsQueue::ICallbacks {
    NConfig::TRowDispatcherConfig Config;
    NConfig::TCommonConfig CommonConfig;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    //TActorId CoordinatorActorId;
    TMaybe<TActorId> CoordinatorActorId;
    TSet<TActorId> CoordinatorChangedSubscribers;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const TString LogPrefix;
    ui64 NextEventQueueId = 0;

    using ConsumerSessionKey = std::pair<TActorId, ui32>;   // ReadActorId / PartitionId
    using TopicSessionKey = std::pair<TString, ui32>;       // TopicPath / PartitionId

    struct ConsumerInfo {
        ConsumerInfo(THolder<NFq::Consumer>&& consumer, TActorId topicSessionId)
            : Consumer(std::move(consumer))
            , TopicSessionId(topicSessionId) {}
        THolder<NFq::Consumer> Consumer;
        TActorId TopicSessionId;
    };

    struct TopicSessionInfo {
        TSet<TActorId> Sessions;
    };

    TMap<ConsumerSessionKey, ConsumerInfo> Consumers;
    TMap<TopicSessionKey, TopicSessionInfo> TopicSessions;
    //TMap<SessionKey, TSharedPtr<NFq::Consumer>> ;


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
    void Handle(NFq::TEvRowDispatcher::TEvAddConsumer::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvSessionConsumerDeleted::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr &ev);

    void Handle(NActors::TEvents::TEvPing::TPtr &ev);
    void SessionClosed(ui64 eventQueueId) override;
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr&);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle)
        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvRowDispatcherRequest, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, Handle);

        hFunc(NFq::TEvRowDispatcher::TEvAddConsumer, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvSessionConsumerDeleted, Handle);

        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvPing, Handle);

        hFunc(NActors::TEvents::TEvPing, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, Handle);
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
    for (auto& [actorId, info] : Consumers) {
        info.Consumer->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected " << ev->Get()->NodeId);
    for (auto& [actorId, info] : Consumers) {
        info.Consumer->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString());
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, Reason: " << ev->Get()->Reason);
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, Data: " << ev->Get()->Data);
    Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());

    for (auto& [actorId, info] : Consumers) {
        info.Consumer->EventsQueue.HandleUndelivered(ev);
    }
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

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvAddConsumer::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvAddConsumer, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    TMaybe<ui64> readOffset;
    if (ev->Get()->Record.HasOffset()) {
        readOffset = ev->Get()->Record.GetOffset();
    }
   // Send(ev->Sender, );

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    LOG_ROW_DISPATCHER_DEBUG("Sessions count " << Consumers.size());
    auto it = Consumers.find(key);
    if (it != Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong"); // TODO
        return;
    }

    TActorId sessionActorId;

    TopicSessionKey key2{ev->Get()->Record.GetSource().GetTopicPath(), ev->Get()->Record.GetPartitionId()};
    TopicSessionInfo& topicSessionInfo = TopicSessions[key2];

    auto consumer = MakeHolder<NFq::Consumer>(ev->Sender, SelfId(), NextEventQueueId++, ev->Get()->Record);
    if (topicSessionInfo.Sessions.empty() || readOffset) {
        LOG_ROW_DISPATCHER_DEBUG("Create new session " << readOffset);
        auto actorId = Register(NewTopicSession(
            SelfId(),
            ev->Get()->Record.GetSource(),
            ev->Get()->Record.GetPartitionId(),
            YqSharedResources->UserSpaceYdbDriver,
            CreateCredentialsProviderFactoryForStructuredToken(
                CredentialsFactory,
                ev->Get()->Record.GetToken(),
                ev->Get()->Record.GetAddBearerToToken())).release());
        topicSessionInfo.Sessions.insert(actorId);
        sessionActorId = actorId;
    } else {
        //TopicSessionInfo& info = *sessions.begin()
        sessionActorId = *topicSessionInfo.Sessions.begin();
    }

    consumer->EventsQueue.Send(new NFq::TEvRowDispatcher::TEvAck(consumer->Proto));
    Consumers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(key),
        std::forward_as_tuple(std::move(consumer), sessionActorId));

    Forward(ev, sessionActorId);
}


void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvGetNextBatch ");

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong"); // TODO
        return;
    }
    Forward(ev, it->second.TopicSessionId);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionConsumerDeleted::TPtr &/*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSessionConsumerDeleted");
    // const auto& consumer = ev->Get()->Consumer;
    // SessionKey key{consumer->SourceParams.GetTopicPath(), consumer->PartitionId};
    // auto sessionInfo = Sessions[key];
    // sessionInfo.TopicSessions.erase(consumer->ReadActorId);

    // LOG_ROW_DISPATCHER_DEBUG("TEvSessionConsumerDeleted TopicSessions size " << sessionInfo.TopicSessions.size());

    // if (sessionInfo.TopicSessions.empty()) {
    //     LOG_ROW_DISPATCHER_DEBUG("Delete session info");
    //     Sessions.erase(key);
    // }

    // LOG_ROW_DISPATCHER_DEBUG("TEvSessionConsumerDeleted Sessions size " << Sessions.size());
}

void TRowDispatcher::Handle(NActors::TEvents::TEvPing::TPtr &/*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("TEvPing");
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());
    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};

    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong consumer"); // TODO
        return;
    }

    Forward(ev, it->second.TopicSessionId);
    Consumers.erase(it);

    // todo update TopicSessionInfo
    

    // SessionKey key{ev->Get()->Record.GetSource().GetTopicPath(), ev->Get()->Record.GetPartitionId()};
    // LOG_ROW_DISPATCHER_DEBUG("Sessions count " << Sessions.size());
    // auto& sessionsMap = Sessions[key].TopicSessions;
    // LOG_ROW_DISPATCHER_DEBUG("Sessions count by key " << Sessions.size());
    // if (sessionsMap.empty()) {
    //     return;
    // }

    // for (auto& [topicSessionActorId, info]: sessionsMap) {
    //     for (auto consumerActorId: info.Consumers) {
    //         if (consumerActorId != ev->Sender) {
    //             continue;
    //         }

    //         // auto event = std::make_unique<TEvRowDispatcher::TEvSessionDeleteConsumer>();
    //         // event->ReadActorId = ev->Sender;
    //         // Send(topicSessionActorId, event.release());

    //         info.Consumers.erase(ev->Sender);
    //         if (info.Consumers.empty()) {
    //             /Send(topicSessionActorId, new NActors::TEvents::TEvPoisonPill());
    //             sessionsMap.erase(topicSessionActorId);
    //             if (sessionsMap.empty()) {
    //                 Sessions.erase(key);
    //             }
    //         }
    //         return;
    //     }
    // }
}

void TRowDispatcher::SessionClosed(ui64 eventQueueId) {
    LOG_ROW_DISPATCHER_DEBUG("SessionClosed " << eventQueueId);

}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& /*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("TEvRetry");
    for (auto& [actorId, info] : Consumers) {
        info.Consumer->EventsQueue.Retry(); // TODO: find EventsQueue
    }
}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr& /*ev*/) {
    LOG_ROW_DISPATCHER_DEBUG("TEvRetryQueuePrivate::TEvPing");
    
    for (auto& [actorId, info] : Consumers) {
        info.Consumer->EventsQueue.Ping(); // TODO: find EventsQueue
    }
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNewDataArrived");

    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong consumer"); // TODO
        return;
    }
    it->second.Consumer->EventsQueue.Send(ev.Release()->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNewDataArrived");

    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong consumer"); // TODO
        return;
    }
    it->second.Consumer->EventsQueue.Send(ev.Release()->Release().Release());
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
