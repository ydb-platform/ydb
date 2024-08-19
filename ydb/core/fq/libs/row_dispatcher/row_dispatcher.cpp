#include "row_dispatcher.h"
#include "coordinator.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/leader_election.h>
#include <ydb/core/fq/libs/row_dispatcher/protos/events.pb.h>

#include <util/generic/queue.h>


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
    TString Tenant;
    NFq::NRowDispatcher::IActorFactory::TPtr ActorFactory;

    using ConsumerSessionKey = std::pair<TActorId, ui32>;   // ReadActorId / PartitionId
    using TopicSessionKey = std::tuple<TString, TString, TString, ui32>;       // Endpoint / Database / TopicPath / PartitionId

    struct ConsumerInfo {
        ConsumerInfo(
            NActors::TActorId readActorId,
            NActors::TActorId selfId,
            ui64 eventQueueId,
            NFq::NRowDispatcherProto::TEvStartSession& proto,
            NYql::NDq::TRetryEventsQueue::ICallbacks* cbs,
            TActorId topicSessionId)
            : ReadActorId(readActorId)
            , SourceParams(proto.GetSource())
            , PartitionId(proto.GetPartitionId())
            , Offset(proto.HasOffset() ? TMaybe<ui64>(proto.GetOffset()) : TMaybe<ui64>())
            , StartingMessageTimestampMs(proto.GetStartingMessageTimestampMs())
            , EventQueueId(eventQueueId)
            , Proto(proto)
            , TopicSessionId(topicSessionId) {
                EventsQueue.Init("txId", selfId, selfId, eventQueueId, /* KeepAlive */ true, cbs);
                EventsQueue.OnNewRecipientId(readActorId);
            }

        NActors::TActorId ReadActorId;
        NYql::NPq::NProto::TDqPqTopicSource SourceParams;
        ui64 PartitionId;
        TString Token;
        bool AddBearerToToken;
        TMaybe<ui64> Offset;
        ui64 StartingMessageTimestampMs;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        TQueue<std::pair<ui64, TString>> Buffer;
        ui64 EventQueueId;
        NFq::NRowDispatcherProto::TEvStartSession Proto;
        TActorId TopicSessionId;
    };

    struct SessionInfo {
        TMap<TActorId, TAtomicSharedPtr<ConsumerInfo>> Consumers;     // key - ReadActorId
    };

    struct TopicSessionInfo {
        TMap<TActorId, SessionInfo> Sessions;                   // key - TopicSessionId
    };

    TMap<ConsumerSessionKey, TAtomicSharedPtr<ConsumerInfo>> Consumers;
    TMap<ui64, TAtomicSharedPtr<ConsumerInfo>> ConsumersByEventQueueId;
    TMap<TopicSessionKey, TopicSessionInfo> TopicSessions;

public:
    explicit TRowDispatcher(
        const NConfig::TRowDispatcherConfig& config,
        const NConfig::TCommonConfig& commonConfig,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const TString& tenant,
        const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER";

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);

    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) ;
    void Handle(NActors::TEvents::TEvWakeup::TPtr &ev);
    void Handle(NActors::TEvents::TEvPong::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr &ev);

    void Handle(NActors::TEvents::TEvPing::TPtr &ev);
    void SessionClosed(ui64 eventQueueId) override;
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr&);

    void DeleteConsumer(const ConsumerSessionKey& key);
    void PrintInternalState(const TString&);

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle)
        hFunc(NActors::TEvents::TEvPong, Handle);

        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvSessionError, Handle);

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
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory)
    : Config(config)
    , CommonConfig(commonConfig)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , CredentialsFactory(credentialsFactory)
    , LogPrefix("RowDispatcher: ")
    , Tenant(tenant)
    , ActorFactory(actorFactory) {
}

void TRowDispatcher::Bootstrap() {
    Become(&TRowDispatcher::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id " << SelfId() << ", tenant " << Tenant);

    if (Config.GetCoordinator().GetEnabled()) {
        const auto& config = Config.GetCoordinator();
        auto coordinatorId = Register(NewCoordinator(SelfId(), config, CredentialsProviderFactory, YqSharedResources, Tenant).release());
        Register(NewLeaderElection(SelfId(), coordinatorId, config, CredentialsProviderFactory, YqSharedResources, Tenant).release());
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
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected " << ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString());
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, Reason: " << ev->Get()->Reason);
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, Data: " << ev->Get()->Data);
    Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());

    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleUndelivered(ev);
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

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorChangesSubscribe from " << ev->Sender);
    CoordinatorChangedSubscribers.insert(ev->Sender);
    if (!CoordinatorActorId) {
        return;
    }
    Send(ev->Sender, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(*CoordinatorActorId), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TRowDispatcher::PrintInternalState(const TString& prefix) {
    TStringStream str;
    str << "Consumers:\n";

    for (auto& [key, consumerInfo] : Consumers) {
        str << "    read actor id: " << key.first << ", partId: " << key.second << "\n";
    }

    str << "\nSessions:\n";
    for (auto& [key, sessionInfo1] : TopicSessions) {
        str << "  endpoint: " << std::get<0>(key) << ", database: " << std::get<1>(key) << ", topic: " << std::get<2>(key) << ",  partId: " << std::get<3>(key) << "\n";
        for (auto& [actorId, sessionInfo2] : sessionInfo1.Sessions) {
            str << "    session actor id: " << actorId << "\n";
            for (auto& [actorId2, consumer] : sessionInfo2.Consumers) {
                str << "      read actor id: " << actorId2  << "\n";
            }
        }
    }
    LOG_ROW_DISPATCHER_DEBUG(prefix << ":\n" << str.Str());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStartSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    PrintInternalState("Before AddConsumer:");
    TMaybe<ui64> readOffset;
    if (ev->Get()->Record.HasOffset()) {
        readOffset = ev->Get()->Record.GetOffset();
    }

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it != Consumers.end()) {
        LOG_ROW_DISPATCHER_ERROR("Сonsumer already exist, ignore StartSession");
        return;
    }

    TActorId sessionActorId;
    TopicSessionKey topicKey{
        ev->Get()->Record.GetSource().GetEndpoint(),
        ev->Get()->Record.GetSource().GetDatabase(),
        ev->Get()->Record.GetSource().GetTopicPath(),
        ev->Get()->Record.GetPartitionId()};
    TopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
    LOG_ROW_DISPATCHER_DEBUG("Topic session count " << topicSessionInfo.Sessions.size());
    Y_ENSURE(topicSessionInfo.Sessions.size() <= 1);

    auto consumerInfo = MakeAtomicShared<ConsumerInfo>(ev->Sender, SelfId(), NextEventQueueId++, ev->Get()->Record, this, TActorId());
    Consumers[key] = consumerInfo;
    ConsumersByEventQueueId[consumerInfo->EventQueueId] = consumerInfo;
    consumerInfo->EventsQueue.OnEventReceived(ev);

    if (topicSessionInfo.Sessions.empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Create new session " << readOffset);
        sessionActorId = ActorFactory->RegisterTopicSession(
            Config,
            SelfId(),
            ev->Get()->Record.GetPartitionId(),
            YqSharedResources->UserSpaceYdbDriver,
            CreateCredentialsProviderFactoryForStructuredToken(
                CredentialsFactory,
                ev->Get()->Record.GetToken(),
                ev->Get()->Record.GetAddBearerToToken()));
        SessionInfo& sessionInfo = topicSessionInfo.Sessions[sessionActorId];
        sessionInfo.Consumers[ev->Sender] = consumerInfo;
    } else {
        auto sessionIt = topicSessionInfo.Sessions.begin();
        SessionInfo& sessionInfo = sessionIt->second;
        sessionInfo.Consumers[ev->Sender] = consumerInfo;
        sessionActorId = sessionIt->first;
    }
    consumerInfo->TopicSessionId = sessionActorId;
    consumerInfo->EventsQueue.Send(new NFq::TEvRowDispatcher::TEvStartSessionAck(consumerInfo->Proto));

    Forward(ev, sessionActorId);
    PrintInternalState("After AddConsumer:");
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr &ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvGetNextBatch from " << ev->Sender);

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvGetNextBatch, no such session");
        return;
    }
    if (!it->second->EventsQueue.OnEventReceived(ev)) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong seq num, ignore message");
        return;
    }
    Forward(ev, it->second->TopicSessionId);
}

void TRowDispatcher::Handle(NActors::TEvents::TEvPing::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvPing from " << ev->Sender);
    Send(ev->Sender, new NActors::TEvents::TEvPong());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong consumer");
        return;
    }
    if (!it->second->EventsQueue.OnEventReceived(ev)) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong seq num, ignore message");
        return;
    }
    DeleteConsumer(key);
}

void TRowDispatcher::DeleteConsumer(const ConsumerSessionKey& key) {
    PrintInternalState("Before DeleteConsumer:");
    LOG_ROW_DISPATCHER_DEBUG("DeleteConsumer, readActorId " << key.first <<
        " partitionId " << key.second);

    auto consumerIt = Consumers.find(key);
    if (consumerIt == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore DeleteConsumer, no such session");
        return;
    }
    const auto& consumer = consumerIt->second;
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
    event->Record.MutableSource()->CopyFrom(consumer->SourceParams);
    event->Record.SetPartitionId(consumer->PartitionId);
    Send(new IEventHandle(consumerIt->second->TopicSessionId, consumer->ReadActorId, event.release(), 0));

    TopicSessionKey topicKey{
        consumer->SourceParams.GetEndpoint(),
        consumer->SourceParams.GetDatabase(),
        consumer->SourceParams.GetTopicPath(),
        consumer->PartitionId};
    TopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
    SessionInfo& sessionInfo = topicSessionInfo.Sessions[consumerIt->second->TopicSessionId];
    Y_ENSURE(sessionInfo.Consumers.count(consumer->ReadActorId));
    sessionInfo.Consumers.erase(consumer->ReadActorId);
    if (sessionInfo.Consumers.empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Session is not used, sent TEvPoisonPill");
        topicSessionInfo.Sessions.erase(consumerIt->second->TopicSessionId);
        Send(consumerIt->second->TopicSessionId, new NActors::TEvents::TEvPoisonPill());
        if (topicSessionInfo.Sessions.empty()) {
            TopicSessions.erase(topicKey);
        }
    }
    ConsumersByEventQueueId.erase(consumerIt->second->EventQueueId);
    Consumers.erase(consumerIt);
    PrintInternalState("After DeleteConsumer:");
}

void TRowDispatcher::SessionClosed(ui64 eventQueueId) {
    LOG_ROW_DISPATCHER_WARN("SessionClosed " << eventQueueId);
    for (auto& [consumerKey, consumer] : Consumers) {
        if (consumer->EventQueueId != eventQueueId) {
            continue;
        }
        DeleteConsumer(consumerKey);
        break;
    }
}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvRetry " << ev->Get()->EventQueueId);
    auto it = ConsumersByEventQueueId.find(ev->Get()->EventQueueId);
    if (it == ConsumersByEventQueueId.end()) {
        LOG_ROW_DISPATCHER_WARN("No consumer with EventQueueId = " << ev->Get()->EventQueueId);
        return;
    }
    it->second->EventsQueue.Retry();
}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvRetryQueuePrivate::TEvPing " << ev->Get()->EventQueueId);
    auto it = ConsumersByEventQueueId.find(ev->Get()->EventQueueId);
    if (it == ConsumersByEventQueueId.end()) {
        LOG_ROW_DISPATCHER_WARN("No consumer with EventQueueId = " << ev->Get()->EventQueueId);
        return;
    }
    it->second->EventsQueue.Ping();
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr &ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvNewDataArrived from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore MessageBatch, no such session");
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Resend TEvNewDataArrived to " << ev->Get()->ReadActorId);
    it->second->EventsQueue.Send(ev.Release()->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr &ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvMessageBatch from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore MessageBatch, no such session");
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Resend TEvMessageBatch to " << ev->Get()->ReadActorId);
    it->second->EventsQueue.Send(ev.Release()->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr &ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvSessionError from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore MessageBatch, no such session");
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Resend TEvSessionError to " << ev->Get()->ReadActorId);
    it->second->EventsQueue.Send(ev.Release()->Release().Release());
    DeleteConsumer(key);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(config, commonConfig, credentialsProviderFactory, yqSharedResources, credentialsFactory, tenant, actorFactory));
}

} // namespace NFq
