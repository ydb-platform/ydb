#include "row_dispatcher.h"
#include "coordinator.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>

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

const ui64 CoordinatorPingPeriodSec = 2;

////////////////////////////////////////////////////////////////////////////////

struct TRowDispatcherMetrics {
    explicit TRowDispatcherMetrics(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters) {
        ErrorsCount = Counters->GetCounter("ErrorsCount");
        ClientsCount = Counters->GetCounter("ClientsCount");
        RowsSent = Counters->GetCounter("RowsSent", true);
    }

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr ErrorsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ClientsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr RowsSent;
};


struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCoordinatorPing = EvBegin + 20,
        EvPrintState,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvCoordinatorPing : NActors::TEventLocal<TEvCoordinatorPing, EvCoordinatorPing> {};
    struct TEvPrintState : public NActors::TEventLocal<TEvPrintState, EvPrintState> {};
};

ui64 PrintStatePeriodSec = 60;

class TRowDispatcher : public TActorBootstrapped<TRowDispatcher> {

    struct ConsumerSessionKey {
        TActorId ReadActorId;
        ui32 PartitionId;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TActorId>()(ReadActorId);
            hash = CombineHashes<ui64>(hash, std::hash<ui32>()(PartitionId));
            return hash;
        }
        bool operator==(const ConsumerSessionKey& other) const {
            return ReadActorId == other.ReadActorId && PartitionId == other.PartitionId;
        }
    };

    struct ConsumerSessionKeyHash {
        int operator()(const ConsumerSessionKey& k) const {
            return k.Hash();
        }
    };

    struct TopicSessionKey {
        TString Endpoint;
        TString Database;
        TString TopicName;
        ui64 PartitionId;

        size_t Hash() const noexcept {
            ui64 hash = std::hash<TString>()(Endpoint);
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(Database));
            hash = CombineHashes<ui64>(hash, std::hash<TString>()(TopicName));
            hash = CombineHashes<ui64>(hash, std::hash<ui64>()(PartitionId));
            return hash;
        }
        bool operator==(const TopicSessionKey& other) const {
            return Endpoint == other.Endpoint && Database == other.Database
                && TopicName == other.TopicName && PartitionId == other.PartitionId;
        }
    };

    struct TopicSessionKeyHash {
        int operator()(const TopicSessionKey& k) const {
            return k.Hash();
        }
    };


    NConfig::TRowDispatcherConfig Config;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;
    TMaybe<TActorId> CoordinatorActorId;
    TSet<TActorId> CoordinatorChangedSubscribers;
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const TString LogPrefix;
    ui64 NextEventQueueId = 0;
    TString Tenant;
    NFq::NRowDispatcher::IActorFactory::TPtr ActorFactory;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TRowDispatcherMetrics Metrics;
    NYql::IPqGateway::TPtr PqGateway;

    struct ConsumerCounters {
        ui64 NewDataArrived = 0;
        ui64 GetNextBatch = 0;
        ui64 MessageBatch = 0;
    };

    struct ConsumerInfo {
        ConsumerInfo(
            NActors::TActorId readActorId,
            NActors::TActorId selfId,
            ui64 eventQueueId,
            NFq::NRowDispatcherProto::TEvStartSession& proto,
            TActorId topicSessionId)
            : ReadActorId(readActorId)
            , SourceParams(proto.GetSource())
            , PartitionId(proto.GetPartitionId())
            , EventQueueId(eventQueueId)
            , Proto(proto)
            , TopicSessionId(topicSessionId)
            , QueryId(proto.GetQueryId()) {
                EventsQueue.Init("txId", selfId, selfId, eventQueueId, /* KeepAlive */ true);
                EventsQueue.OnNewRecipientId(readActorId);
            }

        NActors::TActorId ReadActorId;
        NYql::NPq::NProto::TDqPqTopicSource SourceParams;
        ui64 PartitionId;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        ui64 EventQueueId;
        NFq::NRowDispatcherProto::TEvStartSession Proto;
        TActorId TopicSessionId;
        const TString QueryId;
        ConsumerCounters Counters;
    };

    struct SessionInfo {
        TMap<TActorId, TAtomicSharedPtr<ConsumerInfo>> Consumers;     // key - ReadActor actor id
    };

    struct TopicSessionInfo {
        TMap<TActorId, SessionInfo> Sessions;                         // key - TopicSession actor id
    };

    THashMap<ConsumerSessionKey, TAtomicSharedPtr<ConsumerInfo>, ConsumerSessionKeyHash> Consumers;
    TMap<ui64, TAtomicSharedPtr<ConsumerInfo>> ConsumersByEventQueueId;
    THashMap<TopicSessionKey, TopicSessionInfo, TopicSessionKeyHash> TopicSessions;

public:
    explicit TRowDispatcher(
        const NConfig::TRowDispatcherConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        const TString& tenant,
        const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const NYql::IPqGateway::TPtr& pqGateway);

    void Bootstrap();

    static constexpr char ActorName[] = "FQ_ROW_DISPATCHER";

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev);

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) ;
    void Handle(TEvPrivate::TEvCoordinatorPing::TPtr& ev);
    void Handle(NActors::TEvents::TEvPong::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStatus::TPtr& ev);

    void Handle(NActors::TEvents::TEvPing::TPtr& ev);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed::TPtr&);
    void Handle(NFq::TEvPrivate::TEvPrintState::TPtr&);

    void DeleteConsumer(const ConsumerSessionKey& key);
    void PrintInternalState();

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(TEvPrivate::TEvCoordinatorPing, Handle)
        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvSessionError, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStatus, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvPing, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed, Handle);
        hFunc(NActors::TEvents::TEvPing, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, Handle);
        hFunc(NFq::TEvPrivate::TEvPrintState, Handle);
    })
};

TRowDispatcher::TRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway)
    : Config(config)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources)
    , CredentialsFactory(credentialsFactory)
    , LogPrefix("RowDispatcher: ")
    , Tenant(tenant)
    , ActorFactory(actorFactory)
    , Counters(counters)
    , Metrics(counters)
    , PqGateway(pqGateway) {
}

void TRowDispatcher::Bootstrap() {
    Become(&TRowDispatcher::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("Successfully bootstrapped row dispatcher, id " << SelfId() << ", tenant " << Tenant);

    const auto& config = Config.GetCoordinator();
    auto coordinatorId = Register(NewCoordinator(SelfId(), config, YqSharedResources, Tenant, Counters).release());
    Register(NewLeaderElection(SelfId(), coordinatorId, config, CredentialsProviderFactory, YqSharedResources, Tenant, Counters).release());
    Schedule(TDuration::Seconds(CoordinatorPingPeriodSec), new TEvPrivate::TEvCoordinatorPing());
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new NFq::TEvPrivate::TEvPrintState());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("Coordinator changed, old leader " << CoordinatorActorId << ", new " << ev->Get()->CoordinatorActorId);

    CoordinatorActorId = ev->Get()->CoordinatorActorId;
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    for (auto actorId : CoordinatorChangedSubscribers) {
        Send(
            actorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorChanged(ev->Get()->CoordinatorActorId),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
    }
}

void TRowDispatcher::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("EvNodeConnected, node id " << ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
}

void TRowDispatcher::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvUndelivered, ev: " << ev->Get()->ToString() << ", reason " << ev->Get()->Reason);
    for (auto& [actorId, consumer] : Consumers) {
        consumer->EventsQueue.HandleUndelivered(ev);
    }
}

void TRowDispatcher::Handle(TEvPrivate::TEvCoordinatorPing::TPtr&) {
    Schedule(TDuration::Seconds(CoordinatorPingPeriodSec), new TEvPrivate::TEvCoordinatorPing());
    if (!CoordinatorActorId) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Send ping to " << *CoordinatorActorId);
    Send(*CoordinatorActorId, new NActors::TEvents::TEvPing(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TRowDispatcher::Handle(NActors::TEvents::TEvPong::TPtr&) {
    LOG_ROW_DISPATCHER_TRACE("NActors::TEvents::TEvPong ");
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCoordinatorChangesSubscribe from " << ev->Sender);
    CoordinatorChangedSubscribers.insert(ev->Sender);
    if (!CoordinatorActorId) {
        return;
    }
    Send(ev->Sender, new NFq::TEvRowDispatcher::TEvCoordinatorChanged(*CoordinatorActorId), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void TRowDispatcher::PrintInternalState() {
    if (Consumers.empty()) {
        return;
    }
    TStringStream str;
    str << "Consumers:\n";
    for (auto& [key, consumerInfo] : Consumers) {
        str << "    query id " << consumerInfo->QueryId << ", partId: " << key.PartitionId << ", read actor id: " << key.ReadActorId
            << ", queueId " << consumerInfo->EventQueueId << ", get next " << consumerInfo->Counters.GetNextBatch
            << ", data arrived " << consumerInfo->Counters.NewDataArrived << ", message batch " << consumerInfo->Counters.MessageBatch << "\n";
        str << "        ";
        consumerInfo->EventsQueue.PrintInternalState(str);
    }

    str << "\nSessions:\n";
    for (auto& [key, sessionInfo1] : TopicSessions) {
        str << "  " << key.Endpoint << " / " << key.Database << " / " << key.TopicName << ", id: " << key.PartitionId << "\n";
        for (auto& [actorId, sessionInfo2] : sessionInfo1.Sessions) {
            str << "    session id: " << actorId << "\n";
            for (auto& [actorId2, consumer] : sessionInfo2.Consumers) {
                str << "      read actor id: " << actorId2  << "\n";
            }
        }
    }
    LOG_ROW_DISPATCHER_DEBUG(str.Str());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStartSession from " << ev->Sender << ", topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    TMaybe<ui64> readOffset;
    if (ev->Get()->Record.HasOffset()) {
        readOffset = ev->Get()->Record.GetOffset();
    }

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it != Consumers.end()) {
        LOG_ROW_DISPATCHER_ERROR("Сonsumer already exists, ignore StartSession");
        return;
    }
    const auto& source = ev->Get()->Record.GetSource();

    TActorId sessionActorId;
    TopicSessionKey topicKey{source.GetEndpoint(), source.GetDatabase(), source.GetTopicPath(), ev->Get()->Record.GetPartitionId()};
    TopicSessionInfo& topicSessionInfo = TopicSessions[topicKey];
    LOG_ROW_DISPATCHER_DEBUG("Topic session count " << topicSessionInfo.Sessions.size());
    Y_ENSURE(topicSessionInfo.Sessions.size() <= 1);

    auto consumerInfo = MakeAtomicShared<ConsumerInfo>(ev->Sender, SelfId(), NextEventQueueId++, ev->Get()->Record, TActorId());
    Consumers[key] = consumerInfo;
    ConsumersByEventQueueId[consumerInfo->EventQueueId] = consumerInfo;
    if (!consumerInfo->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        const ui64 seqNo = meta.GetSeqNo();
        LOG_ROW_DISPATCHER_ERROR("TEvStartSession: wrong seq num from " << ev->Sender.ToString() << ", seqNo " << seqNo << ", ignore message");
    }

    if (topicSessionInfo.Sessions.empty()) {
        LOG_ROW_DISPATCHER_DEBUG("Create new session " << readOffset);
        sessionActorId = ActorFactory->RegisterTopicSession(
            source.GetTopicPath(),
            Config,
            SelfId(),
            ev->Get()->Record.GetPartitionId(),
            YqSharedResources->UserSpaceYdbDriver,
            CreateCredentialsProviderFactoryForStructuredToken(
                CredentialsFactory,
                ev->Get()->Record.GetToken(),
                source.GetAddBearerToToken()),
            Counters,
            PqGateway
            );
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
    Metrics.ClientsCount->Set(Consumers.size());
    PrintInternalState();
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    LOG_ROW_DISPATCHER_TRACE("TEvGetNextBatch from " << ev->Sender << ", partId " << ev->Get()->Record.GetPartitionId() << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvGetNextBatch, no such session");
        return;
    }
    if (!it->second->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        const ui64 seqNo = meta.GetSeqNo();
        LOG_ROW_DISPATCHER_ERROR("TEvGetNextBatch: wrong seq num from " << ev->Sender.ToString() << ", seqNo " << seqNo << ", ignore message");
        return;
    }
    it->second->Counters.GetNextBatch++;
    Forward(ev, it->second->TopicSessionId);
}

void TRowDispatcher::Handle(NActors::TEvents::TEvPing::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvPing from " << ev->Sender);
    Send(ev->Sender, new NActors::TEvents::TEvPong());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    ConsumerSessionKey key{ev->Sender, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Wrong consumer, sender " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId());
        return;
    }
    if (!it->second->EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        const ui64 seqNo = meta.GetSeqNo();

        LOG_ROW_DISPATCHER_ERROR("TEvStopSession: wrong seq num from " << ev->Sender.ToString() << ", seqNo " << seqNo << ", ignore message");
        return;
    }
    DeleteConsumer(key);
}

void TRowDispatcher::DeleteConsumer(const ConsumerSessionKey& key) {
    LOG_ROW_DISPATCHER_DEBUG("DeleteConsumer, readActorId " << key.ReadActorId <<
        " partitionId " << key.PartitionId);

    auto consumerIt = Consumers.find(key);
    if (consumerIt == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore DeleteConsumer, no such session");
        return;
    }
    const auto& consumer = consumerIt->second;
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
    *event->Record.MutableSource() = consumer->SourceParams;
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
    Metrics.ClientsCount->Set(Consumers.size());
    PrintInternalState();
}

void TRowDispatcher::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed::TPtr& ev) {
    LOG_ROW_DISPATCHER_WARN("Session closed, event queue id " << ev->Get()->EventQueueId);
    for (auto& [consumerKey, consumer] : Consumers) {
        if (consumer->EventQueueId != ev->Get()->EventQueueId) {
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

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {    
    LOG_ROW_DISPATCHER_TRACE("TEvNewDataArrived from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvNewDataArrived, no such session");
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Forward TEvNewDataArrived to " << ev->Get()->ReadActorId);
    it->second->Counters.NewDataArrived++;
    it->second->EventsQueue.Send(ev.Release()->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvMessageBatch from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore MessageBatch, no such session");
        return;
    }
    Metrics.RowsSent->Add(ev->Get()->Record.MessagesSize());
    LOG_ROW_DISPATCHER_TRACE("Forward TEvMessageBatch to " << ev->Get()->ReadActorId);
    it->second->Counters.MessageBatch++;
    it->second->EventsQueue.Send(ev.Release()->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvSessionError from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore MessageBatch, no such session");
        return;
    }
    Metrics.ErrorsCount->Inc();
    LOG_ROW_DISPATCHER_TRACE("Forward TEvSessionError to " << ev->Get()->ReadActorId);
    it->second->EventsQueue.Send(ev.Release()->Release().Release());
    DeleteConsumer(key);
}

void TRowDispatcher::Handle(NFq::TEvRowDispatcher::TEvStatus::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvStatus from " << ev->Sender);
    ConsumerSessionKey key{ev->Get()->ReadActorId, ev->Get()->Record.GetPartitionId()};
    auto it = Consumers.find(key);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvStatus, no such session");
        return;
    }
    LOG_ROW_DISPATCHER_TRACE("Forward TEvStatus to " << ev->Get()->ReadActorId);
    it->second->EventsQueue.Send(ev.Release()->Release().Release());
}

void TRowDispatcher::Handle(NFq::TEvPrivate::TEvPrintState::TPtr&) {
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new NFq::TEvPrivate::TEvPrintState());
    PrintInternalState();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const NConfig::TRowDispatcherConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway)
{
    return std::unique_ptr<NActors::IActor>(new TRowDispatcher(
        config,
        credentialsProviderFactory,
        yqSharedResources,
        credentialsFactory,
        tenant,
        actorFactory,
        counters,
        pqGateway));
}

} // namespace NFq
