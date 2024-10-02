#include "topic_session.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/runtime/dq_async_stats.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <util/generic/queue.h>

#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>
#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>
#include <ydb/library/yql/public/purecalc/purecalc.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTopicSessionMetrics {
    void Init(const ::NMonitoring::TDynamicCounterPtr& counters, NActors::TActorId selfId) {
        SelfId = selfId;
        SubGroup = counters->GetSubgroup("actor_id", SelfId.ToString());
        InFlyAsyncInputData = SubGroup->GetCounter("InFlyAsyncInputData");
        RowsRead = SubGroup->GetCounter("RowsRead", true);
        InFlySubscribe = SubGroup->GetCounter("InFlySubscribe");
    }

    ~TTopicSessionMetrics() {
        SubGroup->RemoveSubgroup("actor_id", SelfId.ToString());
    }
    NActors::TActorId SelfId;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
    ::NMonitoring::TDynamicCounters::TCounterPtr RowsRead;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlySubscribe;
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPqEventsReady = EvBegin + 10,
        EvCreateSession,
        EvStatus,
        EvDataParsed,
        EvDataAfterFilteration,
        EvDataFiltered,
        EvPrintState,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvPqEventsReady : public NActors::TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};
    struct TEvCreateSession : public NActors::TEventLocal<TEvCreateSession, EvCreateSession> {};
    struct TEvPrintState : public NActors::TEventLocal<TEvPrintState, EvPrintState> {};
    struct TEvStatus : public NActors::TEventLocal<TEvStatus, EvStatus> {};
    struct TEvDataParsed : public NActors::TEventLocal<TEvDataParsed, EvDataParsed> {
        TEvDataParsed(ui64 offset, TList<TString>&& value) 
            : Offset(offset)
            , Value(std::move(value))
        {}
        ui64 Offset = 0; 
        TList<TString> Value;
    };

    struct TEvDataFiltered : public NActors::TEventLocal<TEvDataFiltered, EvDataFiltered> {
        TEvDataFiltered(ui64 offset) 
            : Offset(offset)
        {}
        ui64 Offset = 0; 
    };

    struct TEvDataAfterFilteration : public NActors::TEventLocal<TEvDataAfterFilteration, EvDataAfterFilteration> {
        TEvDataAfterFilteration(ui64 offset, const TString& json, TActorId readActorId)
            : Offset(offset)
            , Json(json)
            , ReadActorId(readActorId) { }
        ui64 Offset; 
        TString Json;
        TActorId ReadActorId;
    };
};

ui64 PrintStatePeriodSec = 60;
ui64 MaxBatchSizeBytes = 10000000;

TVector<TString> GetVector(const google::protobuf::RepeatedPtrField<TString>& value) {
    return {value.begin(), value.end()};
}

class TTopicSession : public TActorBootstrapped<TTopicSession> {

private:
    using TParserInputType = std::pair< TVector<TString>, TVector<TString>>; // TODO: remove after YQ-3594

    struct ClientsInfo {
        ClientsInfo(const NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev)
            : Settings(ev->Get()->Record)
            , ReadActorId(ev->Sender)
        {
            if (Settings.HasOffset()) {
                NextMessageOffset = Settings.GetOffset();
            }
        }
        NFq::NRowDispatcherProto::TEvStartSession Settings;
        NActors::TActorId ReadActorId;
        std::unique_ptr<TJsonFilter> Filter;        // empty if no predicate
        TQueue<std::pair<ui64, TString>> Buffer;
        ui64 UsedSize = 0;
        bool DataArrivedSent = false;
        TMaybe<ui64> NextMessageOffset;
        ui64 LastSendedNextMessageOffset = 0;
    };

    struct TTopicEventProcessor {
        void operator()(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event);
        void operator()(NYdb::NTopic::TSessionClosedEvent& event);
        void operator()(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event);
        void operator()(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event);
        void operator()(NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent& event);
        void operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& event);
        void operator()(NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent&) {}
        void operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent&) { }

        TTopicSession& Self;
        const TString& LogPrefix;    
    };

    const TString TopicPath;
    NActors::TActorId RowDispatcherActorId;
    ui32 PartitionId;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    NYql::ITopicClient::TPtr TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    const i64 BufferSize;
    TString LogPrefix;
    NYql::NDq::TDqAsyncStats IngressStats;
    ui64 LastMessageOffset = 0;
    bool IsWaitingEvents = false;
    THashMap<NActors::TActorId, ClientsInfo> Clients;
    THashSet<NActors::TActorId> ClientsWithoutPredicate;
    std::unique_ptr<TJsonParser> Parser;
    NConfig::TRowDispatcherConfig Config;
    ui64 UsedSize = 0;
    TMaybe<TParserInputType> CurrentParserTypes;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TTopicSessionMetrics Metrics;
    NYql::IPqGateway::TPtr PqGateway;

public:
    explicit TTopicSession(
        const TString& topicPath,
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const NYql::IPqGateway::TPtr& pqGateway);

    void Bootstrap();
    void PassAway() override;

    static constexpr char ActorName[] = "FQ_ROW_DISPATCHER_SESSION";

private:
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const;
    NYql::ITopicClient& GetTopicClient(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams);
    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const;
    void CreateTopicSession();
    void CloseTopicSession();
    void SubscribeOnNextEvent();
    void SendToParsing(ui64 offset, const TString& message);
    void SendData(ClientsInfo& info);
    void InitParser(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams);
    void FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter = nullptr);
    void SendDataArrived(ClientsInfo& client);
    void StopReadSession();
    TString GetSessionId() const;
    void HandleNewEvents();
    TInstant GetMinStartingMessageTimestamp() const;
    void AddDataToClient(ClientsInfo& client, ui64 offset, const TString& json);

    std::pair<NYql::NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    void Handle(NFq::TEvPrivate::TEvPqEventsReady::TPtr&);
    void Handle(NFq::TEvPrivate::TEvCreateSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataParsed::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataAfterFilteration::TPtr&);
    void Handle(NFq::TEvPrivate::TEvStatus::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataFiltered::TPtr&);
    void Handle(NFq::TEvPrivate::TEvPrintState::TPtr&);
    void Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr&);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    void HandleException(const std::exception& err);

    void PrintInternalState();
    void SendSessionError(NActors::TActorId readActorId, const TString& message);

private:

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(NFq::TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(NFq::TEvPrivate::TEvCreateSession, Handle);
        hFunc(NFq::TEvPrivate::TEvDataParsed, Handle);
        hFunc(NFq::TEvPrivate::TEvDataAfterFilteration, Handle);
        hFunc(NFq::TEvPrivate::TEvStatus, Handle);
        hFunc(NFq::TEvPrivate::TEvDataFiltered, Handle);
        hFunc(NFq::TEvPrivate::TEvPrintState, Handle);
        hFunc(TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);,
        ExceptionFunc(std::exception, HandleException)
    )

    STRICT_STFUNC(ErrorState, {
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        IgnoreFunc(NFq::TEvPrivate::TEvPqEventsReady);
        IgnoreFunc(NFq::TEvPrivate::TEvCreateSession);
        IgnoreFunc(NFq::TEvPrivate::TEvDataParsed);
        IgnoreFunc(NFq::TEvPrivate::TEvDataAfterFilteration);
        IgnoreFunc(NFq::TEvPrivate::TEvStatus);
        IgnoreFunc(NFq::TEvPrivate::TEvDataFiltered);
        IgnoreFunc(TEvRowDispatcher::TEvGetNextBatch);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStartSession);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStopSession);
        IgnoreFunc(NFq::TEvPrivate::TEvPrintState);
    })
};

TTopicSession::TTopicSession(
    const TString& topicPath,
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway)
    : TopicPath(topicPath)
    , RowDispatcherActorId(rowDispatcherActorId)
    , PartitionId(partitionId)
    , Driver(std::move(driver))
    , CredentialsProviderFactory(credentialsProviderFactory)
    , BufferSize(16_MB)
    , LogPrefix("TopicSession")
    , Config(config)
    , Counters(counters)
    , PqGateway(pqGateway)
{
}

void TTopicSession::Bootstrap() {
    Become(&TTopicSession::StateFunc);
    Metrics.Init(Counters, SelfId());
    LogPrefix = LogPrefix + " " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Bootstrap " << ", PartitionId " << PartitionId
        << ", Timeout " << Config.GetTimeoutBeforeStartSessionSec() << " sec,  StatusPeriod " << Config.GetSendStatusPeriodSec() << " sec");
    Y_ENSURE(Config.GetSendStatusPeriodSec() > 0);
    Schedule(TDuration::Seconds(Config.GetSendStatusPeriodSec()), new NFq::TEvPrivate::TEvStatus());
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new NFq::TEvPrivate::TEvPrintState());
}

void TTopicSession::PassAway() {
    LOG_ROW_DISPATCHER_DEBUG("PassAway");
    StopReadSession();
    NActors::TActorBootstrapped<TTopicSession>::PassAway();
}

void TTopicSession::SubscribeOnNextEvent() {
    if (!ReadSession || IsWaitingEvents) {
        return;
    }

    if (Config.GetMaxSessionUsedMemory() && UsedSize > Config.GetMaxSessionUsedMemory()) {
        LOG_ROW_DISPATCHER_TRACE("Too much used memory (" << UsedSize << " bytes), skip subscribing to WaitEvent()");
        return;
    }

    LOG_ROW_DISPATCHER_TRACE("SubscribeOnNextEvent");
    IsWaitingEvents = true;
    Metrics.InFlySubscribe->Inc();
    NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
    ReadSession->WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&){
        actorSystem->Send(selfId, new NFq::TEvPrivate::TEvPqEventsReady());
    });
}

NYdb::NTopic::TTopicClientSettings TTopicSession::GetTopicClientSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const {
    NYdb::NTopic::TTopicClientSettings opts;
    opts.Database(sourceParams.GetDatabase())
        .DiscoveryEndpoint(sourceParams.GetEndpoint())
        .SslCredentials(NYdb::TSslCredentials(sourceParams.GetUseSsl()))
        .CredentialsProviderFactory(CredentialsProviderFactory);
    return opts;
}

NYql::ITopicClient& TTopicSession::GetTopicClient(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) {
    if (!TopicClient) {
        TopicClient = PqGateway->GetTopicClient(Driver, GetTopicClientSettings(sourceParams));
    }
    return *TopicClient;
}

TInstant TTopicSession::GetMinStartingMessageTimestamp() const {
    auto result = TInstant::Max();
    Y_ENSURE(!Clients.empty());
    for (const auto& [actorId, info] : Clients) {
       ui64 time = info.Settings.GetStartingMessageTimestampMs();
       result = std::min(result, TInstant::MilliSeconds(time));
    }
    return result;
}

NYdb::NTopic::TReadSessionSettings TTopicSession::GetReadSessionSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const {
    NYdb::NTopic::TTopicReadSettings topicReadSettings;
    topicReadSettings.Path(TopicPath);
    topicReadSettings.AppendPartitionIds(PartitionId);

    TInstant minTime = GetMinStartingMessageTimestamp();
    LOG_ROW_DISPATCHER_INFO("Create topic session, Path " << TopicPath
        << ", StartingMessageTimestamp " << minTime
        << ", BufferSize " << BufferSize << ", WithoutConsumer " << Config.GetWithoutConsumer());

    auto settings = NYdb::NTopic::TReadSessionSettings()
        .AppendTopics(topicReadSettings)
        .MaxMemoryUsageBytes(BufferSize)
        .ReadFromTimestamp(minTime);
    if (Config.GetWithoutConsumer()) {
        settings.WithoutConsumer();
    } else {
        settings.ConsumerName(sourceParams.GetConsumerName());
    }
    return settings;
}

void TTopicSession::CreateTopicSession() {
    if (Clients.empty()) {
        return;
    }

    // Use any sourceParams.
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams = Clients.begin()->second.Settings.GetSource();

    if (!ReadSession) {
        InitParser(sourceParams);
        ReadSession = GetTopicClient(sourceParams).CreateReadSession(GetReadSessionSettings(sourceParams));
        SubscribeOnNextEvent();
    }
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvPqEventsReady::TPtr&) {
    LOG_ROW_DISPATCHER_TRACE("TEvPqEventsReady");
    Metrics.InFlySubscribe->Dec();
    IsWaitingEvents = false;
    HandleNewEvents();
    SubscribeOnNextEvent();
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvCreateSession::TPtr&) {
    CreateTopicSession();
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvDataParsed::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvDataParsed, offset " << ev->Get()->Offset);

    for (auto v: ev->Get()->Value) {
        LOG_ROW_DISPATCHER_TRACE("v " << v);
    }

    for (auto& [actorId, info] : Clients) {
        try {
            if (!info.Filter) {
                continue;
            }
            info.Filter->Push(ev->Get()->Offset, ev->Get()->Value);
        } catch (const std::exception& e) {
            FatalError(e.what(), &info.Filter);
        }
    }
    auto event = std::make_unique<TEvPrivate::TEvDataFiltered>(ev->Get()->Offset);
    Send(SelfId(), event.release());
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvDataAfterFilteration::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvDataAfterFilteration, read actor id " << ev->Get()->ReadActorId.ToString());
    auto it = Clients.find(ev->Get()->ReadActorId);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_ERROR("Skip DataAfterFilteration, wrong read actor, id " << ev->Get()->ReadActorId.ToString());
        return;
    }
    AddDataToClient(it->second, ev->Get()->Offset, ev->Get()->Json);
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvStatus::TPtr&) {
    LOG_ROW_DISPATCHER_TRACE("TEvStatus");
    Schedule(TDuration::Seconds(Config.GetSendStatusPeriodSec()), new NFq::TEvPrivate::TEvStatus());
    for (auto& [actorId, info] : Clients) {
        if (!info.NextMessageOffset) {
            continue;
        }
        if (*info.NextMessageOffset <= info.LastSendedNextMessageOffset) {
            continue;
        }
        auto event = std::make_unique<TEvRowDispatcher::TEvStatus>();
        event->Record.SetPartitionId(PartitionId);
        event->Record.SetNextMessageOffset(*info.NextMessageOffset);
        info.LastSendedNextMessageOffset = *info.NextMessageOffset;
        event->ReadActorId = info.ReadActorId;
        LOG_ROW_DISPATCHER_TRACE("Send status to " << info.ReadActorId << ", offset " << *info.NextMessageOffset);
        Send(RowDispatcherActorId, event.release());
    }
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvDataFiltered::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvDataFiltered, offset " << ev->Get()->Offset);
    for (auto& [actorId, info] : Clients) {
        if (!info.NextMessageOffset
            || *info.NextMessageOffset < ev->Get()->Offset + 1) {
            info.NextMessageOffset = ev->Get()->Offset + 1;
        }
    }
}

void TTopicSession::Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvGetNextBatch from " << ev->Sender.ToString());
    Metrics.InFlyAsyncInputData->Set(0);
    auto it = Clients.find(ev->Sender);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_ERROR("Wrong client, sender " << ev->Sender);
        return;
    }
    SendData(it->second);
    SubscribeOnNextEvent();
}

void TTopicSession::HandleNewEvents() {
    ui64 maxHandledEvents = 50;
    while (true) {
        if (!ReadSession) {
            return;
        }
        if (Config.GetMaxSessionUsedMemory() && UsedSize > Config.GetMaxSessionUsedMemory()) {
            LOG_ROW_DISPATCHER_TRACE("Too much used memory (" << UsedSize << " bytes), stop reading from yds");
            break;
        }
        TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
        if (!event) {
            break;
        }
        std::visit(TTopicEventProcessor{*this, LogPrefix}, *event);
        if (!maxHandledEvents--) {
            break;
        }
    }
}

void TTopicSession::CloseTopicSession() {
    if (!ReadSession) {
        return;
    }
    LOG_ROW_DISPATCHER_DEBUG("Close session");
    ReadSession->Close(TDuration::Zero());
    ReadSession.reset();
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
    Self.Metrics.RowsRead->Add(event.GetMessages().size());
    for (const auto& message : event.GetMessages()) {
        const TString& data = message.GetData();
        Self.IngressStats.Bytes += data.size();
        LOG_ROW_DISPATCHER_TRACE("Data received: " << message.DebugString(true));

        TString item = message.GetData();
        item.Detach();
        Self.SendToParsing(message.GetOffset(), item);
        Self.LastMessageOffset = message.GetOffset();
    }
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TSessionClosedEvent& ev) {
    TString message = TStringBuilder() << "Read session to topic \"" << Self.TopicPath << "\" was closed: " << ev.DebugString();
    LOG_ROW_DISPATCHER_DEBUG(message);
    NYql::TIssues issues;
    issues.AddIssue(message);
    Self.FatalError(issues.ToOneLineString());
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
    LOG_ROW_DISPATCHER_DEBUG("StartPartitionSessionEvent received");

    TMaybe<ui64> minOffset;
    for (const auto& [actorId, info] : Self.Clients) {
         if (!minOffset
            || (info.NextMessageOffset && (info.NextMessageOffset < *minOffset))) {
                minOffset = info.NextMessageOffset;
            } 
    }
    LOG_ROW_DISPATCHER_DEBUG("Confirm StartPartitionSession with offset " << minOffset);
    event.Confirm(minOffset);
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event) {
    LOG_ROW_DISPATCHER_DEBUG("SessionId: " << Self.GetSessionId() << " StopPartitionSessionEvent received");
    event.Confirm();
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent& /*event*/) {
    LOG_ROW_DISPATCHER_WARN("TEndPartitionSessionEvent");
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& /*event*/) {
    LOG_ROW_DISPATCHER_WARN("TPartitionSessionClosedEvent");
}

std::pair<NYql::NUdf::TUnboxedValuePod, i64> TTopicSession::CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    const TString& data = message.GetData();
    i64 usedSpace = data.Size();
    NYql::NUdf::TUnboxedValuePod item = NKikimr::NMiniKQL::MakeString(NYql::NUdf::TStringRef(data.Data(), data.Size()));
    return std::make_pair(item, usedSpace);
}

TString TTopicSession::GetSessionId() const {
    return ReadSession ? ReadSession->GetSessionId() : TString{"empty"};
}

void TTopicSession::SendToParsing(ui64 offset, const TString& message) {
    LOG_ROW_DISPATCHER_TRACE("SendToParsing, message " << message);

    for (auto& readActorId : ClientsWithoutPredicate) {
        auto it = Clients.find(readActorId);
        Y_ENSURE(it != Clients.end(), "Internal error: unknown client");
        auto& info = it->second;
        if (!info.Filter) {
            LOG_ROW_DISPATCHER_TRACE("Send message to client without parsing/filtering");
            AddDataToClient(info, offset, message);
        }
    }

    try {
        Parser->Push(offset, message);
    } catch (const std::exception& e) {
        FatalError(e.what());
    }
}

void TTopicSession::SendData(ClientsInfo& info) {
    info.DataArrivedSent = false;
    if (info.Buffer.empty()) {
        LOG_ROW_DISPATCHER_TRACE("Buffer empty");
    }

    do {
        auto event = std::make_unique<TEvRowDispatcher::TEvMessageBatch>();
        event->Record.SetPartitionId(PartitionId);
        Y_ENSURE(info.NextMessageOffset);
        event->ReadActorId = info.ReadActorId;

        ui64 batchSize = 0;
        while (!info.Buffer.empty()) {
            const auto& [offset, json] = info.Buffer.front();
            info.UsedSize -= json.size();
            UsedSize -= json.size();
            batchSize += json.size();
            NFq::NRowDispatcherProto::TEvMessage message;
            message.SetJson(json);
            message.SetOffset(offset);
            event->Record.AddMessages()->CopyFrom(message);
            event->Record.SetNextMessageOffset(offset + 1);
            info.Buffer.pop();

            if (batchSize > MaxBatchSizeBytes) {
                break;
            }
        }
        if (info.Buffer.empty()) {
            event->Record.SetNextMessageOffset(*info.NextMessageOffset);
        }
        LOG_ROW_DISPATCHER_TRACE("SendData to " << info.ReadActorId << ", batch size " << event->Record.MessagesSize());
        Send(RowDispatcherActorId, event.release());
    } while(!info.Buffer.empty());
    info.LastSendedNextMessageOffset = *info.NextMessageOffset;
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    auto it = Clients.find(ev->Sender);
    if (it != Clients.end()) {
        FatalError("Internal error: sender " + ev->Sender.ToString());
        return;
    }

    LOG_ROW_DISPATCHER_INFO("New client, read actor id " << ev->Sender.ToString());

    auto columns = GetVector(ev->Get()->Record.GetSource().GetColumns());
    auto types = GetVector(ev->Get()->Record.GetSource().GetColumnTypes());
    auto parserType = std::make_pair(columns, types);
    if (CurrentParserTypes && *CurrentParserTypes != parserType) {
        SendSessionError(ev->Sender, "Different columns/types, use same in all queries");
        return;
    }

    try {
        auto& clientInfo = Clients.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ev->Sender), 
            std::forward_as_tuple(ev)).first->second;

        TString predicate = clientInfo.Settings.GetSource().GetPredicate();
        if (!predicate.empty()) {
            clientInfo.Filter = NewJsonFilter(
                columns,
                types,
                predicate,
                [&, actorId = clientInfo.ReadActorId](ui64 offset, const TString& json){
                    Send(SelfId(), new NFq::TEvPrivate::TEvDataAfterFilteration(offset, json, actorId));
                });
        } else {
            ClientsWithoutPredicate.insert(ev->Sender);
        }

        LOG_ROW_DISPATCHER_INFO("New client: offset " << clientInfo.NextMessageOffset << ", predicate: " << clientInfo.Settings.GetSource().GetPredicate());

        if (ReadSession) {
            if (clientInfo.Settings.HasOffset() && (clientInfo.Settings.GetOffset() <= LastMessageOffset)) {
                LOG_ROW_DISPATCHER_INFO("New client has less offset than the last message, stop (restart) topic session");
                StopReadSession();
            }
        }
    } catch (const NYql::NPureCalc::TCompileError& e) {
        FatalError("Adding new client failed: CompileError: sql: " + e.GetYql() + ", error: " + e.GetIssues());
    } catch (const yexception &ex) {
        FatalError(TString{"Adding new client failed: "} + ex.what());
    } catch (...) {
        FatalError("Adding new client failed, " + CurrentExceptionMessage());
    }

    PrintInternalState();
    if (!ReadSession) { 
        Schedule(TDuration::Seconds(Config.GetTimeoutBeforeStartSessionSec()), new NFq::TEvPrivate::TEvCreateSession());
    }
}

void TTopicSession::AddDataToClient(ClientsInfo& info, ui64 offset, const TString& json) {
    if (info.NextMessageOffset && offset < info.NextMessageOffset) {
        return;
    }
    info.NextMessageOffset = offset + 1;
    info.Buffer.push(std::make_pair(offset, json));
    info.UsedSize += json.size();
    UsedSize += json.size();
    SendDataArrived(info);
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    auto it = Clients.find(ev->Sender);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong ClientSettings"); // TODO
        return;
    }
    Clients.erase(it);
    ClientsWithoutPredicate.erase(ev->Sender);
}

void TTopicSession::InitParser(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) {
    if (Parser) {
        return;
    }
    try {
        CurrentParserTypes = std::make_pair(GetVector(sourceParams.GetColumns()), GetVector(sourceParams.GetColumnTypes()));
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        Parser = NewJsonParser(
            GetVector(sourceParams.GetColumns()),
            GetVector(sourceParams.GetColumnTypes()),
            [actorSystem, selfId = SelfId()](ui64 offset, TList<TString>&& value){
                actorSystem->Send(selfId, new NFq::TEvPrivate::TEvDataParsed(offset, std::move(value)));
            });
    } catch (const NYql::NPureCalc::TCompileError& e) {
        FatalError(e.GetIssues());
    }
}

void TTopicSession::FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter) {
    TStringStream str;
    str << message;
    if (Parser) {
        str << ", parser sql: " << Parser->GetSql();
    }
    if (filter) {
        str << ", filter sql:" << (*filter)->GetSql();
    }
    LOG_ROW_DISPATCHER_ERROR("FatalError: " << str.Str());

    for (auto& [readActorId, info] : Clients) {
        LOG_ROW_DISPATCHER_DEBUG("Send TEvSessionError to " << readActorId);
        SendSessionError(readActorId, str.Str());
    }
    StopReadSession();
    Become(&TTopicSession::ErrorState);
    ythrow yexception() << "FatalError: " << str.Str();    // To exit from current stack and call once PassAway() in HandleException().
}

void TTopicSession::SendSessionError(NActors::TActorId readActorId, const TString& message) {
    auto event = std::make_unique<TEvRowDispatcher::TEvSessionError>();
    event->Record.SetMessage(message);
    event->Record.SetPartitionId(PartitionId);
    event->ReadActorId = readActorId;
    Send(RowDispatcherActorId, event.release());
}

void TTopicSession::StopReadSession() {
    if (ReadSession) {
        LOG_ROW_DISPATCHER_DEBUG("Close read session");
        ReadSession->Close(TDuration::Zero());
        ReadSession.reset();
    }
    TopicClient.Reset();
}

void TTopicSession::SendDataArrived(ClientsInfo& info) {
    if (info.Buffer.empty() || info.DataArrivedSent) {
        return;
    }
    info.DataArrivedSent = true;
    LOG_ROW_DISPATCHER_TRACE("Send TEvNewDataArrived to " << info.ReadActorId);
    Metrics.InFlyAsyncInputData->Set(1);
    auto event = std::make_unique<TEvRowDispatcher::TEvNewDataArrived>();
    event->Record.SetPartitionId(PartitionId);
    event->ReadActorId = info.ReadActorId;
    Send(RowDispatcherActorId, event.release());
}

void TTopicSession::HandleException(const std::exception& e) {
    if (CurrentStateFunc() == &TThis::ErrorState) {
        return;
    }
    FatalError(TString("Internal error: exception: ") + e.what());
}

void TTopicSession::PrintInternalState() {
    TStringStream str;
    str << "Clients:\n";
    str << "UsedSize: " << UsedSize << "\n";
    for (auto& [readActorId, info] : Clients) {
        str << "    read actor id " << readActorId << ", buffer size " << info.Buffer.size() 
        << ", used size: " << info.UsedSize << ", data arrived sent " << info.DataArrivedSent 
        << ", next offset " << info.NextMessageOffset << "\n";
    }
    LOG_ROW_DISPATCHER_DEBUG(str.Str());
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvPrintState::TPtr&) {
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new NFq::TEvPrivate::TEvPrintState());
    PrintInternalState();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////
    
std::unique_ptr<NActors::IActor> NewTopicSession(
    const TString& topicPath,
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway) {
    return std::unique_ptr<NActors::IActor>(new TTopicSession(topicPath, config, rowDispatcherActorId, partitionId, std::move(driver), credentialsProviderFactory, counters, pqGateway));
}

} // namespace NFq
