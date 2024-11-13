#include "topic_session.h"

#include <ydb/core/fq/libs/actors/logging/log.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/runtime/dq_async_stats.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <util/string/join.h>
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
        ReconnectRate = SubGroup->GetCounter("ReconnectRate", true);
        RestartSessionByOffsets = counters->GetCounter("RestartSessionByOffsets", true);
    }

    ~TTopicSessionMetrics() {
        SubGroup->RemoveSubgroup("actor_id", SelfId.ToString());
    }
    NActors::TActorId SelfId;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
    ::NMonitoring::TDynamicCounters::TCounterPtr RowsRead;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlySubscribe;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReconnectRate;
    ::NMonitoring::TDynamicCounters::TCounterPtr RestartSessionByOffsets;
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPqEventsReady = EvBegin + 10,
        EvCreateSession,
        EvStatus,
        EvDataAfterFilteration,
        EvDataFiltered,
        EvSendStatistic,
        EvStartParsing,
        EvReconnectSession,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvPqEventsReady : public NActors::TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};
    struct TEvCreateSession : public NActors::TEventLocal<TEvCreateSession, EvCreateSession> {};
    struct TEvSendStatistic : public NActors::TEventLocal<TEvSendStatistic, EvSendStatistic> {};
    struct TEvStatus : public NActors::TEventLocal<TEvStatus, EvStatus> {};
    struct TEvStartParsing : public NActors::TEventLocal<TEvStartParsing, EvStartParsing> {};
    struct TEvReconnectSession : public NActors::TEventLocal<TEvReconnectSession, EvReconnectSession> {};

    struct TEvDataFiltered : public NActors::TEventLocal<TEvDataFiltered, EvDataFiltered> {
        explicit TEvDataFiltered(ui64 offset)
            : Offset(offset)
        {}
        const ui64 Offset;
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

ui64 SendStatisticPeriodSec = 5;
ui64 MaxBatchSizeBytes = 10000000;
ui64 MaxHandledEvents = 1000;

TVector<TString> GetVector(const google::protobuf::RepeatedPtrField<TString>& value) {
    return {value.begin(), value.end()};
}

class TTopicSession : public TActorBootstrapped<TTopicSession> {
private:
    using TParserInputType = TSet<std::pair<TString, TString>>;

    struct TClientsInfo {
        TClientsInfo(const NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev)
            : Settings(ev->Get()->Record)
            , ReadActorId(ev->Sender)
        {
            if (Settings.HasOffset()) {
                NextMessageOffset = Settings.GetOffset();
            }
            Y_UNUSED(TDuration::TryParse(Settings.GetSource().GetReconnectPeriod(), ReconnectPeriod));
        }
        NFq::NRowDispatcherProto::TEvStartSession Settings;
        NActors::TActorId ReadActorId;
        std::unique_ptr<TJsonFilter> Filter;        // empty if no predicate
        TQueue<std::pair<ui64, TString>> Buffer;
        ui64 UnreadBytes = 0;
        bool DataArrivedSent = false;
        TMaybe<ui64> NextMessageOffset;
        ui64 LastSendedNextMessageOffset = 0;
        TVector<ui64> FieldsIds;
        TDuration ReconnectPeriod;
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

    struct TParserSchema {
        TVector<ui64> FieldsMap;    // index - FieldId (from FieldsIndexes), value - parsing schema offset
        TParserInputType InputType;
    };

    struct TFieldDescription {
        ui64 IndexInParserSchema = 0;
        TString Type;
    };

    bool InflightReconnect = false;
    TDuration ReconnectPeriod;
    const TString TopicPath;
    const TString Endpoint;
    const TString Database;
    NActors::TActorId RowDispatcherActorId;
    ui32 PartitionId;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    IPureCalcProgramFactory::TPtr PureCalcProgramFactory;
    NYql::ITopicClient::TPtr TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    const i64 BufferSize;
    TString LogPrefix;
    NYql::NDq::TDqAsyncStats IngressStats;
    ui64 LastMessageOffset = 0;
    bool IsWaitingEvents = false;
    bool IsStartParsingScheduled = false;
    THashMap<NActors::TActorId, TClientsInfo> Clients;
    THashSet<NActors::TActorId> ClientsWithoutPredicate;
    std::unique_ptr<TJsonParser> Parser;
    NConfig::TRowDispatcherConfig Config;
    ui64 UnreadBytes = 0;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TTopicSessionMetrics Metrics;
    TParserSchema ParserSchema;
    THashMap<TString, TFieldDescription> FieldsIndexes;
    NYql::IPqGateway::TPtr PqGateway;
    TMaybe<TString> ConsumerName;
    ui64 RestartSessionByOffsets = 0;

public:
    explicit TTopicSession(
        const TString& topicPath,
        const TString& endpoint,
        const TString& database,
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        IPureCalcProgramFactory::TPtr pureCalcProgramFactory,
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
    void SendToParsing(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages);
    void DoParsing(bool force = false);
    void DoFiltering(const TVector<ui64>& offsets, const TVector<NKikimr::NMiniKQL::TUnboxedValueVector>& parsedValues);
    void SendData(TClientsInfo& info);
    void UpdateParser();
    void FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter, bool addParserDescription);
    void SendDataArrived(TClientsInfo& client);
    void StopReadSession();
    TString GetSessionId() const;
    void HandleNewEvents();
    TInstant GetMinStartingMessageTimestamp() const;
    void AddDataToClient(TClientsInfo& client, ui64 offset, const TString& json);

    std::pair<NYql::NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    void Handle(NFq::TEvPrivate::TEvPqEventsReady::TPtr&);
    void Handle(NFq::TEvPrivate::TEvCreateSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvReconnectSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataAfterFilteration::TPtr&);
    void Handle(NFq::TEvPrivate::TEvStatus::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataFiltered::TPtr&);
    void Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&);
    void Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr&);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    void HandleException(const std::exception& err);

    void SendStatistic();
    void SendSessionError(NActors::TActorId readActorId, const TString& message);
    TVector<const NKikimr::NMiniKQL::TUnboxedValueVector*> RebuildJson(const TClientsInfo& info, const TVector<NKikimr::NMiniKQL::TUnboxedValueVector>& parsedValues);
    void UpdateParserSchema(const TParserInputType& inputType);
    void UpdateFieldsIds(TClientsInfo& clientInfo);
    bool CheckNewClient(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);

private:

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(NFq::TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(NFq::TEvPrivate::TEvCreateSession, Handle);
        hFunc(NFq::TEvPrivate::TEvDataAfterFilteration, Handle);
        hFunc(NFq::TEvPrivate::TEvStatus, Handle);
        hFunc(NFq::TEvPrivate::TEvDataFiltered, Handle);
        hFunc(NFq::TEvPrivate::TEvSendStatistic, Handle);
        hFunc(NFq::TEvPrivate::TEvReconnectSession, Handle);
        hFunc(TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        sFunc(NFq::TEvPrivate::TEvStartParsing, DoParsing);
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);,
        ExceptionFunc(std::exception, HandleException)
    )

    STRICT_STFUNC(ErrorState, {
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        IgnoreFunc(NFq::TEvPrivate::TEvPqEventsReady);
        IgnoreFunc(NFq::TEvPrivate::TEvCreateSession);
        IgnoreFunc(NFq::TEvPrivate::TEvDataAfterFilteration);
        IgnoreFunc(NFq::TEvPrivate::TEvStatus);
        IgnoreFunc(NFq::TEvPrivate::TEvDataFiltered);
        IgnoreFunc(TEvRowDispatcher::TEvGetNextBatch);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStartSession);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStopSession);
        IgnoreFunc(NFq::TEvPrivate::TEvSendStatistic);
    })
};

TTopicSession::TTopicSession(
    const TString& topicPath,
    const TString& endpoint,
    const TString& database,
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    IPureCalcProgramFactory::TPtr pureCalcProgramFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway)
    : TopicPath(topicPath)
    , Endpoint(endpoint)
    , Database(database)
    , RowDispatcherActorId(rowDispatcherActorId)
    , PartitionId(partitionId)
    , Driver(std::move(driver))
    , CredentialsProviderFactory(credentialsProviderFactory)
    , PureCalcProgramFactory(pureCalcProgramFactory)
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
    Schedule(TDuration::Seconds(SendStatisticPeriodSec), new NFq::TEvPrivate::TEvSendStatistic());
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

    if (Config.GetMaxSessionUsedMemory() && UnreadBytes > Config.GetMaxSessionUsedMemory()) {
        LOG_ROW_DISPATCHER_TRACE("Too much used memory (" << UnreadBytes << " bytes), skip subscribing to WaitEvent()");
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
    opts.Database(Database)
        .DiscoveryEndpoint(Endpoint)
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

    if (!ReadSession) {
        UpdateParser();
    
        // Use any sourceParams.
        const NYql::NPq::NProto::TDqPqTopicSource& sourceParams = Clients.begin()->second.Settings.GetSource();
        ReadSession = GetTopicClient(sourceParams).CreateReadSession(GetReadSessionSettings(sourceParams));
        SubscribeOnNextEvent();
    }

    if (!InflightReconnect && Clients) {
        // Use any sourceParams.
        ReconnectPeriod = Clients.begin()->second.ReconnectPeriod;
        if (ReconnectPeriod != TDuration::Zero()) {
            LOG_ROW_DISPATCHER_INFO("ReconnectPeriod " << ReconnectPeriod.ToString());
            Metrics.ReconnectRate->Inc();
            Schedule(ReconnectPeriod, new NFq::TEvPrivate::TEvReconnectSession());
            InflightReconnect = true;
        }
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

TVector<const NKikimr::NMiniKQL::TUnboxedValueVector*> TTopicSession::RebuildJson(const TClientsInfo& info, const TVector<NKikimr::NMiniKQL::TUnboxedValueVector>& parsedValues) {
    TVector<const NKikimr::NMiniKQL::TUnboxedValueVector*> result;
    const auto& offsets = ParserSchema.FieldsMap;
    result.reserve(info.FieldsIds.size());
    for (auto fieldId : info.FieldsIds) {
        Y_ENSURE(fieldId < offsets.size(), "fieldId " << fieldId << ", offsets.size() " << offsets.size());
        auto offset = offsets[fieldId];
        Y_ENSURE(offset < parsedValues.size(), "offset " << offset << ", jsonBatch.size() " << parsedValues.size());
        result.push_back(&parsedValues[offset]); 
    }
    return result;
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvDataAfterFilteration::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvDataAfterFilteration, read actor id " << ev->Get()->ReadActorId.ToString() << ", " << ev->Get()->Json);
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

void TTopicSession::Handle(NFq::TEvPrivate::TEvReconnectSession::TPtr&) {
    Metrics.ReconnectRate->Inc();
    TInstant minTime = GetMinStartingMessageTimestamp();
    LOG_ROW_DISPATCHER_DEBUG("Reconnect topic session, Path " << TopicPath
        << ", StartingMessageTimestamp " << minTime
        << ", BufferSize " << BufferSize << ", WithoutConsumer " << Config.GetWithoutConsumer());
    StopReadSession();
    CreateTopicSession();
    Schedule(ReconnectPeriod, new NFq::TEvPrivate::TEvReconnectSession());
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvDataFiltered::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvDataFiltered, last offset " << ev->Get()->Offset);
    for (auto& [actorId, info] : Clients) {
        if (!info.NextMessageOffset || *info.NextMessageOffset < ev->Get()->Offset + 1) {
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
    for (ui64 i = 0; i < MaxHandledEvents; ++i) {
        if (!ReadSession) {
            return;
        }
        if (Config.GetMaxSessionUsedMemory() && UnreadBytes > Config.GetMaxSessionUsedMemory()) {
            LOG_ROW_DISPATCHER_TRACE("Too much used memory (" << UnreadBytes << " bytes), stop reading from yds");
            break;
        }
        TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
        if (!event) {
            break;
        }
        std::visit(TTopicEventProcessor{*this, LogPrefix}, *event);
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
        LOG_ROW_DISPATCHER_TRACE("Data received: " << message.DebugString(true));

        Self.IngressStats.Bytes += message.GetData().size();
        Self.LastMessageOffset = message.GetOffset();
    }

    Self.SendToParsing(event.GetMessages());
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TSessionClosedEvent& ev) {
    TString message = TStringBuilder() << "Read session to topic \"" << Self.TopicPath << "\" was closed: " << ev.DebugString();
    LOG_ROW_DISPATCHER_DEBUG(message);
    NYql::TIssues issues;
    issues.AddIssue(message);
    Self.FatalError(issues.ToOneLineString(), nullptr, false);
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
    i64 usedSpace = data.size();
    NYql::NUdf::TUnboxedValuePod item = NKikimr::NMiniKQL::MakeString(NYql::NUdf::TStringRef(data.data(), data.size()));
    return std::make_pair(item, usedSpace);
}

TString TTopicSession::GetSessionId() const {
    return ReadSession ? ReadSession->GetSessionId() : TString{"empty"};
}

void TTopicSession::SendToParsing(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) {
    for (const auto& readActorId : ClientsWithoutPredicate) {
        const auto it = Clients.find(readActorId);
        Y_ENSURE(it != Clients.end(), "Internal error: unknown client");
        auto& info = it->second;
        if (info.Filter) {
            continue;
        }

        for (const auto& message : messages) {
            LOG_ROW_DISPATCHER_TRACE("Send message with offset " << message.GetOffset() << " to client " << info.ReadActorId <<" without parsing/filtering");
            AddDataToClient(info, message.GetOffset(), message.GetData());
        }
    }

    if (ClientsWithoutPredicate.size() == Clients.size()) {
        return;
    }

    Parser->AddMessages(messages);
    DoParsing();
}

void TTopicSession::DoParsing(bool force) {
    if (!Parser->IsReady() && !force) {
        const TInstant batchCreationDeadline = Parser->GetCreationDeadline();
        LOG_ROW_DISPATCHER_TRACE("Collecting data to parse, skip parsing, creation deadline " << batchCreationDeadline);
        if (!IsStartParsingScheduled && batchCreationDeadline) {
            IsStartParsingScheduled = true;
            Schedule(batchCreationDeadline, new TEvPrivate::TEvStartParsing());
        }
        return;
    }

    if (!Parser->GetNumberValues()) {
        return;
    }

    IsStartParsingScheduled = false;
    LOG_ROW_DISPATCHER_TRACE("SendToParsing, first offset: " << Parser->GetOffsets().front() << ", number values in buffer " << Parser->GetOffsets().size());

    try {
        const auto& parsedValues = Parser->Parse();
        DoFiltering(Parser->GetOffsets(), parsedValues);
    } catch (const std::exception& e) {
        FatalError(e.what(), nullptr, true);
    }
}

void TTopicSession::DoFiltering(const TVector<ui64>& offsets, const TVector<NKikimr::NMiniKQL::TUnboxedValueVector>& parsedValues) {
    Y_ENSURE(parsedValues, "Expected non empty schema");
    LOG_ROW_DISPATCHER_TRACE("SendToFiltering, first offset: " << offsets.front() << ", last offset: " << offsets.back());

    for (auto& [actorId, info] : Clients) {
        try {
            if (info.Filter) {
                info.Filter->Push(offsets, RebuildJson(info, parsedValues));
            }
        } catch (const std::exception& e) {
            FatalError(e.what(), &info.Filter, false);
        }
    }

    Send(SelfId(), new TEvPrivate::TEvDataFiltered(offsets.back()));
}

void TTopicSession::SendData(TClientsInfo& info) {
    info.DataArrivedSent = false;
    if (info.Buffer.empty()) {
        LOG_ROW_DISPATCHER_TRACE("Buffer empty");
    }

    if (!info.NextMessageOffset) {
        LOG_ROW_DISPATCHER_ERROR("Try SendData() without NextMessageOffset, " << info.ReadActorId 
            << " unread " << info.UnreadBytes << " DataArrivedSent " << info.DataArrivedSent);
        return;
    }

    do {
        auto event = std::make_unique<TEvRowDispatcher::TEvMessageBatch>();
        event->Record.SetPartitionId(PartitionId);
        event->ReadActorId = info.ReadActorId;

        ui64 batchSize = 0;
        while (!info.Buffer.empty()) {
            const auto& [offset, json] = info.Buffer.front();
            info.UnreadBytes -= json.size();
            UnreadBytes -= json.size();
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

void TTopicSession::UpdateFieldsIds(TClientsInfo& info) {
    const auto& source = info.Settings.GetSource();
    for (size_t i = 0; i < source.ColumnsSize(); ++i) {
        const auto& name = source.GetColumns().Get(i);
        auto it = FieldsIndexes.find(name);
        if (it == FieldsIndexes.end()) {
            auto nextIndex = FieldsIndexes.size();
            info.FieldsIds.push_back(nextIndex);
            FieldsIndexes[name] = {nextIndex, source.GetColumnTypes().Get(i)};
        } else {
            info.FieldsIds.push_back(it->second.IndexInParserSchema);
        }
    }
}

bool HasJsonColumns(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) {
    for (const auto& type : sourceParams.GetColumnTypes()) {
        if (type.Contains("Json")) {
            return true;
        }
    }
    return false;
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_INFO("New client: read actor id " << ev->Sender.ToString() << ", predicate: " 
        << ev->Get()->Record.GetSource().GetPredicate() << ", offset: " << ev->Get()->Record.GetOffset());

    if (!CheckNewClient(ev)) {
        return;
    }

    auto columns = GetVector(ev->Get()->Record.GetSource().GetColumns());
    auto types = GetVector(ev->Get()->Record.GetSource().GetColumnTypes());

    try {
        if (Parser) {
            // Parse remains data before adding new client
            DoParsing(true);
        }

        auto& clientInfo = Clients.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ev->Sender), 
            std::forward_as_tuple(ev)).first->second;
        UpdateFieldsIds(clientInfo);

        const auto& source = clientInfo.Settings.GetSource();
        TString predicate = source.GetPredicate();

        // TODO: remove this when the re-parsing is removed from pq read actor
        if (predicate.empty() && HasJsonColumns(source)) {
            predicate = "WHERE TRUE";
        }

        if (!predicate.empty()) {
            clientInfo.Filter = NewJsonFilter(
                columns,
                types,
                predicate,
                [&, actorId = clientInfo.ReadActorId](ui64 offset, const TString& json){
                    Send(SelfId(), new NFq::TEvPrivate::TEvDataAfterFilteration(offset, json, actorId));
                },
                PureCalcProgramFactory,
                {.EnabledLLVM = source.GetEnabledLLVM()}
            );
        } else {
            ClientsWithoutPredicate.insert(ev->Sender);
        }

        if (ReadSession) {
            if (clientInfo.Settings.HasOffset() && (clientInfo.Settings.GetOffset() <= LastMessageOffset)) {
                LOG_ROW_DISPATCHER_INFO("New client has less offset (" << clientInfo.Settings.GetOffset() << ") than the last message (" << LastMessageOffset << "), stop (restart) topic session");
                Metrics.RestartSessionByOffsets->Inc();
                ++RestartSessionByOffsets;
                StopReadSession();
            }
        }
    } catch (const NYql::NPureCalc::TCompileError& e) {
        FatalError("Adding new client failed: CompileError: sql: " + e.GetYql() + ", error: " + e.GetIssues(), nullptr, true);
    } catch (const yexception &ex) {
        FatalError(TString{"Adding new client failed: "} + ex.what(), nullptr, true);
    } catch (...) {
        FatalError("Adding new client failed, " + CurrentExceptionMessage(), nullptr, true);
    }
    ConsumerName = ev->Get()->Record.GetSource().GetConsumerName();
    UpdateParser();
    SendStatistic();
    if (!ReadSession) { 
        Schedule(TDuration::Seconds(Config.GetTimeoutBeforeStartSessionSec()), new NFq::TEvPrivate::TEvCreateSession());
    }
}

void TTopicSession::AddDataToClient(TClientsInfo& info, ui64 offset, const TString& json) {
    if (info.NextMessageOffset && offset < info.NextMessageOffset) {
        return;
    }
    info.NextMessageOffset = offset + 1;
    info.Buffer.push(std::make_pair(offset, json));
    info.UnreadBytes += json.size();
    UnreadBytes += json.size();
    SendDataArrived(info);
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    auto it = Clients.find(ev->Sender);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong ClientSettings");
        return;
    }
    auto& info = it->second;
    UnreadBytes -= info.UnreadBytes;
    Clients.erase(it);
    ClientsWithoutPredicate.erase(ev->Sender);
    if (Clients.empty()) {
        StopReadSession();
    }
    UpdateParser();
    SubscribeOnNextEvent();
}

void CollectColumns(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams, TSet<std::pair<TString, TString>>& columns) {
    auto size = sourceParams.GetColumns().size();
    Y_ENSURE(size == sourceParams.GetColumnTypes().size());

    for (int i = 0; i < size; ++i) {
        auto name = sourceParams.GetColumns().Get(i);
        auto type = sourceParams.GetColumnTypes().Get(i);
        columns.emplace(name, type);
    }
}

void TTopicSession::UpdateParserSchema(const TParserInputType& inputType) {
    ParserSchema.FieldsMap.clear();
    ParserSchema.FieldsMap.resize(FieldsIndexes.size());
    ui64 offset = 0;
    for (const auto& [name, type]: inputType) {
        Y_ENSURE(FieldsIndexes.contains(name));
        ui64 index = FieldsIndexes[name].IndexInParserSchema;
        ParserSchema.FieldsMap[index] = offset++;
    }
    ParserSchema.InputType = inputType;
}

void TTopicSession::UpdateParser() {
    TSet<std::pair<TString, TString>> namesWithTypes;
    for (auto& [readActorId, info] : Clients) {
        CollectColumns(info.Settings.GetSource(), namesWithTypes);
    }

    if (namesWithTypes == ParserSchema.InputType) {
        return;
    }
    if (namesWithTypes.empty()) {
        LOG_ROW_DISPATCHER_INFO("No columns to parse, reset parser");
        Parser.reset();
        return;
    }

    try {
        UpdateParserSchema(namesWithTypes);

        TVector<TString> names;
        TVector<TString> types;
        names.reserve(namesWithTypes.size());
        types.reserve(namesWithTypes.size());
        for (const auto& [name, type] : namesWithTypes) {
            names.push_back(name);
            types.push_back(type);
        }

        LOG_ROW_DISPATCHER_TRACE("Init JsonParser with columns: " << JoinSeq(',', names));
        const auto& parserConfig = Config.GetJsonParser();
        Parser = NewJsonParser(names, types, parserConfig.GetBatchSizeBytes(), TDuration::MilliSeconds(parserConfig.GetBatchCreationTimeoutMs()));
    } catch (const NYql::NPureCalc::TCompileError& e) {
        FatalError(e.GetIssues(), nullptr, true);
    }
}

void TTopicSession::FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter, bool addParserDescription) {
    TStringStream str;
    str << message;
    if (Parser && addParserDescription) {
        str << ", parser description:\n" << Parser->GetDescription();
    }
    if (filter) {
        str << ", filter sql:\n" << (*filter)->GetSql();
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

void TTopicSession::SendDataArrived(TClientsInfo& info) {
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
    FatalError(TString("Internal error: exception: ") + e.what(), nullptr, false);
}

void TTopicSession::SendStatistic() {
    TopicSessionStatistic stat;
    stat.Common.UnreadBytes = UnreadBytes;
    stat.Common.RestartSessionByOffsets = RestartSessionByOffsets;
    stat.SessionKey = TopicSessionParams{Endpoint, Database, TopicPath, PartitionId};
    stat.Clients.reserve(Clients.size());
    for (auto& [readActorId, info] : Clients) {
        TopicSessionClientStatistic client;
        client.PartitionId = PartitionId;
        client.ReadActorId = readActorId;
        client.UnreadRows = info.Buffer.size();
        client.UnreadBytes = info.UnreadBytes;
        client.Offset = info.NextMessageOffset.GetOrElse(0);
        stat.Clients.emplace_back(std::move(client));
    }
    auto event = std::make_unique<TEvRowDispatcher::TEvSessionStatistic>(stat);
    Send(RowDispatcherActorId, event.release());
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&) {
    Schedule(TDuration::Seconds(SendStatisticPeriodSec), new NFq::TEvPrivate::TEvSendStatistic());
    SendStatistic();
}

bool TTopicSession::CheckNewClient(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    auto it = Clients.find(ev->Sender);
    if (it != Clients.end()) {
        LOG_ROW_DISPATCHER_ERROR("Such a client already exists");
        SendSessionError(ev->Sender, "Internal error: such a client already exists");
        return false;
    }

    const auto& source = ev->Get()->Record.GetSource();
    if (!Config.GetWithoutConsumer() && ConsumerName && ConsumerName != source.GetConsumerName()) {
        LOG_ROW_DISPATCHER_INFO("Different consumer, expected " <<  ConsumerName << ", actual " << source.GetConsumerName() << ", send error");
        SendSessionError(ev->Sender, TStringBuilder() << "Use the same consumer in all queries via RD (current consumer " << ConsumerName << ")");
        return false;
    }

    Y_ENSURE(source.ColumnsSize() == source.ColumnTypesSize());
    for (size_t i = 0; i < source.ColumnsSize(); ++i) {
        const auto& name = source.GetColumns().Get(i);
        const auto& type = source.GetColumnTypes().Get(i);
        const auto it = FieldsIndexes.find(name);
        if (it != FieldsIndexes.end() && it->second.Type != type) {
            LOG_ROW_DISPATCHER_INFO("Different column `" << name << "` type, expected " << it->second.Type << ", actual " << type << ", send error");
            SendSessionError(ev->Sender, TStringBuilder() << "Use the same column type in all queries via RD, current type for column `" << name << "` is " << it->second.Type << " (requested type is " << type <<")");
            return false;
        }
    }

    return true;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////
    
std::unique_ptr<NActors::IActor> NewTopicSession(
    const TString& topicPath,
    const TString& endpoint,
    const TString& database,
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    IPureCalcProgramFactory::TPtr pureCalcProgramFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway) {
    return std::unique_ptr<NActors::IActor>(new TTopicSession(topicPath, endpoint, database, config, rowDispatcherActorId, partitionId, std::move(driver), credentialsProviderFactory, pureCalcProgramFactory, counters, pqGateway));
}

} // namespace NFq
