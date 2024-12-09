#include "topic_session.h"

#include "common.h"

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
#include <yql/essentials/public/purecalc/purecalc.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTopicSessionMetrics {
    void Init(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& topicPath, ui32 partitionId) {

        TopicGroup = counters->GetSubgroup("topic", CleanupCounterValueString(topicPath));
        PartitionGroup = TopicGroup->GetSubgroup("partition", ToString(partitionId));
        InFlyAsyncInputData = PartitionGroup->GetCounter("InFlyAsyncInputData");
        InFlySubscribe = PartitionGroup->GetCounter("InFlySubscribe");
        ReconnectRate = PartitionGroup->GetCounter("ReconnectRate", true);
        RestartSessionByOffsets = counters->GetCounter("RestartSessionByOffsets", true);
        SessionDataRate = PartitionGroup->GetCounter("SessionDataRate", true);
        WaitEventTimeMs = PartitionGroup->GetHistogram("WaitEventTimeMs", NMonitoring::ExponentialHistogram(13, 2, 1)); // ~ 1ms -> ~ 8s
        AllSessionsDataRate = counters->GetCounter("AllSessionsDataRate", true);
        InFlightCompileRequests = PartitionGroup->GetCounter("InFlightCompileRequests");
    }

    ::NMonitoring::TDynamicCounterPtr TopicGroup;
    ::NMonitoring::TDynamicCounterPtr PartitionGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlySubscribe;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReconnectRate;
    ::NMonitoring::TDynamicCounters::TCounterPtr RestartSessionByOffsets;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionDataRate;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlightCompileRequests;
    ::NMonitoring::THistogramPtr WaitEventTimeMs;
    ::NMonitoring::TDynamicCounters::TCounterPtr AllSessionsDataRate;

};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPqEventsReady = EvBegin + 10,
        EvCreateSession,
        EvSendStatisticToReadActor,
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

ui64 SendStatisticPeriodSec = 2;
ui64 MaxBatchSizeBytes = 10000000;
ui64 MaxHandledEventsCount = 1000;
ui64 MaxHandledEventsSize = 1000000;

TVector<TString> GetVector(const google::protobuf::RepeatedPtrField<TString>& value) {
    return {value.begin(), value.end()};
}

class TTopicSession : public TActorBootstrapped<TTopicSession> {
private:
    using TParserInputType = TSet<std::pair<TString, TString>>;

    struct TStats {
        void Add(ui64 dataSize, ui64 events) {
            Bytes += dataSize;
            Events += events;
        }
        void Clear() {
            Bytes = 0;
            Events = 0;
            ParseAndFilterLatency = TDuration::Zero();
        }
        ui64 Bytes = 0;
        ui64 Events = 0;
        TDuration ParseAndFilterLatency;
    };

    struct TClientsInfo {
        TClientsInfo(
            const NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev,
            NMonitoring::TDynamicCounterPtr& counters,
            TMaybe<ui64> offset)
            : Settings(ev->Get()->Record)
            , ReadActorId(ev->Sender)
            , FilteredDataRate(counters->GetCounter("FilteredDataRate", true))
            , RestartSessionByOffsetsByQuery(counters->GetCounter("RestartSessionByOffsetsByQuery", true))
        {
            if (offset) {
                NextMessageOffset = *offset;
                InitialOffset = *offset;
            }
            Y_UNUSED(TDuration::TryParse(Settings.GetSource().GetReconnectPeriod(), ReconnectPeriod));
        }
        NFq::NRowDispatcherProto::TEvStartSession Settings;
        NActors::TActorId ReadActorId;
        std::unique_ptr<TJsonFilter> Filter;        // empty if no predicate
        ui64 InFlightCompilationId = 0;
        TQueue<std::pair<ui64, TString>> Buffer;
        ui64 UnreadBytes = 0;
        bool DataArrivedSent = false;
        TMaybe<ui64> NextMessageOffset;                 // offset to restart topic session
        TMaybe<ui64> ProcessedNextMessageOffset;        // offset of fully processed data (to save to checkpoint)
        TVector<ui64> FieldsIds;
        TDuration ReconnectPeriod;
        TStats FilteredStat;
        NMonitoring::TDynamicCounters::TCounterPtr FilteredDataRate;    // filtered
        NMonitoring::TDynamicCounters::TCounterPtr RestartSessionByOffsetsByQuery;
        ui64 InitialOffset = 0;
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
        ui64& dataReceivedEventSize;
    };

    struct TParserSchema {
        TVector<ui64> FieldsMap;    // index - FieldId (from FieldsIndexes), value - parsing schema offset
        TParserInputType InputType;
    };

    struct TFieldDescription {
        ui64 IndexInParserSchema = 0;
        TString Type;
    };

    struct TFiltersCompileState {
        ui64 FreeId = 0;
        std::unordered_map<ui64, NActors::TActorId> InFlightCompilations = {};
    };

    bool InflightReconnect = false;
    TDuration ReconnectPeriod;
    const TString TopicPath;
    const TString TopicPathPartition;
    const TString Endpoint;
    const TString Database;
    NActors::TActorId RowDispatcherActorId;
    NActors::TActorId CompileServiceActorId;
    ui32 PartitionId;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    NYql::ITopicClient::TPtr TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    const i64 BufferSize;
    TString LogPrefix;
    TStats Statistics;
    ui64 LastMessageOffset = 0;
    bool IsWaitingEvents = false;
    bool IsStartParsingScheduled = false;
    THashMap<NActors::TActorId, TClientsInfo> Clients;
    THashSet<NActors::TActorId> ClientsWithoutPredicate;
    std::unique_ptr<TJsonParser> Parser;
    TFiltersCompileState FiltersCompilation;
    NConfig::TRowDispatcherConfig Config;
    ui64 UnreadBytes = 0;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    TTopicSessionMetrics Metrics;
    TParserSchema ParserSchema;
    THashMap<TString, TFieldDescription> FieldsIndexes;
    NYql::IPqGateway::TPtr PqGateway;
    TMaybe<TString> ConsumerName;
    ui64 RestartSessionByOffsets = 0;
    TInstant WaitEventStartedAt;

public:
    explicit TTopicSession(
        const TString& topicPath,
        const TString& endpoint,
        const TString& database,
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        NActors::TActorId compileServiceActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const NYql::IPqGateway::TPtr& pqGateway,
        ui64 maxBufferSize);

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
    void DoFiltering(ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& parsedValues);
    void SendData(TClientsInfo& info);
    void UpdateParser();
    void FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter, bool addParserDescription, const TMaybe<TString>& fieldName);
    void SendDataArrived(TClientsInfo& client);
    void StopReadSession();
    TString GetSessionId() const;
    void HandleNewEvents();
    TInstant GetMinStartingMessageTimestamp() const;
    void AddDataToClient(TClientsInfo& client, ui64 offset, const TString& json);
    void StartClientSession(TClientsInfo& info);

    std::pair<NYql::NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    void Handle(NFq::TEvPrivate::TEvPqEventsReady::TPtr&);
    void Handle(NFq::TEvPrivate::TEvCreateSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvReconnectSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataAfterFilteration::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataFiltered::TPtr&);
    void Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&);
    void Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr&);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    void Handle(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev);
    void HandleException(const std::exception& err);

    void SendStatistics();
    void SendSessionError(NActors::TActorId readActorId, const TString& message);
    TVector<const TVector<NYql::NUdf::TUnboxedValue>*> RebuildJson(const TClientsInfo& info, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& parsedValues);
    void UpdateParserSchema(const TParserInputType& inputType);
    void UpdateFieldsIds(TClientsInfo& clientInfo);
    bool CheckNewClient(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    TString GetAnyQueryIdByFieldName(const TString& fieldName);
    TMaybe<ui64> GetOffset(const NFq::NRowDispatcherProto::TEvStartSession& settings);

private:

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(NFq::TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(NFq::TEvPrivate::TEvCreateSession, Handle);
        hFunc(NFq::TEvPrivate::TEvDataAfterFilteration, Handle);
        hFunc(NFq::TEvPrivate::TEvDataFiltered, Handle);
        hFunc(NFq::TEvPrivate::TEvSendStatistic, Handle);
        hFunc(NFq::TEvPrivate::TEvReconnectSession, Handle);
        hFunc(TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        hFunc(TEvRowDispatcher::TEvPurecalcCompileResponse, Handle);
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
        IgnoreFunc(NFq::TEvPrivate::TEvDataFiltered);
        IgnoreFunc(TEvRowDispatcher::TEvGetNextBatch);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStartSession);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStopSession);
        IgnoreFunc(NFq::TEvPrivate::TEvSendStatistic);
        IgnoreFunc(TEvRowDispatcher::TEvPurecalcCompileResponse);
    })
};

TTopicSession::TTopicSession(
    const TString& topicPath,
    const TString& endpoint,
    const TString& database,
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    NActors::TActorId compileServiceActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway,
    ui64 maxBufferSize)
    : TopicPath(topicPath)
    , TopicPathPartition(TStringBuilder() << topicPath << "/" << partitionId)
    , Endpoint(endpoint)
    , Database(database)
    , RowDispatcherActorId(rowDispatcherActorId)
    , CompileServiceActorId(compileServiceActorId)
    , PartitionId(partitionId)
    , Driver(std::move(driver))
    , CredentialsProviderFactory(credentialsProviderFactory)
    , BufferSize(maxBufferSize)
    , LogPrefix("TopicSession")
    , Config(config)
    , Counters(counters)
    , PqGateway(pqGateway)
{
}

void TTopicSession::Bootstrap() {
    Become(&TTopicSession::StateFunc);
    Metrics.Init(Counters, TopicPath, PartitionId);
    LogPrefix = LogPrefix + " " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Bootstrap " << TopicPathPartition
        << ", Timeout " << Config.GetTimeoutBeforeStartSessionSec() << " sec,  StatusPeriod " << Config.GetSendStatusPeriodSec() << " sec");
    Y_ENSURE(Config.GetSendStatusPeriodSec() > 0);
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
    WaitEventStartedAt = TInstant::Now();
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
    LOG_ROW_DISPATCHER_INFO("Create topic session, Path " << TopicPathPartition
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
    auto waitEventDurationMs = (TInstant::Now() - WaitEventStartedAt).MilliSeconds();
    Metrics.WaitEventTimeMs->Collect(waitEventDurationMs);
    HandleNewEvents();
    SubscribeOnNextEvent();
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvCreateSession::TPtr&) {
    CreateTopicSession();
}

TVector<const TVector<NYql::NUdf::TUnboxedValue>*> TTopicSession::RebuildJson(const TClientsInfo& info, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& parsedValues) {
    TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result;
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

void TTopicSession::Handle(NFq::TEvPrivate::TEvReconnectSession::TPtr&) {
    Metrics.ReconnectRate->Inc();
    TInstant minTime = GetMinStartingMessageTimestamp();
    LOG_ROW_DISPATCHER_DEBUG("Reconnect topic session, " << TopicPathPartition
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
        if (info.Buffer.empty()) {
            if (!info.ProcessedNextMessageOffset || *info.ProcessedNextMessageOffset < ev->Get()->Offset + 1) {
                info.ProcessedNextMessageOffset = ev->Get()->Offset + 1;
            }
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
    ui64 handledEventsSize = 0;

    for (ui64 i = 0; i < MaxHandledEventsCount; ++i) {
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

        std::visit(TTopicEventProcessor{*this, LogPrefix, handledEventsSize}, *event);
        if (handledEventsSize >= MaxHandledEventsSize) {
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
    ui64 dataSize = 0;
    for (const auto& message : event.GetMessages()) {
        LOG_ROW_DISPATCHER_TRACE("Data received: " << message.DebugString(true));
        dataSize += message.GetData().size();
        Self.LastMessageOffset = message.GetOffset();
    }

    Self.Statistics.Add(dataSize, event.GetMessages().size());
    Self.Metrics.SessionDataRate->Add(dataSize);
    Self.Metrics.AllSessionsDataRate->Add(dataSize);
    dataReceivedEventSize += dataSize;
    Self.SendToParsing(event.GetMessages());
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TSessionClosedEvent& ev) {
    TString message = TStringBuilder() << "Read session to topic \"" << Self.TopicPathPartition << "\" was closed: " << ev.DebugString();
    LOG_ROW_DISPATCHER_DEBUG(message);
    NYql::TIssues issues;
    issues.AddIssue(message);
    Self.FatalError(issues.ToOneLineString(), nullptr, false, Nothing());
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

    TInstant startParseAndFilter = TInstant::Now();
    try {
        Parser->Parse(); // this call processes parse and filter steps
    } catch (const NFq::TJsonParserError& e) {
        FatalError(e.what(), nullptr, true, e.GetField());
    } catch (const std::exception& e) {
        FatalError(e.what(), nullptr, true, Nothing());
    }
    auto parseAndFilterLatency = TInstant::Now() - startParseAndFilter;
    Statistics.ParseAndFilterLatency = Max(Statistics.ParseAndFilterLatency, parseAndFilterLatency);
}

void TTopicSession::DoFiltering(ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& parsedValues) {
    const auto& offsets = Parser->GetOffsets();
    Y_ENSURE(rowsOffset < offsets.size(), "Invalid first row ofset");
    Y_ENSURE(numberRows, "Expected non empty parsed batch");
    Y_ENSURE(parsedValues, "Expected non empty schema");
    auto lastOffset = offsets[rowsOffset + numberRows - 1];
    LOG_ROW_DISPATCHER_TRACE("SendToFiltering, first offset: " << offsets[rowsOffset] << ", last offset: " << lastOffset);

    for (auto& [actorId, info] : Clients) {
        if (info.InFlightCompilationId) {                                           // filter compilation in flight
            continue;
        }
        if (info.NextMessageOffset && lastOffset < info.NextMessageOffset) {        // the batch has already been processed
            continue;
        }
        try {
            if (info.Filter) {
                info.Filter->Push(offsets, RebuildJson(info, parsedValues), rowsOffset, numberRows);
            }
        } catch (const std::exception& e) {
            FatalError(e.what(), &info.Filter, false, Nothing());
        }
    }

    Send(SelfId(), new TEvPrivate::TEvDataFiltered(offsets.back()));
}

void TTopicSession::SendData(TClientsInfo& info) {
    info.DataArrivedSent = false;
    if (info.Buffer.empty()) {
        LOG_ROW_DISPATCHER_TRACE("Buffer empty");
    }
    ui64 dataSize = 0;
    ui64 eventsSize = info.Buffer.size();

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
        dataSize += batchSize;
        if (info.Buffer.empty()) {
            event->Record.SetNextMessageOffset(*info.NextMessageOffset);
        }
        LOG_ROW_DISPATCHER_TRACE("SendData to " << info.ReadActorId << ", batch size " << event->Record.MessagesSize());
        Send(RowDispatcherActorId, event.release());
    } while(!info.Buffer.empty());
    info.FilteredStat.Add(dataSize, eventsSize);
    info.FilteredDataRate->Add(dataSize);
    info.ProcessedNextMessageOffset = *info.NextMessageOffset;
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

void TTopicSession::StartClientSession(TClientsInfo& info) {
    if (ReadSession) {
        auto offset = GetOffset(info.Settings);

        if (offset && (offset <= LastMessageOffset)) {
            LOG_ROW_DISPATCHER_INFO("New client has less offset (" << offset << ") than the last message (" << LastMessageOffset << "), stop (restart) topic session");
            Metrics.RestartSessionByOffsets->Inc();
            ++RestartSessionByOffsets;
            info.RestartSessionByOffsetsByQuery->Inc();
            StopReadSession();
        }
    }

    if (Parser) {
        // Parse remains data before changing parsing schema
        DoParsing(true);
    }
    UpdateParser();

    if (!ReadSession) {
        Schedule(TDuration::Seconds(Config.GetTimeoutBeforeStartSessionSec()), new NFq::TEvPrivate::TEvCreateSession());
    }
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    auto offset = GetOffset(ev->Get()->Record);
    LOG_ROW_DISPATCHER_INFO("New client: read actor id " << ev->Sender.ToString() << ", predicate: " 
        << ev->Get()->Record.GetSource().GetPredicate() << ", offset: " << offset);

    if (!CheckNewClient(ev)) {
        return;
    }

    auto columns = GetVector(ev->Get()->Record.GetSource().GetColumns());
    auto types = GetVector(ev->Get()->Record.GetSource().GetColumnTypes());

    try {
        auto queryGroup = Counters->GetSubgroup("queryId", ev->Get()->Record.GetQueryId());
        auto topicGroup = queryGroup->GetSubgroup("topic", CleanupCounterValueString(TopicPath));
        auto& clientInfo = Clients.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ev->Sender), 
            std::forward_as_tuple(ev, topicGroup, GetOffset(ev->Get()->Record))).first->second;
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
                {.EnabledLLVM = source.GetEnabledLLVM()}
            );

            clientInfo.InFlightCompilationId = ++FiltersCompilation.FreeId;
            Y_ENSURE(FiltersCompilation.InFlightCompilations.emplace(clientInfo.InFlightCompilationId, ev->Sender).second, "Got duplicated compilation event id");
            LOG_ROW_DISPATCHER_TRACE("Send compile request with id " << clientInfo.InFlightCompilationId);

            Send(CompileServiceActorId, clientInfo.Filter->GetCompileRequest().release(), 0, clientInfo.InFlightCompilationId);
            Metrics.InFlightCompileRequests->Inc();
        } else {
            ClientsWithoutPredicate.insert(ev->Sender);

            // In case of in flight compilation topic session will be checked after getting compile response
            StartClientSession(clientInfo);
        }
    } catch (...) {
        FatalError("Adding new client failed, got unexpected exception: " + CurrentExceptionMessage(), nullptr, true, Nothing());
    }
    ConsumerName = ev->Get()->Record.GetSource().GetConsumerName();
    SendStatistics();
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
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession from " << ev->Sender << " topicPath " << ev->Get()->Record.GetSource().GetTopicPath() << " clients count " << Clients.size());

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
        Parser = NewJsonParser(names, types, [this](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& parsedValues) {
            DoFiltering(rowsOffset, numberRows, parsedValues);
        }, parserConfig.GetBatchSizeBytes(), TDuration::MilliSeconds(parserConfig.GetBatchCreationTimeoutMs()), parserConfig.GetBufferCellCount());
    } catch (const NFq::TJsonParserError& e) {
        FatalError(e.what(), nullptr, true, e.GetField());
    } catch (const NYql::NPureCalc::TCompileError& e) {
        FatalError(e.GetIssues(), nullptr, true, Nothing());
    }
}

void TTopicSession::FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter, bool addParserDescription, const TMaybe<TString>& fieldName) {
    TStringStream str;
    str << message;
    if (Parser && addParserDescription) {
        str << ", parser description:\n" << Parser->GetDescription();
    }
    if (filter) {
        str << ", filter sql:\n" << (*filter)->GetSql();
    }
    if (fieldName) {
        auto queryId = GetAnyQueryIdByFieldName(*fieldName);
        str << ", the field (" << *fieldName <<  ") has been added by query: " + queryId;
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
    FatalError(TString("Internal error: exception: ") + e.what(), nullptr, false, Nothing());
}

void TTopicSession::SendStatistics() {
    LOG_ROW_DISPATCHER_TRACE("SendStatistics");
    TopicSessionStatistic sessionStatistic;
    auto& commonStatistic = sessionStatistic.Common;
    commonStatistic.UnreadBytes = UnreadBytes;
    commonStatistic.RestartSessionByOffsets = RestartSessionByOffsets;
    commonStatistic.ReadBytes = Statistics.Bytes;
    commonStatistic.ReadEvents = Statistics.Events;
    commonStatistic.ParseAndFilterLatency = Statistics.ParseAndFilterLatency;
    commonStatistic.LastReadedOffset = LastMessageOffset;

    sessionStatistic.SessionKey = TopicSessionParams{Endpoint, Database, TopicPath, PartitionId};
    sessionStatistic.Clients.reserve(Clients.size());
    for (auto& [readActorId, info] : Clients) {
        TopicSessionClientStatistic clientStatistic;
        clientStatistic.PartitionId = PartitionId;
        clientStatistic.ReadActorId = readActorId;
        clientStatistic.UnreadRows = info.Buffer.size();
        clientStatistic.UnreadBytes = info.UnreadBytes;
        clientStatistic.Offset = info.ProcessedNextMessageOffset.GetOrElse(0);
        clientStatistic.FilteredReadBytes = info.FilteredStat.Bytes;
        clientStatistic.ReadBytes = Statistics.Bytes;
        clientStatistic.IsWaiting = LastMessageOffset + 1 < info.NextMessageOffset.GetOrElse(0);
        clientStatistic.ReadLagMessages = info.NextMessageOffset.GetOrElse(0) - LastMessageOffset - 1;
        clientStatistic.InitialOffset = info.InitialOffset;
        info.FilteredStat.Clear();
        sessionStatistic.Clients.emplace_back(std::move(clientStatistic));
    }
    Statistics.Clear();
    auto event = std::make_unique<TEvRowDispatcher::TEvSessionStatistic>(sessionStatistic);
    Send(RowDispatcherActorId, event.release());
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&) {
    SendStatistics();
    Schedule(TDuration::Seconds(SendStatisticPeriodSec), new NFq::TEvPrivate::TEvSendStatistic());
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

void TTopicSession::Handle(TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("Got compile response for reauest with id " << ev->Cookie);

    const auto requestIt = FiltersCompilation.InFlightCompilations.find(ev->Cookie);
    if (requestIt == FiltersCompilation.InFlightCompilations.end()) {
        LOG_ROW_DISPATCHER_TRACE("Compile response ignored for id " << ev->Cookie);
        return;
    }

    const auto clientId = requestIt->second;
    FiltersCompilation.InFlightCompilations.erase(requestIt);
    Metrics.InFlightCompileRequests->Dec();

    if (!ev->Get()->ProgramHolder) {
        TString message = TStringBuilder() << "Filed to compile purecalc program, error: " << ev->Get()->Error;
        LOG_ROW_DISPATCHER_ERROR(message);
        FatalError(message, nullptr, false, Nothing());
        return;
    }

    const auto clientIt = Clients.find(clientId);
    if (clientIt == Clients.end()) {
        LOG_ROW_DISPATCHER_TRACE("Compile response ignored for id " << ev->Cookie << ", client with id " << clientId << " not found");
        return;
    }

    auto& clientInfo = clientIt->second;
    if (ev->Cookie != clientInfo.InFlightCompilationId) {
        LOG_ROW_DISPATCHER_TRACE("Outdated compiler response ignored for id " << ev->Cookie << ", client with id " << clientId << " changed");
        return;
    }

    Y_ENSURE(clientInfo.Filter, "Unexpected completion response for client without filter");
    clientInfo.Filter->OnCompileResponse(std::move(ev));
    clientInfo.InFlightCompilationId = 0;
    StartClientSession(clientInfo);
}

TString TTopicSession::GetAnyQueryIdByFieldName(const TString& fieldName) {
    TSet<std::pair<TString, TString>> namesWithTypes;
    for (auto& [readActorId, info] : Clients) {
        for (const auto& name : info.Settings.GetSource().GetColumns()) {
            if (name != fieldName) {
                continue;
            }
            return info.Settings.GetQueryId();
        }
    }
    return "Unknown";
}

TMaybe<ui64> TTopicSession::GetOffset(const NFq::NRowDispatcherProto::TEvStartSession& settings) {
    for (auto p: settings.GetOffset()) {
        if (p.GetPartitionId() != PartitionId) {
            continue;
        }
        return p.GetOffset();
    }
    return Nothing();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////
    
std::unique_ptr<NActors::IActor> NewTopicSession(
    const TString& topicPath,
    const TString& endpoint,
    const TString& database,
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    NActors::TActorId compileServiceActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway,
    ui64 maxBufferSize) {
    return std::unique_ptr<NActors::IActor>(new TTopicSession(topicPath, endpoint, database, config, rowDispatcherActorId, compileServiceActorId, partitionId, std::move(driver), credentialsProviderFactory, counters, pqGateway, maxBufferSize));
}

} // namespace NFq
