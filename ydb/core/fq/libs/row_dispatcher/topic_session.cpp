#include "topic_session.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/metrics/sanitize_label.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/format_handler.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/queue.h>

namespace NFq {

using namespace NActors;
using namespace NRowDispatcher;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTopicSessionMetrics {
    void Init(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& topicPath, const TString& readGroup, ui32 partitionId) {
        TopicGroup = counters->GetSubgroup("topic", SanitizeLabel(topicPath));
        ReadGroup = TopicGroup->GetSubgroup("read_group", SanitizeLabel(readGroup));
        PartitionGroup = ReadGroup->GetSubgroup("partition", ToString(partitionId));

        AllSessionsDataRate = ReadGroup->GetCounter("AllSessionsDataRate", true);
        InFlyAsyncInputData = PartitionGroup->GetCounter("InFlyAsyncInputData");
        InFlySubscribe = PartitionGroup->GetCounter("InFlySubscribe");
        ReconnectRate = PartitionGroup->GetCounter("ReconnectRate", true);
        RestartSessionByOffsets = PartitionGroup->GetCounter("RestartSessionByOffsets", true);
        SessionDataRate = PartitionGroup->GetCounter("SessionDataRate", true);
        WaitEventTimeMs = PartitionGroup->GetHistogram("WaitEventTimeMs", NMonitoring::ExplicitHistogram({5, 20, 100, 500, 2000}));
        QueuedBytes = PartitionGroup->GetCounter("QueuedBytes");
    }

    ::NMonitoring::TDynamicCounterPtr TopicGroup;
    ::NMonitoring::TDynamicCounterPtr ReadGroup;
    ::NMonitoring::TDynamicCounterPtr PartitionGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlySubscribe;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReconnectRate;
    ::NMonitoring::TDynamicCounters::TCounterPtr RestartSessionByOffsets;
    ::NMonitoring::TDynamicCounters::TCounterPtr SessionDataRate;
    ::NMonitoring::THistogramPtr WaitEventTimeMs;
    ::NMonitoring::TDynamicCounters::TCounterPtr AllSessionsDataRate;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueuedBytes;
};

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvPqEventsReady = EvBegin + 10,
        EvCreateSession,
        EvSendStatistic,
        EvReconnectSession,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvPqEventsReady : public TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};
    struct TEvCreateSession : public TEventLocal<TEvCreateSession, EvCreateSession> {};
    struct TEvSendStatistic : public TEventLocal<TEvSendStatistic, EvSendStatistic> {};
    struct TEvReconnectSession : public TEventLocal<TEvReconnectSession, EvReconnectSession> {};
};

constexpr ui64 SendStatisticPeriodSec = 2;
constexpr ui64 MaxHandledEventsCount = 1000;
constexpr ui64 MaxHandledEventsSize = 1000000;

class TTopicSession : public TActorBootstrapped<TTopicSession> {
private:
    using TBase = TActorBootstrapped<TTopicSession>;

    struct TStats {
        void Add(ui64 dataSize, ui64 events) {
            Bytes += dataSize;
            Events += events;
        }
        void Clear() {
            Bytes = 0;
            Events = 0;
        }
        ui64 Bytes = 0;
        ui64 Events = 0;
    };

    struct TClientsInfo : public IClientDataConsumer {
        using TPtr = TIntrusivePtr<TClientsInfo>;

        TClientsInfo(TTopicSession& self, const TString& logPrefix, const ITopicFormatHandler::TSettings& handlerSettings, const NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev, const NMonitoring::TDynamicCounterPtr& counters, const TString& readGroup, TMaybe<ui64> offset)
            : Self(self)
            , LogPrefix(logPrefix)
            , HandlerSettings(handlerSettings)
            , Settings(ev->Get()->Record)
            , ReadActorId(ev->Sender)
            , Counters(counters)
        {
            if (offset) {
                NextMessageOffset = *offset;
                InitialOffset = *offset;
            }
            Y_UNUSED(TDuration::TryParse(Settings.GetSource().GetReconnectPeriod(), ReconnectPeriod));
            auto queryGroup = Counters->GetSubgroup("query_id", ev->Get()->Record.GetQueryId());
            auto readSubGroup = queryGroup->GetSubgroup("read_group", SanitizeLabel(readGroup));
            FilteredDataRate = readSubGroup->GetCounter("FilteredDataRate", true);
            RestartSessionByOffsetsByQuery = readSubGroup->GetCounter("RestartSessionByOffsetsByQuery", true);
        }

        ~TClientsInfo() {
            Counters->RemoveSubgroup("query_id", Settings.GetQueryId());
        }

        TActorId GetClientId() const override {
            return ReadActorId;
        }

        std::optional<ui64> GetNextMessageOffset() const override {
            return NextMessageOffset;
        }

        TVector<TSchemaColumn> GetColumns() const override {
            const auto& source = Settings.GetSource();
            Y_ENSURE(source.ColumnsSize() == source.ColumnTypesSize(), "Columns size and types size should be equal, but got " << source.ColumnsSize() << " columns and " << source.ColumnTypesSize() << " types");

            TVector<TSchemaColumn> Columns;
            Columns.reserve(source.ColumnsSize());
            for (ui64 i = 0; i < source.ColumnsSize(); ++i) {
                Columns.emplace_back(TSchemaColumn{.Name = source.GetColumns().Get(i), .TypeYson = source.GetColumnTypes().Get(i)});
            }

            return Columns;
        }

        const TString& GetWhereFilter() const override {
            return Settings.GetSource().GetPredicate();
        }

        TPurecalcCompileSettings GetPurecalcSettings() const override {
            return {.EnabledLLVM = Settings.GetSource().GetEnabledLLVM()};
        }

        void OnClientError(TStatus status) override {
            Self.SendSessionError(ReadActorId, status);
        }

        void StartClientSession() override {
            Self.StartClientSession(*this);
        }

        void AddDataToClient(ui64 offset, ui64 rowSize) override {
            Y_ENSURE(!NextMessageOffset || offset >= *NextMessageOffset, "Unexpected historical offset");

            LOG_ROW_DISPATCHER_TRACE("AddDataToClient to " << ReadActorId << ", offset: " << offset << ", serialized size: " << rowSize);

            NextMessageOffset = offset + 1;
            QueuedRows++;
            QueuedBytes += rowSize;
            Self.QueuedBytes += rowSize;
            Self.SendDataArrived(*this);
            Self.Metrics.QueuedBytes->Add(rowSize);
        }

        void UpdateClientOffset(ui64 offset) override {
            LOG_ROW_DISPATCHER_TRACE("UpdateClientOffset for " << ReadActorId << ", new offset: " << offset);
            if (!NextMessageOffset || *NextMessageOffset < offset + 1) {
                NextMessageOffset = offset + 1;
            }
            if (!QueuedRows) {
                if (!ProcessedNextMessageOffset || *ProcessedNextMessageOffset < offset + 1) {
                    ProcessedNextMessageOffset = offset + 1;
                }
            }
        }

        // Settings
        TTopicSession& Self;
        const TString& LogPrefix;
        const ITopicFormatHandler::TSettings HandlerSettings;
        const NFq::NRowDispatcherProto::TEvStartSession Settings;
        const TActorId ReadActorId;
        TDuration ReconnectPeriod;

        // State
        ui64 QueuedRows = 0;
        ui64 QueuedBytes = 0;
        bool DataArrivedSent = false;
        std::optional<ui64> NextMessageOffset;          // offset to restart topic session
        TMaybe<ui64> ProcessedNextMessageOffset;        // offset of fully processed data (to save to checkpoint)

        // Metrics
        ui64 InitialOffset = 0;
        TStats FilteredStat;
        const ::NMonitoring::TDynamicCounterPtr Counters;
        NMonitoring::TDynamicCounters::TCounterPtr FilteredDataRate;    // filtered
        NMonitoring::TDynamicCounters::TCounterPtr RestartSessionByOffsetsByQuery;
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
        ui64& DataReceivedEventSize;
    };

    // Settings
    const TString ReadGroup;
    const TString TopicPath;
    const TString TopicPathPartition;
    const TString Endpoint;
    const TString Database;
    const TActorId RowDispatcherActorId;
    const ui32 PartitionId;
    const NYdb::TDriver Driver;
    const NYql::IPqGateway::TPtr PqGateway;
    const std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    const NConfig::TRowDispatcherConfig Config;
    const TFormatHandlerConfig FormatHandlerConfig;
    const i64 BufferSize;
    TString LogPrefix;

    // State
    bool InflightReconnect = false;
    TDuration ReconnectPeriod;

    NYql::ITopicClient::TPtr TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    std::map<ITopicFormatHandler::TSettings, ITopicFormatHandler::TPtr> FormatHandlers;
    std::unordered_map<TActorId, TClientsInfo::TPtr> Clients;

    ui64 LastMessageOffset = 0;
    bool IsWaitingEvents = false;
    ui64 QueuedBytes = 0;
    TMaybe<TString> ConsumerName;

    // Metrics
    TInstant WaitEventStartedAt;
    ui64 RestartSessionByOffsets = 0;
    TStats Statistics;
    TTopicSessionMetrics Metrics;
    const ::NMonitoring::TDynamicCounterPtr Counters;
    const ::NMonitoring::TDynamicCounterPtr CountersRoot;

public:
    explicit TTopicSession(
        const TString& readGroup,
        const TString& topicPath,
        const TString& endpoint,
        const TString& database,
        const NConfig::TRowDispatcherConfig& config,
        TActorId rowDispatcherActorId,
        TActorId compileServiceActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        const ::NMonitoring::TDynamicCounterPtr& countersRoot,
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
    void SendToParsing(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages);
    void SendData(TClientsInfo& info);
    void FatalError(const TStatus& status);
    void ThrowFatalError(const TStatus& status);
    void SendDataArrived(TClientsInfo& client);
    void StopReadSession();
    TString GetSessionId() const;
    void HandleNewEvents();
    TInstant GetMinStartingMessageTimestamp() const;
    void StartClientSession(TClientsInfo& info);

    void Handle(NFq::TEvPrivate::TEvPqEventsReady::TPtr&);
    void Handle(NFq::TEvPrivate::TEvCreateSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvReconnectSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&);
    void Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr&);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    void HandleException(const std::exception& err);

    void SendStatistics();
    bool CheckNewClient(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev);
    TMaybe<ui64> GetOffset(const NFq::NRowDispatcherProto::TEvStartSession& settings);
    void SendSessionError(TActorId readActorId, TStatus status);
    void RestartSessionIfOldestClient(const TClientsInfo& info);
    void RefreshParsers();

private:

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(NFq::TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(NFq::TEvPrivate::TEvCreateSession, Handle);
        hFunc(NFq::TEvPrivate::TEvSendStatistic, Handle);
        hFunc(NFq::TEvPrivate::TEvReconnectSession, Handle);
        hFunc(TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);,
        ExceptionFunc(std::exception, HandleException)
    )

    STRICT_STFUNC_EXC(ErrorState,
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        IgnoreFunc(NFq::TEvPrivate::TEvPqEventsReady);
        IgnoreFunc(NFq::TEvPrivate::TEvCreateSession);
        IgnoreFunc(TEvRowDispatcher::TEvGetNextBatch);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStartSession);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStopSession);
        IgnoreFunc(NFq::TEvPrivate::TEvSendStatistic);
        IgnoreFunc(NFq::TEvPrivate::TEvReconnectSession);,
        ExceptionFunc(std::exception, HandleException)
    )
};

TTopicSession::TTopicSession(
    const TString& readGroup,
    const TString& topicPath,
    const TString& endpoint,
    const TString& database,
    const NConfig::TRowDispatcherConfig& config,
    TActorId rowDispatcherActorId,
    TActorId compileServiceActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const ::NMonitoring::TDynamicCounterPtr& countersRoot,
    const NYql::IPqGateway::TPtr& pqGateway,
    ui64 maxBufferSize)
    : ReadGroup(readGroup)
    , TopicPath(topicPath)
    , TopicPathPartition(TStringBuilder() << topicPath << "/" << partitionId)
    , Endpoint(endpoint)
    , Database(database)
    , RowDispatcherActorId(rowDispatcherActorId)
    , PartitionId(partitionId)
    , Driver(std::move(driver))
    , PqGateway(pqGateway)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , Config(config)
    , FormatHandlerConfig(CreateFormatHandlerConfig(config, compileServiceActorId))
    , BufferSize(maxBufferSize)
    , LogPrefix("TopicSession")
    , Counters(counters)
    , CountersRoot(countersRoot)
{}

void TTopicSession::Bootstrap() {
    Become(&TTopicSession::StateFunc);
    Metrics.Init(Counters, TopicPath, ReadGroup, PartitionId);
    LogPrefix = LogPrefix + " " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Bootstrap " << TopicPathPartition
        << ", Timeout " << Config.GetTimeoutBeforeStartSessionSec() << " sec,  StatusPeriod " << Config.GetSendStatusPeriodSec() << " sec");
    Y_ENSURE(Config.GetSendStatusPeriodSec() > 0);
    Schedule(TDuration::Seconds(SendStatisticPeriodSec), new NFq::TEvPrivate::TEvSendStatistic());
}

void TTopicSession::PassAway() {
    LOG_ROW_DISPATCHER_DEBUG("PassAway");
    StopReadSession();
    FormatHandlers.clear();
    TBase::PassAway();
}

void TTopicSession::SubscribeOnNextEvent() {
    if (!ReadSession || IsWaitingEvents) {
        return;
    }

    if (Config.GetMaxSessionUsedMemory() && QueuedBytes > Config.GetMaxSessionUsedMemory()) {
        LOG_ROW_DISPATCHER_TRACE("Too much used memory (" << QueuedBytes << " bytes), skip subscribing to WaitEvent()");
        return;
    }

    LOG_ROW_DISPATCHER_TRACE("SubscribeOnNextEvent");
    IsWaitingEvents = true;
    Metrics.InFlySubscribe->Inc();
    TActorSystem* actorSystem = TActivationContext::ActorSystem();
    WaitEventStartedAt = TInstant::Now();
    ReadSession->WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&){
        actorSystem->Send(selfId, new NFq::TEvPrivate::TEvPqEventsReady());
    });
}

NYdb::NTopic::TTopicClientSettings TTopicSession::GetTopicClientSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const {
    return PqGateway->GetTopicClientSettings()
        .Database(Database)
        .DiscoveryEndpoint(Endpoint)
        .SslCredentials(NYdb::TSslCredentials(sourceParams.GetUseSsl()))
        .CredentialsProviderFactory(CredentialsProviderFactory);
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
       ui64 time = info->Settings.GetStartingMessageTimestampMs();
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
        // Use any sourceParams.
        const NYql::NPq::NProto::TDqPqTopicSource& sourceParams = Clients.begin()->second->Settings.GetSource();
        ReadSession = GetTopicClient(sourceParams).CreateReadSession(GetReadSessionSettings(sourceParams));
        SubscribeOnNextEvent();
    }

    if (!InflightReconnect && !Clients.empty()) {
        // Use any sourceParams.
        ReconnectPeriod = Clients.begin()->second->ReconnectPeriod;
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

void TTopicSession::Handle(NFq::TEvPrivate::TEvReconnectSession::TPtr&) {
    Metrics.ReconnectRate->Inc();
    TInstant minTime = GetMinStartingMessageTimestamp();
    LOG_ROW_DISPATCHER_DEBUG("Reconnect topic session, " << TopicPathPartition
        << ", StartingMessageTimestamp " << minTime
        << ", BufferSize " << BufferSize << ", WithoutConsumer " << Config.GetWithoutConsumer());
    RefreshParsers();
    StopReadSession();
    CreateTopicSession();
    Schedule(ReconnectPeriod, new NFq::TEvPrivate::TEvReconnectSession());
}

void TTopicSession::Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr& ev) {
    LOG_ROW_DISPATCHER_TRACE("TEvGetNextBatch from " << ev->Sender.ToString());
    Metrics.InFlyAsyncInputData->Set(0);
    auto it = Clients.find(ev->Sender);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_ERROR("Wrong client, sender " << ev->Sender);
        return;
    }
    SendData(*it->second);
    SubscribeOnNextEvent();
}

void TTopicSession::HandleNewEvents() {
    ui64 handledEventsSize = 0;

    for (ui64 i = 0; i < MaxHandledEventsCount; ++i) {
        if (!ReadSession) {
            return;
        }
        if (Config.GetMaxSessionUsedMemory() && QueuedBytes > Config.GetMaxSessionUsedMemory()) {
            LOG_ROW_DISPATCHER_TRACE("Too much used memory (" << QueuedBytes << " bytes), stop reading from yds");
            break;
        }
        std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
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
    DataReceivedEventSize += dataSize;
    Self.SendToParsing(event.GetMessages());
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TSessionClosedEvent& ev) {
    const TString message = TStringBuilder() << "Read session to topic \"" << Self.TopicPathPartition << "\" was closed";
    LOG_ROW_DISPATCHER_DEBUG(message << ": " << ev.DebugString());

    Self.ThrowFatalError(TStatus::Fail(
        NYql::NDq::YdbStatusToDqStatus(static_cast<Ydb::StatusIds::StatusCode>(ev.GetStatus())),
        NYdb::NAdapters::ToYqlIssues(ev.GetIssues())
    ).AddParentIssue(message));
}

void TTopicSession::TTopicEventProcessor::operator()(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
    LOG_ROW_DISPATCHER_DEBUG("StartPartitionSessionEvent received");

    std::optional<ui64> minOffset;
    for (const auto& [actorId, info] : Self.Clients) {
        if (!minOffset || (info->NextMessageOffset && *info->NextMessageOffset < *minOffset)) {
            minOffset = info->NextMessageOffset;
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

TString TTopicSession::GetSessionId() const {
    return ReadSession ? TString{ReadSession->GetSessionId()} : TString{"empty"};
}

void TTopicSession::SendToParsing(const std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages) {
    LOG_ROW_DISPATCHER_TRACE("SendToParsing, messages: " << messages.size());
    for (const auto& [_, formatHandler] : FormatHandlers) {
        if (formatHandler->HasClients()) {
            formatHandler->ParseMessages(messages);
        }
    }
}

void TTopicSession::SendData(TClientsInfo& info) {
    TQueue<std::pair<TRope, TVector<ui64>>> buffer;
    if (const auto formatIt = FormatHandlers.find(info.HandlerSettings); formatIt != FormatHandlers.end()) {
        buffer = formatIt->second->ExtractClientData(info.GetClientId());
    }

    info.DataArrivedSent = false;
    if (buffer.empty()) {
        LOG_ROW_DISPATCHER_TRACE("Buffer empty");
    }
    ui64 dataSize = 0;
    ui64 eventsSize = info.QueuedRows;

    if (!info.NextMessageOffset) {
        LOG_ROW_DISPATCHER_ERROR("Try SendData() without NextMessageOffset, " << info.ReadActorId 
            << " unread " << info.QueuedBytes << " DataArrivedSent " << info.DataArrivedSent);
        return;
    }

    do {
        auto event = std::make_unique<TEvRowDispatcher::TEvMessageBatch>();
        event->Record.SetPartitionId(PartitionId);
        event->ReadActorId = info.ReadActorId;

        ui64 batchSize = 0;
        while (!buffer.empty()) {
            auto [serializedData, offsets] = std::move(buffer.front());
            Y_ENSURE(!offsets.empty(), "Expected non empty message batch");
            buffer.pop();

            batchSize += serializedData.GetSize();

            NFq::NRowDispatcherProto::TEvMessage message;
            message.SetPayloadId(event->AddPayload(std::move(serializedData)));
            message.MutableOffsets()->Assign(offsets.begin(), offsets.end());
            event->Record.AddMessages()->CopyFrom(std::move(message));
            event->Record.SetNextMessageOffset(offsets.back() + 1);

            if (batchSize > MAX_BATCH_SIZE) {
                break;
            }
        }
        dataSize += batchSize;
        if (buffer.empty()) {
            event->Record.SetNextMessageOffset(*info.NextMessageOffset);
        }
        LOG_ROW_DISPATCHER_TRACE("SendData to " << info.ReadActorId << ", batch size " << event->Record.MessagesSize());
        Send(RowDispatcherActorId, event.release());
    } while(!buffer.empty());

    QueuedBytes -= info.QueuedBytes;
    Metrics.QueuedBytes->Sub(info.QueuedBytes);
    info.QueuedRows = 0;
    info.QueuedBytes = 0;

    info.FilteredStat.Add(dataSize, eventsSize);
    info.FilteredDataRate->Add(dataSize);
    info.ProcessedNextMessageOffset = *info.NextMessageOffset;
}

void TTopicSession::StartClientSession(TClientsInfo& info) {
    if (ReadSession) {
        auto offset = GetOffset(info.Settings);
        if (offset && offset <= LastMessageOffset) {
            LOG_ROW_DISPATCHER_INFO("New client has less offset (" << offset << ") than the last message (" << LastMessageOffset << "), stop (restart) topic session");
            Metrics.RestartSessionByOffsets->Inc();
            ++RestartSessionByOffsets;
            info.RestartSessionByOffsetsByQuery->Inc();
            StopReadSession();
        }
    }

    if (!ReadSession) {
        Schedule(TDuration::Seconds(Config.GetTimeoutBeforeStartSessionSec()), new NFq::TEvPrivate::TEvCreateSession());
    }
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    auto offset = GetOffset(ev->Get()->Record);
    const auto& source = ev->Get()->Record.GetSource();
    LOG_ROW_DISPATCHER_INFO("New client: read actor id " << ev->Sender.ToString() << ", predicate: " << source.GetPredicate() << ", offset: " << offset);

    if (!CheckNewClient(ev)) {
        return;
    }

    const TString& format = source.GetFormat();
    ITopicFormatHandler::TSettings handlerSettings = {.ParsingFormat = format ? format : "raw"};

    auto clientInfo = Clients.insert({ev->Sender, MakeIntrusive<TClientsInfo>(*this, LogPrefix, handlerSettings, ev, Counters, ReadGroup, offset)}).first->second;
    auto formatIt = FormatHandlers.find(handlerSettings);
    if (formatIt == FormatHandlers.end()) {
        formatIt = FormatHandlers.insert({handlerSettings, CreateTopicFormatHandler(
            ActorContext(),
            FormatHandlerConfig,
            handlerSettings,
            {.CountersRoot = CountersRoot, .CountersSubgroup = Metrics.PartitionGroup}
        )}).first;
    }

    if (auto status = formatIt->second->AddClient(clientInfo); status.IsFail()) {
        SendSessionError(clientInfo->ReadActorId, status);
        return;
    }

    ConsumerName = source.GetConsumerName();
    SendStatistics();
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession from " << ev->Sender << " topicPath " << ev->Get()->Record.GetSource().GetTopicPath() << " clients count " << Clients.size());

    auto it = Clients.find(ev->Sender);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_WARN("Ignore TEvStopSession from " << ev->Sender << ", no client");
        return;
    }
    auto& info = *it->second;
    RestartSessionIfOldestClient(info);

    QueuedBytes -= info.QueuedBytes;
    Metrics.QueuedBytes->Sub(info.QueuedBytes);
    if (const auto formatIt = FormatHandlers.find(info.HandlerSettings); formatIt != FormatHandlers.end()) {
        formatIt->second->RemoveClient(info.GetClientId());
        if (!formatIt->second->HasClients()) {
            FormatHandlers.erase(formatIt);
        }
    }
    Clients.erase(it);
    if (Clients.empty()) {
        StopReadSession();
    }
    SubscribeOnNextEvent();
}

void TTopicSession::RestartSessionIfOldestClient(const TClientsInfo& info) {
    // if we read historical data (because of this client), then we restart the session.

    if (!ReadSession || !info.NextMessageOffset) {
        return;
    }
    std::optional<ui64> minMessageOffset;
    for (auto& [readActorId, clientPtr] : Clients) {
        if (info.ReadActorId == readActorId || !clientPtr->NextMessageOffset) {
            continue;
        }
        if (!minMessageOffset) {
            minMessageOffset = clientPtr->NextMessageOffset;
            continue;
        }
        minMessageOffset = std::min(minMessageOffset, clientPtr->NextMessageOffset);
    }
    if (!minMessageOffset) {
        return;
    }

    if (info.NextMessageOffset >= minMessageOffset) {
        return;
    }
    LOG_ROW_DISPATCHER_INFO("Client (on StopSession) has less offset (" << info.NextMessageOffset << ") than others clients (" << minMessageOffset << "), stop (restart) topic session");
    Metrics.RestartSessionByOffsets->Inc();
    ++RestartSessionByOffsets;
    info.RestartSessionByOffsetsByQuery->Inc();
    RefreshParsers();
    StopReadSession();

    if (!ReadSession) {
        Schedule(TDuration::Seconds(Config.GetTimeoutBeforeStartSessionSec()), new NFq::TEvPrivate::TEvCreateSession());
    }
}

void TTopicSession::FatalError(const TStatus& status) {
    LOG_ROW_DISPATCHER_ERROR("FatalError: " << status.GetErrorMessage());

    for (auto& [readActorId, info] : Clients) {
        LOG_ROW_DISPATCHER_DEBUG("Send TEvSessionError to " << readActorId);
        SendSessionError(readActorId, status);
    }
    StopReadSession();
    Become(&TTopicSession::ErrorState);
}

void TTopicSession::ThrowFatalError(const TStatus& status) {
    FatalError(status);
    ythrow yexception() << "FatalError: " << status.GetErrorMessage();
}

void TTopicSession::SendSessionError(TActorId readActorId, TStatus status) {
    LOG_ROW_DISPATCHER_WARN("SendSessionError to " << readActorId << ", status: " << status.GetErrorMessage());
    auto event = std::make_unique<TEvRowDispatcher::TEvSessionError>();
    event->Record.SetStatusCode(status.GetStatus());
    NYql::IssuesToMessage(status.GetErrorDescription(), event->Record.MutableIssues());
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
    if (!info.QueuedBytes || info.DataArrivedSent) {
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
    FatalError(TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Session error, got unexpected exception: " << e.what()));
}

void TTopicSession::SendStatistics() {
    LOG_ROW_DISPATCHER_TRACE("SendStatistics");
    TTopicSessionStatistic sessionStatistic;
    auto& commonStatistic = sessionStatistic.Common;
    commonStatistic.QueuedBytes = QueuedBytes;
    commonStatistic.RestartSessionByOffsets = RestartSessionByOffsets;
    commonStatistic.ReadBytes = Statistics.Bytes;
    commonStatistic.ReadEvents = Statistics.Events;
    commonStatistic.LastReadedOffset = LastMessageOffset;

    sessionStatistic.SessionKey = TTopicSessionParams{ReadGroup, Endpoint, Database, TopicPath, PartitionId};
    sessionStatistic.Clients.reserve(Clients.size());
    for (const auto& [readActorId, infoPtr] : Clients) {
        auto& info = *infoPtr;
        TTopicSessionClientStatistic clientStatistic;
        clientStatistic.PartitionId = PartitionId;
        clientStatistic.ReadActorId = readActorId;
        clientStatistic.QueuedRows = info.QueuedRows;
        clientStatistic.QueuedBytes = info.QueuedBytes;
        clientStatistic.Offset = info.ProcessedNextMessageOffset.GetOrElse(0);
        clientStatistic.FilteredBytes = info.FilteredStat.Bytes;
        clientStatistic.FilteredRows = info.FilteredStat.Events;
        clientStatistic.ReadBytes = Statistics.Bytes;
        clientStatistic.IsWaiting = LastMessageOffset + 1 < info.NextMessageOffset.value_or(0);
        clientStatistic.ReadLagMessages = info.NextMessageOffset.value_or(0) - LastMessageOffset - 1;
        clientStatistic.InitialOffset = info.InitialOffset;
        info.FilteredStat.Clear();
        sessionStatistic.Clients.emplace_back(std::move(clientStatistic));
    }

    commonStatistic.FormatHandlers.reserve(FormatHandlers.size());
    for (const auto& [settings, handler] : FormatHandlers) {
        commonStatistic.FormatHandlers.emplace(settings.ParsingFormat, handler->GetStatistics());
    }

    auto event = std::make_unique<TEvRowDispatcher::TEvSessionStatistic>(sessionStatistic);
    Send(RowDispatcherActorId, event.release());
    Statistics.Clear();
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvSendStatistic::TPtr&) {
    SendStatistics();
    Schedule(TDuration::Seconds(SendStatisticPeriodSec), new NFq::TEvPrivate::TEvSendStatistic());
}

bool TTopicSession::CheckNewClient(NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev) {
    auto it = Clients.find(ev->Sender);
    if (it != Clients.end()) {
        LOG_ROW_DISPATCHER_ERROR("Such a client already exists");
        SendSessionError(ev->Sender, TStatus::Fail(EStatusId::INTERNAL_ERROR, TStringBuilder() << "Client with id " << ev->Sender << " already exists"));
        return false;
    }

    const auto& source = ev->Get()->Record.GetSource();
    if (!Config.GetWithoutConsumer() && ConsumerName && ConsumerName != source.GetConsumerName()) {
        LOG_ROW_DISPATCHER_INFO("Different consumer, expected " <<  ConsumerName << ", actual " << source.GetConsumerName() << ", send error");
        SendSessionError(ev->Sender, TStatus::Fail(EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Use the same consumer in all queries via RD (current consumer " << ConsumerName << ")"));
        return false;
    }

    return true;
}

TMaybe<ui64> TTopicSession::GetOffset(const NFq::NRowDispatcherProto::TEvStartSession& settings) {
    for (auto p : settings.GetOffsets()) {
        if (p.GetPartitionId() != PartitionId) {
            continue;
        }
        return p.GetOffset();
    }
    return Nothing();
}

void TTopicSession::RefreshParsers() {
    for (const auto& [_, formatHandler] : FormatHandlers) {
        formatHandler->ForceRefresh();
    }
}

}  // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> NewTopicSession(
    const TString& readGroup,
    const TString& topicPath,
    const TString& endpoint,
    const TString& database,
    const NConfig::TRowDispatcherConfig& config,
    TActorId rowDispatcherActorId,
    TActorId compileServiceActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const ::NMonitoring::TDynamicCounterPtr& countersRoot,
    const NYql::IPqGateway::TPtr& pqGateway,
    ui64 maxBufferSize) {
    return std::unique_ptr<IActor>(new TTopicSession(readGroup, topicPath, endpoint, database, config, rowDispatcherActorId, compileServiceActorId, partitionId, std::move(driver), credentialsProviderFactory, counters, countersRoot, pqGateway, maxBufferSize));
}

}  // namespace NFq
