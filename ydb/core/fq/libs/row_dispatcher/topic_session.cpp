#include "topic_session.h"

#include <ydb/library/actors/core/interconnect.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/runtime/dq_async_stats.h>

#include <util/generic/queue.h>
#include <util/string/join.h>
#include <queue>

#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>
#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>
#include <ydb/library/yql/public/purecalc/purecalc.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////


class TTopicSession : public TActorBootstrapped<TTopicSession> {

    struct TReadyBatch {
    public:
        TReadyBatch(TMaybe<TInstant> watermark, ui32 dataCapacity)
          : Watermark(watermark) {
            Data.reserve(dataCapacity);
        }

    public:
        TMaybe<TInstant> Watermark;
        //NKikimr::NMiniKQL::TUnboxedValueVector Data;
        TVector<std::pair<ui64, TString>> Data;     // first - offset
        i64 UsedSpace = 0;
        ui64 LastOffset = 0;
    };

private:
    NActors::TActorId RowDispatcherActorId;
    ui32 PartitionId;
    //TMaybe<ui64> ReadOffset;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;

    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    const i64 BufferSize;
    TString LogPrefix;
    NYql::NDq::TDqAsyncStats IngressStats;
    std::queue<TReadyBatch> ReadyBuffer;
    ui32 BatchCapacity;
    bool IsStopped = false;

    struct ConsumersInfo {
        NFq::NRowDispatcherProto::TEvStartSession Consumer;
        NActors::TActorId ReadActorId;
        ui64 LastSendedMessage = 0;
        std::unique_ptr<TJsonFilter> Filter;
        ui64 EventQueueId = 0;
        TQueue<std::pair<ui64, TString>> Buffer;
        bool DataArrivedSent = false;
    };
    TMap<NActors::TActorId, ConsumersInfo> Consumers;
    std::unique_ptr<TJsonParser> Parser;
    NConfig::TRowDispatcherConfig Config;

public:
    explicit TTopicSession(
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    void Bootstrap();
    TVector<TString> GetVector(const google::protobuf::RepeatedPtrField<TString>& value);
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const;
    NYdb::NTopic::TTopicClient& GetTopicClient(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams);
    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const;

    void CreateTopicSession();
    void CloseTopicSession();
    void SubscribeOnNextEvent();

    void ParseData();
    void DataParsed(ui64 offset, TList<TString>&& value);
    void SendData(ConsumersInfo& info);
    void InitParser(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams);
    void FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter = nullptr);
    void SendDataArrived();
    void StopReadSession();

    TString GetSessionId() const;
    void HandleNewEvents();
    void PassAway() override;

    TInstant GetMinStartingMessageTimestamp() const;
    void AddDataToConsumer(ConsumersInfo& consumer, ui64 offset, const TString& json);

    std::optional<NYql::TIssues> ProcessDataReceivedEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event);
    std::optional<NYql::TIssues> ProcessSessionClosedEvent(NYdb::NTopic::TSessionClosedEvent& ev);
    std::optional<NYql::TIssues> ProcessStartPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event);
    std::optional<NYql::TIssues> ProcessStopPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event);
    std::pair<NYql::NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message);
    TReadyBatch& GetActiveBatch(/*const TPartitionKey& partitionKey, TInstant time*/);

    void Handle(NFq::NTopicSession::TEvPrivate::TEvPqEventsReady::TPtr&);
    void Handle(NFq::NTopicSession::TEvPrivate::TEvCreateSession::TPtr&);
    void Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr&);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr &ev);

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER_SESSION";

private:

    STRICT_STFUNC(StateFunc,
        hFunc(NFq::NTopicSession::TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(NFq::NTopicSession::TEvPrivate::TEvCreateSession, Handle);
        hFunc(TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSession, Handle);
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        hFunc(NFq::TEvRowDispatcher::TEvStopSession, Handle);
    )

};

TVector<TString> TTopicSession::GetVector(const google::protobuf::RepeatedPtrField<TString>& value) {
    TVector<TString> result;
    for (const auto& v : value) {
        result.push_back(v);
    }
    return result;
}

TTopicSession::TTopicSession(
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory)
    : RowDispatcherActorId(rowDispatcherActorId)
    , PartitionId(partitionId)
    , Driver(driver)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , BufferSize(16_MB)
    , LogPrefix("TopicSession")
    , Config(config)
{
}

void TTopicSession::Bootstrap() {
    Become(&TTopicSession::StateFunc);
    LogPrefix = LogPrefix + " " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Bootstrap " << ", PartitionId " << PartitionId);
}

void TTopicSession::PassAway() {
    LOG_ROW_DISPATCHER_DEBUG("PassAway");
    StopReadSession();
    NActors::TActorBootstrapped<TTopicSession>::PassAway();
}

void TTopicSession::SubscribeOnNextEvent() {
    if (!ReadSession) {
        return;
    }

    LOG_ROW_DISPATCHER_DEBUG("SubscribeOnNextEvent");

    NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
    ReadSession->WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&){
        actorSystem->Send(selfId, new NFq::NTopicSession::TEvPrivate::TEvPqEventsReady());
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

NYdb::NTopic::TTopicClient& TTopicSession::GetTopicClient(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) {
    if (!TopicClient) {
        TopicClient = std::make_unique<NYdb::NTopic::TTopicClient>(Driver, GetTopicClientSettings(sourceParams));
    }
    return *TopicClient;
}

TInstant TTopicSession::GetMinStartingMessageTimestamp() const {
    auto result = TInstant::Max();
    Y_ENSURE(!Consumers.empty());
    for (const auto& [actorId, info] : Consumers) {
       ui64 time = info.Consumer.GetStartingMessageTimestampMs();
       result = std::min(result, TInstant::MilliSeconds(time));
    }
    return result;
}

NYdb::NTopic::TReadSessionSettings TTopicSession::GetReadSessionSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const {
    NYdb::NTopic::TTopicReadSettings topicReadSettings;
    topicReadSettings.Path(sourceParams.GetTopicPath());
    topicReadSettings.AppendPartitionIds(PartitionId);

    TInstant minTime = GetMinStartingMessageTimestamp();
    LOG_ROW_DISPATCHER_TRACE("TopicPath " << sourceParams.GetTopicPath() << ", StartingMessageTimestamp " << minTime);
    return NYdb::NTopic::TReadSessionSettings()
        .AppendTopics(topicReadSettings)
        .WithoutConsumer()
        .MaxMemoryUsageBytes(BufferSize)
        .ReadFromTimestamp(minTime);
}

void TTopicSession::CreateTopicSession() {
    if (Consumers.empty()) {
        return;
    }

    // Use any sourceParams.
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams = Consumers.begin()->second.Consumer.GetSource();

    if (!ReadSession) {
        InitParser(sourceParams);
        LOG_ROW_DISPATCHER_DEBUG("CreateTopicSession");
        ReadSession = GetTopicClient(sourceParams).CreateReadSession(GetReadSessionSettings(sourceParams));
        SubscribeOnNextEvent();
    }
}

void TTopicSession::Handle(NFq::NTopicSession::TEvPrivate::TEvPqEventsReady::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvPqEventsReady");
    HandleNewEvents();
    SubscribeOnNextEvent();
    ParseData();
}

void TTopicSession::Handle(NFq::NTopicSession::TEvPrivate::TEvCreateSession::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvCreateSession");
    CreateTopicSession();
}

void TTopicSession::Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvGetNextBatch");
    auto it = Consumers.find(ev->Sender);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong consumer"); // TODO
        return;
    }
    SendData(it->second);
}

void TTopicSession::HandleNewEvents() {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__); // TODO ?
    alloc.DisableStrictAllocationCheck();

    if (!ReadSession) {
        return;
    }

    while (true) {
        TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
        if (!event) {
            break;
        }
        std::optional<NYql::TIssues> issues;

        if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            issues = ProcessDataReceivedEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
            issues = ProcessSessionClosedEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            issues = ProcessStartPartitionSessionEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            issues = ProcessStopPartitionSessionEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&*event)) {
            LOG_ROW_DISPATCHER_WARN("TEndPartitionSessionEvent");
        }

        if (issues) {
            FatalError(issues->ToOneLineString());
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

std::optional<NYql::TIssues> TTopicSession::ProcessDataReceivedEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
    // const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
    // const auto partitionKeyStr = ToString(partitionKey);
    for (const auto& message : event.GetMessages()) {
        const TString& data = message.GetData();
        IngressStats.Bytes += data.size();
        LOG_ROW_DISPATCHER_TRACE("Data received: " << message.DebugString(true));

        const TString& item = message.GetData();
        size_t size = item.size();

        auto& curBatch = GetActiveBatch(/*partitionKey, message.GetWriteTime()*/);
        curBatch.Data.emplace_back(std::make_pair(message.GetOffset(), std::move(item)));
        curBatch.UsedSpace += size;
    }
    return std::nullopt;
}

std::optional<NYql::TIssues> TTopicSession::ProcessSessionClosedEvent(NYdb::NTopic::TSessionClosedEvent& ev) {
    TString message = (TStringBuilder() << "Read session to topic \"" << "\" was closed");
    LOG_ROW_DISPATCHER_DEBUG(message << ": " << ev.DebugString());

    NYql::TIssues issues;
    issues.AddIssue(TStringBuilder() << "Read session to topic \"" << "\" was closed: " << ev.DebugString());
    return issues;
}

std::optional<NYql::TIssues> TTopicSession::ProcessStartPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
    LOG_ROW_DISPATCHER_DEBUG("StartPartitionSessionEvent received");

    TMaybe<ui64> minOffset;
    for (const auto& [actorId, info] : Consumers) {
        if (!minOffset
            || (info.Consumer.HasOffset() && (info.Consumer.GetOffset() < *minOffset))) {
                minOffset = info.Consumer.GetOffset();
            } 
    }
    LOG_ROW_DISPATCHER_DEBUG("minOffset " << minOffset);

    event.Confirm(minOffset);
    return std::nullopt;
}

std::optional<NYql::TIssues> TTopicSession::ProcessStopPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event) {
    //  const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
    // const auto partitionKeyStr = ToString(partitionKey);
    LOG_ROW_DISPATCHER_DEBUG("SessionId: " << GetSessionId() << " StopPartitionSessionEvent received");
    event.Confirm();
    return std::nullopt;
}

TTopicSession::TReadyBatch& TTopicSession::GetActiveBatch(/*const TPartitionKey& partitionKey, TInstant time*/) {
    if (Y_UNLIKELY(ReadyBuffer.empty() || ReadyBuffer.back().Watermark.Defined())) {
        ReadyBuffer.emplace(Nothing(), BatchCapacity);
    }

    TReadyBatch& activeBatch = ReadyBuffer.back();
    return activeBatch;
}

std::pair<NYql::NUdf::TUnboxedValuePod, i64> TTopicSession::CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    const TString& data = message.GetData();

    i64 usedSpace = 0;
    NYql::NUdf::TUnboxedValuePod item;
    item = NKikimr::NMiniKQL::MakeString(NYql::NUdf::TStringRef(data.Data(), data.Size()));
    usedSpace += data.Size();
    return std::make_pair(item, usedSpace);
}

TString TTopicSession::GetSessionId() const {
    return ReadSession ? ReadSession->GetSessionId() : TString{"empty"};
}

void TTopicSession::ParseData() {
    LOG_ROW_DISPATCHER_DEBUG("ParseData: " );
    if (ReadyBuffer.empty()) {
        return;
    }
    auto& readyBatch = ReadyBuffer.front();
    for (const auto& [offset, value] : readyBatch.Data) {
        try {
            Parser->Push(offset, value);
        } catch (const yexception& e) {
            FatalError(e.what());
        } 
    }
    ReadyBuffer.pop();
}

void TTopicSession::DataParsed(ui64 offset, TList<TString>&& value) {
    LOG_ROW_DISPATCHER_TRACE("DataParsed, offset " << offset);
    
    for (auto v: value) {
        LOG_ROW_DISPATCHER_TRACE("v " << v);
    }

    for (auto& [actorId, info] : Consumers) {
        try {
            info.Filter->Push(offset, value);
        } catch (const yexception& e) {
            FatalError(e.what(), &info.Filter);
        }
    }
}

void TTopicSession::SendData(ConsumersInfo& info) {
    info.DataArrivedSent = false;
    if (info.Buffer.empty()) {

        LOG_ROW_DISPATCHER_DEBUG("Buffer empty");
        return;
    }

    auto event = std::make_unique<TEvRowDispatcher::TEvMessageBatch>();
    event->Record.SetPartitionId(PartitionId);    
    event->ReadActorId = info.ReadActorId;
    
    while (!info.Buffer.empty()) {
        const auto [offset, json] = info.Buffer.front();
        NFq::NRowDispatcherProto::TEvMessage message;
        message.SetJson(json);
        message.SetOffset(offset);
        event->Record.AddMessages()->CopyFrom(message);
        info.Buffer.pop();
    }
    LOG_ROW_DISPATCHER_DEBUG("SendData to " << info.ReadActorId << ", batch size " << event->Record.MessagesSize());
    Send(RowDispatcherActorId, event.release());
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStartSession");
    if (IsStopped) {
        // TODO
        LOG_ROW_DISPATCHER_DEBUG("Session is stopped, event ignored");
        return;
    }

    auto it = Consumers.find(ev->Sender);// TODO : mv to try
    if (it != Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong consumer"); // TODO
        return;
    }

    auto& consumerInfo = Consumers[ev->Sender];
    consumerInfo.Consumer = ev->Get()->Record;
    consumerInfo.ReadActorId = ev->Sender;

    TString predicate;
    try {
        predicate = ev->Get()->Record.GetSource().GetPredicate();
        LOG_ROW_DISPATCHER_DEBUG("Predicate: " << predicate);

        consumerInfo.Filter = NewJsonFilter(
            GetVector(ev->Get()->Record.GetSource().GetColumns()),
            GetVector(ev->Get()->Record.GetSource().GetColumnTypes()),
            predicate,
            [&, actorId = consumerInfo.ReadActorId](ui64 offset, const TString& json){
                auto it = Consumers.find(actorId);
                if (it == Consumers.end()) {
                    LOG_ROW_DISPATCHER_ERROR("Wrong consumer"); // TODO
                    return;
                }
                AddDataToConsumer(it->second, offset, json);
                LOG_ROW_DISPATCHER_TRACE("JsonFilter data: " << json);
                SendDataArrived();
            });

        LOG_ROW_DISPATCHER_DEBUG("Consumers size " << Consumers.size());
        Schedule(TDuration::Seconds(Config.GetTimeoutBeforeStartSessionSec()), new NFq::NTopicSession::TEvPrivate::TEvCreateSession());

    } catch (NYql::NPureCalc::TCompileError& e) {
        LOG_ROW_DISPATCHER_DEBUG("CompileError: sql: " << e.GetYql() << ", error: " << e.GetIssues());
        throw;
    } catch (const yexception &ex) {
        LOG_ROW_DISPATCHER_DEBUG("FormatWhere failed: "  << ex.what());
    }
}

void TTopicSession::AddDataToConsumer(ConsumersInfo& info, ui64 offset, const TString& json) {
    if (info.Consumer.HasOffset() && (offset < info.Consumer.GetOffset())) {
        return;
    }
    info.Buffer.push(std::make_pair(offset, json));
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    auto it = Consumers.find(ev->Sender);
    if (it == Consumers.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong consumer"); // TODO
        return;
    }

    Consumers.erase(it);
}

void TTopicSession::InitParser(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) {
    if (Parser) {
        return;
    }
    try {
        Parser = NewJsonParser(
            "",
            GetVector(sourceParams.GetColumns()),
            [&](ui64 offset, TList<TString>&& value){
                DataParsed(offset, std::move(value));
            });
    } catch (NYql::NPureCalc::TCompileError& e) {
        FatalError(e.GetIssues());
    }
}

void TTopicSession::FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter) {

    TStringStream str;
    str << message << ", parser sql: " << Parser->GetSql();
    if (filter) {
        str << ", filter sql:" << (*filter)->GetSql();
    }
    LOG_ROW_DISPATCHER_DEBUG("FatalError: " << str.Str());

    for (auto& [readActorId, info] : Consumers) {
        LOG_ROW_DISPATCHER_DEBUG("Send TEvSessionError to " << readActorId);
        auto event = std::make_unique<TEvRowDispatcher::TEvSessionError>();
        event->Record.SetMessage(str.Str());
        event->Record.SetPartitionId(PartitionId);
        event->ReadActorId = readActorId;
        Send(RowDispatcherActorId, event.release());
    }
    StopReadSession();
}

void TTopicSession::StopReadSession() {
    if (ReadSession) {
        LOG_ROW_DISPATCHER_DEBUG("Close read session");
        ReadSession->Close(TDuration::Zero());
        ReadSession.reset();
    }
    TopicClient.reset();
    IsStopped = true;
}

void TTopicSession::SendDataArrived() {
    for (auto& [readActorId, info] : Consumers) {
        if (info.Buffer.empty() || info.DataArrivedSent) {
            continue;
        }
        info.DataArrivedSent = true;
        LOG_ROW_DISPATCHER_DEBUG("Send TEvNewDataArrived to " << readActorId);
        auto event = std::make_unique<TEvRowDispatcher::TEvNewDataArrived>();
        event->Record.SetPartitionId(PartitionId);
        event->ReadActorId = readActorId;
        Send(RowDispatcherActorId, event.release());
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////
    
std::unique_ptr<NActors::IActor> NewTopicSession(
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    return std::unique_ptr<NActors::IActor>(new TTopicSession(config, rowDispatcherActorId, partitionId, driver, credentialsProviderFactory));
}

} // namespace NFq
