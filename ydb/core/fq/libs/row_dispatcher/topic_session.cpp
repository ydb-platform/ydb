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
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/strip.h>
#include <queue>

#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>
#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>

#include <ydb/core/fq/libs/row_dispatcher/predicate_builder.h>

#include <ydb/library/yql/public/purecalc/purecalc.h>

namespace NFq {

using namespace NActors;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPqEventsReady = EvBegin + 10,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvPqEventsReady : public TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};
};



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
    const NYql::NPq::NProto::TDqPqTopicSource SourceParams;
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
    std::vector<std::tuple<TString, NYql::NDq::TPqMetaExtractor::TPqMetaExtractorLambda>> MetadataFields;
    bool IsStopped = false;

    struct ConsumersInfo {
        NFq::NRowDispatcherProto::TEvAddConsumer Consumer;
        NActors::TActorId ReadActorId;
        ui64 LastSendedMessage = 0;
        std::unique_ptr<TJsonFilter> Filter;
        ui64 EventQueueId = 0;
        TQueue<std::pair<ui64, TString>> Buffer;
    };
    TMap<NActors::TActorId, ConsumersInfo> Consumers;
    std::unique_ptr<TJsonParser> Parser;

public:
    explicit TTopicSession(
        NActors::TActorId rowDispatcherActorId,
        const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
        ui32 partitionId,
        //TMaybe<ui64> readOffset,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    void Bootstrap();
    TVector<TString> GetVector(const google::protobuf::RepeatedPtrField<TString>& value);
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const;
    NYdb::NTopic::TTopicClient& GetTopicClient();
    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings() const;
    NYdb::NTopic::IReadSession& GetReadSession();
    void SubscribeOnNextEvent();
    void ParseData();
    void DataParsed(ui64 offset, TList<TString>&& value);
    void SendData(ConsumersInfo& info);
    void CloseSession();
    void InitParser();
    void FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter = nullptr);
    void SendDataArrived();
    void StopReadSession();

    TString GetSessionId() const;
    void HandleNewEvents();
    void PassAway() override;

    TInstant GetMinStartingMessageTimestamp() const;

    std::optional<NYql::TIssues> ProcessDataReceivedEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event);
    std::optional<NYql::TIssues> ProcessSessionClosedEvent(NYdb::NTopic::TSessionClosedEvent& ev);
    std::optional<NYql::TIssues> ProcessStartPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event);
    std::optional<NYql::TIssues> ProcessStopPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event);
    std::pair<NYql::NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message);
    TReadyBatch& GetActiveBatch(/*const TPartitionKey& partitionKey, TInstant time*/);

    void Handle(TEvPrivate::TEvPqEventsReady::TPtr&);
    void Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr&);
    //void Handle(TEvRowDispatcher::TEvSessionAddConsumer::TPtr&);
    void Handle(TEvRowDispatcher::TEvSessionDeleteConsumer::TPtr&);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvAddConsumer::TPtr &ev);
    

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER_SESSION";

private:

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(TEvRowDispatcher::TEvGetNextBatch, Handle);
        //hFunc(TEvRowDispatcher::TEvSessionAddConsumer, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvAddConsumer, Handle);
        hFunc(TEvRowDispatcher::TEvSessionDeleteConsumer, Handle);
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
    NActors::TActorId rowDispatcherActorId,
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory)
    : RowDispatcherActorId(rowDispatcherActorId)
    , SourceParams(sourceParams)
    , PartitionId(partitionId)
    , Driver(driver)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , BufferSize(16_MB)
    , LogPrefix("TopicSession")
{
    LOG_ROW_DISPATCHER_DEBUG("MetadataFieldsSize " << SourceParams.MetadataFieldsSize());

    MetadataFields.reserve(SourceParams.MetadataFieldsSize());
    NYql::NDq::TPqMetaExtractor fieldsExtractor;
    for (const auto& fieldName : SourceParams.GetMetadataFields()) {
        MetadataFields.emplace_back(fieldName, fieldsExtractor.FindExtractorLambda(fieldName));
    }
}

void TTopicSession::Bootstrap() {
    Become(&TTopicSession::StateFunc);
    LogPrefix = LogPrefix + " " + SelfId().ToString() + " ";
    LOG_ROW_DISPATCHER_DEBUG("Bootstrap " << SourceParams.GetTopicPath() << ", PartitionId " << PartitionId);
    std::cerr << " TTopicSession::Bootstrap" << std::endl;
    InitParser();
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
        actorSystem->Send(selfId, new TEvPrivate::TEvPqEventsReady());
    });
}

NYdb::NTopic::TTopicClientSettings TTopicSession::GetTopicClientSettings() const {
    NYdb::NTopic::TTopicClientSettings opts;
    opts.Database(SourceParams.GetDatabase())
        .DiscoveryEndpoint(SourceParams.GetEndpoint())
        .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
        .CredentialsProviderFactory(CredentialsProviderFactory);

    return opts;
}

NYdb::NTopic::TTopicClient& TTopicSession::GetTopicClient() {
    if (!TopicClient) {
        TopicClient = std::make_unique<NYdb::NTopic::TTopicClient>(Driver, GetTopicClientSettings());
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

NYdb::NTopic::TReadSessionSettings TTopicSession::GetReadSessionSettings() const {
    NYdb::NTopic::TTopicReadSettings topicReadSettings;
    topicReadSettings.Path(SourceParams.GetTopicPath());
    topicReadSettings.AppendPartitionIds(PartitionId);

    TInstant minTime = GetMinStartingMessageTimestamp();
    LOG_ROW_DISPATCHER_DEBUG("StartingMessageTimestamp " << minTime);
    return NYdb::NTopic::TReadSessionSettings()
        .AppendTopics(topicReadSettings)
        .WithoutConsumer()
        .MaxMemoryUsageBytes(BufferSize)
        .ReadFromTimestamp(minTime);
}

NYdb::NTopic::IReadSession& TTopicSession::GetReadSession() {
    if (!ReadSession) {
        ReadSession = GetTopicClient().CreateReadSession(GetReadSessionSettings());
        LOG_ROW_DISPATCHER_DEBUG("CreateReadSession");
    }
    return *ReadSession;
}

void TTopicSession::Handle(TEvPrivate::TEvPqEventsReady::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvPqEventsReady");
    HandleNewEvents();
    SubscribeOnNextEvent();
    ParseData();
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
      //  ui32 batchItemsEstimatedCount = 0;

        if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            issues = ProcessDataReceivedEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
            issues = ProcessSessionClosedEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            issues = ProcessStartPartitionSessionEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            issues = ProcessStopPartitionSessionEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {

        }

         // if NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent
        // auto issues = std::visit(TTopicEventProcessor{*this, batchItemsEstimatedCount, LogPrefix}, event);
        if (issues) {
            CloseSession();
            // Callbacks->OnAsyncOutputError(OutputIndex, *issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            break;
        }
    }
}

void TTopicSession::CloseSession() {
    LOG_ROW_DISPATCHER_DEBUG("CloseSession");


    if (!ReadSession) {
        LOG_ROW_DISPATCHER_DEBUG("CloseSession ?");

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
        // LWPROBE(PqReadDataReceived, TString(TStringBuilder() << Self.TxId), Self.SourceParams.GetTopicPath(), data);
        LOG_ROW_DISPATCHER_DEBUG("Data received: " << message.DebugString(true));

        // if (message.GetWriteTime() < StartingMessageTimestamp) {
        //     LOG_ROW_DISPATCHER_DEBUG("Skip data. StartingMessageTimestamp: " << StartingMessageTimestamp << ". Write time: " << message.GetWriteTime());
        //     continue;
        // } // TODOt

        const TString& item = message.GetData();
        
        size_t size = item.size();

     //   auto [item, size] = CreateItem(message);

        auto& curBatch = GetActiveBatch(/*partitionKey, message.GetWriteTime()*/);
        curBatch.Data.emplace_back(std::make_pair(message.GetOffset(), std::move(item)));
        curBatch.UsedSpace += size;
      //  curBatch.LastOffset = message.GetOffset() + 1;
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

// std::optional<NYql::TIssues> operator()(NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent& /*event*/) {
//     LOG_ROW_DISPATCHER_DEBUG("SessionId: " << Self.GetSessionId() << " EndPartitionSessionEvent received");
//     return std::nullopt;
// }

// std::optional<NYql::TIssues> operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent&) {
//     return std::nullopt;
// }

// std::optional<NYql::TIssues> operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& /*event*/) {
//     LOG_ROW_DISPATCHER_DEBUG("SessionId: " << Self.GetSessionId()  << " PartitionSessionClosedEvent received");
//     return std::nullopt;
// }

TTopicSession::TReadyBatch& TTopicSession::GetActiveBatch(/*const TPartitionKey& partitionKey, TInstant time*/) {
    if (Y_UNLIKELY(ReadyBuffer.empty() || ReadyBuffer.back().Watermark.Defined())) {
        ReadyBuffer.emplace(Nothing(), BatchCapacity);
    }

    TReadyBatch& activeBatch = ReadyBuffer.back();
     return activeBatch;

    // if (!Self.WatermarkTracker) {
    //     // Watermark tracker disabled => there is no way more than one batch will be used
    //     return activeBatch;
    // }

//     const auto maybeNewWatermark = WatermarkTracker->NotifyNewPartitionTime(
//         partitionKey,
//         time,
//         TInstant::Now());
//     if (!maybeNewWatermark) {
//         // Watermark wasn't moved => use current active batch
//         return activeBatch;
//     }

//   //  PushWatermarkToReady(*maybeNewWatermark);
//     return ReadyBuffer.emplace(Nothing(), BatchCapacity); // And open new batch
}

std::pair<NYql::NUdf::TUnboxedValuePod, i64> TTopicSession::CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    const TString& data = message.GetData();

    i64 usedSpace = 0;
    NYql::NUdf::TUnboxedValuePod item;
    if (MetadataFields.empty()) {
        item = NKikimr::NMiniKQL::MakeString(NYql::NUdf::TStringRef(data.Data(), data.Size()));
        usedSpace += data.Size();
    } else {
        // NYql::NUdf::TUnboxedValue* itemPtr;

        // NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Eval");
        // NKikimr::NMiniKQL::THolderFactory holderFactory(Alloc.Ref(), memInfo);

        // item = holderFactory.CreateDirectArrayHolder(MetadataFields.size() + 1, itemPtr);
        // *(itemPtr++) = NKikimr::NMiniKQL::MakeString(NYql::NUdf::TStringRef(data.Data(), data.Size()));
        // usedSpace += data.Size();

        // for (const auto& [name, extractor] : MetadataFields) {
        //     auto [ub, size] = extractor(message);
        //     *(itemPtr++) = std::move(ub);
        //     usedSpace += size;
        // }
    }

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
    LOG_ROW_DISPATCHER_DEBUG("DataParsed, offset " << offset);
    
    for (auto v: value) {
        LOG_ROW_DISPATCHER_DEBUG("v " << v);
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
    if (info.Buffer.empty()) {
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
    LOG_ROW_DISPATCHER_DEBUG("SendData to ");
    Send(RowDispatcherActorId, event.release());
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvAddConsumer::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSessionAddConsumer");
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
        predicate = FormatWhere(ev->Get()->Record.GetSource().GetPredicate());
        LOG_ROW_DISPATCHER_DEBUG("predicate " << predicate);

        consumerInfo.Filter = NewJsonFilter(
            GetVector(ev->Get()->Record.GetSource().GetColumns()),
            GetVector(ev->Get()->Record.GetSource().GetColumnTypes()),
            predicate,
            [&, actorId = consumerInfo.ReadActorId](ui64 offset, const TString& json){
                auto it = Consumers.find(actorId);
                if (it == Consumers.end()) {
                    LOG_ROW_DISPATCHER_DEBUG("Wrong consumer"); // TODO
                    return;
                }
                auto& consumerInfo = it->second;
                consumerInfo.Buffer.push(std::make_pair(offset, json));
                LOG_ROW_DISPATCHER_DEBUG("JsonFilter data: " << json);
                
                SendDataArrived();

            });

        LOG_ROW_DISPATCHER_DEBUG("Consumers size " << Consumers.size());
        GetReadSession();
        SubscribeOnNextEvent();
    } catch (NYql::NPureCalc::TCompileError& e) {
        LOG_ROW_DISPATCHER_DEBUG("CompileError: sql: " << e.GetYql() << ", error: " << e.GetIssues());
        throw;
    } catch (const yexception &ex) {
        LOG_ROW_DISPATCHER_DEBUG("FormatWhere failed: "  << ex.what());
    }
}

void TTopicSession::Handle(TEvRowDispatcher::TEvSessionDeleteConsumer::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvSessionDeleteConsumer: " << ev->Get()->ReadActorId);
    // TODO
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

    if (Consumers.empty()) {
        // LOG_ROW_DISPATCHER_DEBUG("No consumer, delete this session");
        // TActorBootstrapped<TTopicSession>::PassAway();
    }
}

void TTopicSession::InitParser() {
    try {
        Parser = NewJsonParser(
            "",
            GetVector(SourceParams.GetColumns()),
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
        if (info.Buffer.empty()) {
            continue;
        }
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
    NActors::TActorId rowDispatcherActorId,
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    return std::unique_ptr<NActors::IActor>(new TTopicSession(rowDispatcherActorId, sourceParams, partitionId, driver, credentialsProviderFactory));
}

} // namespace NFq
