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

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPqEventsReady = EvBegin + 10,
        EvCreateSession,
        EvDataParsed,
        EvDataAfterFilteration,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvPqEventsReady : public NActors::TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};
    struct TEvCreateSession : public NActors::TEventLocal<TEvCreateSession, EvCreateSession> {};
    struct TEvDataParsed : public NActors::TEventLocal<TEvDataParsed, EvDataParsed> {
        TEvDataParsed(ui64 offset, TList<TString>&& value) 
            : Offset(offset)
            , Value(value)
        {}
        ui64 Offset = 0; 
        TList<TString> Value;
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


class TTopicSession : public TActorBootstrapped<TTopicSession> {


private:
    NActors::TActorId RowDispatcherActorId;
    ui32 PartitionId;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    const i64 BufferSize;
    TString LogPrefix;
    NYql::NDq::TDqAsyncStats IngressStats;
    ui64 LastMessageOffset = 0;
    bool IsWaitingEvents = false;

    struct ClientsInfo {
        ClientsInfo(const NFq::TEvRowDispatcher::TEvStartSession::TPtr& ev, TActorId /*selfId*/)
            : Settings(ev->Get()->Record)
            , ReadActorId(ev->Sender)
        {
            if (Settings.HasOffset()) {
                NextMessageOffset = Settings.GetOffset();
            }
        }
        NFq::NRowDispatcherProto::TEvStartSession Settings;
        NActors::TActorId ReadActorId;
        std::unique_ptr<TJsonFilter> Filter;
        ui64 EventQueueId = 0;
        TQueue<std::pair<ui64, TString>> Buffer;
        ui64 UsedSize = 0;
        bool DataArrivedSent = false;
        TMaybe<ui64> NextMessageOffset;
    };
    TMap<NActors::TActorId, ClientsInfo> Clients;
    std::unique_ptr<TJsonParser> Parser;
    NConfig::TRowDispatcherConfig Config;
    ui64 UsedSize = 0;

public:
    explicit TTopicSession(
        const NConfig::TRowDispatcherConfig& config,
        NActors::TActorId rowDispatcherActorId,
        ui32 partitionId,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    void Bootstrap();
    static TVector<TString> GetVector(const google::protobuf::RepeatedPtrField<TString>& value);
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const;
    NYdb::NTopic::TTopicClient& GetTopicClient(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams);
    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) const;

    void CreateTopicSession();
    void CloseTopicSession();
    void SubscribeOnNextEvent();

    void SendToParsing(ui64 offset, const TString& message);
    void SendData(ClientsInfo& info);
    void InitParser(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams);
    void FatalError(const TString& message, const std::unique_ptr<TJsonFilter>* filter = nullptr);
    void SendDataArrived();
    void StopReadSession();

    TString GetSessionId() const;
    void HandleNewEvents();
    void PassAway() override;

    TInstant GetMinStartingMessageTimestamp() const;
    void AddDataToClient(ClientsInfo& client, ui64 offset, const TString& json);

    std::optional<NYql::TIssues> ProcessDataReceivedEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event);
    std::optional<NYql::TIssues> ProcessSessionClosedEvent(NYdb::NTopic::TSessionClosedEvent& ev);
    std::optional<NYql::TIssues> ProcessStartPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event);
    std::optional<NYql::TIssues> ProcessStopPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event);
    std::pair<NYql::NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    void Handle(NFq::TEvPrivate::TEvPqEventsReady::TPtr&);
    void Handle(NFq::TEvPrivate::TEvCreateSession::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataParsed::TPtr&);
    void Handle(NFq::TEvPrivate::TEvDataAfterFilteration::TPtr&);
    void Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr&);
    void Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSession::TPtr &ev);
    void HandleException(const std::exception& err);

    void PrintInternalState();

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER_SESSION";

private:

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(NFq::TEvPrivate::TEvPqEventsReady, Handle);
        hFunc(NFq::TEvPrivate::TEvCreateSession, Handle);
        hFunc(NFq::TEvPrivate::TEvDataParsed, Handle);
        hFunc(NFq::TEvPrivate::TEvDataAfterFilteration, Handle);
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
        IgnoreFunc(NFq::TEvPrivate::TEvDataAfterFilteration);
        IgnoreFunc(TEvRowDispatcher::TEvGetNextBatch);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStartSession);
        IgnoreFunc(NFq::TEvRowDispatcher::TEvStopSession);
    })
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
    LOG_ROW_DISPATCHER_DEBUG("Timeout " << Config.GetTimeoutBeforeStartSessionSec() << " sec");
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

    if (UsedSize > Config.GetMaxSessionUsedMemory()) {
        LOG_ROW_DISPATCHER_DEBUG("Too much used memory, skip subscribing to WaitEvent()");
        return;
    }

    LOG_ROW_DISPATCHER_DEBUG("SubscribeOnNextEvent");
    IsWaitingEvents = true;
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

NYdb::NTopic::TTopicClient& TTopicSession::GetTopicClient(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) {
    if (!TopicClient) {
        TopicClient = std::make_unique<NYdb::NTopic::TTopicClient>(Driver, GetTopicClientSettings(sourceParams));
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
    topicReadSettings.Path(sourceParams.GetTopicPath());
    topicReadSettings.AppendPartitionIds(PartitionId);

    TInstant minTime = GetMinStartingMessageTimestamp();
    LOG_ROW_DISPATCHER_INFO("Create topic session, Path " << sourceParams.GetTopicPath() << ", StartingMessageTimestamp " << minTime);
    return NYdb::NTopic::TReadSessionSettings()
        .AppendTopics(topicReadSettings)
        .WithoutConsumer()
        .MaxMemoryUsageBytes(BufferSize)
        .ReadFromTimestamp(minTime);
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
    LOG_ROW_DISPATCHER_DEBUG("TEvPqEventsReady");
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
            info.Filter->Push(ev->Get()->Offset, ev->Get()->Value);
        } catch (const yexception& e) {
            FatalError(e.what(), &info.Filter);
        }
    }
}

void TTopicSession::Handle(NFq::TEvPrivate::TEvDataAfterFilteration::TPtr& ev) {
    auto it = Clients.find(ev->Get()->ReadActorId);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_ERROR("Skip DataAfterFilteration, wrong read actor, id " << ev->Get()->ReadActorId.ToString());
        return;
    }
    AddDataToClient(it->second, ev->Get()->Offset, ev->Get()->Json);
    LOG_ROW_DISPATCHER_TRACE("JsonFilter data [" << ev->Get()->ReadActorId.ToString() << "] " << ev->Get()->Json);
    SendDataArrived();
}

void TTopicSession::Handle(TEvRowDispatcher::TEvGetNextBatch::TPtr& ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvGetNextBatch from " << ev->Sender.ToString());
    auto it = Clients.find(ev->Sender);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong ClientSettings"); // TODO
        return;
    }
    SendData(it->second);
    SubscribeOnNextEvent();
}

void TTopicSession::HandleNewEvents() {
    while (true) {
        if (!ReadSession) {
            return;
        }
        if (UsedSize > Config.GetMaxSessionUsedMemory()) {
            LOG_ROW_DISPATCHER_TRACE("Too much used memory, stop reading from yds");
            break;
        }
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
    for (const auto& message : event.GetMessages()) {
        const TString& data = message.GetData();
        IngressStats.Bytes += data.size();
        LOG_ROW_DISPATCHER_TRACE("Data received: " << message.DebugString(true));

        const TString& item = message.GetData();

        SendToParsing(message.GetOffset(), item);
        LastMessageOffset = message.GetOffset();
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
    for (const auto& [actorId, info] : Clients) {
         if (!minOffset
            || (info.NextMessageOffset && (info.NextMessageOffset < *minOffset))) {
                minOffset = info.NextMessageOffset;
            } 
    }
    LOG_ROW_DISPATCHER_DEBUG("Confirm StartPartitionSession with offset " << minOffset);

    event.Confirm(minOffset);
    return std::nullopt;
}

std::optional<NYql::TIssues> TTopicSession::ProcessStopPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event) {
    LOG_ROW_DISPATCHER_DEBUG("SessionId: " << GetSessionId() << " StopPartitionSessionEvent received");
    event.Confirm();
    return std::nullopt;
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

void TTopicSession::SendToParsing(ui64 offset, const TString& message) {
    LOG_ROW_DISPATCHER_DEBUG("ParseData: " );
    try {
        Parser->Push(offset, message);
    } catch (const yexception& e) {
        FatalError(e.what());
    } 
}

void TTopicSession::SendData(ClientsInfo& info) {
    info.DataArrivedSent = false;
    if (info.Buffer.empty()) {
        LOG_ROW_DISPATCHER_TRACE("Buffer empty");
    }

    auto event = std::make_unique<TEvRowDispatcher::TEvMessageBatch>();
    event->Record.SetPartitionId(PartitionId);    
    event->ReadActorId = info.ReadActorId;
    
    while (!info.Buffer.empty()) {
        const auto& [offset, json] = info.Buffer.front();
        info.UsedSize -= json.size();
        UsedSize -= json.size();
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
    auto it = Clients.find(ev->Sender);
    if (it != Clients.end()) {
        FatalError("Internal error: sender " + ev->Sender.ToString());
        return;
    }

    LOG_ROW_DISPATCHER_INFO("New client, read actor id " << ev->Sender.ToString());

    try {
        auto& clientInfo = (Clients.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ev->Sender), 
            std::forward_as_tuple(ev, SelfId())).first)->second;

        TString predicate = clientInfo.Settings.GetSource().GetPredicate();

        clientInfo.Filter = NewJsonFilter(
            GetVector(ev->Get()->Record.GetSource().GetColumns()),
            GetVector(ev->Get()->Record.GetSource().GetColumnTypes()),
            predicate,
            [&, actorId = clientInfo.ReadActorId](ui64 offset, const TString& json){
                TString LogPrefix{"TopicSession"};
                LOG_ROW_DISPATCHER_TRACE("Send");
                Send(SelfId(), new NFq::TEvPrivate::TEvDataAfterFilteration(offset, json, actorId));
            });
            
        LOG_ROW_DISPATCHER_INFO("New client: offset " << clientInfo.NextMessageOffset << ", predicate: " << clientInfo.Settings.GetSource().GetPredicate());

        if (ReadSession) {
            if (clientInfo.Settings.HasOffset() && (clientInfo.Settings.GetOffset() <= LastMessageOffset)) {
                LOG_ROW_DISPATCHER_INFO("New client has less offset than the last message, stop (restart) topic session");
                StopReadSession();
            }
        }
    } catch (NYql::NPureCalc::TCompileError& e) {
        FatalError("Adding new client failed: CompileError: sql: " + e.GetYql() + ", error: " + e.GetIssues());
    } catch (const yexception &ex) {
        FatalError(TString{"Adding new client failed: "} + ex.what());
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
}

void TTopicSession::Handle(NFq::TEvRowDispatcher::TEvStopSession::TPtr &ev) {
    LOG_ROW_DISPATCHER_DEBUG("TEvStopSession, topicPath " << ev->Get()->Record.GetSource().GetTopicPath() <<
        " partitionId " << ev->Get()->Record.GetPartitionId());

    auto it = Clients.find(ev->Sender);
    if (it == Clients.end()) {
        LOG_ROW_DISPATCHER_DEBUG("Wrong ClientSettings"); // TODO
        return;
    }

    Clients.erase(it);
}

void TTopicSession::InitParser(const NYql::NPq::NProto::TDqPqTopicSource& sourceParams) {
    if (Parser) {
        return;
    }
    try {
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        Parser = NewJsonParser(
            "",
            GetVector(sourceParams.GetColumns()),
            [actorSystem, selfId = SelfId()](ui64 offset, TList<TString>&& value){
                actorSystem->Send(selfId, new NFq::TEvPrivate::TEvDataParsed(offset, std::move(value)));
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
    LOG_ROW_DISPATCHER_ERROR("FatalError: " << str.Str());

    for (auto& [readActorId, info] : Clients) {
        LOG_ROW_DISPATCHER_DEBUG("Send TEvSessionError to " << readActorId);
        auto event = std::make_unique<TEvRowDispatcher::TEvSessionError>();
        event->Record.SetMessage(str.Str());
        event->Record.SetPartitionId(PartitionId);
        event->ReadActorId = readActorId;
        Send(RowDispatcherActorId, event.release());
    }
    StopReadSession();
    Become(&TTopicSession::ErrorState);
    PassAway();
}

void TTopicSession::StopReadSession() {
    if (ReadSession) {
        LOG_ROW_DISPATCHER_DEBUG("Close read session");
        ReadSession->Close(TDuration::Zero());
        ReadSession.reset();
    }
    TopicClient.reset();
}

void TTopicSession::SendDataArrived() {
    for (auto& [readActorId, info] : Clients) {
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

void TTopicSession::HandleException(const std::exception& e) {
    FatalError(TString("Internal error: exception: ") + e.what());
}


void TTopicSession::PrintInternalState() {
    TStringStream str;
    str << "Clients:\n";
    str << "UsedSize: " << UsedSize << "\n";
    for (auto& [readActorId, info] : Clients) {
        str << "    read actor id " << readActorId << ", queue id " << info.EventQueueId 
            << ", buffer size " << info.Buffer.size() << ", used size: " << info.UsedSize
            << ", data arrived sent " << info.DataArrivedSent << "\n";
    }
    LOG_ROW_DISPATCHER_DEBUG(str.Str());
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
