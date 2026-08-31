#include "write_session_logic_actor.h"
#include "actors.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/wilson_tracing_control.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/dataplane/dataplane.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/persqueue/constants.h>
#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/lib/base/msgbus.h>

#include <util/generic/algorithm.h>
#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/string/vector.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_WRITE_PROXY

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr::NPQ::NDataplane::NWrite {

namespace {

static const TDuration SOURCEID_UPDATE_PERIOD = TDuration::Hours(1);
static const ui64 WRITE_BLOCK_SIZE = 4_KB;
static constexpr ui64 MAX_METADATA_SIZE_PER_MESSAGE = 4096;
static constexpr auto PARTITION_KEY_META_KEY = "__partition_key";

bool InternalErrorCode(Ydb::PersQueue::ErrorCode::ErrorCode errorCode) {
    switch (errorCode) {
        case Ydb::PersQueue::ErrorCode::ERROR:
        case Ydb::PersQueue::ErrorCode::INITIALIZING:
        case Ydb::PersQueue::ErrorCode::OVERLOAD:
        case Ydb::PersQueue::ErrorCode::WRITE_ERROR_DISK_IS_FULL:
        case Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED:
            return true;
        default:
            return false;
    }
}

Ydb::PersQueue::ErrorCode::ErrorCode ConvertOldCode(NPersQueue::NErrorCode::EErrorCode code) {
    if (code == NPersQueue::NErrorCode::OK) {
        return Ydb::PersQueue::ErrorCode::OK;
    }
    return Ydb::PersQueue::ErrorCode::ErrorCode(code + 500000);
}

TString CleanupCounterValueString(const TString& value) {
    TString clean;
    constexpr auto valueLenghtLimit = 200;
    for (auto c : value) {
        switch (c) {
        case '|':
        case '*':
        case '?':
        case '"':
        case '\'':
        case '`':
        case '\\':
            continue;
        default:
            clean.push_back(c);
            if (clean.size() == valueLenghtLimit) {
                break;
            }
        }
    }
    return clean;
}

TString DropUserAgentSuffix(const TString& userAgent) {
    auto ua = TStringBuf(userAgent);
    TStringBuf beforeParen, afterParen;
    ua.Split('(', beforeParen, afterParen);
    while (beforeParen.ends_with(' ')) {
        beforeParen.Chop(1);
    }
    return TString(beforeParen);
}

bool ValidateWriteWithCodec(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const ui32 codecID, TString& error) {
    error.clear();
    if (pqTabletConfig.has_codecs()) {
        const auto& ids = pqTabletConfig.codecs().ids();
        if (!ids.empty() && Find(ids, codecID) == ids.end()) {
            const auto& names = pqTabletConfig.codecs().codecs();
            AFL_ENSURE(ids.size() == names.size())("reason", "PQ tablet supported codecs configuration is invalid");
            TStringBuilder errorBuilder;
            errorBuilder << "given codec (id " << static_cast<i32>(codecID) << ") is not configured for the topic. Configured codecs are " << names[0] << " (id " << ids[0] << ")";
            for (i32 i = 1; i != ids.size(); ++i) {
                errorBuilder << ", " << names[i] << " (id " << ids[i] << ")";
            }
            error = errorBuilder;
            return false;
        }
    }
    return true;
}

// PQ error codes stay as in the old write session. Do not use NDescriber::Convert():
// that maps NOT_FOUND to Ydb NOT_FOUND, while UNKNOWN_TOPIC still becomes SCHEME_ERROR.
Ydb::PersQueue::ErrorCode::ErrorCode ErrorCodeFromDescriberStatus(NDescriber::EStatus status) {
    switch (status) {
        case NDescriber::EStatus::SUCCESS:
            return Ydb::PersQueue::ErrorCode::OK;
        case NDescriber::EStatus::NOT_FOUND:
            // Was UNKNOWN_TOPIC, Marker# PQ15 / PQ193 (missing topic / no balancer)
            return Ydb::PersQueue::ErrorCode::UNKNOWN_TOPIC;
        case NDescriber::EStatus::NOT_TOPIC:
            // Was UNKNOWN_TOPIC, Marker# PQ13
            return Ydb::PersQueue::ErrorCode::UNKNOWN_TOPIC;
        case NDescriber::EStatus::UNAUTHORIZED:
            // Was UNKNOWN_TOPIC, Marker# PQ15: scheme cache hid the path (PathErrorUnknown)
            return Ydb::PersQueue::ErrorCode::UNKNOWN_TOPIC;
        case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
            // Was ACCESS_DENIED, Marker# PQ1125 (DescribeSchema ok, no WriteTopic)
            return Ydb::PersQueue::ErrorCode::ACCESS_DENIED;
        case NDescriber::EStatus::BAD_REQUEST:
            // Was BAD_REQUEST, Marker# PQ14
            return Ydb::PersQueue::ErrorCode::BAD_REQUEST;
        case NDescriber::EStatus::UNKNOWN_ERROR:
            // Was ERROR, Marker# PQ1 / PQ99
            return Ydb::PersQueue::ErrorCode::ERROR;
    }
    Y_ABORT("unexpected describer status");
}

void CloseSpan(NWilson::TSpan& span, const TString& errorReason, Ydb::PersQueue::ErrorCode::ErrorCode errorCode) {
    if (span) {
        if (errorCode == Ydb::PersQueue::ErrorCode::OK) {
            span.EndOk();
        } else {
            span.EndError(errorReason);
        }
        span = {};
    }
}

NKikimrPQClient::TDataChunk BuildInitMeta(
    const THashMap<TString, TString>& sessionMeta,
    const TString& topic,
    const TString& peerName)
{
    NKikimrPQClient::TDataChunk data;
    TString server;
    TString ident;
    TString logType;
    TString file;

    for (const auto& [key, value] : sessionMeta) {
        if (key == "server") {
            server = value;
        } else if (key == "ident") {
            ident = value;
        } else if (key == "logtype") {
            logType = value;
        } else if (key == "file") {
            file = value;
        } else if (key == NPersQueue::WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX) {
            continue;
        } else {
            auto* item = data.MutableExtraFields()->AddItems();
            item->SetKey(key);
            item->SetValue(value);
        }
    }

    if (server.empty()) {
        server = peerName;
    }
    if (ident.empty()) {
        auto p = SplitString(topic, "--");
        ident = (p.size() == 3) ? p[1] : TString("unknown");
    }
    if (logType.empty()) {
        auto p = SplitString(topic, "--");
        logType = (p.size() == 3) ? p.back() : TString("unknown");
    }

    data.MutableMeta()->SetServer(server);
    data.MutableMeta()->SetIdent(ident);
    data.MutableMeta()->SetLogType(logType);
    if (!file.empty()) {
        data.MutableMeta()->SetFile(file);
    }
    data.SetIp(peerName);
    return data;
}

TString SerializeMessageData(const NKikimrPQClient::TDataChunk& init, const TWriteSessionMessage& msg) {
    NKikimrPQClient::TDataChunk proto;
    proto.CopyFrom(init);
    proto.SetSeqNo(msg.SeqNo);
    proto.SetCreateTime(msg.CreateTimeMs);
    if (msg.ChunkCodec) {
        proto.SetCodec(*msg.ChunkCodec);
    }
    proto.SetData(msg.Data);
    for (const auto& [key, value] : msg.Metadata) {
        auto* item = proto.AddMessageMeta();
        item->set_key(key);
        item->set_value(value);
    }

    TString str;
    AFL_ENSURE(proto.SerializeToString(&str));
    return str;
}

TWriteSessionAck MakeAck(const TPersQueuePartitionResponse::TCmdWriteResult& res) {
    TWriteSessionAck ack;
    ack.SeqNo = res.GetSeqNo();
    ack.Offset = res.GetOffset();
    ack.AlreadyWritten = res.GetAlreadyWritten();
    ack.WrittenInTx = res.HasWrittenInTx() && res.GetWrittenInTx();
    ack.TotalTimeInPartitionQueueMs = res.GetTotalTimeInPartitionQueueMs();
    ack.PartitionQuotedTimeMs = res.GetPartitionQuotedTimeMs();
    ack.TopicQuotedTimeMs = res.GetTopicQuotedTimeMs();
    ack.WriteTimeMs = res.GetWriteTimeMs();
    return ack;
}

} // namespace

TWriteSessionLogicActor::TWriteSessionLogicActor(TWriteSessionSettings settings)
    : TBase(NKikimrServices::PQ_WRITE_PROXY)
    , TRlHelpers({}, std::move(settings.RlContext), WRITE_BLOCK_SIZE, false)
    , Owner(settings.Owner)
    , Protocol(std::move(settings.Protocol))
    , UserAgent(std::move(settings.UserAgent))
    , SdkBuildInfo(std::move(settings.SdkBuildInfo))
    , DatabaseName(std::move(settings.DatabaseName))
    , SerializedToken(std::move(settings.SerializedToken))
    , TraceId(std::move(settings.TraceId))
    , RequestType(std::move(settings.RequestType))
    , WilsonTraceId(std::move(settings.WilsonTraceId))
    , State(ES_CREATED)
    , Cookie(settings.Cookie)
    , TopicsController(std::move(settings.TopicsController))
    , Partition(0)
    , PreferedPartition(Max<ui32>())
    , WritesDone(false)
    , Counters(std::move(settings.Counters))
    , BytesInflight_(0)
    , BytesInflightTotal_(0)
    , NextRequestInited(false)
    , NextRequestCookie(0)
    , Token(nullptr)
    , Auth(settings.YdbToken.value_or(TString()))
    , UpdateTokenInProgress(false)
    , UpdateTokenAuthenticated(false)
    , ACLCheckInProgress(true)
    , FirstACLCheck(true)
    , RequestNotChecked(false)
    , LastACLCheckTimestamp(TInstant::Zero())
    , LogSessionDeadline(TInstant::Zero())
    , ClientDC(settings.ClientDC ? *settings.ClientDC : "other")
    , LastSourceIdUpdate(TInstant::Zero())
{
}

void TWriteSessionLogicActor::Bootstrap() {
    Span = NWilson::TSpan(
        TWilsonTopic::TopicTopLevel,
        std::move(WilsonTraceId),
        Protocol.SessionSpanName);
    Become(&TThis::StateFunc);
    StartTime = TActivationContext::Now();
}

STFUNC(TWriteSessionLogicActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvPoison, Handle);

        hFunc(TEvInit, Handle);
        hFunc(TEvWrite, Handle);
        hFunc(TEvUpdateToken, Handle);
        hFunc(TEvTokenRefreshed, Handle);
        hFunc(TEvClientDone, Handle);
        hFunc(TEvDieCommand, Handle);
        hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);

        hFunc(TEvPartitionWriter::TEvInitResult, Handle);
        hFunc(TEvPartitionWriter::TEvWriteAccepted, Handle);
        hFunc(TEvPartitionWriter::TEvWriteResponse, Handle);
        hFunc(TEvPartitionWriter::TEvDisconnected, Handle);

        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);

        hFunc(TEvPartitionChooser::TEvChooseResult, Handle);
        hFunc(TEvPartitionChooser::TEvChooseError, Handle);
    default:
        break;
    }
}

TString TWriteSessionLogicActor::BuildLogPrefix() const {
    return TStringBuilder() << " (Cookie=" << Cookie << ", SessionId=" << OwnerCookie << ") ";
}

void TWriteSessionLogicActor::PassAway() {
    if (State == ES_DYING) {
        YDB_LOG_INFO("Session v1 is already DEAD",
            {"cookie", Cookie},
            {"sessionId", OwnerCookie});
        return;
    }

    if (SessionsActive) {
        SessionsActive.Dec();
        if (BytesInflight && BytesInflightTotal) {
            BytesInflight.Dec(BytesInflight_);
            BytesInflightTotal.Dec(BytesInflightTotal_);
        }
    }

    YDB_LOG_INFO("Session v1 is DEAD",
        {"cookie", Cookie},
        {"sessionId", OwnerCookie});

    DestroyPartitionWriterCache();
    if (PartitionChooser) {
        Send(PartitionChooser, new TEvents::TEvPoison());
    }
    if (Describer) {
        Send(Describer, new TEvents::TEvPoison());
        Describer = {};
    }

    State = ES_DYING;
    TRlHelpers::PassAway(SelfId());
    TBase::PassAway();
}

void TWriteSessionLogicActor::OnException(const std::exception&) {
    // Do not PassAway here: TBaseActor::OnUnhandledException will PassAway.
    CompleteSession("Internal error", Ydb::PersQueue::ErrorCode::ERROR);
}

void TWriteSessionLogicActor::Handle(TEvents::TEvPoison::TPtr&) {
    if (!SessionClosed) {
        CloseSpans("Done", Ydb::PersQueue::ErrorCode::OK);
    }
    PassAway();
}

void TWriteSessionLogicActor::CheckFinish() {
    if (!WritesDone) {
        return;
    }
    if (State != ES_INITED) {
        CloseSession("out of order Writes done before initialization", Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }
    if (PendingRequests.empty() && !PendingQuotaRequest && SentRequests.empty() && AcceptedRequests.empty()) {
        CloseSession("", Ydb::PersQueue::ErrorCode::OK);
    }
}

void TWriteSessionLogicActor::Handle(TEvClientDone::TPtr&) {
    WritesDone = true;
    CheckFinish();
}

void TWriteSessionLogicActor::Handle(TEvDieCommand::TPtr& ev) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ev->Get()->StatusOverride);
}

void TWriteSessionLogicActor::OnWriteAccessGranted() {
    ACLCheckInProgress = false;
    if (FirstACLCheck) {
        FirstACLCheck = false;
        DiscoverPartition();
    }
    if (UpdateTokenInProgress && UpdateTokenAuthenticated) {
        UpdateTokenInProgress = false;
        Send(Owner, new TEvUpdateTokenAck());
    }
}

void TWriteSessionLogicActor::Handle(TEvInit::TPtr& ev) {
    InitSpan = GenerateInitSpan();
    auto& init = *ev->Get();

    if (State != ES_CREATED) {
        CloseSession("got second init request", Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }

    TopicPath = std::move(init.TopicPath);
    if (TopicPath.empty()) {
        CloseSession("no topic in init request", Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }

    DiscoveryConverter = TopicsController.GetWriteTopicConverter(TopicPath, DatabaseName.value_or("/Root"));
    if (!DiscoveryConverter->IsValid()) {
        CloseSession(
            TStringBuilder() << "topic " << TopicPath << " could not be recognized: " << DiscoveryConverter->GetReason(),
            Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }

    PeerName = std::move(init.PeerName);
    SourceId = std::move(init.SourceId);
    UseDeduplication = init.UseDeduplication;
    PreferedPartition = init.PreferedPartition;
    ExpectedGeneration = init.ExpectedGeneration;
    SessionMeta = std::move(init.SessionMeta);
    TrackProducerId = init.TrackProducerId;

    YDB_LOG_INFO("Session request",
        {"cookie", Cookie},
        {"topicPath", TopicPath},
        {"sourceId", SourceId},
        {"peerName", PeerName});
    if (!UseDeduplication) {
        YDB_LOG_DEBUG("Session request Disable deduplication for empty producer id",
            {"cookie", Cookie});
    }

    if (SerializedToken.empty()) {
        if (AppData()->EnforceUserTokenRequirement || AppData()->PQConfig.GetRequireCredentialsInNewProtocol()) {
            Send(Owner, new TEvUnauthenticated(
                "Unauthenticated access is forbidden, please provide credentials"));
            PassAway();
            return;
        }
    } else {
        Token = new NACLib::TUserToken(SerializedToken);
    }

    InitCheckSchema(true, InitSpan.GetTraceId());
}

bool TWriteSessionLogicActor::InitAfterDiscovery() {
    if (SourceId.empty() && UseDeduplication) {
        CloseSession("Internal server error: got empty SourceId with enabled deduplication", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        return false;
    }

    InitMeta = BuildInitMeta(SessionMeta, FullConverter->GetClientsideName(), PeerName);

    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", FullConverter->GetAccount()}}, {"total"}}};

    SLITotal = TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
    SLIErrors = TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);
    SLITotal.Inc();
    return true;
}

void TWriteSessionLogicActor::SetupBytesWrittenByUserAgentCounter(const TString& topicPath) {
    BytesWrittenByUserAgent = GetServiceCounters(Counters, "pqproxy|userAgents", false)
        ->GetSubgroup("host", "")
        ->GetSubgroup("protocol", Protocol.CounterName)
        ->GetSubgroup("topic", topicPath)
        ->GetSubgroup("sdk_build_info", CleanupCounterValueString(SdkBuildInfo))
        ->GetSubgroup("user_agent", DropUserAgentSuffix(CleanupCounterValueString(UserAgent)))
        ->GetExpiringNamedCounter("sensor", "BytesWrittenByUserAgent", true);
}

void TWriteSessionLogicActor::SetupCounters() {
    if (SessionsCreated) {
        return;
    }

    auto subGroup = GetServiceCounters(Counters, "pqproxy|writeSession");
    auto aggr = NPersQueue::GetLabels(FullConverter);

    BytesInflight = TMultiCounter(subGroup, aggr, {}, {"BytesInflight"}, false);
    BytesInflightTotal = TMultiCounter(subGroup, aggr, {}, {"BytesInflightTotal"}, false);
    SessionsCreated = TMultiCounter(subGroup, aggr, {}, {"SessionsCreated"}, true);
    SessionsActive = TMultiCounter(subGroup, aggr, {}, {"SessionsActive"}, false);
    Errors = TMultiCounter(subGroup, aggr, {}, {"Errors"}, true);

    CodecCounters.push_back(TMultiCounter(subGroup, aggr, {{"codec", "user"}}, {"MessagesWrittenByCodec"}, true));

    auto allNames = GetEnumAllCppNames<Ydb::Topic::Codec>();
    allNames.erase(allNames.begin());
    allNames.pop_back();
    allNames.pop_back();
    for (auto& name : allNames) {
        auto nm = to_lower(name).substr(18);
        CodecCounters.push_back(TMultiCounter(subGroup, aggr, {{"codec", nm}}, {"MessagesWrittenByCodec"}, true));
    }
    SessionsCreated.Inc();
    SessionsActive.Inc();

    SetupBytesWrittenByUserAgentCounter(FullConverter->GetFederationPath());
}

void TWriteSessionLogicActor::SetupCounters(
    const TString& cloudId, const TString& dbId, const TString& dbPath, bool isServerless, const TString& folderId)
{
    if (SessionsCreated) {
        return;
    }

    auto subGroup = NPersQueue::GetCountersForTopic(Counters, isServerless);
    auto subgroups = NPersQueue::GetSubgroupsForTopic(FullConverter, cloudId, dbId, dbPath, folderId);

    SessionsCreated = TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.sessions_created"}, true, "name");
    SessionsActive = TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.sessions_active_count"}, false, "name");
    Errors = TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.errors"}, true, "name");

    SessionsCreated.Inc();
    SessionsActive.Inc();

    SetupBytesWrittenByUserAgentCounter(NPersQueue::GetFullTopicPath(dbPath, FullConverter->GetPrimaryPath()));
}

void TWriteSessionLogicActor::InitCheckSchema(bool needWaitSchema, NWilson::TTraceId traceId) {
    YDB_LOG_INFO("Init check schema");

    if (!needWaitSchema) {
        ACLCheckInProgress = true;
    }
    if (Describer) {
        Send(Describer, new TEvents::TEvPoison());
        Describer = {};
    }

    AFL_ENSURE(DiscoveryConverter);
    AFL_ENSURE(DiscoveryConverter->IsValid());
    const TString describePath = DiscoveryConverter->GetPrimaryPath();

    Describer = RegisterWithSameMailbox(NDescriber::CreateDescriberActor(
        SelfId(),
        CanonizePath(DatabaseName.value_or("/Root")),
        {describePath},
        NDescriber::TDescribeSettings{
            .UserToken = Token,
            .AccessRights = NACLib::EAccessRights::UpdateRow,
            .TraceId = std::move(traceId),
        }));
    if (needWaitSchema) {
        State = ES_WAIT_SCHEME;
    }
}

void TWriteSessionLogicActor::Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
    if (ev->Sender != Describer) {
        return;
    }
    Describer = {};

    AFL_ENSURE(ev->Get()->Topics.size() == 1);
    const auto& [requestedPath, info] = *ev->Get()->Topics.begin();

    if (info.Status != NDescriber::EStatus::SUCCESS) {
        const TString& pathForClient = !info.RealPath.empty() ? info.RealPath : requestedPath;
        CloseSession(
            NDescriber::Description(pathForClient, info.Status),
            ErrorCodeFromDescriberStatus(info.Status));
        return;
    }

    AFL_ENSURE(info.Info);
    PQGroupInfo = info.Info;
    const auto& config = PQGroupInfo->Description;
    Chooser = PQGroupInfo->PartitionChooser;
    AFL_ENSURE(Chooser);
    PartitionGraph = PQGroupInfo->PartitionGraph;
    AFL_ENSURE(PartitionGraph);

    AFL_ENSURE(config.PartitionsSize() > 0);
    AFL_ENSURE(config.HasPQTabletConfig());
    InitialPQTabletConfig = config.GetPQTabletConfig();
    // Scheme cache often omits TopicPath; metacache used to fill it from the navigate path.
    // UpgradeToFullConverter AFL_ENSUREs a non-empty path when TopicName is also empty.
    if (InitialPQTabletConfig.GetTopicPath().empty() && !info.RealPath.empty()) {
        InitialPQTabletConfig.SetTopicPath(info.RealPath);
    }
    DescribedRealPath = info.RealPath;
    if (!DiscoveryConverter->IsValid()) {
        TString errorReason = Sprintf("Internal server error with topic '%s', Marker# PQ503", TopicLogName().c_str());
        CloseSession(errorReason, Ydb::PersQueue::ErrorCode::ERROR);
        return;
    }
    if (!AppData()->PQConfig.GetTopicsAreFirstClassCitizen() && !config.GetPQTabletConfig().GetLocalDC()) {
        TString errorReason = Sprintf("Write to mirrored topic '%s' is forbidden", TopicLogName().c_str());
        CloseSession(errorReason, Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }

    FullConverter = DiscoveryConverter->UpgradeToFullConverter(
        InitialPQTabletConfig,
        AppData()->PQConfig.GetTestDatabaseRoot());
    if (!InitAfterDiscovery()) {
        return;
    }

    if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        const auto& tabletConfig = config.GetPQTabletConfig();
        SetupCounters(tabletConfig.GetYcCloudId(), tabletConfig.GetYdbDatabaseId(),
                      tabletConfig.GetYdbDatabasePath(), info.IsServerless,
                      tabletConfig.GetYcFolderId());
    } else {
        SetupCounters();
    }

    AFL_ENSURE(info.SecurityObject);
    ACL = info.SecurityObject;
    YDB_LOG_INFO("Session v1 describe result for acl check",
        {"cookie", Cookie},
        {"sessionId", OwnerCookie});

    const auto meteringMode = config.GetPQTabletConfig().GetMeteringMode();
    if (meteringMode != GetMeteringMode().GetOrElse(meteringMode)) {
        return CloseSession("Metering mode has been changed", Ydb::PersQueue::ErrorCode::OVERLOAD);
    }

    SetMeteringMode(meteringMode);

    if (FirstACLCheck) {
        LogSession();
    }

    if (!Token) {
        AFL_ENSURE(FirstACLCheck);
        FirstACLCheck = false;
        DiscoverPartition();
        return;
    }

    if (FirstACLCheck && IsQuotaRequired()) {
        AFL_ENSURE(TRlHelpers::MaybeRequestQuota(1, EWakeupTag::RlInit, ActorContext(), InitSpan.GetTraceId()));
    } else {
        OnWriteAccessGranted();
    }
}

void TWriteSessionLogicActor::DiscoverPartition() {
    State = ES_WAIT_PARTITION;

    if (PartitionChooser) {
        Send(PartitionChooser, new TEvents::TEvPoison());
    }

    std::optional<ui32> preferedPartition = PreferedPartition == Max<ui32>() ? std::nullopt : std::optional(PreferedPartition);
    AFL_ENSURE(PQGroupInfo);
    const auto& config = PQGroupInfo->Description;
    PartitionChooser = RegisterWithSameMailbox(CreatePartitionChooserActor(
        SelfId(),
        config,
        Chooser,
        PartitionGraph,
        FullConverter,
        SourceId,
        preferedPartition,
        InitSpan.GetTraceId()));
}

void TWriteSessionLogicActor::Handle(TEvPartitionChooser::TEvChooseResult::TPtr& ev) {
    auto* r = ev->Get();
    PartitionTabletId = r->TabletId;
    InitialSeqNo = r->SeqNo;
    LastSourceIdUpdate = TActivationContext::Now();
    ProceedPartition(r->PartitionId);
}

void TWriteSessionLogicActor::Handle(TEvPartitionChooser::TEvChooseError::TPtr& ev) {
    CloseSession(ev->Get()->ErrorMessage, ev->Get()->Code);
}

void TWriteSessionLogicActor::ProceedPartition(const ui32 partition) {
    Partition = partition;

    YDB_LOG_DEBUG("ProceedPartition. session",
        {"cookie", Cookie},
        {"sessionId", OwnerCookie},
        {"partition", Partition},
        {"expectedGeneration", ExpectedGeneration});

    if (!PartitionTabletId) {
        CloseSession(
            Sprintf("no partition %u in topic '%s', Marker# PQ4", Partition,
                    TopicLogName().c_str()),
            Ydb::PersQueue::ErrorCode::UNKNOWN_TOPIC);
        return;
    }

    if (!CreatePartitionWriterCache()) {
        return;
    }

    State = ES_WAIT_WRITER_INIT;

    ui32 border = AppData()->PQConfig.GetWriteInitLatencyBigMs();
    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");

    InitLatency = CreateSLIDurationCounter(subGroup, Aggr, "WriteInit", border, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
    SLIBigLatency = TMultiCounter(subGroup, Aggr, {}, {"RequestsBigLatency"}, true, "sensor", false);

    ui32 initDurationMs = (TActivationContext::Now() - StartTime).MilliSeconds();
    InitLatency.IncFor(initDurationMs, 1);
    if (initDurationMs >= border) {
        SLIBigLatency.Inc();
    }
}

bool TWriteSessionLogicActor::CreatePartitionWriterCache() {
    TPartitionWriterOpts opts;

    opts.WithDeduplication(UseDeduplication);
    opts.WithSourceId(SourceId);
    opts.WithInitialSeqNo(InitialSeqNo);
    opts.WithExpectedGeneration(ExpectedGeneration);

    if (TrackProducerId) {
        opts.WithTrackProducerId(*TrackProducerId);
    }

    opts.WithTopicPath(TopicPath);
    if (Protocol.AttachRequestContextToPartitionWriter) {
        if (DatabaseName) {
            opts.WithDatabase(*DatabaseName);
        }
        if (!SerializedToken.empty()) {
            opts.WithToken(SerializedToken);
        }
        if (TraceId) {
            opts.WithTraceId(*TraceId);
        }
        if (RequestType) {
            opts.WithRequestType(*RequestType);
        }
    }

    PartitionWriterCache = RegisterWithSameMailbox(
        CreatePartitionWriterCacheActor(SelfId(), Partition, PartitionTabletId, opts));
    return true;
}

void TWriteSessionLogicActor::DestroyPartitionWriterCache() {
    if (PartitionWriterCache == TActorId()) {
        return;
    }
    Send(PartitionWriterCache, new TEvents::TEvPoisonPill());
}

void TWriteSessionLogicActor::CloseSpans(const TString& errorReason, Ydb::PersQueue::ErrorCode::ErrorCode errorCode) {
    CloseSpan(InitSpan, errorReason, errorCode);
    CloseSpan(UpdateTokenSpan, errorReason, errorCode);
    for (auto& writeInfoPtr : PendingRequests) {
        CloseSpan(writeInfoPtr->QuotaSpan, errorReason, errorCode);
        CloseSpan(writeInfoPtr->Span, errorReason, errorCode);
    }
    CloseSpan(Span, errorReason, errorCode);
}

void TWriteSessionLogicActor::CompleteSession(
    const TString& errorReason,
    Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
    std::optional<Ydb::StatusIds::StatusCode> statusOverride)
{
    if (SessionClosed) {
        return;
    }
    SessionClosed = true;

    if (errorCode != Ydb::PersQueue::ErrorCode::OK) {
        if (InternalErrorCode(errorCode)) {
            SLIErrors.Inc();
        }
        if (Errors) {
            Errors.Inc();
        } else {
            ++(*GetServiceCounters(Counters, "pqproxy|writeSession")->GetCounter("Errors", true));
        }
        YDB_LOG_INFO("Session v1 error",
            {"cookie", Cookie},
            {"reason", errorReason},
            {"sessionId", OwnerCookie});
    } else {
        YDB_LOG_INFO("Session v1 closed",
            {"cookie", Cookie},
            {"sessionId", OwnerCookie});
    }

    auto closed = MakeHolder<TEvClosed>();
    closed->ErrorReason = errorReason;
    closed->ErrorCode = errorCode;
    closed->StatusOverride = statusOverride;
    Send(Owner, closed.Release());

    CloseSpans(errorReason, errorCode);
}

void TWriteSessionLogicActor::CloseSession(
    const TString& errorReason,
    Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
    std::optional<Ydb::StatusIds::StatusCode> statusOverride)
{
    CompleteSession(errorReason, errorCode, statusOverride);
    PassAway();
}

void TWriteSessionLogicActor::MakeAndSendInitResponse(const std::optional<ui64>& maxSeqNo) {
    auto ack = MakeHolder<TEvInitAck>();
    ack->SessionId = OwnerCookie;
    ack->PartitionId = Partition;
    ack->LastSeqNo = maxSeqNo;
    if (InitialPQTabletConfig.HasCodecs()) {
        for (const auto& codecName : InitialPQTabletConfig.GetCodecs().GetCodecs()) {
            ack->SupportedCodecNames.push_back(codecName);
        }
    }
    ack->FederationPath = FullConverter->GetFederationPath();
    ack->Cluster = FullConverter->GetCluster();
    ack->BatchingSupported = IsTopicMessagesBatchingEnabled(ActorContext());

    InitSpan.End();
    InitSpan = {};

    YDB_LOG_INFO("Session inited",
        {"cookie", Cookie},
        {"partition", Partition},
        {"maxSeqNo", maxSeqNo},
        {"sessionId", OwnerCookie});

    Send(Owner, ack.Release());

    State = ES_INITED;
    Schedule(TDuration::Seconds(AppData()->PQConfig.GetACLRetryTimeoutSec()), new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));
    NextRequestInited = true;
    Send(Owner, new TEvReadNext());
}

void TWriteSessionLogicActor::Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev) {
    const auto& result = *ev->Get();

    if (State != ES_WAIT_WRITER_INIT) {
        return CloseSession("got init result but not wait for it", Ydb::PersQueue::ErrorCode::ERROR);
    }

    AFL_ENSURE(!result.SessionId && !result.TxId);

    if (!result.IsSuccess()) {
        const auto& error = result.GetError();
        if (error.Response.HasErrorCode()) {
            return CloseSession("status is not ok: " + error.Response.GetErrorReason(), ConvertOldCode(error.Response.GetErrorCode()));
        } else {
            return CloseSession("error at writer init: " + error.Reason, Ydb::PersQueue::ErrorCode::ERROR);
        }
    }

    OwnerCookie = result.GetResult().OwnerCookie;
    const auto& maxSeqNo = result.GetResult().SourceIdInfo.GetSeqNo();
    OwnerCookie = result.GetResult().OwnerCookie;
    MakeAndSendInitResponse(maxSeqNo);
}

void TWriteSessionLogicActor::Handle(TEvPartitionWriter::TEvWriteAccepted::TPtr& ev) {
    if (State != ES_INITED) {
        return CloseSession("got write permission but not wait for it", Ydb::PersQueue::ErrorCode::ERROR);
    }

    AFL_ENSURE(!SentRequests.empty());
    auto writeRequest = std::move(SentRequests.front());

    if (ev->Get()->Cookie != writeRequest->Cookie) {
        return CloseSession("out of order reserve bytes response from server, may be previous is lost", Ydb::PersQueue::ErrorCode::ERROR);
    }

    SentRequests.pop_front();

    ui64 diff = writeRequest->ByteSize;
    AcceptedRequests.emplace_back(std::move(writeRequest));

    BytesInflight_ -= diff;
    if (BytesInflight) {
        BytesInflight.Dec(diff);
    }
    if (!NextRequestInited && BytesInflight_ < AppData()->PQConfig.GetMaxWriteSessionBytesInflight()) {
        NextRequestInited = true;
        Send(Owner, new TEvReadNext());
    }

    if (!IsQuotaRequired() && !PendingRequests.empty()) {
        SendWriteRequest(std::move(PendingRequests.front()));
        PendingRequests.pop_front();
    }
}

void TWriteSessionLogicActor::ProcessWriteResponse(
    const NKikimrClient::TPersQueuePartitionResponse& response)
{
    auto writeRequest = std::move(AcceptedRequests.front());
    AcceptedRequests.pop_front();
    writeRequest->Span.End();

    ui32 partitionCmdWriteResultIndex = 0;
    for (const auto& userWriteRequest : writeRequest->UserWriteRequests) {
        auto ackEv = MakeHolder<TEvWriteAck>();
        ackEv->PartitionId = Partition;

        for (const auto& message : userWriteRequest.Messages) {
            if (partitionCmdWriteResultIndex == response.CmdWriteResultSize()) {
                CloseSession("too few responses from server", Ydb::PersQueue::ErrorCode::ERROR);
                return;
            }
            const auto& partitionCmdWriteResult = response.GetCmdWriteResult(partitionCmdWriteResultIndex);
            if (UseDeduplication && partitionCmdWriteResult.GetSeqNo() != message.ExpectedAckSeqNo) {
                CloseSession(TStringBuilder() << "Expected partition " << Partition
                                              << " write result for message with sequence number "
                                              << message.ExpectedAckSeqNo << " but got for "
                                              << partitionCmdWriteResult.GetSeqNo(),
                             Ydb::PersQueue::ErrorCode::ERROR);
                return;
            }
            if (!UseDeduplication) {
                AFL_ENSURE(!partitionCmdWriteResult.GetAlreadyWritten());
            } else if (partitionCmdWriteResult.GetAlreadyWritten()) {
                AFL_ENSURE(UseDeduplication);
            }
            ackEv->Acks.push_back(MakeAck(partitionCmdWriteResult));
            ++partitionCmdWriteResultIndex;
        }

        Send(Owner, ackEv.Release());
    }

    ui64 diff = writeRequest->ByteSize;
    BytesInflightTotal_ -= diff;
    if (BytesInflightTotal) {
        BytesInflightTotal.Dec(diff);
    }

    CheckFinish();
}

void TWriteSessionLogicActor::Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
    const auto& result = *ev->Get();
    if (!result.IsSuccess()) {
        const auto& record = result.Record;
        if (record.HasErrorCode()) {
            return CloseSession("status is not ok: " + record.GetErrorReason(), ConvertOldCode(record.GetErrorCode()));
        } else {
            return CloseSession("error at write: " + result.GetError().Reason, Ydb::PersQueue::ErrorCode::ERROR);
        }
    }

    if (State != ES_INITED) {
        return CloseSession(TStringBuilder() << "got write response but not wait for it (" << static_cast<int>(State) << ")", Ydb::PersQueue::ErrorCode::ERROR);
    }

    if (AcceptedRequests.empty()) {
        return CloseSession("got too many replies from server, internal error", Ydb::PersQueue::ErrorCode::ERROR);
    }

    const auto& writeRequest = AcceptedRequests.front();
    const auto& resp = result.Record.GetPartitionResponse();

    if (resp.GetCookie() != writeRequest->Cookie) {
        return CloseSession("out of order write response from server, may be previous is lost", Ydb::PersQueue::ErrorCode::ERROR);
    }

    ProcessWriteResponse(resp);
}

void TWriteSessionLogicActor::Handle(TEvPartitionWriter::TEvDisconnected::TPtr& ev) {
    CloseSession(TStringBuilder() << "pipe to partition's " << Partition << " tablet is dead #" << static_cast<int>(ev->Get()->ErrorCode),
                 Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED);
}

void TWriteSessionLogicActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    TEvTabletPipe::TEvClientConnected* msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        CloseSession(TStringBuilder() << "pipe to tablet is dead " << msg->TabletId, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED);
    }
}

void TWriteSessionLogicActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    CloseSession(TStringBuilder() << "pipe to tablet is dead " << ev->Get()->TabletId, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED);
}

void TWriteSessionLogicActor::PrepareRequest(THolder<TEvWrite>&& ev) {
    const auto& incoming = *ev;

    auto sameDestination = [&](const TWriteRequestInfo::TUserWriteRequest& last) {
        const bool lastHasTx = last.Tx.has_value();
        const bool incomingHasTx = incoming.Tx.has_value();
        if (incomingHasTx || lastHasTx) {
            if (incomingHasTx != lastHasTx) {
                return false;
            }
            return incoming.Tx->first == last.Tx->first && incoming.Tx->second == last.Tx->second;
        }
        const bool lastHasDeferred = last.DeferredPublish.has_value();
        const bool incomingHasDeferred = incoming.DeferredPublish.has_value();
        if (incomingHasDeferred != lastHasDeferred) {
            return false;
        }
        if (incomingHasDeferred) {
            return incoming.DeferredPublish->IntPublicationId == last.DeferredPublish->IntPublicationId;
        }
        return true;
    };

    if (PendingRequests.empty()) {
        PendingRequests.emplace_back(new TWriteRequestInfo(++NextRequestCookie, GenerateWriteSpan()));
    } else {
        AFL_ENSURE(!PendingRequests.back()->UserWriteRequests.empty());
        if (!sameDestination(PendingRequests.back()->UserWriteRequests.back())) {
            PendingRequests.emplace_back(new TWriteRequestInfo(++NextRequestCookie, GenerateWriteSpan()));
        }
    }

    const auto& pendingRequest = PendingRequests.back();
    auto& request = pendingRequest->PartitionWriteRequest->Record;
    ui64 payloadSize = 0;
    ui64 maxMessageMetadataSize = 0;

    for (const auto& message : incoming.Messages) {
        auto* w = request.MutablePartitionRequest()->AddCmdWrite();
        w->SetData(SerializeMessageData(InitMeta, message));
        if (UseDeduplication) {
            w->SetSourceId(NSourceIdEncoding::EncodeSimple(SourceId));
        } else if (Protocol.SetDisableDeduplicationWhenUnused) {
            w->SetDisableDeduplication(true);
        }
        w->SetSeqNo(message.CmdSeqNo);
        w->SetCreateTimeMS(message.CreateTimeMs);
        w->SetUncompressedSize(message.UncompressedSize);
        w->SetClientDC(ClientDC);
        w->SetIgnoreQuotaDeadline(true);

        if (message.LogicalMessageCount) {
            w->SetLogicalMessageCount(*message.LogicalMessageCount);
            w->SetIsBatch(true);
        }
        if (message.MaxSeqNo) {
            w->SetMaxSeqNo(*message.MaxSeqNo);
        }

        payloadSize += w->GetData().size() + w->GetSourceId().size();

        ui64 currMetadataSize = 0;
        const bool isBatch = w->HasLogicalMessageCount() && w->GetLogicalMessageCount() > 1;
        for (const auto& [key, value] : message.Metadata) {
            if (key == PARTITION_KEY_META_KEY) {
                if (isBatch) {
                    auto* partitionKey = w->AddPartitionKeys();
                    partitionKey->SetKey(value);
                } else {
                    w->SetChoosePartitionKey(value);
                }
            }
            currMetadataSize += key.size() + value.size();
        }
        maxMessageMetadataSize = std::max(maxMessageMetadataSize, currMetadataSize);
    }

    TWriteRequestInfo::TUserWriteRequest userWrite;
    userWrite.Messages = std::move(ev->Messages);
    userWrite.Tx = incoming.Tx;
    userWrite.DeferredPublish = incoming.DeferredPublish;
    userWrite.UserRequestByteSize = incoming.UserRequestByteSize;
    pendingRequest->UserWriteRequests.emplace_back(std::move(userWrite));
    pendingRequest->ByteSize = request.ByteSize();

    if (maxMessageMetadataSize > MAX_METADATA_SIZE_PER_MESSAGE) {
        CloseSession(
            TStringBuilder() << "Message level metadata size is limited to " << MAX_METADATA_SIZE_PER_MESSAGE
                             << " per message",
            Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }

    if (const auto ru = CalcRuConsumption(payloadSize)) {
        pendingRequest->RequiredQuota += ru;
        PendingRequests.front()->StartQuotaSpan();
        MaybeRequestQuota(EWakeupTag::RlAllowed);
    } else {
        if (!PendingQuotaRequest) {
            SendWriteRequest(std::move(PendingRequests.front()));
            PendingRequests.pop_front();
        }
    }
}

void TWriteSessionLogicActor::SendWriteRequest(TWriteRequestInfo::TPtr&& request) {
    AFL_ENSURE(request->PartitionWriteRequest);

    i64 diff = 0;
    for (const auto& userWrite : request->UserWriteRequests) {
        diff -= userWrite.UserRequestByteSize;
    }

    AFL_ENSURE(-diff <= (i64)BytesInflight_);
    const auto byteSize = request->PartitionWriteRequest->Record.ByteSize();
    diff += byteSize;
    request->Span.Attribute("bytes", byteSize);

    BytesInflight_ += diff;
    BytesInflightTotal_ += diff;
    if (BytesInflight && BytesInflightTotal) {
        BytesInflight.Inc(diff);
        BytesInflightTotal.Inc(diff);
    }

    auto [sessionId, txId] = request->GetTransactionId();
    auto event = std::make_unique<TEvPartitionWriter::TEvTxWriteRequest>(
        sessionId,
        txId,
        std::move(request->PartitionWriteRequest),
        ToMaybe(request->GetDeferredPublishOpts()));

    Send(PartitionWriterCache, std::move(event), 0, 0, request->Span.GetTraceId());

    if (BytesWrittenByUserAgent) {
        BytesWrittenByUserAgent->Add(request->ByteSize);
    }

    SentRequests.push_back(std::move(request));
}

void TWriteSessionLogicActor::Handle(TEvUpdateToken::TPtr& ev) {
    UpdateTokenSpan = GenerateUpdateTokenSpan();
    if (State != ES_INITED) {
        CloseSession("got 'update_token_request' but write session is not initialized", Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }
    if (UpdateTokenInProgress) {
        CloseSession("got another 'update_token_request' while previous still in progress, only single token update is allowed at a time", Ydb::PersQueue::ErrorCode::OVERLOAD);
        return;
    }

    const auto& token = ev->Get()->Token;
    if (token == Auth || (token.empty() && !(AppData()->EnforceUserTokenRequirement || AppData()->PQConfig.GetRequireCredentialsInNewProtocol()))) {
        Send(Owner, new TEvUpdateTokenAck());
    } else if (token.empty()) {
        Send(Owner, new TEvUnauthenticated("'token' in 'update_token_request' is empty"));
        PassAway();
        return;
    } else {
        UpdateTokenInProgress = true;
        UpdateTokenAuthenticated = false;
        Auth = token;
        auto refresh = MakeHolder<TEvRefreshToken>();
        refresh->Token = Auth;
        refresh->TraceId = UpdateTokenSpan.GetTraceId();
        Send(Owner, refresh.Release());
    }

    NextRequestInited = true;
    Send(Owner, new TEvReadNext());
}

void TWriteSessionLogicActor::Handle(TEvTokenRefreshed::TPtr& ev) {
    YDB_LOG_INFO("Updating token");

    UpdateTokenSpan.EndOk();
    Token = ev->Get()->InternalToken;
    SerializedToken = Token->GetSerializedToken();
    UpdateTokenAuthenticated = true;
    if (!ACLCheckInProgress) {
        InitCheckSchema();
    }
    UpdateTokenSpan = {};
}

void TWriteSessionLogicActor::Handle(TEvWrite::TPtr& ev) {
    RequestNotChecked = true;

    if (State != ES_INITED) {
        CloseSession("write in not inited session", Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }

    auto& write = *ev->Get();

    if (write.DeferredPublish) {
        const auto& deferredPublish = *write.DeferredPublish;
        const ui64 intPublicationId = deferredPublish.IntPublicationId;
        if (deferredPublish.ExtPublicationId) {
            const auto& extPublicationId = *deferredPublish.ExtPublicationId;
            const auto knownExt = DeferredPublicationExtByInt.FindPtr(intPublicationId);
            if (knownExt && *knownExt != extPublicationId) {
                YDB_LOG_WARN("Deferred publish ext_publication_id mismatch",
                    {"cookie", Cookie},
                    {"sessionId", OwnerCookie},
                    {"intPublicationId", intPublicationId},
                    {"expectedExtPublicationId", *knownExt},
                    {"actualExtPublicationId", extPublicationId});
            } else if (!knownExt) {
                DeferredPublicationExtByInt[intPublicationId] = extPublicationId;
            }
        }
    }

    if (write.Messages.empty()) {
        CloseSession(TStringBuilder() << "messages meta repeated fields are empty, write request contains no messages",
                     Ydb::PersQueue::ErrorCode::BAD_REQUEST);
        return;
    }

    for (size_t messageIndex = 0; messageIndex != write.Messages.size(); ++messageIndex) {
        const auto& message = write.Messages[messageIndex];
        if (message.SeqNo <= 0) {
            CloseSession(TStringBuilder() << "bad write request - sequence number must be greater than 0. Value at position " << messageIndex << " has seq_no " << message.SeqNo,
                         Ydb::PersQueue::ErrorCode::BAD_REQUEST);
            return;
        }
        if (messageIndex > 0 && message.SeqNo <= write.Messages[messageIndex - 1].SeqNo) {
            CloseSession(TStringBuilder() << "bad write request - sequence numbers are unsorted. Value " << message.SeqNo << " at position " << messageIndex
                << " is less than or equal to value " << write.Messages[messageIndex - 1].SeqNo << " at position " << (messageIndex - 1),
                         Ydb::PersQueue::ErrorCode::BAD_REQUEST);
            return;
        }

        if (!message.SkipCodecValidation) {
            ui32 codecForValidation = message.ChunkCodec.value_or(message.CodecId);
            TString error;
            if (!ValidateWriteWithCodec(InitialPQTabletConfig, codecForValidation, error)) {
                CloseSession(TStringBuilder() << "bad write request - codec is invalid: " << error,
                             Ydb::PersQueue::ErrorCode::BAD_REQUEST);
                return;
            }
        }

        ui32 intCodec = message.CodecId + Protocol.CodecCounterIndexOffset;
        if (intCodec >= CodecCounters.size()) {
            intCodec = 0;
        }
        if (CodecCounters.size() > intCodec) {
            CodecCounters[intCodec].Inc();
        }
    }

    ui64 diff = write.UserRequestByteSize;
    BytesInflight_ += diff;
    BytesInflightTotal_ += diff;
    if (BytesInflight && BytesInflightTotal) {
        BytesInflight.Inc(diff);
        BytesInflightTotal.Inc(diff);
    }

    if (BytesInflight_ < AppData()->PQConfig.GetMaxWriteSessionBytesInflight()) {
        AFL_ENSURE(NextRequestInited);
        Send(Owner, new TEvReadNext());
    } else {
        NextRequestInited = false;
    }

    PrepareRequest(THolder<TEvWrite>(ev->Release()));
}

void TWriteSessionLogicActor::LogSession() {
    YDB_LOG_INFO("Write session: userAgent=",
        {"cookie", Cookie},
        {"sessionId", OwnerCookie},
        {"userAgent", UserAgent},
        {"ip", PeerName},
        {"proto", Protocol.Name},
        {"user", (Token ? Token->GetUserSID() : "-")},
        {"topic", TopicLogName()},
        {"durationSec", (TActivationContext::Now() - StartTime).Seconds()});

    LogSessionDeadline = TActivationContext::Now() + TDuration::Hours(1) + TDuration::Seconds(rand() % 60);
}

TString TWriteSessionLogicActor::TopicLogName() const {
    if (!DescribedRealPath.empty()) {
        return DescribedRealPath;
    }
    if (FullConverter) {
        return FullConverter->GetInternalName();
    }
    return TopicPath;
}

void TWriteSessionLogicActor::Handle(TEvents::TEvWakeup::TPtr& ev) {
    const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
    OnWakeup(tag);
    switch (tag) {
        case EWakeupTag::RlInit:
            return OnWriteAccessGranted();

        case EWakeupTag::RecheckAcl:
            return RecheckACL();

        case EWakeupTag::RlAllowed: {
            Send(Owner, new TEvConsumedRequestUnits(PendingQuotaRequest->RequiredQuota));
            PendingQuotaRequest->QuotaSpan.EndOk();
            SendWriteRequest(std::move(PendingQuotaRequest));
            MaybeRequestQuota(EWakeupTag::RlAllowed);
            break;
        }

        case EWakeupTag::RlNoResource:
        case EWakeupTag::RlInitNoResource:
            if (PendingQuotaRequest) {
                PendingQuotaRequest->QuotaSpan.EndError("Timeout");
                PendingQuotaRequest->StartQuotaSpan();
                PendingQuotaRequest->SetSpanParamRequestedQuota();
                AFL_ENSURE(TRlHelpers::MaybeRequestQuota(PendingQuotaRequest->RequiredQuota, EWakeupTag::RlAllowed, ActorContext()));
            } else {
                return CloseSession("Throughput limit exceeded", Ydb::PersQueue::ErrorCode::OVERLOAD);
            }
            break;
    }
}

void TWriteSessionLogicActor::RecheckACL() {
    if (State != ES_INITED) {
        YDB_LOG_ERROR("WriteSessionActor state is wrong. Actual state",
            {"state", (int)State});
        return CloseSession("erroneous internal state", Ydb::PersQueue::ErrorCode::ERROR);
    }

    auto now = TActivationContext::Now();

    Schedule(TDuration::Seconds(AppData()->PQConfig.GetACLRetryTimeoutSec()), new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));
    if (Token && !ACLCheckInProgress && RequestNotChecked && (now - LastACLCheckTimestamp > TDuration::Seconds(AppData()->PQConfig.GetACLRetryTimeoutSec()))) {
        RequestNotChecked = false;
        InitCheckSchema();
    }

    if (PartitionChooser && now > LastSourceIdUpdate) {
        Send(PartitionChooser, new TEvPartitionChooser::TEvRefreshRequest());
        LastSourceIdUpdate = now + SOURCEID_UPDATE_PERIOD;
    }
    if (now >= LogSessionDeadline) {
        LogSession();
    }
}

NWilson::TSpan TWriteSessionLogicActor::GenerateSpan(NJaegerTracing::ERequestType subrequestType, TStringBuf name) const {
    if (Span) {
        return Span.CreateChild(TWilsonTopic::TopicBasic, TString(name));
    }

    NWilson::TTraceId traceId = NJaegerTracing::HandleTracing(NJaegerTracing::TRequestDiscriminator{
        .RequestType = subrequestType,
        .Database = ToMaybe(DatabaseName),
    }, {});

    if (traceId) {
        TString spanName(name);
        if (!Protocol.ChildSpanNameSuffix.empty()) {
            spanName += Protocol.ChildSpanNameSuffix;
        }
        return NWilson::TSpan(
            TWilsonTopic::TopicTopLevel,
            std::move(traceId),
            spanName);
    }

    return {};
}

NWilson::TSpan TWriteSessionLogicActor::GenerateInitSpan() const {
    return GenerateSpan(NJaegerTracing::ERequestType::TOPIC_STREAMWRITE_INIT, "Topic.WriteSession.Init");
}

NWilson::TSpan TWriteSessionLogicActor::GenerateWriteSpan() const {
    return GenerateSpan(NJaegerTracing::ERequestType::TOPIC_STREAMWRITE_WRITE, "Topic.WriteSession.Write");
}

NWilson::TSpan TWriteSessionLogicActor::GenerateUpdateTokenSpan() const {
    return GenerateSpan(NJaegerTracing::ERequestType::TOPIC_STREAMWRITE_UPDATE_TOKEN, "Topic.WriteSession.UpdateToken");
}

void TWriteSessionLogicActor::MaybeRequestQuota(EWakeupTag tag) {
    if (!PendingQuotaRequest && !PendingRequests.empty()) {
        auto& pending = PendingRequests.front();
        if (TRlHelpers::MaybeRequestQuota(pending->RequiredQuota, tag, ActorContext(), pending->QuotaSpan.GetTraceId())) {
            PendingQuotaRequest = std::move(pending);
            PendingRequests.pop_front();
            PendingQuotaRequest->SetSpanParamRequestedQuota();
        }
    }
}

NActors::IActor* CreateWriteSessionLogicActor(TWriteSessionSettings settings) {
    return new TWriteSessionLogicActor(std::move(settings));
}

} // namespace NKikimr::NPQ::NDataplane::NWrite
