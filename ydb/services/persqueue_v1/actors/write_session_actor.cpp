#include "write_session_actor.h"

#include "codecs.h"
#include "helpers.h"

#include <ydb/services/metadata/manager/common.h>

#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/core/persqueue/pq_database.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/actors/core/log.h>
#include <google/protobuf/util/time_util.h>
#include <util/string/hex.h>
#include <util/string/vector.h>
#include <util/string/escape.h>
#include <util/string/printf.h>


using namespace NActors;
using namespace NKikimrClient;



namespace NKikimr {
using namespace NSchemeCache;
using namespace NPQ;

template <bool UseMigrationProtocol>
using ECodec = std::conditional_t<UseMigrationProtocol, Ydb::PersQueue::V1::Codec, i32>;

static constexpr ui64 MAX_METADATA_SIZE_PER_MESSAGE = 4096;

template <bool UseMigrationProtocol>
ECodec<UseMigrationProtocol> CodecByName(const TString& codec) {
    THashMap<TString, ECodec<UseMigrationProtocol>> codecsByName;
    if constexpr (UseMigrationProtocol) {
        codecsByName = {
            { "raw",  Ydb::PersQueue::V1::CODEC_RAW  },
            { "gzip", Ydb::PersQueue::V1::CODEC_GZIP },
            { "lzop", Ydb::PersQueue::V1::CODEC_LZOP },
            { "zstd", Ydb::PersQueue::V1::CODEC_ZSTD },
        };
    }
    if constexpr (!UseMigrationProtocol) {
        codecsByName = {
            { "raw",  (i32)Ydb::Topic::CODEC_RAW  },
            { "gzip", (i32)Ydb::Topic::CODEC_GZIP },
            { "lzop", (i32)Ydb::Topic::CODEC_LZOP },
            { "zstd", (i32)Ydb::Topic::CODEC_ZSTD },
        };
    }

    auto codecIt = codecsByName.find(codec);
    if (codecIt == codecsByName.end()) {
        if constexpr (UseMigrationProtocol) {
            return Ydb::PersQueue::V1::CODEC_UNSPECIFIED;
        }
        if constexpr (!UseMigrationProtocol) {
            return (i32)Ydb::Topic::CODEC_UNSPECIFIED;
        }
        Y_ABORT("Unsupported codec enum");
    }
    return codecIt->second;
}

//explicit instantation
template Ydb::PersQueue::V1::Codec CodecByName<true>(const TString& codec);
template i32 CodecByName<false>(const TString& codec);

template <>
inline void FillExtraFieldsForDataChunk(
    const Ydb::PersQueue::V1::StreamingWriteClientMessage::InitRequest& init,
    NKikimrPQClient::TDataChunk& data,
    TString& server,
    TString& ident,
    TString& logType,
    TString& file
) {
    for (const auto& item : init.session_meta()) {
        if (item.first == "server") {
            server = item.second;
        } else if (item.first == "ident") {
            ident = item.second;
        } else if (item.first == "logtype") {
            logType = item.second;
        } else if (item.first == "file") {
            file = item.second;
        } else {
            auto res = data.MutableExtraFields()->AddItems();
            res->SetKey(item.first);
            res->SetValue(item.second);
        }
    }
}

template <>
inline void FillExtraFieldsForDataChunk(
    const Ydb::Topic::StreamWriteMessage::InitRequest& init,
    NKikimrPQClient::TDataChunk& data,
    TString& server,
    TString& ident,
    TString& logType,
    TString& file
) {
    for (const auto& item : init.write_session_meta()) {
        if (item.first == "server") {
            server = item.second;
        } else if (item.first == "ident") {
            ident = item.second;
        } else if (item.first == "logtype") {
            logType = item.second;
        } else if (item.first == "file") {
            file = item.second;
        } else {
            auto res = data.MutableExtraFields()->AddItems();
            res->SetKey(item.first);
            res->SetValue(item.second);
        }
    }
}

template <>
inline void FillChunkDataFromReq(
    NKikimrPQClient::TDataChunk& proto,
    const Ydb::PersQueue::V1::StreamingWriteClientMessage::WriteRequest& writeRequest,
    const i32 messageIndex
) {
    proto.SetSeqNo(writeRequest.sequence_numbers(messageIndex));
    proto.SetCreateTime(writeRequest.created_at_ms(messageIndex));
    proto.SetCodec(writeRequest.blocks_headers(messageIndex).front());
    proto.SetData(writeRequest.blocks_data(messageIndex));
}

template <>
inline void FillChunkDataFromReq(
    NKikimrPQClient::TDataChunk& proto,
    const Ydb::Topic::StreamWriteMessage::WriteRequest& writeRequest,
    const i32 messageIndex
) {
    const auto& msg = writeRequest.messages(messageIndex);
    proto.SetSeqNo(msg.seq_no());
    proto.SetCreateTime(::google::protobuf::util::TimeUtil::TimestampToMilliseconds(msg.created_at()));
    // TODO (ildar-khisam@): refactor codec enum convert
    if (writeRequest.codec() > 0) {
        proto.SetCodec(writeRequest.codec() - 1);
    }
    proto.SetData(msg.data());
    auto* msgMeta = proto.MutableMessageMeta();
    *msgMeta = msg.metadata_items();
}

namespace NGRpcProxy::V1 {

using namespace Ydb::PersQueue::V1;

static const ui32 MAX_BYTES_INFLIGHT = 1_MB;
static const TDuration SOURCEID_UPDATE_PERIOD = TDuration::Hours(1);

// metering
static const ui64 WRITE_BLOCK_SIZE = 4_KB;

//TODO: add here tracking of bytes in/out


template<bool UseMigrationProtocol>
TWriteSessionActor<UseMigrationProtocol>::TWriteSessionActor(
        TEvStreamWriteRequest* request, const ui64 cookie,
        const NActors::TActorId& schemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsController
)
    : TRlHelpers({}, request, WRITE_BLOCK_SIZE, false)
    , Request(request)
    , State(ES_CREATED)
    , SchemeCache(schemeCache)
    , PeerName("")
    , Cookie(cookie)
    , TopicsController(topicsController)
    , Partition(0)
    , PreferedPartition(Max<ui32>())
    , WritesDone(false)
    , Counters(counters)
    , BytesInflight_(0)
    , BytesInflightTotal_(0)
    , NextRequestInited(false)
    , NextRequestCookie(0)
    , Token(nullptr)
    , UpdateTokenInProgress(false)
    , UpdateTokenAuthenticated(false)
    , ACLCheckInProgress(true)
    , FirstACLCheck(true)
    , RequestNotChecked(false)
    , LastACLCheckTimestamp(TInstant::Zero())
    , LogSessionDeadline(TInstant::Zero())
    , ClientDC(clientDC ? *clientDC : "other")
    , LastSourceIdUpdate(TInstant::Zero())
{
    Y_ASSERT(Request);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Bootstrap(const TActorContext& ctx) {

    Y_ABORT_UNLESS(Request);

    Request->GetStreamCtx()->Attach(ctx.SelfID);
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "grpc read failed at start");
        Die(ctx);
        return;
    }
    TSelf::Become(&TSelf::TThis::StateFunc);
    StartTime = ctx.Now();
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::HandleDone(const TActorContext& ctx) {

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc closed");
    Die(ctx);
}

template<typename TClientMessage>
TString WriteRequestToLog(const TClientMessage& proto) {
    switch (proto.client_message_case()) {
        case TClientMessage::kInitRequest:
            return proto.ShortDebugString();
            break;
        case TClientMessage::kWriteRequest:
            return " write_request[data omitted]";
            break;
        case TClientMessage::kUpdateTokenRequest:
            return " update_token_request [content omitted]";
        default:
            return TString();
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read done: success: " << ev->Get()->Success << " data: " << WriteRequestToLog(ev->Get()->Record));
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
        ctx.Send(ctx.SelfID, new TEvPQProxy::TEvDone());
        return;
    }

    auto& req = ev->Get()->Record;

    switch(req.client_message_case()) {
        case TClientMessage::kInitRequest:
            ctx.Send(ctx.SelfID, new TEvWriteInit(std::move(req), Request->GetStreamCtx()->GetPeerName()));
            break;
        case TClientMessage::kWriteRequest:
            ctx.Send(ctx.SelfID, new TEvWrite(std::move(req)));
            break;
        case TClientMessage::kUpdateTokenRequest: {
            ctx.Send(ctx.SelfID, new TEvUpdateToken(std::move(req)));
            break;
        }
        case TClientMessage::CLIENT_MESSAGE_NOT_SET: {
            CloseSession("'client_message' is not set", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc write failed");
        Die(ctx);
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Die(const TActorContext& ctx) {
    if (State == ES_DYING) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " is already DEAD");
        return;
    }

    if (SessionsActive) {
        SessionsActive.Dec();
        if (BytesInflight && BytesInflightTotal) {
            BytesInflight.Dec(BytesInflight_);
            BytesInflightTotal.Dec(BytesInflightTotal_);
        }
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " is DEAD");

    ctx.Send(GetPQWriteServiceActorID(), new TEvPQProxy::TEvSessionDead(Cookie));

    DestroyPartitionWriterCache(ctx);
    if (PartitionChooser) {
        ctx.Send(PartitionChooser,  new TEvents::TEvPoison());
    }

    State = ES_DYING;
    TRlHelpers::PassAway(TActorBootstrapped<TWriteSessionActor>::SelfId());
    TActorBootstrapped<TWriteSessionActor>::Die(ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::CheckFinish(const TActorContext& ctx) {
    if (!WritesDone)
        return;
    if (State != ES_INITED) {
        CloseSession("out of order Writes done before initialization", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (PendingRequests.empty() && !PendingQuotaRequest && SentRequests.empty() && AcceptedRequests.empty()) {
        CloseSession("", PersQueue::ErrorCode::OK, ctx);
        return;
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext& ctx) {
    WritesDone = true;
    CheckFinish(ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::CheckACL(const TActorContext& ctx) {
    //Y_ABORT_UNLESS(ACLCheckInProgress);

    NACLib::EAccessRights rights = NACLib::EAccessRights::UpdateRow;

    Y_ABORT_UNLESS(ACL);
    if (ACL->CheckAccess(rights, *Token)) {
        ACLCheckInProgress = false;
        if (FirstACLCheck) {
            FirstACLCheck = false;
            DiscoverPartition(ctx);
        }
        if (UpdateTokenInProgress && UpdateTokenAuthenticated) {
            UpdateTokenInProgress = false;
            TServerMessage serverMessage;
            serverMessage.set_status(Ydb::StatusIds::SUCCESS);
            serverMessage.mutable_update_token_response();
            if (!Request->GetStreamCtx()->Write(std::move(serverMessage))) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc write failed");
                Die(ctx);
            }
        }
    } else {
        TString errorReason = Sprintf("access to topic '%s' denied for '%s' due to 'no WriteTopic rights', Marker# PQ1125",
            DiscoveryConverter->GetPrintableString().c_str(),
            Token->GetUserSID().c_str());
        CloseSession(errorReason, PersQueue::ErrorCode::ACCESS_DENIED, ctx);
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(typename TEvWriteInit::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvWriteInit> event(ev->Release());

    if (State != ES_CREATED) {
        //answer error
        CloseSession("got second init request",  PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    InitRequest = event->Request.init_request();

    TString topic_path = [this]() {
        if constexpr (UseMigrationProtocol) {
            return InitRequest.topic();
        } else {
            return InitRequest.path();
        }
    }();
    if (topic_path.empty()) {
        CloseSession("no topic in init request",  PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if constexpr (UseMigrationProtocol) {
        if (InitRequest.message_group_id().empty()) {
            CloseSession("no message_group_id in init request",  PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
    } else {
        // Supported scenarios:
        //    1. Non-empty producer_id
        //      1.1. producer_id == message_group_id (partition is selected using hash from message_group_id)
        //      1.2. non-empty partition_id (explicit partitioning)
        //      1.3. non-empty partition_with_generation (explicit partitioning && direct write to partition host)
        //    2. Empty producer id (no deduplication, partition is selected using round-robin).
        bool isScenarioSupported =
            !InitRequest.producer_id().empty() && (
                InitRequest.has_message_group_id() && InitRequest.message_group_id() == InitRequest.producer_id() ||
                InitRequest.message_group_id().empty() ||
                InitRequest.has_partition_id() ||
                InitRequest.has_partition_with_generation())
            ||
            InitRequest.producer_id().empty() ;

        if (!isScenarioSupported) {
            CloseSession("unsupported producer_id / message_group_id / partition_id settings in init request",
                         PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
    }

    DiscoveryConverter = TopicsController.GetWriteTopicConverter(topic_path, Request->GetDatabaseName().GetOrElse("/Root"));
    if (!DiscoveryConverter->IsValid()) {
        CloseSession(
                TStringBuilder() << "topic " << topic_path << " could not be recognized: " << DiscoveryConverter->GetReason(),
                PersQueue::ErrorCode::BAD_REQUEST, ctx
        );
        return;
    }

    PeerName = event->PeerName;

    SourceId = [this]() {
        if constexpr (UseMigrationProtocol) {
            return InitRequest.message_group_id();
        } else {
            if (InitRequest.producer_id().empty() && InitRequest.message_group_id().empty()) {
                UseDeduplication = false;
            }
            return !InitRequest.message_group_id().empty() ? InitRequest.message_group_id() : InitRequest.producer_id();
        }
    }();
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session request cookie: " << Cookie << " " << InitRequest.ShortDebugString() << " from " << PeerName);
    if (!UseDeduplication) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session request cookie: " << Cookie << ". Disable deduplication for empty producer id");
    }
    LogSession(ctx);

    if (Request->GetSerializedToken().empty()) { // session without auth
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            Request->ReplyUnauthenticated("Unauthenticated access is forbidden, please provide credentials");
            Die(ctx);
            return;
        }
    }

    InitCheckSchema(ctx, true);

    PreferedPartition = Max<ui32>();
    if constexpr (UseMigrationProtocol) {
        if (InitRequest.partition_group_id() > 0) {
            PreferedPartition = InitRequest.partition_group_id() - 1;
        }
        const auto& preferredCluster = InitRequest.preferred_cluster();
        if (!preferredCluster.empty()) {
            this->Send(GetPQWriteServiceActorID(), new TEvPQProxy::TEvSessionSetPreferredCluster(Cookie, preferredCluster));
        }
    } else {
        if (InitRequest.has_partition_id()) {
            PreferedPartition = InitRequest.partition_id();
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session to partition: " << PreferedPartition);
        }
        else if (InitRequest.has_partition_with_generation()) {
            PreferedPartition = InitRequest.partition_with_generation().partition_id();
            ExpectedGeneration = InitRequest.partition_with_generation().generation();
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session to partition: " << PreferedPartition << ", generation: " << ExpectedGeneration);
        }
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::InitAfterDiscovery(const TActorContext& ctx) {
    Y_UNUSED(ctx);

    if (SourceId.empty() && UseDeduplication) {
        CloseSession("Internal server error: got empty SourceId with enabled deduplication", PersQueue::ErrorCode::VALIDATION_ERROR, ctx);
        return;
    }

    InitMeta = GetInitialDataChunk(InitRequest, FullConverter->GetClientsideName(), PeerName); // ToDo[migration] - check?

    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", FullConverter->GetAccount()}}, {"total"}}};

    SLITotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
    SLIErrors = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);
    SLITotal.Inc();

}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::SetupCounters()
{
    if (SessionsCreated) {
        return;
    }

    //now topic is checked, can create group for real topic, not garbage
    auto subGroup = GetServiceCounters(Counters, "pqproxy|writeSession");
    auto aggr = NPersQueue::GetLabels(FullConverter);

    BytesInflight = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"BytesInflight"}, false);
    BytesInflightTotal = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"BytesInflightTotal"}, false);
    SessionsCreated = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"SessionsCreated"}, true);
    SessionsActive = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"SessionsActive"}, false);
    Errors = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"Errors"}, true);

    CodecCounters.push_back(NKikimr::NPQ::TMultiCounter(subGroup, aggr, {{"codec", "user"}}, {"MessagesWrittenByCodec"}, true));

    auto allNames = GetEnumAllCppNames<Ydb::Topic::Codec>();
    allNames.erase(allNames.begin());
    allNames.pop_back();
    allNames.pop_back();
    for (auto &name : allNames)  {
        auto nm = to_lower(name).substr(18);
        CodecCounters.push_back(NKikimr::NPQ::TMultiCounter(subGroup, aggr, {{"codec", nm}}, {"MessagesWrittenByCodec"}, true));
    }
    SessionsCreated.Inc();
    SessionsActive.Inc();
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::SetupCounters(const TString& cloudId, const TString& dbId, const TString& dbPath, const bool isServerless, const TString& folderId)
{
    if (SessionsCreated) {
        return;
    }

    //now topic is checked, can create group for real topic, not garbage
    auto subGroup = NPersQueue::GetCountersForTopic(Counters, isServerless);
    auto subgroups = NPersQueue::GetSubgroupsForTopic(FullConverter, cloudId, dbId, dbPath, folderId);

    SessionsCreated = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.sessions_created"}, true, "name");
    SessionsActive = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.sessions_active_count"}, false, "name");
    Errors = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.errors"}, true, "name");

    SessionsCreated.Inc();
    SessionsActive.Inc();
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::InitCheckSchema(const TActorContext& ctx, bool needWaitSchema) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "init check schema");

    if (!needWaitSchema) {
        ACLCheckInProgress = true;
    }
    ctx.Send(SchemeCache, new TEvDescribeTopicsRequest({DiscoveryConverter}));
    if (needWaitSchema) {
        State = ES_WAIT_SCHEME;
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    auto& res = ev->Get()->Result;
    Y_ABORT_UNLESS(res->ResultSet.size() == 1);

    auto& entry = res->ResultSet[0];
    TString errorReason;
    auto processResult = ProcessMetaCacheTopicResponse(entry);
    if (processResult.IsFatal) {
        CloseSession(processResult.Reason, processResult.ErrorCode, ctx);
        return;
    }
    Y_ABORT_UNLESS(entry.PQGroupInfo); // checked at ProcessMetaCacheTopicResponse()
    Config = std::move(entry.PQGroupInfo->Description);
    Y_ABORT_UNLESS(Config.PartitionsSize() > 0);
    Y_ABORT_UNLESS(Config.HasPQTabletConfig());
    InitialPQTabletConfig = Config.GetPQTabletConfig();
    if (!DiscoveryConverter->IsValid()) {
        errorReason = Sprintf("Internal server error with topic '%s', Marker# PQ503", DiscoveryConverter->GetPrintableString().c_str());
        CloseSession(errorReason, PersQueue::ErrorCode::ERROR, ctx);
        return;
    }
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen() && !Config.GetPQTabletConfig().GetLocalDC()) {
        errorReason = Sprintf("Write to mirrored topic '%s' is forbidden", DiscoveryConverter->GetPrintableString().c_str());
        CloseSession(errorReason, PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    FullConverter = DiscoveryConverter->UpgradeToFullConverter(InitialPQTabletConfig,
                                                               AppData(ctx)->PQConfig.GetTestDatabaseRoot());
    InitAfterDiscovery(ctx);

    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        const auto& tabletConfig = Config.GetPQTabletConfig();
        SetupCounters(tabletConfig.GetYcCloudId(), tabletConfig.GetYdbDatabaseId(),
                        tabletConfig.GetYdbDatabasePath(), entry.DomainInfo->IsServerless(),
                      tabletConfig.GetYcFolderId());
    } else {
        SetupCounters();
    }

    Y_ABORT_UNLESS(entry.SecurityObject);
    ACL.Reset(new TAclWrapper(entry.SecurityObject));
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " describe result for acl check");

    const auto meteringMode = Config.GetPQTabletConfig().GetMeteringMode();
    if (meteringMode != GetMeteringMode().GetOrElse(meteringMode)) {
        return CloseSession("Metering mode has been changed", PersQueue::ErrorCode::OVERLOAD, ctx);
    }

    SetMeteringMode(meteringMode);

    if (Request->GetSerializedToken().empty()) { // session without auth
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            Request->ReplyUnauthenticated("Unauthenticated access is forbidden, please provide credentials");
            Die(ctx);
            return;
        }
        Y_ABORT_UNLESS(FirstACLCheck);
        FirstACLCheck = false;
        DiscoverPartition(ctx);
    } else {
        Y_ABORT_UNLESS(Request->GetYdbToken());
        Auth = *Request->GetYdbToken();
        Token = new NACLib::TUserToken(Request->GetSerializedToken());

        if (FirstACLCheck && IsQuotaRequired()) {
            Y_ABORT_UNLESS(MaybeRequestQuota(1, EWakeupTag::RlInit, ctx));
        } else {
            CheckACL(ctx);
        }
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::DiscoverPartition(const NActors::TActorContext& ctx) {
    State = ES_WAIT_PARTITION;

    if (PartitionChooser) {
        ctx.Send(PartitionChooser,  new TEvents::TEvPoison());
    }

    std::optional<ui32> preferedPartition = PreferedPartition == Max<ui32>() ? std::nullopt : std::optional(PreferedPartition);
    PartitionChooser = ctx.RegisterWithSameMailbox(NPQ::CreatePartitionChooserActor(ctx.SelfID, Config, FullConverter, SourceId, preferedPartition));
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(NPQ::TEvPartitionChooser::TEvChooseResult::TPtr& ev, const NActors::TActorContext& ctx) {
    auto* r = ev->Get();
    PartitionTabletId = r->TabletId;
    InitialSeqNo = r->SeqNo;
    LastSourceIdUpdate = ctx.Now();

    ProceedPartition(r->PartitionId, ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(NPQ::TEvPartitionChooser::TEvChooseError::TPtr& ev, const NActors::TActorContext& ctx) {
    CloseSession(ev->Get()->ErrorMessage, ev->Get()->Code, ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::ProceedPartition(const ui32 partition, const TActorContext& ctx) {
    Partition = partition;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "ProceedPartition. session cookie: " << Cookie << " sessionId: " << OwnerCookie << " partition: " << Partition << " expectedGeneration: " << ExpectedGeneration);

    if (!PartitionTabletId) {
        CloseSession(
                Sprintf("no partition %u in topic '%s', Marker# PQ4", Partition,
                        DiscoveryConverter->GetPrintableString().c_str()),
                PersQueue::ErrorCode::UNKNOWN_TOPIC, ctx
        );
        return;
    }

    CreatePartitionWriterCache(ctx);

    State = ES_WAIT_WRITER_INIT;

    ui32 border = AppData(ctx)->PQConfig.GetWriteInitLatencyBigMs();
    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");

    InitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "WriteInit", border, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
    SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsBigLatency"}, true, "sesnor", false);

    ui32 initDurationMs = (ctx.Now() - StartTime).MilliSeconds();
    InitLatency.IncFor(initDurationMs, 1);
    if (initDurationMs >= border) {
        SLIBigLatency.Inc();
    }
}

template <bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::CreatePartitionWriterCache(const TActorContext& ctx)
{
    NPQ::TPartitionWriterOpts opts;

    opts.WithDeduplication(UseDeduplication);
    opts.WithSourceId(SourceId);
    opts.WithInitialSeqNo(InitialSeqNo);
    opts.WithExpectedGeneration(ExpectedGeneration);

    if constexpr (UseMigrationProtocol) {
        opts.WithTopicPath(InitRequest.topic());
    } else {
        if (Request->GetDatabaseName()) {
            opts.WithDatabase(*Request->GetDatabaseName());
        }
        opts.WithTopicPath(InitRequest.path());
        if (Request->GetSerializedToken()) {
            opts.WithToken(Request->GetSerializedToken());
        }
        if (Request->GetTraceId()) {
            opts.WithTraceId(*Request->GetTraceId());
        }
        if (Request->GetRequestType()) {
            opts.WithRequestType(*Request->GetRequestType());
        }
    }

    auto actor =
        std::make_unique<TPartitionWriterCacheActor>(ctx.SelfID,
                                                     Partition,
                                                     PartitionTabletId,
                                                     opts);

    PartitionWriterCache = ctx.RegisterWithSameMailbox(actor.release());
}

template <bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::DestroyPartitionWriterCache(const TActorContext& ctx)
{
    if (PartitionWriterCache == TActorId()) {
        return;
    }

    ctx.Send(PartitionWriterCache, new TEvents::TEvPoisonPill());
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {
    if (SessionClosed) {
        return;
    }
    SessionClosed = true;

    if (errorCode != PersQueue::ErrorCode::OK) {

        if (InternalErrorCode(errorCode)) {
            SLIErrors.Inc();
        }

        if (Errors) {
            Errors.Inc();
        } else {
            ++(*GetServiceCounters(Counters, "pqproxy|writeSession")->GetCounter("Errors", true));
        }

        TServerMessage result;
        result.set_status(ConvertPersQueueInternalCodeToStatus(errorCode));
        FillIssue(result.add_issues(), errorCode, errorReason);

        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 error cookie: " << Cookie << " reason: " << errorReason << " sessionId: " << OwnerCookie);

        if (!Request->GetStreamCtx()->WriteAndFinish(std::move(result), grpc::Status::OK)) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc last write failed");
        }
    } else {
        if (!Request->GetStreamCtx()->Finish(grpc::Status::OK)) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " double finish call");
        }
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 closed cookie: " << Cookie << " sessionId: " << OwnerCookie);
    }
    Die(ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::MakeAndSentInitResponse(
        const TMaybe<ui64>& maxSeqNo, const TActorContext& ctx
) {
    TServerMessage response;
    response.set_status(Ydb::StatusIds::SUCCESS);
    auto init = response.mutable_init_response();

    if (!OwnerCookie.empty()) {
        init->set_session_id(EscapeC(OwnerCookie));
    }
    if constexpr (UseMigrationProtocol) {
        if (maxSeqNo.Defined()) {
            init->set_last_sequence_number(*maxSeqNo);
        }
        init->set_partition_id(Partition);
        init->set_topic(FullConverter->GetFederationPath());
        init->set_cluster(FullConverter->GetCluster());
        init->set_block_format_version(0);
        if (InitialPQTabletConfig.HasCodecs()) {
            for (const auto& codecName : InitialPQTabletConfig.GetCodecs().GetCodecs()) {
                init->add_supported_codecs(CodecByName<UseMigrationProtocol>(codecName));
            }
        }
    } else {
        //init->set_session_id(EscapeC(OwnerCookie));
        if (maxSeqNo.Defined()) {
            init->set_last_seq_no(*maxSeqNo);
        }
        init->set_partition_id(Partition);
        if (InitialPQTabletConfig.HasCodecs()) {
            for (const auto& codecName : InitialPQTabletConfig.GetCodecs().GetCodecs()) {
                init->mutable_supported_codecs()->add_codecs(CodecByName<UseMigrationProtocol>(codecName));
            }
        }
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session inited cookie: " << Cookie << " partition: " << Partition
                                                      << " MaxSeqNo: " << maxSeqNo << " sessionId: " << OwnerCookie);

    if (!Request->GetStreamCtx()->Write(std::move(response))) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc write failed");
        Die(ctx);
        return;
    }

    State = ES_INITED;

    ctx.Schedule(TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()), new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));

    //init completed; wait for first data chunk
    NextRequestInited = true;
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
        Die(ctx);
        return;
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx) {
    const auto& result = *ev->Get();

    if (State != ES_WAIT_WRITER_INIT) {
        return CloseSession("got init result but not wait for it", PersQueue::ErrorCode::ERROR, ctx);
    }

    Y_ABORT_UNLESS(!result.SessionId && !result.TxId);

    if (!result.IsSuccess()) {
        const auto& error = result.GetError();
        if (error.Response.HasErrorCode()) {
            return CloseSession("status is not ok: " + error.Response.GetErrorReason(), ConvertOldCode(error.Response.GetErrorCode()), ctx);
        } else {
            return CloseSession("error at writer init: " + error.Reason, PersQueue::ErrorCode::ERROR, ctx);
        }
    }

    OwnerCookie = result.GetResult().OwnerCookie;

    const auto& maxSeqNo = result.GetResult().SourceIdInfo.GetSeqNo();

    // ToDo: uncomment after fixing KIKIMR-21124
    // if (!UseDeduplication) {
    //     if (maxSeqNo != 0) {
    //         return CloseSession("Internal server error: have maxSeqNo != with deduplication disabled",
    //                             PersQueue::ErrorCode::ERROR, ctx);
    //     }
    // }

    OwnerCookie = result.GetResult().OwnerCookie;
    MakeAndSentInitResponse(maxSeqNo, ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_INITED) {
        return CloseSession("got write permission but not wait for it", PersQueue::ErrorCode::ERROR, ctx);
    }

    Y_ABORT_UNLESS(!SentRequests.empty());
    auto writeRequest = std::move(SentRequests.front());

    if (ev->Get()->Cookie != writeRequest->Cookie) {
        return CloseSession("out of order reserve bytes response from server, may be previous is lost", PersQueue::ErrorCode::ERROR, ctx);
    }

    SentRequests.pop_front();

    ui64 diff = writeRequest->ByteSize;

    AcceptedRequests.emplace_back(std::move(writeRequest));

    BytesInflight_ -= diff;
    if (BytesInflight) {
        BytesInflight.Dec(diff);
    }
    if (!NextRequestInited && BytesInflight_ < MAX_BYTES_INFLIGHT) { //allow only one big request to be readed but not sended
        NextRequestInited = true;
        if (!Request->GetStreamCtx()->Read()) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
            Die(ctx);
            return;
        }
    }

    if (!IsQuotaRequired() && !PendingRequests.empty()) {
        SendWriteRequest(std::move(PendingRequests.front()), ctx);
        PendingRequests.pop_front();
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::ProcessWriteResponse(
        const NKikimrClient::TPersQueuePartitionResponse& response, const TActorContext& ctx
) {
    auto writeRequest = std::move(AcceptedRequests.front());
    AcceptedRequests.pop_front();

    auto addAckMigration = [this](
            const TPersQueuePartitionResponse::TCmdWriteResult& res,
            StreamingWriteServerMessage::BatchWriteResponse* batchWriteResponse,
            StreamingWriteServerMessage::WriteStatistics* stat) {

        batchWriteResponse->add_sequence_numbers(res.GetSeqNo());
        batchWriteResponse->add_offsets(res.GetOffset());
        if (!UseDeduplication) {
            Y_ABORT_UNLESS(!res.GetAlreadyWritten());
        }
        batchWriteResponse->add_already_written(res.GetAlreadyWritten());
        stat->set_queued_in_partition_duration_ms(
                Max((i64)res.GetTotalTimeInPartitionQueueMs(), stat->queued_in_partition_duration_ms()));
        stat->set_throttled_on_partition_duration_ms(
                Max((i64)res.GetPartitionQuotedTimeMs(), stat->throttled_on_partition_duration_ms()));
        stat->set_throttled_on_topic_duration_ms(Max(static_cast<i64>(res.GetTopicQuotedTimeMs()), stat->throttled_on_topic_duration_ms()));
        stat->set_persist_duration_ms(
                Max((i64)res.GetWriteTimeMs(), stat->persist_duration_ms()));
    };

    auto addAck = [this](const TPersQueuePartitionResponse::TCmdWriteResult& res,
                         Topic::StreamWriteMessage::WriteResponse* writeResponse,
                         Topic::StreamWriteMessage::WriteResponse::WriteStatistics* stat) {
        auto ack = writeResponse->add_acks();
        // TODO (ildar-khisam@): validate res before filling ack fields
        ack->set_seq_no(res.GetSeqNo());
        if (res.GetAlreadyWritten()) {
            Y_ABORT_UNLESS(UseDeduplication);
            ack->mutable_skipped()->set_reason(Topic::StreamWriteMessage::WriteResponse::WriteAck::Skipped::REASON_ALREADY_WRITTEN);
        } else if (res.GetWrittenInTx()) {
            ack->mutable_written_in_tx();
        } else {
            ack->mutable_written()->set_offset(res.GetOffset());
        }

        using ::google::protobuf::Duration;
        using ::google::protobuf::util::TimeUtil;

        auto persisting_time_ms = Max<i64>(res.GetWriteTimeMs(), TimeUtil::DurationToMilliseconds(stat->persisting_time()));
        *stat->mutable_persisting_time() = TimeUtil::MillisecondsToDuration(persisting_time_ms);

        auto min_queue_wait_time_ms = (stat->min_queue_wait_time() == Duration())
                                      ? (i64)res.GetTotalTimeInPartitionQueueMs()
                                      : Min<i64>(res.GetTotalTimeInPartitionQueueMs(), TimeUtil::DurationToMilliseconds(stat->min_queue_wait_time()));
        *stat->mutable_min_queue_wait_time() = TimeUtil::MillisecondsToDuration(min_queue_wait_time_ms);

        auto max_queue_wait_time_ms = Max<i64>(res.GetTotalTimeInPartitionQueueMs(), TimeUtil::DurationToMilliseconds(stat->max_queue_wait_time()));
        *stat->mutable_max_queue_wait_time() = TimeUtil::MillisecondsToDuration(max_queue_wait_time_ms);

        auto partition_quota_wait_time_ms = Max<i64>(res.GetPartitionQuotedTimeMs(), TimeUtil::DurationToMilliseconds(stat->partition_quota_wait_time()));
        *stat->mutable_partition_quota_wait_time() = TimeUtil::MillisecondsToDuration(partition_quota_wait_time_ms);

        auto topic_quota_wait_time_ms = Max<i64>(res.GetTopicQuotedTimeMs(), TimeUtil::DurationToMilliseconds(stat->topic_quota_wait_time()));
        *stat->mutable_topic_quota_wait_time() = TimeUtil::MillisecondsToDuration(topic_quota_wait_time_ms);
    };

    ui32 partitionCmdWriteResultIndex = 0;
    // TODO: Send single batch write response for all user write requests up to some max size/count
    for (const auto& userWriteRequest : writeRequest->UserWriteRequests) {
        TServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);

        if constexpr (UseMigrationProtocol) {
            auto batchWriteResponse = result.mutable_batch_write_response();
            batchWriteResponse->set_partition_id(Partition);

            for (size_t messageIndex = 0, endIndex = userWriteRequest->Request.write_request().sequence_numbers_size();
                    messageIndex != endIndex; ++messageIndex) {

                if (partitionCmdWriteResultIndex == response.CmdWriteResultSize()) {
                    CloseSession("too few responses from server", PersQueue::ErrorCode::ERROR, ctx);
                    return;
                }
                const auto& partitionCmdWriteResult = response.GetCmdWriteResult(partitionCmdWriteResultIndex);
                const auto writtenSequenceNumber = userWriteRequest->Request.write_request().sequence_numbers(messageIndex);
                if (UseDeduplication && partitionCmdWriteResult.GetSeqNo() != writtenSequenceNumber) {
                    CloseSession(TStringBuilder() << "Expected partition " << Partition << " write result for message with sequence number " << writtenSequenceNumber << " but got for " << partitionCmdWriteResult.GetSeqNo(), PersQueue::ErrorCode::ERROR, ctx);
                    return;
                }

                addAckMigration(partitionCmdWriteResult, batchWriteResponse, batchWriteResponse->mutable_write_statistics());
                ++partitionCmdWriteResultIndex;
            }

        } else {
            auto batchWriteResponse = result.mutable_write_response();
            batchWriteResponse->set_partition_id(Partition);

            for (size_t messageIndex = 0, endIndex = userWriteRequest->Request.write_request().messages_size();
                    messageIndex != endIndex; ++messageIndex) {

                if (partitionCmdWriteResultIndex == response.CmdWriteResultSize()) {
                    CloseSession("too few responses from server", PersQueue::ErrorCode::ERROR, ctx);
                    return;
                }
                const auto& partitionCmdWriteResult = response.GetCmdWriteResult(partitionCmdWriteResultIndex);
                const auto writtenSequenceNumber = userWriteRequest->Request.write_request().messages(messageIndex).seq_no();
                if (UseDeduplication && partitionCmdWriteResult.GetSeqNo() != writtenSequenceNumber) {
                    CloseSession(TStringBuilder() << "Expected partition " << Partition
                                                  << " write result for message with sequence number "
                                                  << writtenSequenceNumber << " but got for "
                                                  << partitionCmdWriteResult.GetSeqNo(), PersQueue::ErrorCode::ERROR, ctx);
                    return;
                }

                addAck(partitionCmdWriteResult, batchWriteResponse, batchWriteResponse->mutable_write_statistics());
                ++partitionCmdWriteResultIndex;
            }

        }

        if (!Request->GetStreamCtx()->Write(std::move(result))) {
            // TODO: Log gRPC write error code
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc write failed");
            Die(ctx);
            return;
        }
    }

    ui64 diff = writeRequest->ByteSize;

    BytesInflightTotal_ -= diff;
    if (BytesInflightTotal) {
        BytesInflightTotal.Dec(diff);
    }

    CheckFinish(ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& result = *ev->Get();
    if (!result.IsSuccess()) {
        const auto& record = result.Record;
        if (record.HasErrorCode()) {
            return CloseSession("status is not ok: " + record.GetErrorReason(), ConvertOldCode(record.GetErrorCode()), ctx);
        } else {
            return CloseSession("error at write: " + result.GetError().Reason, PersQueue::ErrorCode::ERROR, ctx);
        }
    }

    if (State != ES_INITED) {
        return CloseSession(TStringBuilder() << "got write response but not wait for it (" << static_cast<int>(State) << ")", PersQueue::ErrorCode::ERROR, ctx);
    }

    if (AcceptedRequests.empty()) {
        return CloseSession("got too many replies from server, internal error", PersQueue::ErrorCode::ERROR, ctx);
    }

    const auto& writeRequest = AcceptedRequests.front();
    const auto& resp = result.Record.GetPartitionResponse();

    if (resp.GetCookie() != writeRequest->Cookie) {
        return CloseSession("out of order write response from server, may be previous is lost", PersQueue::ErrorCode::ERROR, ctx);
    }

    ProcessWriteResponse(resp, ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx) {
    CloseSession(TStringBuilder() << "pipe to partition's " << Partition << " tablet is dead #" << static_cast<int>(ev->Get()->ErrorCode),
             PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED, ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    //TODO: add here retries for connecting to PQRB
    if (msg->Status != NKikimrProto::OK) {
        CloseSession(TStringBuilder() << "pipe to tablet is dead " << msg->TabletId, PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED, ctx);
        return;
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    //TODO: add here retries for connecting to PQRB
    CloseSession(TStringBuilder() << "pipe to tablet is dead " << ev->Get()->TabletId, PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED, ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::PrepareRequest(THolder<TEvWrite>&& ev, const TActorContext& ctx) {
    const auto& writeRequest = ev->Request.write_request();

    if constexpr (!UseMigrationProtocol) {
        if (writeRequest.has_tx() && !AppData(ctx)->FeatureFlags.GetEnableTopicServiceTx()) {
            CloseSession("Disabled transaction support for TopicService.",
                         PersQueue::ErrorCode::ERROR, ctx);
            return;
        }
    }

    if (PendingRequests.empty()) {
        PendingRequests.emplace_back(new TWriteRequestInfo(++NextRequestCookie));
    } else if constexpr (!UseMigrationProtocol) {
        Y_ABORT_UNLESS(!PendingRequests.back()->UserWriteRequests.empty());

        auto& last = PendingRequests.back()->UserWriteRequests.back()->Request.write_request();

        if (writeRequest.has_tx()) {
            if (last.has_tx()) {
                if ((writeRequest.tx().session() != last.tx().session()) ||
                    (writeRequest.tx().id() != last.tx().id())) {
                    PendingRequests.emplace_back(new TWriteRequestInfo(++NextRequestCookie));
                }
            } else {
                PendingRequests.emplace_back(new TWriteRequestInfo(++NextRequestCookie));
            }
        } else if (last.has_tx()) {
            PendingRequests.emplace_back(new TWriteRequestInfo(++NextRequestCookie));
        }
    }

    auto pendingRequest = PendingRequests.back();
    auto& request = pendingRequest->PartitionWriteRequest->Record;
    ui64 payloadSize = 0;

    auto addDataMigration = [&](const StreamingWriteClientMessage::WriteRequest& writeRequest, const i32 messageIndex) {
        auto w = request.MutablePartitionRequest()->AddCmdWrite();
        w->SetData(GetSerializedData(InitMeta, writeRequest, messageIndex));
        if (UseDeduplication) {
            w->SetSourceId(NPQ::NSourceIdEncoding::EncodeSimple(SourceId));
        }
        w->SetSeqNo(writeRequest.sequence_numbers(messageIndex));
        if (!UseDeduplication)
            SeqNoInflight.push_back(w->GetSeqNo());

        w->SetCreateTimeMS(writeRequest.created_at_ms(messageIndex));
        w->SetUncompressedSize(writeRequest.blocks_uncompressed_sizes(messageIndex));
        w->SetClientDC(ClientDC);
        w->SetIgnoreQuotaDeadline(true);
        payloadSize += w->GetData().size() + w->GetSourceId().size();
    };

    ui64 maxMessageMetadataSize = 0;
    auto addData = [&](const Topic::StreamWriteMessage::WriteRequest& writeRequest, const i32 messageIndex) {
        const auto& msg = writeRequest.messages(messageIndex);

        auto w = request.MutablePartitionRequest()->AddCmdWrite();
        w->SetData(GetSerializedData(InitMeta, writeRequest, messageIndex));
        if (UseDeduplication) {
            w->SetSourceId(NPQ::NSourceIdEncoding::EncodeSimple(SourceId));
        } else {
            w->SetDisableDeduplication(true);
        }
        w->SetSeqNo(msg.seq_no());
        SeqNoInflight.push_back(w->GetSeqNo());
        w->SetCreateTimeMS(::google::protobuf::util::TimeUtil::TimestampToMilliseconds(msg.created_at()));
        w->SetUncompressedSize(msg.uncompressed_size());
        w->SetClientDC(ClientDC);
        w->SetIgnoreQuotaDeadline(true);

        payloadSize += w->GetData().size() + w->GetSourceId().size();

        ui64 currMetadataSize = 0;
        for (const auto& metaItem : msg.metadata_items()) {
            currMetadataSize += metaItem.key().size() + metaItem.value().size();
        }
        maxMessageMetadataSize = std::max(maxMessageMetadataSize, currMetadataSize);
    };

    if constexpr (UseMigrationProtocol) {
        for (i32 messageIndex = 0; messageIndex != writeRequest.sequence_numbers_size(); ++messageIndex) {
            addDataMigration(writeRequest, messageIndex);
        }
    } else {
        for (i32 messageIndex = 0; messageIndex != writeRequest.messages_size(); ++messageIndex) {
            addData(writeRequest, messageIndex);
        }
    }

    pendingRequest->UserWriteRequests.push_back(std::move(ev));
    pendingRequest->ByteSize = request.ByteSize();

    auto msgMetaEnabled = AppData(ctx)->FeatureFlags.GetEnableTopicMessageMeta();
    if (!msgMetaEnabled && maxMessageMetadataSize > 0) {
        CloseSession("Message level metadata support is disabled on server size", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (maxMessageMetadataSize > MAX_METADATA_SIZE_PER_MESSAGE) {
        CloseSession(
                TStringBuilder() << "Message level metadata size is limited to " << MAX_METADATA_SIZE_PER_MESSAGE
                                 << " per message",
                PersQueue::ErrorCode::BAD_REQUEST, ctx
        );
        return;
    }

    if (const auto ru = CalcRuConsumption(payloadSize)) {
        pendingRequest->RequiredQuota += ru;

        if (!PendingQuotaRequest) {
            if (MaybeRequestQuota(PendingRequests.front()->RequiredQuota, EWakeupTag::RlAllowed, ctx)) {
                PendingQuotaRequest = std::move(PendingRequests.front());
                PendingRequests.pop_front();
            }
        }
    } else {
        if (!PendingQuotaRequest) {
            SendWriteRequest(std::move(PendingRequests.front()), ctx);
            PendingRequests.pop_front();
        }
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::SendWriteRequest(typename TWriteRequestInfo::TPtr&& request, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(request->PartitionWriteRequest);

    i64 diff = 0;
    for (const auto& w : request->UserWriteRequests) {
        diff -= w->Request.ByteSize();
    }

    Y_ABORT_UNLESS(-diff <= (i64)BytesInflight_);
    diff += request->PartitionWriteRequest->Record.ByteSize();

    BytesInflight_ += diff;
    BytesInflightTotal_ += diff;
    if (BytesInflight && BytesInflightTotal) {
        BytesInflight.Inc(diff);
        BytesInflightTotal.Inc(diff);
    }

    auto [sessionId, txId] = request->GetTransactionId();
    auto event =
        std::make_unique<NPQ::TEvPartitionWriter::TEvTxWriteRequest>(sessionId, txId,
                                                                     std::move(request->PartitionWriteRequest));

    ctx.Send(PartitionWriterCache, std::move(event));

    SentRequests.push_back(std::move(request));
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(typename TEvUpdateToken::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_INITED) {
        CloseSession("got 'update_token_request' but write session is not initialized", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (UpdateTokenInProgress) {
        CloseSession("got another 'update_token_request' while previous still in progress, only single token update is allowed at a time", PersQueue::ErrorCode::OVERLOAD, ctx);
        return;
    }

    const auto& token = ev->Get()->Request.update_token_request().token();
    if (token == Auth || (token.empty() && !AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol())) {
        // Got same token or empty token with no non-empty token requirement, do not trigger any checks
        TServerMessage serverMessage;
        serverMessage.set_status(Ydb::StatusIds::SUCCESS);
        serverMessage.mutable_update_token_response();
        if (!Request->GetStreamCtx()->Write(std::move(serverMessage))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc write failed");
            Die(ctx);
            return;
        }
    }
    else if (token.empty()) {
        Request->ReplyUnauthenticated("'token' in 'update_token_request' is empty");
        Die(ctx);
        return;
    }
    else {
        UpdateTokenInProgress = true;
        UpdateTokenAuthenticated = false;
        Auth = token;
        Request->RefreshToken(Auth, ctx, ctx.SelfID);
    }

    NextRequestInited = true;
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
        Die(ctx);
        return;
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr &ev , const TActorContext& ctx) {
    Y_UNUSED(ctx);
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "updating token");

    if (ev->Get()->Authenticated && ev->Get()->InternalToken && !ev->Get()->InternalToken->GetSerializedToken().empty()) {
        Token = ev->Get()->InternalToken;
        Request->SetInternalToken(ev->Get()->InternalToken);
        UpdateTokenAuthenticated = true;
        if (!ACLCheckInProgress) {
            InitCheckSchema(ctx);
        }
    } else {
        if (ev->Get()->Retryable) {
            TServerMessage serverMessage;
            serverMessage.set_status(Ydb::StatusIds::UNAVAILABLE);
            Request->GetStreamCtx()->WriteAndFinish(std::move(serverMessage), grpc::Status::OK);
        } else {
            Request->RaiseIssues(ev->Get()->Issues);
            Request->ReplyUnauthenticated("refreshed token is invalid");
        }
        Die(ctx);
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(typename TEvWrite::TPtr& ev, const TActorContext& ctx) {

    RequestNotChecked = true;

    if (State != ES_INITED) {
        //answer error
        CloseSession("write in not inited session", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    const auto& writeRequest = ev->Get()->Request.write_request();

    if constexpr (UseMigrationProtocol) {
    if (!AllEqual(writeRequest.sequence_numbers_size(), writeRequest.created_at_ms_size(), writeRequest.sent_at_ms_size(), writeRequest.message_sizes_size())) {
        CloseSession(TStringBuilder() << "messages meta repeated fields do not have same size, 'sequence_numbers' size is " << writeRequest.sequence_numbers_size()
            << ", 'message_sizes' size is " << writeRequest.message_sizes_size() << ", 'created_at_ms' size is " << writeRequest.created_at_ms_size()
            << " and 'sent_at_ms' size is " << writeRequest.sent_at_ms_size(), PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (!AllEqual(writeRequest.blocks_offsets_size(), writeRequest.blocks_part_numbers_size(), writeRequest.blocks_message_counts_size(), writeRequest.blocks_uncompressed_sizes_size(), writeRequest.blocks_headers_size(), writeRequest.blocks_data_size())) {
        CloseSession(TStringBuilder() << "blocks repeated fields do no have same size, 'blocks_offsets' size is " << writeRequest.blocks_offsets_size()
            << ", 'blocks_part_numbers' size is " << writeRequest.blocks_part_numbers_size() << ", 'blocks_message_counts' size is " << writeRequest.blocks_message_counts_size()
            << ", 'blocks_uncompressed_sizes' size is " << writeRequest.blocks_uncompressed_sizes_size() << ", 'blocks_headers' size is " << writeRequest.blocks_headers_size()
            << " and 'blocks_data' size is " << writeRequest.blocks_data_size(), PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    const i32 messageCount = writeRequest.sequence_numbers_size();
    const i32 blockCount = writeRequest.blocks_offsets_size();
    if (messageCount == 0) {
        CloseSession(TStringBuilder() << "messages meta repeated fields are empty, write request contains no messages", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (messageCount != blockCount) {
        CloseSession(TStringBuilder() << "messages meta repeated fields and blocks repeated fields do not have same size, messages meta fields size is " << messageCount
            << " and blocks fields size is " << blockCount << ", only one message per block is supported in blocks format version 0", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    auto dataCheck = [&](const StreamingWriteClientMessage::WriteRequest& data, const i32 messageIndex) -> bool {
        if (data.sequence_numbers(messageIndex) <= 0) {
            CloseSession(TStringBuilder() << "bad write request - 'sequence_numbers' items must be greater than 0. Value at position " << messageIndex << " is " << data.sequence_numbers(messageIndex), PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return false;
        }

        if (messageIndex > 0 && data.sequence_numbers(messageIndex) <= data.sequence_numbers(messageIndex - 1)) {
            CloseSession(TStringBuilder() << "bad write request - 'sequence_numbers' are unsorted. Value " << data.sequence_numbers(messageIndex) << " at position " << messageIndex
                << " is less than or equal to value " << data.sequence_numbers(messageIndex - 1) << " at position " << (messageIndex - 1), PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return false;
        }

        if (data.blocks_headers(messageIndex).size() != CODEC_ID_SIZE) {
            CloseSession(TStringBuilder() << "bad write request - 'blocks_headers' at position " << messageIndex <<  " has incorrect size " << data.blocks_headers(messageIndex).size() << " [B]. Only headers of size " << CODEC_ID_SIZE << " [B] (with codec identifier) are supported in block format version 0", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return false;
        }

        const char& codecID = data.blocks_headers(messageIndex).front();
        TString error;
        if (!ValidateWriteWithCodec(InitialPQTabletConfig, codecID, error)) {
            CloseSession(TStringBuilder() << "bad write request - 'blocks_headers' at position " << messageIndex << " is invalid: " << error, PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return false;
        }
        ui32 intCodec = ((ui32)codecID + 1) < CodecCounters.size() ? ((ui32)codecID + 1) : 0;
        if (CodecCounters.size() > intCodec) {
            CodecCounters[intCodec].Inc();
        }

        if (data.blocks_message_counts(messageIndex) != 1) {
            CloseSession(TStringBuilder() << "bad write request - 'blocks_message_counts' at position " << messageIndex << " is " << data.blocks_message_counts(messageIndex)
                                          << ", only single message per block is supported by block format version 0", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return false;
        }
        return true;
    };
    for (i32 messageIndex = 0; messageIndex != messageCount; ++messageIndex) {
        if (!dataCheck(writeRequest, messageIndex)) {
            return;
        }
    }

    } else {
        const i32 messageCount = writeRequest.messages_size();
        if (messageCount == 0) {
            CloseSession(TStringBuilder() << "messages meta repeated fields are empty, write request contains no messages", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        auto dataCheck = [&](const Topic::StreamWriteMessage::WriteRequest& data, const i32 messageIndex) -> bool {
            if (data.messages(messageIndex).seq_no() <= 0) {
                CloseSession(TStringBuilder() << "bad write request - sequence number must be greater than 0. Value at position " << messageIndex << " has seq_no " << data.messages(messageIndex).seq_no(), PersQueue::ErrorCode::BAD_REQUEST, ctx);
                return false;
            }

            if (messageIndex > 0 && data.messages(messageIndex).seq_no() <= data.messages(messageIndex - 1).seq_no()) {
                CloseSession(TStringBuilder() << "bad write request - sequence numbers are unsorted. Value " << data.messages(messageIndex).seq_no() << " at position " << messageIndex
                    << " is less than or equal to value " << data.messages(messageIndex - 1).seq_no() << " at position " << (messageIndex - 1), PersQueue::ErrorCode::BAD_REQUEST, ctx);
                return false;
            }

            const ui32 codecID = data.codec();
            TString error = "unspecified (id 0)";
            if (codecID == 0 || !ValidateWriteWithCodec(InitialPQTabletConfig, codecID - 1, error)) {
                CloseSession(TStringBuilder() << "bad write request - codec is invalid: " << error, PersQueue::ErrorCode::BAD_REQUEST, ctx);
                return false;
            }
            ui32 intCodec = codecID < CodecCounters.size() ? codecID : 0;
            if (CodecCounters.size() > intCodec) {
                CodecCounters[intCodec].Inc();
            }

            return true;
        };
        for (i32 messageIndex = 0; messageIndex != messageCount; ++messageIndex) {
            if (!dataCheck(writeRequest, messageIndex)) {
                return;
            }
        }

    }

    ui64 diff = ev->Get()->Request.ByteSize();
    BytesInflight_ += diff;
    BytesInflightTotal_ += diff;
    if (BytesInflight && BytesInflightTotal) {
        BytesInflight.Inc(diff);
        BytesInflightTotal.Inc(diff);
    }

    if (BytesInflight_ < MAX_BYTES_INFLIGHT) { //allow only one big request to be readed but not sended
        Y_ABORT_UNLESS(NextRequestInited);
        if (!Request->GetStreamCtx()->Read()) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
            Die(ctx);
            return;

        }
    } else {
        NextRequestInited = false;
    }

    PrepareRequest(THolder<TEvWrite>(ev->Release()), ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::LogSession(const TActorContext& ctx) {
    TString topic_path = [this]() {
        if constexpr (UseMigrationProtocol) {
            return InitRequest.topic();
        } else {
            return InitRequest.path();
        }
    }();
    LOG_INFO_S(
            ctx, NKikimrServices::PQ_WRITE_PROXY,
            "write session:  cookie=" << Cookie << " sessionId=" << OwnerCookie << " userAgent=\"" << UserAgent
                                      << "\" ip=" << PeerName << " proto=" << ProtoName << " "
                                      << " topic=" << topic_path
                                      << " durationSec=" << (ctx.Now() - StartTime).Seconds()
    );

    LogSessionDeadline = ctx.Now() + TDuration::Hours(1) + TDuration::Seconds(rand() % 60);
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
    const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
    OnWakeup(tag);
    switch (tag) {
        case EWakeupTag::RlInit:
            return CheckACL(ctx);

        case EWakeupTag::RecheckAcl:
            return RecheckACL(ctx);

        case EWakeupTag::RlAllowed: {
            if (auto counters = Request->GetCounters()) {
                counters->AddConsumedRequestUnits(PendingQuotaRequest->RequiredQuota);
            }

            SendWriteRequest(std::move(PendingQuotaRequest), ctx);

            if (!PendingRequests.empty()) {
                Y_ABORT_UNLESS(MaybeRequestQuota(PendingRequests.front()->RequiredQuota, EWakeupTag::RlAllowed, ctx));
                PendingQuotaRequest = std::move(PendingRequests.front());
                PendingRequests.pop_front();
            }

            break;
        }

        case EWakeupTag::RlNoResource:
        case EWakeupTag::RlInitNoResource:
            if (PendingQuotaRequest) {
                Y_ABORT_UNLESS(MaybeRequestQuota(PendingQuotaRequest->RequiredQuota, EWakeupTag::RlAllowed, ctx));
            } else {
                return CloseSession("Throughput limit exceeded", PersQueue::ErrorCode::OVERLOAD, ctx);
            }
            break;
    }
}

template<bool UseMigrationProtocol>
void TWriteSessionActor<UseMigrationProtocol>::RecheckACL(const TActorContext& ctx) {
    if (State != ES_INITED) {
        LOG_ERROR_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "WriteSessionActor state is wrong. Actual state '" << (int)State << "'");
        return CloseSession("erroneous internal state", PersQueue::ErrorCode::ERROR, ctx);
    }

    auto now = ctx.Now();

    ctx.Schedule(TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()), new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));
    if (Token && !ACLCheckInProgress && RequestNotChecked && (now - LastACLCheckTimestamp > TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()))) {
        RequestNotChecked = false;
        InitCheckSchema(ctx);
    }

    if (PartitionChooser && now > LastSourceIdUpdate) {
        ctx.Send(PartitionChooser, new NPQ::TEvPartitionChooser::TEvRefreshRequest());
        LastSourceIdUpdate = now + SOURCEID_UPDATE_PERIOD;
    }
    if (now >= LogSessionDeadline) {
        LogSession(ctx);
    }
}

//explicit instantation
template class TWriteSessionActor<true>;
template class TWriteSessionActor<false>;

}
}
