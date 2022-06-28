#include "write_session_actor.h"
#include <ydb/library/persqueue/topic_parser/counters.h>

#include "codecs.h"

#include <ydb/core/persqueue/pq_database.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/services/lib/sharding/sharding.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/string/hex.h>
#include <util/string/vector.h>
#include <util/string/escape.h>
#include <util/string/printf.h>

using namespace NActors;
using namespace NKikimrClient;



namespace NKikimr {
using namespace NSchemeCache;
using namespace NPQ;

Ydb::PersQueue::V1::Codec CodecByName(const TString& codec) {
    static const THashMap<TString, Ydb::PersQueue::V1::Codec> codecsByName = {
        { "raw",  Ydb::PersQueue::V1::CODEC_RAW  },
        { "gzip", Ydb::PersQueue::V1::CODEC_GZIP },
        { "lzop", Ydb::PersQueue::V1::CODEC_LZOP },
        { "zstd", Ydb::PersQueue::V1::CODEC_ZSTD },
    };
    auto codecIt = codecsByName.find(codec);
    return codecIt != codecsByName.end() ? codecIt->second : Ydb::PersQueue::V1::CODEC_UNSPECIFIED;
}

template <>
void FillExtraFieldsForDataChunk(
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
void FillChunkDataFromReq(
    NKikimrPQClient::TDataChunk& proto,
    const Ydb::PersQueue::V1::StreamingWriteClientMessage::WriteRequest& writeRequest,
    const i32 messageIndex
) {
    proto.SetSeqNo(writeRequest.sequence_numbers(messageIndex));
    proto.SetCreateTime(writeRequest.created_at_ms(messageIndex));
    proto.SetCodec(writeRequest.blocks_headers(messageIndex).front());
    proto.SetData(writeRequest.blocks_data(messageIndex));
}

namespace NGRpcProxy {
namespace V1 {

using namespace Ydb::PersQueue::V1;

static const ui32 MAX_RESERVE_REQUESTS_INFLIGHT = 5;

static const ui32 MAX_BYTES_INFLIGHT = 1_MB;
static const TDuration SOURCEID_UPDATE_PERIOD = TDuration::Hours(1);

//TODO: add here tracking of bytes in/out


TWriteSessionActor::TWriteSessionActor(
        NKikimr::NGRpcService::TEvStreamPQWriteRequest* request, const ui64 cookie,
        const NActors::TActorId& schemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsController
)
    : Request(request)
    , State(ES_CREATED)
    , SchemeCache(schemeCache)
    , PeerName("")
    , Cookie(cookie)
    , TopicsController(topicsController)
    , Partition(0)
    , PreferedPartition(Max<ui32>())
    , NumReserveBytesRequests(0)
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
    , BalancerTabletId(0)
    , ClientDC(clientDC ? *clientDC : "other")
    , LastSourceIdUpdate(TInstant::Zero())
    , SourceIdCreateTime(0)
    , SourceIdUpdatesInflight(0)
{
    Y_ASSERT(Request);
}


TWriteSessionActor::~TWriteSessionActor() = default;

void TWriteSessionActor::Bootstrap(const TActorContext& ctx) {

    Y_VERIFY(Request);
    SelectSourceIdQuery = GetSourceIdSelectQueryFromPath(AppData(ctx)->PQConfig.GetSourceIdTablePath());
    UpdateSourceIdQuery = GetUpdateIdSelectQueryFromPath(AppData(ctx)->PQConfig.GetSourceIdTablePath());

    Request->GetStreamCtx()->Attach(ctx.SelfID);
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "grpc read failed at start");
        Die(ctx);
        return;
    }
    Become(&TThis::StateFunc);
    StartTime = ctx.Now();
}


void TWriteSessionActor::HandleDone(const TActorContext& ctx) {

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc closed");
    Die(ctx);
}

TString WriteRequestToLog(const Ydb::PersQueue::V1::StreamingWriteClientMessage& proto) {
    switch (proto.client_message_case()) {
        case StreamingWriteClientMessage::kInitRequest:
            return proto.ShortDebugString();
            break;
        case StreamingWriteClientMessage::kWriteRequest:
            return " write_request[data omitted]";
            break;
        case StreamingWriteClientMessage::kUpdateTokenRequest:
            return " update_token_request [content omitted]";
        default:
            return TString();
    }
}

void TWriteSessionActor::Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read done: success: " << ev->Get()->Success << " data: " << WriteRequestToLog(ev->Get()->Record));
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
        ctx.Send(ctx.SelfID, new TEvPQProxy::TEvDone());
        return;
    }

    switch(ev->Get()->Record.client_message_case()) {
        case StreamingWriteClientMessage::kInitRequest:
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvWriteInit(std::move(ev->Get()->Record), Request->GetStreamCtx()->GetPeerName()));
            break;
        case StreamingWriteClientMessage::kWriteRequest:
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvWrite(std::move(ev->Get()->Record)));
            break;
        case StreamingWriteClientMessage::kUpdateTokenRequest: {
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvUpdateToken(std::move(ev->Get()->Record)));
            break;
        }
        case StreamingWriteClientMessage::CLIENT_MESSAGE_NOT_SET: {
            CloseSession("'client_message' is not set", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
    }
}


void TWriteSessionActor::Handle(IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc write failed");
        Die(ctx);
    }
}


void TWriteSessionActor::Die(const TActorContext& ctx) {
    if (Writer)
        ctx.Send(Writer, new TEvents::TEvPoisonPill());

    if (SessionsActive) {
        SessionsActive.Dec();
        BytesInflight.Dec(BytesInflight_);
        BytesInflightTotal.Dec(BytesInflightTotal_);
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " is DEAD");

    ctx.Send(GetPQWriteServiceActorID(), new TEvPQProxy::TEvSessionDead(Cookie));

    TActorBootstrapped<TWriteSessionActor>::Die(ctx);
}

void TWriteSessionActor::CheckFinish(const TActorContext& ctx) {
    if (!WritesDone)
        return;
    if (State != ES_INITED) {
        CloseSession("out of order Writes done before initialization", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (Writes.empty() && FormedWrites.empty() && SentMessages.empty()) {
        CloseSession("", PersQueue::ErrorCode::OK, ctx);
        return;
    }
}

void TWriteSessionActor::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext& ctx) {
    WritesDone = true;
    CheckFinish(ctx);
}

void TWriteSessionActor::CheckACL(const TActorContext& ctx) {
    //Y_VERIFY(ACLCheckInProgress);

    NACLib::EAccessRights rights = NACLib::EAccessRights::UpdateRow;

    Y_VERIFY(ACL);
    if (ACL->CheckAccess(rights, *Token)) {
        ACLCheckInProgress = false;
        if (FirstACLCheck) {
            FirstACLCheck = false;
            DiscoverPartition(ctx);
        }
        if (UpdateTokenInProgress && UpdateTokenAuthenticated) {
            UpdateTokenInProgress = false;
            StreamingWriteServerMessage serverMessage;
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

void TWriteSessionActor::Handle(TEvPQProxy::TEvWriteInit::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvPQProxy::TEvWriteInit> event(ev->Release());

    if (State != ES_CREATED) {
        //answer error
        CloseSession("got second init request",  PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    InitRequest = event->Request.init_request();

    if (InitRequest.topic().empty() || InitRequest.message_group_id().empty()) {
        CloseSession("no topic or message_group_id in init request",  PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    DiscoveryConverter = TopicsController.GetWriteTopicConverter(InitRequest.topic(), Request->GetDatabaseName().GetOrElse("/Root"));
    if (!DiscoveryConverter->IsValid()) {
        CloseSession(
                TStringBuilder() << "topic " << InitRequest.topic() << " could not be recognized: " << DiscoveryConverter->GetReason(),
                PersQueue::ErrorCode::BAD_REQUEST, ctx
        );
        return;
    }

    PeerName = event->PeerName;

    SourceId = InitRequest.message_group_id();

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session request cookie: " << Cookie << " " << InitRequest << " from " << PeerName);
    //TODO: get user agent from headers
    UserAgent = "pqv1 server";
    LogSession(ctx);

    if (Request->GetInternalToken().empty()) { // session without auth
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            Request->ReplyUnauthenticated("Unauthenticated access is forbidden, please provide credentials");
            Die(ctx);
            return;
        }
    }

    InitCheckSchema(ctx, true);

    PreferedPartition = InitRequest.partition_group_id() > 0 ? InitRequest.partition_group_id() - 1 : Max<ui32>();

    const auto& preferredCluster = InitRequest.preferred_cluster();
    if (!preferredCluster.empty()) {
        Send(GetPQWriteServiceActorID(), new TEvPQProxy::TEvSessionSetPreferredCluster(Cookie, preferredCluster));
    }
}

void TWriteSessionActor::InitAfterDiscovery(const TActorContext& ctx) {
    try {
        // "Bad" hash - in legacy mode calculated from full name ("rt3...", not short name);
        // Here we had a bug for all the time being and now have to keep compatibility was invalid hashes
        // Generally GetTopicForSrcIdHash for encoding. Do not copy-paste this line;
        EncodedSourceId = NSourceIdEncoding::EncodeSrcId(FullConverter->GetTopicForSrcId(), SourceId);

        // Good hash and proper way of calcultion;
        CompatibleHash = NSourceIdEncoding::EncodeSrcId(FullConverter->GetTopicForSrcIdHash(), SourceId).Hash;
    } catch (yexception& e) {
        CloseSession(TStringBuilder() << "incorrect sourceId \"" << SourceId << "\": " << e.what(),  PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    InitMeta = GetInitialDataChunk(InitRequest, FullConverter->GetClientsideName(), PeerName); // ToDo[migration] - check?

    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", FullConverter->GetAccount()}}, {"total"}}};

    SLITotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
    SLIErrors = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);
    SLITotal.Inc();

}

void TWriteSessionActor::SetupCounters()
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

    SessionsCreated.Inc();
    SessionsActive.Inc();
}

void TWriteSessionActor::SetupCounters(const TString& cloudId, const TString& dbId, const TString& folderId)
{
    if (SessionsCreated) {
        return;
    }

    //now topic is checked, can create group for real topic, not garbage
    auto subGroup = NPersQueue::GetCountersForStream(Counters);
    auto aggr = NPersQueue::GetLabelsForStream(FullConverter, cloudId, dbId, folderId);

    BytesInflight = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"stream.internal_write.bytes_proceeding"}, false, "name");
    BytesInflightTotal = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"stream.internal_write.bytes_proceeding_total"}, false, "name");
    SessionsCreated = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"stream.internal_write.sessions_created_per_second"}, true, "name");
    SessionsActive = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"stream.internal_write.sessions_active"}, false, "name");
    Errors = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"stream.internal_write.errors_per_second"}, true, "name");

    SessionsCreated.Inc();
    SessionsActive.Inc();
}

void TWriteSessionActor::InitCheckSchema(const TActorContext& ctx, bool needWaitSchema) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "init check schema");

    if (!needWaitSchema) {
        ACLCheckInProgress = true;
    }
    ctx.Send(SchemeCache, new TEvDescribeTopicsRequest({DiscoveryConverter}));
    if (needWaitSchema) {
        State = ES_WAIT_SCHEME_2;
    }
}

void TWriteSessionActor::Handle(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    auto& res = ev->Get()->Result;
    Y_VERIFY(res->ResultSet.size() == 1);

    auto& entry = res->ResultSet[0];
    TString errorReason;
    auto processResult = ProcessMetaCacheTopicResponse(entry);
    if (processResult.IsFatal) {
        CloseSession(processResult.Reason, processResult.ErrorCode, ctx);
        return;
    }
    Y_VERIFY(entry.PQGroupInfo); // checked at ProcessMetaCacheTopicResponse()
    auto& description = entry.PQGroupInfo->Description;
    Y_VERIFY(description.PartitionsSize() > 0);
    Y_VERIFY(description.HasPQTabletConfig());
    InitialPQTabletConfig = description.GetPQTabletConfig();
    if (!DiscoveryConverter->IsValid()) {
        errorReason = Sprintf("Internal server error with topic '%s', Marker# PQ503", DiscoveryConverter->GetPrintableString().c_str());
        CloseSession(errorReason, PersQueue::ErrorCode::ERROR, ctx);
        return;
    }
    FullConverter = DiscoveryConverter->UpgradeToFullConverter(InitialPQTabletConfig,
                                                               AppData(ctx)->PQConfig.GetTestDatabaseRoot());
    InitAfterDiscovery(ctx);

    BalancerTabletId = description.GetBalancerTabletID();

    for (ui32 i = 0; i < description.PartitionsSize(); ++i) {
        const auto& pi = description.GetPartitions(i);
        PartitionToTablet[pi.GetPartitionId()] = pi.GetTabletId();
    }

    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        const auto& tabletConfig = description.GetPQTabletConfig();
        SetupCounters(tabletConfig.GetYcCloudId(), tabletConfig.GetYdbDatabaseId(),
                      tabletConfig.GetYcFolderId());
    } else {
        SetupCounters();
    }

    Y_VERIFY(entry.SecurityObject);
    ACL.Reset(new TAclWrapper(entry.SecurityObject));
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " describe result for acl check");

    if (Request->GetInternalToken().empty()) { // session without auth
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            Request->ReplyUnauthenticated("Unauthenticated access is forbidden, please provide credentials");
            Die(ctx);
            return;
        }
        Y_VERIFY(FirstACLCheck);
        FirstACLCheck = false;
        DiscoverPartition(ctx);
    } else {
        Y_VERIFY(Request->GetYdbToken());
        Auth = *Request->GetYdbToken();

        Token = new NACLib::TUserToken(Request->GetInternalToken());
        CheckACL(ctx);
    }
}

void TWriteSessionActor::DiscoverPartition(const NActors::TActorContext& ctx) {

    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) { // ToDo[migration] - separate flag for having config tables
        auto partitionId = PreferedPartition < Max<ui32>() ? PreferedPartition
                                    : NKikimr::NDataStreams::V1::ShardFromDecimal(NKikimr::NDataStreams::V1::HexBytesToDecimal(MD5::Calc(SourceId)), PartitionToTablet.size());
        ProceedPartition(partitionId, ctx);
        return;
    }
    SendSelectPartitionRequest(CompatibleHash, FullConverter->GetClientsideName(), ctx);
    SendSelectPartitionRequest(EncodedSourceId.Hash, FullConverter->GetClientsideName(), ctx);
    State = ES_WAIT_TABLE_REQUEST_1;
}

void TWriteSessionActor::SendSelectPartitionRequest(ui32 hash, const TString& topic, const NActors::TActorContext& ctx) {
    //read from DS
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    ev->Record.MutableRequest()->SetKeepSession(false);
    ev->Record.MutableRequest()->SetQuery(SelectSourceIdQuery);
    ev->Record.MutableRequest()->SetDatabase(NKikimr::NPQ::GetDatabaseFromConfig(AppData(ctx)->PQConfig));
    // fill tx settings: set commit tx flag & begin new serializable tx.
    ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    // keep compiled query in cache.
    ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
    NClient::TParameters parameters;

    parameters["$Hash"] = hash;
    parameters["$Topic"] = topic;
    parameters["$SourceId"] = EncodedSourceId.EscapedSourceId;

    ev->Record.MutableRequest()->MutableParameters()->Swap(&parameters);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    SelectSrcIdsInflight++;
}


void TWriteSessionActor::UpdatePartition(const TActorContext& ctx) {
    Y_VERIFY(State == ES_WAIT_TABLE_REQUEST_1 || State == ES_WAIT_NEXT_PARTITION);
    SendUpdateSrcIdsRequests(ctx);
    State = ES_WAIT_TABLE_REQUEST_2;
}

void TWriteSessionActor::RequestNextPartition(const TActorContext& ctx) {
    Y_VERIFY(State == ES_WAIT_TABLE_REQUEST_1);
    State = ES_WAIT_NEXT_PARTITION;
    THolder<TEvPersQueue::TEvGetPartitionIdForWrite> x(new TEvPersQueue::TEvGetPartitionIdForWrite);
    Y_VERIFY(!PipeToBalancer);
    Y_VERIFY(BalancerTabletId);
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
    PipeToBalancer = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, BalancerTabletId, clientConfig));

    NTabletPipe::SendData(ctx, PipeToBalancer, x.Release());
}

void TWriteSessionActor::Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(State == ES_WAIT_NEXT_PARTITION);
    Partition = ev->Get()->Record.GetPartitionId();
    UpdatePartition(ctx);
}

void TWriteSessionActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record.GetRef();

    if (record.GetYdbStatus() == Ydb::StatusIds::ABORTED) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " messageGroupId "
            << SourceId << " escaped " << EncodedSourceId.EscapedSourceId << " discover partition race, retrying");
        DiscoverPartition(ctx);
        return;
    }

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        TStringBuilder errorReason;
        errorReason << "internal error in kqp Marker# PQ50 : " <<  record;
        if (State == EState::ES_INITED) {
            LOG_WARN_S(ctx, NKikimrServices::PQ_WRITE_PROXY, errorReason);
            SourceIdUpdatesInflight--;
        } else {
            CloseSession(errorReason, PersQueue::ErrorCode::ERROR, ctx);
        }
        return;
    }

    if (State == EState::ES_WAIT_TABLE_REQUEST_1) {
        SelectSrcIdsInflight--;
        auto& t = record.GetResponse().GetResults(0).GetValue().GetStruct(0);

        if (t.ListSize() != 0) {
            auto& tt = t.GetList(0).GetStruct(0);
            if (tt.HasOptional() && tt.GetOptional().HasUint32()) { //already got partition
                auto accessTime = t.GetList(0).GetStruct(2).GetOptional().GetUint64();
                if (accessTime > MaxSrcIdAccessTime) { // AccessTime
                    Partition = tt.GetOptional().GetUint32();
                    PartitionFound = true;
                    SourceIdCreateTime = t.GetList(0).GetStruct(1).GetOptional().GetUint64();
                    MaxSrcIdAccessTime = accessTime;
                }
            }
        }
        if (SelectSrcIdsInflight != 0) {
            return;
        }
        if (PartitionFound && PreferedPartition < Max<ui32>() && Partition != PreferedPartition) {
            CloseSession(TStringBuilder() << "MessageGroupId " << SourceId << " is already bound to PartitionGroupId " << (Partition + 1) << ", but client provided " << (PreferedPartition + 1) << ". MessageGroupId->PartitionGroupId binding cannot be changed, either use another MessageGroupId, specify PartitionGroupId " << (Partition + 1) << ", or do not specify PartitionGroupId at all.",
                         PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        if (SourceIdCreateTime == 0) {
            SourceIdCreateTime = TInstant::Now().MilliSeconds();
        }

        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " messageGroupId "
            << SourceId << " escaped " << EncodedSourceId.EscapedSourceId << " hash " << CompatibleHash << " partition " << Partition << " partitions "
            << PartitionToTablet.size() << "(" << CompatibleHash % PartitionToTablet.size() << ") create " << SourceIdCreateTime << " result " << t);

        if (!PartitionFound && (PreferedPartition < Max<ui32>() || !AppData(ctx)->PQConfig.GetRoundRobinPartitionMapping())) {
            Partition = PreferedPartition < Max<ui32>() ? PreferedPartition : CompatibleHash % PartitionToTablet.size(); //choose partition default value
            PartitionFound = true;
        }

        if (PartitionFound) {
            UpdatePartition(ctx);
        } else {
            RequestNextPartition(ctx);
        }
        return;
    } else if (State == EState::ES_WAIT_TABLE_REQUEST_2) {
        SourceIdUpdatesInflight--;
        if (!SourceIdUpdatesInflight) {
            LastSourceIdUpdate = ctx.Now();
            ProceedPartition(Partition, ctx);
        }
    } else if (State == EState::ES_INITED) {
        SourceIdUpdatesInflight--;
        if (!SourceIdUpdatesInflight) {
            LastSourceIdUpdate = ctx.Now();
        }
    } else {
        Y_FAIL("Wrong state");
    }
}

THolder<NKqp::TEvKqp::TEvQueryRequest> TWriteSessionActor::MakeUpdateSourceIdMetadataRequest(
        ui32 hash, const TString& topic, const NActors::TActorContext& ctx
) {

    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    ev->Record.MutableRequest()->SetQuery(UpdateSourceIdQuery);
    ev->Record.MutableRequest()->SetDatabase(NKikimr::NPQ::GetDatabaseFromConfig(AppData(ctx)->PQConfig));
    ev->Record.MutableRequest()->SetKeepSession(false);
    // fill tx settings: set commit tx flag & begin new serializable tx.
    ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    // keep compiled query in cache.
    ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

    NClient::TParameters parameters;
    parameters["$Hash"] = hash;
    parameters["$Topic"] = topic;
    parameters["$SourceId"] = EncodedSourceId.EscapedSourceId;

    parameters["$CreateTime"] = SourceIdCreateTime;
    parameters["$AccessTime"] = TInstant::Now().MilliSeconds();
    parameters["$Partition"] = Partition;
    ev->Record.MutableRequest()->MutableParameters()->Swap(&parameters);

    return ev;
}

void TWriteSessionActor::SendUpdateSrcIdsRequests(const TActorContext& ctx) {
    {
        //full legacy name (rt3.dc--acc--topic)
        auto ev = MakeUpdateSourceIdMetadataRequest(CompatibleHash, FullConverter->GetClientsideName(), ctx);
        SourceIdUpdatesInflight++;
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }
    {
        auto ev = MakeUpdateSourceIdMetadataRequest(EncodedSourceId.Hash, FullConverter->GetClientsideName(), ctx);
        SourceIdUpdatesInflight++;
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }
}

void TWriteSessionActor::Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record;

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session cookie: " << Cookie << " sessionId: " << OwnerCookie << " sourceID "
            << SourceId << " escaped " << EncodedSourceId.EscapedSourceId << " discover partition error - " << record);

    CloseSession("Internal error on discovering partition", PersQueue::ErrorCode::ERROR, ctx);
}


void TWriteSessionActor::ProceedPartition(const ui32 partition, const TActorContext& ctx) {
    Partition = partition;
    auto it = PartitionToTablet.find(Partition);

    ui64 tabletId = it != PartitionToTablet.end() ? it->second : 0;

    if (!tabletId) {
        CloseSession(
                Sprintf("no partition %u in topic '%s', Marker# PQ4", Partition,
                        DiscoveryConverter->GetPrintableString().c_str()),
                PersQueue::ErrorCode::UNKNOWN_TOPIC, ctx
        );
        return;
    }

    Writer = ctx.RegisterWithSameMailbox(NPQ::CreatePartitionWriter(ctx.SelfID, tabletId, Partition, SourceId));
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

void TWriteSessionActor::CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {

    if (errorCode != PersQueue::ErrorCode::OK) {

        if (InternalErrorCode(errorCode)) {
            SLIErrors.Inc();
        }

        if (Errors) {
            Errors.Inc();
        } else {
            ++(*GetServiceCounters(Counters, "pqproxy|writeSession")->GetCounter("Errors", true));
        }

        StreamingWriteServerMessage result;
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

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_WAIT_WRITER_INIT) {
        return CloseSession("got init result but not wait for it", PersQueue::ErrorCode::ERROR, ctx);
    }

    const auto& result = *ev->Get();
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

    StreamingWriteServerMessage response;
    response.set_status(Ydb::StatusIds::SUCCESS);
    auto init = response.mutable_init_response();
    init->set_session_id(EscapeC(OwnerCookie));
    init->set_last_sequence_number(maxSeqNo);
    init->set_partition_id(Partition);
    init->set_topic(FullConverter->GetFederationPath());
    init->set_cluster(FullConverter->GetCluster());
    init->set_block_format_version(0);
    if (InitialPQTabletConfig.HasCodecs()) {
        for (const auto& codecName : InitialPQTabletConfig.GetCodecs().GetCodecs()) {
            init->add_supported_codecs(CodecByName(codecName));
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

    ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());

    //init completed; wait for first data chunk
    NextRequestInited = true;
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
        Die(ctx);
        return;
    }
}

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_INITED) {
        return CloseSession("got write permission but not wait for it", PersQueue::ErrorCode::ERROR, ctx);
    }

    Y_VERIFY(!FormedWrites.empty());
    TWriteRequestBatchInfo::TPtr writeRequest = std::move(FormedWrites.front());

    if (ev->Get()->Cookie != writeRequest->Cookie) {
        return CloseSession("out of order reserve bytes response from server, may be previous is lost", PersQueue::ErrorCode::ERROR, ctx);
    }

    FormedWrites.pop_front();

    ui64 diff = writeRequest->ByteSize;

    SentMessages.emplace_back(std::move(writeRequest));

    BytesInflight_ -= diff;
    BytesInflight.Dec(diff);

    if (!NextRequestInited && BytesInflight_ < MAX_BYTES_INFLIGHT) { //allow only one big request to be readed but not sended
        NextRequestInited = true;
        if (!Request->GetStreamCtx()->Read()) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
            Die(ctx);
            return;
        }
    }

    --NumReserveBytesRequests;
    if (!Writes.empty())
        GenerateNextWriteRequest(ctx);
}

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_INITED) {
        return CloseSession("got write response but not wait for it", PersQueue::ErrorCode::ERROR, ctx);
    }

    const auto& result = *ev->Get();
    if (!result.IsSuccess()) {
        const auto& record = result.Record;
        if (record.HasErrorCode()) {
            return CloseSession("status is not ok: " + record.GetErrorReason(), ConvertOldCode(record.GetErrorCode()), ctx);
        } else {
            return CloseSession("error at write: " + result.GetError().Reason, PersQueue::ErrorCode::ERROR, ctx);
        }
    }

    const auto& resp = result.Record.GetPartitionResponse();

    if (SentMessages.empty()) {
        CloseSession("got too many replies from server, internal error", PersQueue::ErrorCode::ERROR, ctx);
        return;
    }

    TWriteRequestBatchInfo::TPtr writeRequest = std::move(SentMessages.front());
    SentMessages.pop_front();

    if (resp.GetCookie() != writeRequest->Cookie) {
        return CloseSession("out of order write response from server, may be previous is lost", PersQueue::ErrorCode::ERROR, ctx);
    }

    auto addAck = [](const TPersQueuePartitionResponse::TCmdWriteResult& res, StreamingWriteServerMessage::BatchWriteResponse* batchWriteResponse,
                         StreamingWriteServerMessage::WriteStatistics* stat) {
        batchWriteResponse->add_sequence_numbers(res.GetSeqNo());
        batchWriteResponse->add_offsets(res.GetOffset());
        batchWriteResponse->add_already_written(res.GetAlreadyWritten());

        stat->set_queued_in_partition_duration_ms(
            Max((i64)res.GetTotalTimeInPartitionQueueMs(), stat->queued_in_partition_duration_ms()));
        stat->set_throttled_on_partition_duration_ms(
            Max((i64)res.GetPartitionQuotedTimeMs(), stat->throttled_on_partition_duration_ms()));
        stat->set_throttled_on_topic_duration_ms(Max(static_cast<i64>(res.GetTopicQuotedTimeMs()), stat->throttled_on_topic_duration_ms()));
        stat->set_persist_duration_ms(
            Max((i64)res.GetWriteTimeMs(), stat->persist_duration_ms()));
    };

    ui32 partitionCmdWriteResultIndex = 0;
    // TODO: Send single batch write response for all user write requests up to some max size/count
    for (const auto& userWriteRequest : writeRequest->UserWriteRequests) {
        StreamingWriteServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);
        auto batchWriteResponse = result.mutable_batch_write_response();
        batchWriteResponse->set_partition_id(Partition);

        for (size_t messageIndex = 0, endIndex = userWriteRequest->Request.write_request().sequence_numbers_size(); messageIndex != endIndex; ++messageIndex) {
            if (partitionCmdWriteResultIndex == resp.CmdWriteResultSize()) {
                CloseSession("too less responses from server", PersQueue::ErrorCode::ERROR, ctx);
                return;
            }
            const auto& partitionCmdWriteResult = resp.GetCmdWriteResult(partitionCmdWriteResultIndex);
            const auto writtenSequenceNumber = userWriteRequest->Request.write_request().sequence_numbers(messageIndex);
            if (partitionCmdWriteResult.GetSeqNo() != writtenSequenceNumber) {
                CloseSession(TStringBuilder() << "Expected partition " << Partition << " write result for message with sequence number " << writtenSequenceNumber << " but got for " << partitionCmdWriteResult.GetSeqNo(), PersQueue::ErrorCode::ERROR, ctx);
                return;
            }

            addAck(partitionCmdWriteResult, batchWriteResponse, batchWriteResponse->mutable_write_statistics());
            ++partitionCmdWriteResultIndex;
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
    BytesInflightTotal.Dec(diff);

    CheckFinish(ctx);
}

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr&, const TActorContext& ctx) {
    CloseSession("pipe to partition's tablet is dead", PersQueue::ErrorCode::ERROR, ctx);
}

void TWriteSessionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    //TODO: add here retries for connecting to PQRB
    if (msg->Status != NKikimrProto::OK) {
        CloseSession(TStringBuilder() << "pipe to tablet is dead " << msg->TabletId, PersQueue::ErrorCode::ERROR, ctx);
        return;
    }
}

void TWriteSessionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    //TODO: add here retries for connecting to PQRB
    CloseSession(TStringBuilder() << "pipe to tablet is dead " << ev->Get()->TabletId, PersQueue::ErrorCode::ERROR, ctx);
}

void TWriteSessionActor::GenerateNextWriteRequest(const TActorContext& ctx) {
    TWriteRequestBatchInfo::TPtr writeRequest = new TWriteRequestBatchInfo();

    auto ev = MakeHolder<NPQ::TEvPartitionWriter::TEvWriteRequest>(++NextRequestCookie);
    NKikimrClient::TPersQueueRequest& request = ev->Record;

    writeRequest->UserWriteRequests = std::move(Writes);
    Writes.clear();

    i64 diff = 0;
    auto addData = [&](const StreamingWriteClientMessage::WriteRequest& writeRequest, const i32 messageIndex) {
        auto w = request.MutablePartitionRequest()->AddCmdWrite();
        w->SetData(GetSerializedData(InitMeta, writeRequest, messageIndex));
        w->SetSeqNo(writeRequest.sequence_numbers(messageIndex));
        w->SetSourceId(NPQ::NSourceIdEncoding::EncodeSimple(SourceId));
        w->SetCreateTimeMS(writeRequest.created_at_ms(messageIndex));
        w->SetUncompressedSize(writeRequest.blocks_uncompressed_sizes(messageIndex));
        w->SetClientDC(ClientDC);
    };

    for (const auto& write : writeRequest->UserWriteRequests) {
        diff -= write->Request.ByteSize();
        const auto& writeRequest = write->Request.write_request();
        for (i32 messageIndex = 0; messageIndex != writeRequest.sequence_numbers_size(); ++messageIndex) {
            addData(writeRequest, messageIndex);
        }
    }

    writeRequest->Cookie = request.GetPartitionRequest().GetCookie();

    Y_VERIFY(-diff <= (i64)BytesInflight_);
    diff += request.ByteSize();
    BytesInflight_ += diff;
    BytesInflightTotal_ += diff;
    BytesInflight.Inc(diff);
    BytesInflightTotal.Inc(diff);

    writeRequest->ByteSize = request.ByteSize();
    FormedWrites.push_back(writeRequest);

    ctx.Send(Writer, std::move(ev));
    ++NumReserveBytesRequests;
}

void TWriteSessionActor::Handle(TEvPQProxy::TEvUpdateToken::TPtr& ev, const TActorContext& ctx) {
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
        StreamingWriteServerMessage serverMessage;
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

void TWriteSessionActor::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr &ev , const TActorContext& ctx) {
    Y_UNUSED(ctx);
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "updating token");

    if (ev->Get()->Authenticated && !ev->Get()->InternalToken.empty()) {
        Token = new NACLib::TUserToken(ev->Get()->InternalToken);
        Request->SetInternalToken(ev->Get()->InternalToken);
        UpdateTokenAuthenticated = true;
        if (!ACLCheckInProgress) {
            InitCheckSchema(ctx);
        }
    } else {
        Request->ReplyUnauthenticated("refreshed token is invalid");
        Die(ctx);
    }
}

void TWriteSessionActor::Handle(TEvPQProxy::TEvWrite::TPtr& ev, const TActorContext& ctx) {

    RequestNotChecked = true;

    if (State != ES_INITED) {
        //answer error
        CloseSession("write in not inited session", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    const auto& writeRequest = ev->Get()->Request.write_request();
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

    THolder<TEvPQProxy::TEvWrite> event(ev->Release());
    Writes.push_back(std::move(event));

    ui64 diff = Writes.back()->Request.ByteSize();
    BytesInflight_ += diff;
    BytesInflightTotal_ += diff;
    BytesInflight.Inc(diff);
    BytesInflightTotal.Inc(diff);

    if (BytesInflight_ < MAX_BYTES_INFLIGHT) { //allow only one big request to be readed but not sended
        Y_VERIFY(NextRequestInited);
        if (!Request->GetStreamCtx()->Read()) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session v1 cookie: " << Cookie << " sessionId: " << OwnerCookie << " grpc read failed");
            Die(ctx);
            return;

        }
    } else {
        NextRequestInited = false;
    }

    if (NumReserveBytesRequests < MAX_RESERVE_REQUESTS_INFLIGHT) {
        GenerateNextWriteRequest(ctx);
    }
}

void TWriteSessionActor::HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}

void TWriteSessionActor::LogSession(const TActorContext& ctx) {
    LOG_INFO_S(
            ctx, NKikimrServices::PQ_WRITE_PROXY,
            "write session:  cookie=" << Cookie << " sessionId=" << OwnerCookie << " userAgent=\"" << UserAgent
                                      << "\" ip=" << PeerName << " proto=v1 "
                                      << " topic=" << InitRequest.topic()
                                      << " durationSec=" << (ctx.Now() - StartTime).Seconds()
    );

    LogSessionDeadline = ctx.Now() + TDuration::Hours(1) + TDuration::Seconds(rand() % 60);
}

void TWriteSessionActor::HandleWakeup(const TActorContext& ctx) {
    Y_VERIFY(State == ES_INITED);
    ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());
    if (Token && !ACLCheckInProgress && RequestNotChecked && (ctx.Now() - LastACLCheckTimestamp > TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()))) {
        RequestNotChecked = false;
        InitCheckSchema(ctx);
    }
    // ToDo[migration] - separate flag for having config tables
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()
                                && !SourceIdUpdatesInflight
                                && ctx.Now() - LastSourceIdUpdate > SOURCEID_UPDATE_PERIOD
    ) {
        SendUpdateSrcIdsRequests(ctx);
    }
    if (ctx.Now() >= LogSessionDeadline) {
        LogSession(ctx);
    }
}

}
}
}
