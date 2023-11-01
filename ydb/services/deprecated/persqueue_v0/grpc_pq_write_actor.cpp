#include "grpc_pq_actor.h"

#include <ydb/services/metadata/manager/common.h>
#include <ydb/core/persqueue/writer/metadata_initializers.h>

#include <ydb/core/persqueue/pq_database.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/services/lib/sharding/sharding.h>

#include <library/cpp/actors/core/log.h>
#include <util/string/hex.h>
#include <util/string/vector.h>
#include <util/string/escape.h>

using namespace NActors;
using namespace NKikimrClient;


namespace NKikimr {
using namespace  NMsgBusProxy::NPqMetaCacheV2;
using namespace NSchemeCache;
using namespace NPQ;

template <>
void FillChunkDataFromReq(NKikimrPQClient::TDataChunk& proto, const NPersQueue::TWriteRequest::TData& data) {
    proto.SetData(data.GetData());
    proto.SetSeqNo(data.GetSeqNo());
    proto.SetCreateTime(data.GetCreateTimeMs());
    proto.SetCodec(data.GetCodec());
}

template <>
void FillExtraFieldsForDataChunk(
    const NPersQueue::TWriteRequest::TInit& init,
    NKikimrPQClient::TDataChunk& data,
    TString& server,
    TString& ident,
    TString& logType,
    TString& file
) {
    for (ui32 i = 0; i < init.GetExtraFields().ItemsSize(); ++i) {
        const auto& item = init.GetExtraFields().GetItems(i);
        if (item.GetKey() == "server") {
            server = item.GetValue();
        } else if (item.GetKey() == "ident") {
            ident = item.GetValue();
        } else if (item.GetKey() == "logtype") {
            logType = item.GetValue();
        } else if (item.GetKey() == "file") {
            file = item.GetValue();
        } else {
            auto res = data.MutableExtraFields()->AddItems();
            res->SetKey(item.GetKey());
            res->SetValue(item.GetValue());
        }
    }
}

namespace NGRpcProxy {

using namespace NPersQueue;

static const ui32 MAX_RESERVE_REQUESTS_INFLIGHT = 5;

static const ui32 MAX_BYTES_INFLIGHT = 1_MB;
static const TDuration SOURCEID_UPDATE_PERIOD = TDuration::Hours(1);

//TODO: add here tracking of bytes in/out

TWriteSessionActor::TWriteSessionActor(IWriteSessionHandlerRef handler, const ui64 cookie,  const TActorId& schemeCache,
                                       TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TString& localDC,
                                       const TMaybe<TString> clientDC)
    : Handler(handler)
    , State(ES_CREATED)
    , SchemeCache(schemeCache)
    , PeerName("")
    , Cookie(cookie)
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
    , ACLCheckInProgress(true)
    , FirstACLCheck(true)
    , ForceACLCheck(false)
    , RequestNotChecked(true)
    , LastACLCheckTimestamp(TInstant::Zero())
    , LogSessionDeadline(TInstant::Zero())
    , BalancerTabletId(0)
    , PipeToBalancer()
    , LocalDC(localDC)
    , ClientDC(clientDC ? *clientDC : "other")
    , LastSourceIdUpdate(TInstant::Zero())
    , SourceIdCreateTime(0)
    , SourceIdUpdatesInflight(0)

{
    Y_ASSERT(Handler);
}

TWriteSessionActor::~TWriteSessionActor() = default;


void TWriteSessionActor::Bootstrap(const TActorContext& ctx) {
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ++(*GetServiceCounters(Counters, "pqproxy|writeSession")->GetCounter("SessionsCreatedTotal", true));
    }
    Become(&TThis::StateFunc);
    const auto& pqConfig = AppData(ctx)->PQConfig;
    SrcIdTableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                                    : ESourceIdTableGeneration::SrcIdMeta2;

    Database = NKikimr::NPQ::GetDatabaseFromConfig(pqConfig);
    const auto& root = pqConfig.GetRoot();
    SelectSourceIdQuery = GetSelectSourceIdQuery(root, SrcIdTableGeneration);
    UpdateSourceIdQuery = GetUpdateSourceIdQuery(root, SrcIdTableGeneration);
    ConverterFactory = MakeHolder<NPersQueue::TTopicNamesConverterFactory>(
            pqConfig, LocalDC
    );
    StartTime = ctx.Now();
}


void TWriteSessionActor::Die(const TActorContext& ctx) {
    if (State == ES_DYING)
        return;
    if (Writer)
        ctx.Send(Writer, new TEvents::TEvPoisonPill());

    if (PipeToBalancer)
        NTabletPipe::CloseClient(ctx, PipeToBalancer);

    if (SessionsActive) {
        SessionsActive.Dec();
        BytesInflight.Dec(BytesInflight_);
        BytesInflightTotal.Dec(BytesInflightTotal_);
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session cookie: " << Cookie << " sessionId: " << OwnerCookie << " is DEAD");

    TryCloseSession(ctx);

    if (!Handler->IsShuttingDown())
        Handler->Finish();

    if (State == ES_WAIT_SESSION) { // final die will be done later, on session discover
        State = ES_DYING;
        return;
    }

    State = ES_DYING;

    TryCloseSession(ctx);

    TActorBootstrapped<TWriteSessionActor>::Die(ctx);
}

void TWriteSessionActor::TryCloseSession(const TActorContext& ctx) {
    if (KqpSessionId) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
        KqpSessionId = "";
    }
}

void TWriteSessionActor::CheckFinish(const TActorContext& ctx) {
    if (!WritesDone)
        return;
    if (State != ES_INITED) {
        CloseSession("out of order Writes done before initialization", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (Writes.empty() && FormedWrites.empty() && SentMessages.empty()) {
        CloseSession("", NPersQueue::NErrorCode::OK, ctx);
        return;
    }
}

void TWriteSessionActor::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext& ctx) {
    WritesDone = true;
    CheckFinish(ctx);
}

void TWriteSessionActor::CheckACL(const TActorContext& ctx) {
    Y_ABORT_UNLESS(ACLCheckInProgress);
    Y_ABORT_UNLESS(SecurityObject);
    NACLib::EAccessRights rights = NACLib::EAccessRights::UpdateRow;
    if (!AppData(ctx)->PQConfig.GetCheckACL() || SecurityObject->CheckAccess(rights, *Token)) {
        ACLCheckInProgress = false;
        if (FirstACLCheck) {
            FirstACLCheck = false;
            DiscoverPartition(ctx);
        }
    } else {
        TString errorReason = Sprintf("access to topic '%s' denied for '%s' due to 'no WriteTopic rights', Marker# PQ1125",
            DiscoveryConverter->GetPrintableString().c_str(),
            Token->GetUserSID().c_str());
        CloseSession(errorReason, NPersQueue::NErrorCode::ACCESS_DENIED, ctx);
    }
}

void TWriteSessionActor::Handle(TEvPQProxy::TEvWriteInit::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvPQProxy::TEvWriteInit> event(ev->Release());

    if (State != ES_CREATED) {
        //answer error
        CloseSession("got second init request",  NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    const auto& init = event->Request.GetInit();

    if (init.GetTopic().empty() || init.GetSourceId().empty()) {
        CloseSession("no topic or SourceId in init request",  NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (init.GetProxyCookie() != ctx.SelfID.NodeId() && init.GetProxyCookie() != MAGIC_COOKIE_VALUE) {
        CloseSession("you must perform ChooseProxy request at first and go to ProxyName server with ProxyCookie",
                     NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    //1. Database - !(Root or empty) (Need to bring root DB(s) list to PQConfig) - ONLY search modern path /Database/Path
    //2. No database. Try parse and resolve account to database. If possible, try search this path.
    //3. Fallback from 2 - legacy mode.

    DiscoveryConverter = ConverterFactory->MakeDiscoveryConverter(init.GetTopic(), true, LocalDC, Database);
    if (!DiscoveryConverter->IsValid()) {
        CloseSession(
                TStringBuilder() << "incorrect topic \"" << DiscoveryConverter->GetOriginalTopic()
                                 << "\": " << DiscoveryConverter->GetReason(),
                NPersQueue::NErrorCode::BAD_REQUEST,
                ctx
        );
        return;
    }
    PeerName = event->PeerName;
    if (!event->Database.empty()) {
        Database = event->Database;
    }

    SourceId = init.GetSourceId();
    //TODO: check that sourceId does not have characters '"\_% - espace them on client may be?

    Auth = event->Request.GetCredentials();
    event->Request.ClearCredentials();
    Y_PROTOBUF_SUPPRESS_NODISCARD Auth.SerializeToString(&AuthStr);

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session request cookie: " << Cookie << " " << init << ", Database: '" << Database << "' from " << PeerName);
    UserAgent = init.GetVersion();
    LogSession(ctx);

    auto* request = new TEvDescribeTopicsRequest({DiscoveryConverter});
    //TODO: GetNode for /Root/PQ then describe from balancer
    ctx.Send(SchemeCache, request);
    State = ES_WAIT_SCHEME;
    InitRequest = init;
    PreferedPartition = init.GetPartitionGroup() > 0 ? init.GetPartitionGroup() - 1 : Max<ui32>();
}

void TWriteSessionActor::InitAfterDiscovery(const TActorContext& ctx) {
    try {
        EncodedSourceId = NSourceIdEncoding::EncodeSrcId(FullConverter->GetTopicForSrcIdHash(), SourceId,
                                                         SrcIdTableGeneration);
    } catch (yexception& e) {
        CloseSession(TStringBuilder() << "incorrect sourceId \"" << SourceId << "\": " << e.what(),  NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    InitMeta = GetInitialDataChunk(InitRequest, FullConverter->GetClientsideName(), PeerName);

    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", FullConverter->GetAccount()}}, {"total"}}};

    SLIErrors = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);
    SLITotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
    SLITotal.Inc();
}


void TWriteSessionActor::SetupCounters()
{
    if (SessionsCreated) {
        return;
    }

    //now topic is checked, can create group for real topic, not garbage
    auto subGroup = GetServiceCounters(Counters, "pqproxy|writeSession");
    Y_ABORT_UNLESS(FullConverter);
    auto aggr = GetLabels(FullConverter);

    BytesInflight = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"BytesInflight"}, false);
    SessionsWithoutAuth = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"WithoutAuth"}, true);
    BytesInflightTotal = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"BytesInflightTotal"}, false);
    SessionsCreated = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"SessionsCreated"}, true);
    SessionsActive = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"SessionsActive"}, false);
    Errors = NKikimr::NPQ::TMultiCounter(subGroup, aggr, {}, {"Errors"}, true);

    CodecCounters.push_back(NKikimr::NPQ::TMultiCounter(subGroup, aggr, {{"codec", "user"}}, {"MessagesWrittenByCodec"}, true));

    auto allNames = GetEnumAllCppNames<NPersQueueCommon::ECodec>();
    allNames.pop_back();
    allNames.pop_back();
    for (auto &name : allNames)  {
        auto nm = to_lower(name).substr(18);
        CodecCounters.push_back(NKikimr::NPQ::TMultiCounter(subGroup, aggr, {{"codec", nm}}, {"MessagesWrittenByCodec"}, true));
    }

    SessionsCreated.Inc();
    SessionsActive.Inc();
}


void TWriteSessionActor::SetupCounters(const TString& cloudId, const TString& dbId, const TString& dbPath,
                                       bool isServerless, const TString& folderId)
{
    if (SessionsCreated) {
        return;
    }

    //now topic is checked, can create group for real topic, not garbage
    auto subGroup = GetCountersForTopic(Counters, isServerless);
    Y_ABORT_UNLESS(FullConverter);
    auto subgroups = GetSubgroupsForTopic(FullConverter, cloudId, dbId, dbPath, folderId);

    SessionsCreated = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.sessions_created"}, true, "name");
    SessionsActive = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.sessions_active_count"}, false, "name");
    Errors = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_write.errors"}, true, "name");

    SessionsCreated.Inc();
    SessionsActive.Inc();
}


void TWriteSessionActor::Handle(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(State == ES_WAIT_SCHEME || State == ES_INITED);
    auto& res = ev->Get()->Result;
    Y_ABORT_UNLESS(res->ResultSet.size() == 1);

    auto& entry = res->ResultSet[0];
    TString errorReason;

    auto& path = entry.Path;
    auto& topic = ev->Get()->TopicsRequested[0];
    switch (entry.Status) {
        case TSchemeCacheNavigate::EStatus::RootUnknown: {
            errorReason = Sprintf("path '%s' has incorrect root prefix, Marker# PQ14", JoinPath(path).c_str());
            CloseSession(errorReason, NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx);
            return;
        }
        case TSchemeCacheNavigate::EStatus::PathErrorUnknown: {
            errorReason = Sprintf("no path '%s', Marker# PQ151", JoinPath(path).c_str());
            CloseSession(errorReason, NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx);
            return;
        }
        case TSchemeCacheNavigate::EStatus::Ok:
            break;
        default: {
            errorReason = Sprintf("topic '%s' describe error, Status# %s, Marker# PQ1", path.back().c_str(),
                                  ToString(entry.Status).c_str());
            CloseSession(errorReason, NPersQueue::NErrorCode::ERROR, ctx);
            break;
        }
    }
    if (!entry.PQGroupInfo) {

        errorReason = Sprintf("topic '%s' describe error, reason - could not retrieve topic metadata, Marker# PQ99",
                              topic->GetPrintableString().c_str());
        CloseSession(errorReason, NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    PQInfo = entry.PQGroupInfo;
    const auto& description = PQInfo->Description;
    //const TString topicName = description.GetName();

    if (entry.Kind != TSchemeCacheNavigate::EKind::KindTopic) {
        errorReason = Sprintf("item '%s' is not a topic, Marker# PQ13", DiscoveryConverter->GetPrintableString().c_str());
        CloseSession(errorReason, NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (!DiscoveryConverter->IsValid()) {
        errorReason = Sprintf("Internal server error with topic '%s', Marker# PQ503", DiscoveryConverter->GetPrintableString().c_str());
        CloseSession(errorReason, NPersQueue::NErrorCode::ERROR, ctx);
        return;
    }
    FullConverter = DiscoveryConverter->UpgradeToFullConverter(description.GetPQTabletConfig(),
                                                               AppData(ctx)->PQConfig.GetTestDatabaseRoot());
    InitAfterDiscovery(ctx);
    SecurityObject = entry.SecurityObject;

    Y_ABORT_UNLESS(description.PartitionsSize() > 0);

    for (ui32 i = 0; i < description.PartitionsSize(); ++i) {
        const auto& pi = description.GetPartitions(i);
        PartitionToTablet[pi.GetPartitionId()] = pi.GetTabletId();
    }
    BalancerTabletId = description.GetBalancerTabletID();
    DatabaseId = description.GetPQTabletConfig().GetYdbDatabaseId();
    FolderId = description.GetPQTabletConfig().GetYcFolderId();

    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        const auto& tabletConfig = description.GetPQTabletConfig();
        SetupCounters(tabletConfig.GetYcCloudId(), tabletConfig.GetYdbDatabaseId(),
                      tabletConfig.GetYdbDatabasePath(), entry.DomainInfo->IsServerless(),
                      tabletConfig.GetYcFolderId());
    } else {
        SetupCounters();
    }

    if (!PipeToBalancer) {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        PipeToBalancer = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, BalancerTabletId, clientConfig));
    }

    if (Auth.GetCredentialsCase() == NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET) {
        //ACLCheckInProgress is still true - no recheck will be done
        LOG_WARN_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session without AuthInfo : " << DiscoveryConverter->GetPrintableString()
                                                         << " sourceId " << SourceId << " from " << PeerName);
        SessionsWithoutAuth.Inc();
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            CloseSession("Unauthenticated access is forbidden, please provide credentials", NPersQueue::NErrorCode::ACCESS_DENIED, ctx);
            return;
        }
        if (FirstACLCheck) {
            FirstACLCheck = false;
            DiscoverPartition(ctx);
            return;
        }
    }

    InitCheckACL(ctx);
}

void TWriteSessionActor::InitCheckACL(const TActorContext& ctx) {

    Y_ABORT_UNLESS(ACLCheckInProgress);

    TString ticket;
    switch (Auth.GetCredentialsCase()) {
        case NPersQueueCommon::TCredentials::kTvmServiceTicket:
            ticket = Auth.GetTvmServiceTicket();
            break;
        case NPersQueueCommon::TCredentials::kOauthToken:
            ticket = Auth.GetOauthToken();
            break;
        default:
            CloseSession("Uknown Credentials case", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return;
     }

    auto entries = NKikimr::NGRpcProxy::V1::GetTicketParserEntries(DatabaseId, FolderId);
    ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
            .Database = Database,
            .Ticket = ticket,
            .PeerName = PeerName,
            .Entries = entries
        }));
}

void TWriteSessionActor::Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(ACLCheckInProgress);
    TString ticket = ev->Get()->Ticket;
    TString maskedTicket = ticket.size() > 5 ? (ticket.substr(0, 5) + "***" + ticket.substr(ticket.size() - 5)) : "***";
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "CheckACL ticket " << maskedTicket << " got result from TICKET_PARSER response: error: "
                            << ev->Get()->Error << " user: "
                            << (ev->Get()->Error.empty() ? ev->Get()->Token->GetUserSID() : ""));

    if (!ev->Get()->Error.empty()) {
        CloseSession(TStringBuilder() << "Ticket parsing error: " << ev->Get()->Error, NPersQueue::NErrorCode::ACCESS_DENIED, ctx);
        return;
    }
    Token = ev->Get()->Token;


    Y_ABORT_UNLESS(ACLCheckInProgress);
    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session cookie: " << Cookie << " sessionId: " << OwnerCookie << " describe result for acl check");
    CheckACL(ctx);
}


void TWriteSessionActor::DiscoverPartition(const NActors::TActorContext& ctx) {
    const auto &pqConfig = AppData(ctx)->PQConfig;
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        if (pqConfig.GetUseSrcIdMetaMappingInFirstClass()) {
            return SendCreateManagerRequest(ctx);
        }
        auto partitionId = PreferedPartition < Max<ui32>()
                ? PreferedPartition
                : NDataStreams::V1::CalculateShardFromSrcId(SourceId, PartitionToTablet.size());

        ProceedPartition(partitionId, ctx);
        return;
    }
    else {
        StartSession(ctx);
    }
}

TString TWriteSessionActor::GetDatabaseName(const NActors::TActorContext& ctx) {
    switch (SrcIdTableGeneration) {
        case ESourceIdTableGeneration::SrcIdMeta2:
            return NKikimr::NPQ::GetDatabaseFromConfig(AppData(ctx)->PQConfig);
        case ESourceIdTableGeneration::PartitionMapping:
            return AppData(ctx)->TenantName;
    }
}

void TWriteSessionActor::StartSession(const NActors::TActorContext& ctx) {

    auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

    State = ES_WAIT_SESSION;
}

void TWriteSessionActor::SendCreateManagerRequest(const TActorContext& ctx) {
    ctx.Send(
            NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
            new NMetadata::NProvider::TEvPrepareManager(V1::TSrcIdMetaInitManager::GetInstant())
    );
}

void TWriteSessionActor::Handle(
        NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx
) {
    StartSession(ctx);
}

void TWriteSessionActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr &ev, const NActors::TActorContext& ctx)
{
    Y_ABORT_UNLESS(State == ES_WAIT_SESSION || State == ES_DYING);

    const auto& record = ev->Get()->Record;

    KqpSessionId = record.GetResponse().GetSessionId();

    if (State == ES_DYING) {
        TryCloseSession(ctx);
        TActorBootstrapped<TWriteSessionActor>::Die(ctx);
        return;
    }

    State = ES_WAIT_TABLE_REQUEST_1;

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        TStringBuilder errorReason;
        errorReason << "kqp error Marker# PQ53 : " <<  record;
        CloseSession(errorReason, NPersQueue::NErrorCode::ERROR, ctx);
        return;
    }

    KqpSessionId = record.GetResponse().GetSessionId();
    Y_ABORT_UNLESS(!KqpSessionId.empty());

    //read from DS
    // Hash was always valid here, so new and old are the same
    //topic was like "rt3.dc--account--topic"
    SendSelectPartitionRequest(FullConverter->GetTopicForSrcId(), ctx);
}


void TWriteSessionActor::SendSelectPartitionRequest(const TString &topic, const NActors::TActorContext &ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    ev->Record.MutableRequest()->SetQuery(SelectSourceIdQuery);
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(false);
    ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

    NYdb::TParamsBuilder paramsBuilder = NYdb::TParamsBuilder();

    SetHashToTParamsBuilder(paramsBuilder, EncodedSourceId);

    paramsBuilder
        .AddParam("$Topic")
            .Utf8(topic)
            .Build()
        .AddParam("$SourceId")
            .Utf8(EncodedSourceId.EscapedSourceId)
            .Build();

    NYdb::TParams params = paramsBuilder.Build();

    ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    SelectReqsInflight++;
}


void TWriteSessionActor::UpdatePartition(const TActorContext& ctx) {
    Y_ABORT_UNLESS(State == ES_WAIT_TABLE_REQUEST_1 || State == ES_WAIT_NEXT_PARTITION);
    //Previously Topic contained legacy name with DC (rt3.dc1--acc--topic)
    auto ev = MakeUpdateSourceIdMetadataRequest(FullConverter->GetTopicForSrcId(), ctx);
    ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    SourceIdUpdatesInflight++;

    State = ES_WAIT_TABLE_REQUEST_2;
}

void TWriteSessionActor::RequestNextPartition(const TActorContext& ctx) {
    Y_ABORT_UNLESS(State == ES_WAIT_TABLE_REQUEST_1);
    State = ES_WAIT_NEXT_PARTITION;
    THolder<TEvPersQueue::TEvGetPartitionIdForWrite> x(new TEvPersQueue::TEvGetPartitionIdForWrite);
    Y_ABORT_UNLESS(PipeToBalancer);

    NTabletPipe::SendData(ctx, PipeToBalancer, x.Release());
}

void TWriteSessionActor::Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(State == ES_WAIT_NEXT_PARTITION);
    Partition = ev->Get()->Record.GetPartitionId();
    UpdatePartition(ctx);
}

void TWriteSessionActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr &ev, const TActorContext &ctx) {
    auto& record = ev->Get()->Record.GetRef();
    const auto& pqConfig = AppData(ctx)->PQConfig;

    if (record.GetYdbStatus() == Ydb::StatusIds::ABORTED) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session cookie: " << Cookie << " sessionId: " << OwnerCookie << " sourceID "
            << SourceId << " escaped " << EncodedSourceId.EscapedSourceId << " discover partition race, retrying");
        DiscoverPartition(ctx);
        return;
    }

    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        TStringBuilder errorReason;
        errorReason << "kqp error Marker# PQ50 : " <<  record;
        if (State == EState::ES_INITED) {
            LOG_WARN_S(ctx, NKikimrServices::PQ_WRITE_PROXY, errorReason);
            SourceIdUpdatesInflight--;
        } else {
            CloseSession(errorReason, NPersQueue::NErrorCode::ERROR, ctx);
        }
        return;
    }

    if (State == EState::ES_WAIT_TABLE_REQUEST_1) {
        SelectReqsInflight--;
        auto& t = record.GetResponse().GetResults(0).GetValue().GetStruct(0);

        TxId = record.GetResponse().GetTxMeta().id();

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
        if (SelectReqsInflight != 0) {
            return;
        }
        if (SourceIdCreateTime == 0) {
            SourceIdCreateTime = TInstant::Now().MilliSeconds();
        }
        if (PartitionFound && PreferedPartition < Max<ui32>() && Partition != PreferedPartition) {
            CloseSession(TStringBuilder() << "SourceId " << SourceId << " is already bound to PartitionGroup "
                                          << (Partition + 1) << ", but client provided " << (PreferedPartition + 1)
                                          << ". SourceId->PartitionGroup binding cannot be changed, either use another SourceId, specify PartitionGroup "
                                          << (Partition + 1) << ", or do not specify PartitionGroup at all.",
                         NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return;
        }
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session cookie: " << Cookie << " sessionId: " << OwnerCookie << " sourceID "
            << SourceId << " escaped " << EncodedSourceId.EscapedSourceId << " hash " << EncodedSourceId.Hash << " partition " << Partition << " partitions "
            << PartitionToTablet.size() << "(" << EncodedSourceId.Hash % PartitionToTablet.size() << ") create " << SourceIdCreateTime << " result " << t);

        if (!PartitionFound) {
            auto partition = V1::GetPartitionFromConfigOptions(PreferedPartition, EncodedSourceId,
                                                               PartitionToTablet.size(),
                                                               pqConfig.GetTopicsAreFirstClassCitizen(),
                                                               pqConfig.GetRoundRobinPartitionMapping());
            if (partition.Defined()) {
                PartitionFound = true;
                Partition = *partition;
            }
        }
        if (PartitionFound) {
            UpdatePartition(ctx);
        } else {
            RequestNextPartition(ctx);
        }
        return;
    } else if (State == EState::ES_WAIT_TABLE_REQUEST_2) {
        Y_ABORT_UNLESS(SourceIdUpdatesInflight > 0);
        SourceIdUpdatesInflight--;
        if (SourceIdUpdatesInflight == 0) {
            LastSourceIdUpdate = ctx.Now();
            TryCloseSession(ctx);
            ProceedPartition(Partition, ctx);
        }
    } else if (State == EState::ES_INITED) {
        Y_ABORT_UNLESS(SourceIdUpdatesInflight > 0);
        SourceIdUpdatesInflight--;
        if (SourceIdUpdatesInflight == 0) {
            LastSourceIdUpdate = ctx.Now();
        }
    } else {
        Y_ABORT("Wrong state");
    }
}

THolder<NKqp::TEvKqp::TEvQueryRequest> TWriteSessionActor::MakeUpdateSourceIdMetadataRequest(
        const TString& topic, const TActorContext& ctx
) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    ev->Record.MutableRequest()->SetQuery(UpdateSourceIdQuery);
    ev->Record.MutableRequest()->SetDatabase(GetDatabaseName(ctx));
    if (KqpSessionId) {
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
    }
    ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
    if (TxId) {
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        TxId = "";
    } else {
        ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    }
    ev->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);

    NYdb::TParamsBuilder paramsBuilder = NYdb::TParamsBuilder();

    SetHashToTParamsBuilder(paramsBuilder, EncodedSourceId);

    paramsBuilder
        .AddParam("$Topic") //Previously Topic contained legacy name with DC (rt3.dc1--acc--topic)
            .Utf8(topic)
            .Build()
        .AddParam("$SourceId")
            .Utf8(EncodedSourceId.EscapedSourceId)
            .Build()
        .AddParam("$CreateTime")
            .Uint64(SourceIdCreateTime)
            .Build()
        .AddParam("$AccessTime")
            .Uint64(TInstant::Now().MilliSeconds())
            .Build()
        .AddParam("$Partition")
            .Uint32(Partition)
            .Build();

    NYdb::TParams params = paramsBuilder.Build();

    ev->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));
    return ev;
}

void TWriteSessionActor::ProceedPartition(const ui32 partition, const TActorContext& ctx) {
    Partition = partition;
    auto it = PartitionToTablet.find(Partition);

    ui64 tabletId = it != PartitionToTablet.end() ? it->second : 0;

    if (!tabletId) {
        CloseSession(
                Sprintf("no partition %u in topic '%s', Marker# PQ4", Partition, DiscoveryConverter->GetPrintableString().c_str()),
                NPersQueue::NErrorCode::UNKNOWN_TOPIC, ctx
        );
        return;
    }

    Writer = ctx.RegisterWithSameMailbox(NPQ::CreatePartitionWriter(ctx.SelfID, {/* topicPath */}, tabletId, Partition, { /* expectedGeneration */}, SourceId));
    State = ES_WAIT_WRITER_INIT;

    ui32 border = AppData(ctx)->PQConfig.GetWriteInitLatencyBigMs();
    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");

    InitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "WriteInit", border, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
    SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsBigLatency"}, true, "sensor", false);

    ui32 initDurationMs = (ctx.Now() - StartTime).MilliSeconds();
    InitLatency.IncFor(initDurationMs, 1);
    if (initDurationMs >= border) {
        SLIBigLatency.Inc();
    }
}

void TWriteSessionActor::CloseSession(const TString& errorReason, const NPersQueue::NErrorCode::EErrorCode errorCode, const NActors::TActorContext& ctx) {
    if (errorCode != NPersQueue::NErrorCode::OK) {
        if (InternalErrorCode(errorCode)) {
            SLIErrors.Inc();
        }

        if (Errors) {
            Errors.Inc();
        } else if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            ++(*GetServiceCounters(Counters, "pqproxy|writeSession")->GetCounter("Errors", true));
        }

        TWriteResponse result;

        auto error = result.MutableError();
        error->SetDescription(errorReason);
        error->SetCode(errorCode);

        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY,
                   "session error cookie: " << Cookie << " reason: \"" << errorReason << "\" code: "
                                            << EErrorCode_Name(errorCode) << " sessionId: " << OwnerCookie);

        Handler->Reply(result);
    } else {
        LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session closed cookie: " << Cookie << " sessionId: " << OwnerCookie);
    }

    Die(ctx);
}

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_WAIT_WRITER_INIT) {
        return CloseSession("got init result but not wait for it", NPersQueue::NErrorCode::ERROR, ctx);
    }

    const auto& result = *ev->Get();
    if (!result.IsSuccess()) {
        const auto& error = result.GetError();
        if (error.Response.HasErrorCode()) {
            return CloseSession("status is not ok: " + error.Response.GetErrorReason(), error.Response.GetErrorCode(), ctx);
        } else {
            return CloseSession("error at writer init: " + error.Reason, NPersQueue::NErrorCode::ERROR, ctx);
        }
    }

    OwnerCookie = result.GetResult().OwnerCookie;
    const auto& maxSeqNo = result.GetResult().SourceIdInfo.GetSeqNo();

    TWriteResponse response;
    auto init = response.MutableInit();
    init->SetSessionId(EscapeC(OwnerCookie));
    init->SetMaxSeqNo(maxSeqNo);
    init->SetPartition(Partition);
    Y_ABORT_UNLESS(FullConverter);
    init->SetTopic(FullConverter->GetClientsideName());

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "session inited cookie: " << Cookie << " partition: " << Partition
                            << " MaxSeqNo: " << maxSeqNo << " sessionId: " << OwnerCookie);

    Handler->Reply(response);

    State = ES_INITED;

    ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());

    //init completed; wait for first data chunk
    NextRequestInited = true;
    Handler->ReadyForNextRead();
}

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_INITED) {
        return CloseSession("got write permission but not wait for it", NPersQueue::NErrorCode::ERROR, ctx);
    }

    Y_ABORT_UNLESS(!FormedWrites.empty());
    TWriteRequestBatchInfo::TPtr writeRequest = std::move(FormedWrites.front());

    if (ev->Get()->Cookie != writeRequest->Cookie) {
        return CloseSession("out of order reserve bytes response from server, may be previous is lost", NPersQueue::NErrorCode::ERROR, ctx);
    }

    FormedWrites.pop_front();

    ui64 diff = writeRequest->ByteSize;

    SentMessages.emplace_back(std::move(writeRequest));

    BytesInflight_ -= diff;
    BytesInflight.Dec(diff);

    if (!NextRequestInited && BytesInflight_ < MAX_BYTES_INFLIGHT) { //allow only one big request to be readed but not sended
        NextRequestInited = true;
        Handler->ReadyForNextRead();
    }

    --NumReserveBytesRequests;
    if (!Writes.empty())
        GenerateNextWriteRequest(ctx);
}

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx) {
    if (State != ES_INITED) {
        return CloseSession("got write response but not wait for it", NPersQueue::NErrorCode::ERROR, ctx);
    }

    const auto& result = *ev->Get();
    if (!result.IsSuccess()) {
        const auto& record = result.Record;
        if (record.HasErrorCode()) {
            return CloseSession("status is not ok: " + record.GetErrorReason(), record.GetErrorCode(), ctx);
        } else {
            return CloseSession("error at write: " + result.GetError().Reason, NPersQueue::NErrorCode::ERROR, ctx);
        }
    }

    const auto& resp = result.Record.GetPartitionResponse();

    if (SentMessages.empty()) {
        CloseSession("got too many replies from server, internal error", NPersQueue::NErrorCode::ERROR, ctx);
        return;
    }

    TWriteRequestBatchInfo::TPtr writeRequest = std::move(SentMessages.front());
    SentMessages.pop_front();

    if (resp.GetCookie() != writeRequest->Cookie) {
        return CloseSession("out of order write response from server, may be previous is lost", NPersQueue::NErrorCode::ERROR, ctx);
    }

    auto addAck = [](const TPersQueuePartitionResponse::TCmdWriteResult& res, TWriteResponse::TAck* ack, TWriteResponse::TStat* stat) {
        ack->SetSeqNo(res.GetSeqNo());
        ack->SetOffset(res.GetOffset());
        ack->SetAlreadyWritten(res.GetAlreadyWritten());

        stat->SetTotalTimeInPartitionQueueMs(
            Max(res.GetTotalTimeInPartitionQueueMs(), stat->GetTotalTimeInPartitionQueueMs()));
        stat->SetPartitionQuotedTimeMs(
            Max(res.GetPartitionQuotedTimeMs(), stat->GetPartitionQuotedTimeMs()));
        stat->SetTopicQuotedTimeMs(
            Max(res.GetTopicQuotedTimeMs(), stat->GetTopicQuotedTimeMs()));
        stat->SetWriteTimeMs(
            Max(res.GetWriteTimeMs(), stat->GetWriteTimeMs()));
    };

    size_t cmdWriteResultIndex = 0;
    for (const auto& userWriteRequest : writeRequest->UserWriteRequests) {
        TWriteResponse result;
        if (userWriteRequest->Request.HasDataBatch()) {
            if (resp.CmdWriteResultSize() - cmdWriteResultIndex < userWriteRequest->Request.GetDataBatch().DataSize()) {
                CloseSession("too less responses from server", NPersQueue::NErrorCode::ERROR, ctx);
                return;
            }
            for (size_t endIndex = cmdWriteResultIndex + userWriteRequest->Request.GetDataBatch().DataSize(); cmdWriteResultIndex < endIndex; ++cmdWriteResultIndex) {
                addAck(resp.GetCmdWriteResult(cmdWriteResultIndex),
                       result.MutableAckBatch()->AddAck(),
                       result.MutableAckBatch()->MutableStat());
            }
        } else {
            Y_ABORT_UNLESS(userWriteRequest->Request.HasData());
            if (cmdWriteResultIndex >= resp.CmdWriteResultSize()) {
                CloseSession("too less responses from server", NPersQueue::NErrorCode::ERROR, ctx);
                return;
            }
            auto* ack = result.MutableAck();
            addAck(resp.GetCmdWriteResult(cmdWriteResultIndex), ack, ack->MutableStat());
            ++cmdWriteResultIndex;
        }
        Handler->Reply(result);
    }

    ui64 diff = writeRequest->ByteSize;

    BytesInflightTotal_ -= diff;
    BytesInflightTotal.Dec(diff);

    CheckFinish(ctx);
}

void TWriteSessionActor::Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr&, const TActorContext& ctx) {
    CloseSession("pipe to partition's tablet is dead", NPersQueue::NErrorCode::ERROR, ctx);
}

void TWriteSessionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        CloseSession(TStringBuilder() << "pipe to tablet is dead " << msg->TabletId, NPersQueue::NErrorCode::ERROR, ctx);
        return;
    }
}

void TWriteSessionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    CloseSession(TStringBuilder() << "pipe to tablet is dead " << ev->Get()->TabletId, NPersQueue::NErrorCode::ERROR, ctx);
}

void TWriteSessionActor::GenerateNextWriteRequest(const TActorContext& ctx) {
    TWriteRequestBatchInfo::TPtr writeRequest = new TWriteRequestBatchInfo();

    auto ev = MakeHolder<NPQ::TEvPartitionWriter::TEvWriteRequest>(++NextRequestCookie);
    NKikimrClient::TPersQueueRequest& request = ev->Record;

    writeRequest->UserWriteRequests = std::move(Writes);
    Writes.clear();

    i64 diff = 0;
    auto addData = [&](const TWriteRequest::TData& data) {
        auto w = request.MutablePartitionRequest()->AddCmdWrite();
        w->SetData(GetSerializedData(InitMeta, data));
        w->SetClientDC(ClientDC);
        w->SetSeqNo(data.GetSeqNo());
        w->SetSourceId(NPQ::NSourceIdEncoding::EncodeSimple(SourceId)); // EncodeSimple is needed for compatibility with LB
        //TODO: add in SourceID clientId when TVM will be ready
        w->SetCreateTimeMS(data.GetCreateTimeMs());
        w->SetUncompressedSize(data.GetUncompressedSize());
        w->SetIgnoreQuotaDeadline(true);
    };

    for (const auto& write : writeRequest->UserWriteRequests) {
        diff -= write->Request.ByteSize();
        if (write->Request.HasDataBatch()) {
            for (const TWriteRequest::TData& data : write->Request.GetDataBatch().GetData()) {
                addData(data);
            }
        } else { // single data
            Y_ABORT_UNLESS(write->Request.HasData());
            addData(write->Request.GetData());
        }
    }

    writeRequest->Cookie = request.GetPartitionRequest().GetCookie();

    Y_ABORT_UNLESS(-diff <= (i64)BytesInflight_);
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

TString TWriteSessionActor::CheckSupportedCodec(const ui32 codecId) {
    TString err;
    const auto& description = PQInfo->Description;
    if (!description.GetPQTabletConfig().HasCodecs() || description.GetPQTabletConfig().GetCodecs().IdsSize() == 0)
        return "";

    Y_ABORT_UNLESS(description.PartitionsSize() > 0);
    for (const auto& codec : description.GetPQTabletConfig().GetCodecs().GetIds()) {
        if (codecId == codec) {
            return "";
        }
    }
    err = "Unsupported codec provided. Supported codecs for this topic are:";
    bool first = true;
    for (const auto& codec : description.GetPQTabletConfig().GetCodecs().GetCodecs()) {
        if (first) {
            first = false;
        } else {
            err += ",";
        }
        err += " " + codec;
    }
    return err;
}


void TWriteSessionActor::Handle(TEvPQProxy::TEvWrite::TPtr& ev, const TActorContext& ctx) {

    RequestNotChecked = true;

    if (State != ES_INITED) {
        //answer error
        CloseSession("write in not inited session", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    auto auth = ev->Get()->Request.GetCredentials();
    ev->Get()->Request.ClearCredentials();
    TString tmp;
    Y_PROTOBUF_SUPPRESS_NODISCARD auth.SerializeToString(&tmp);
    if (auth.GetCredentialsCase() != NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET && tmp != AuthStr) {
        Auth = auth;
        AuthStr = tmp;
        ForceACLCheck = true;
    }
    auto dataCheck = [&](const TWriteRequest::TData& data) -> bool {
        if (!data.GetSeqNo()) {
            CloseSession("bad write request - SeqNo must be positive", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return false;
        }

        if (data.GetData().empty()) {
            CloseSession("bad write request - data must be non-empty", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return false;
        }
        TString err = CheckSupportedCodec((ui32)data.GetCodec());
        if (!err.empty()) {
            CloseSession(err, NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return false;
        }
        ui32 intCodec = ((ui32)data.GetCodec() + 1) < CodecCounters.size() ? ((ui32)data.GetCodec() + 1) : 0;
        if (CodecCounters.size() > intCodec) {
            CodecCounters[intCodec].Inc();
        }

        return true;
    };
    if (ev->Get()->Request.HasDataBatch()) {
        for (const auto& data : ev->Get()->Request.GetDataBatch().GetData()) {
            if (!dataCheck(data)) {
                return;
            }
        }
    } else {
        Y_ABORT_UNLESS(ev->Get()->Request.HasData());
        if (!dataCheck(ev->Get()->Request.GetData())) {
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
        Y_ABORT_UNLESS(NextRequestInited);
        Handler->ReadyForNextRead();
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

    LOG_INFO_S(ctx, NKikimrServices::PQ_WRITE_PROXY, "write session:  cookie=" << Cookie << " sessionId=" << OwnerCookie
                            << " userAgent=\"" << UserAgent << "\" ip=" << PeerName << " proto=v0 "
                            << " topic=" << DiscoveryConverter->GetPrintableString() << " durationSec=" << (ctx.Now() - StartTime).Seconds());

    LogSessionDeadline = ctx.Now() + TDuration::Hours(1) + TDuration::Seconds(rand() % 60);
}

void TWriteSessionActor::HandleWakeup(const TActorContext& ctx) {
    Y_ABORT_UNLESS(State == ES_INITED);
    ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());
    const auto& pqConfig = AppData(ctx)->PQConfig;
    if (!ACLCheckInProgress && (ForceACLCheck || (ctx.Now() - LastACLCheckTimestamp > TDuration::Seconds(pqConfig.GetACLRetryTimeoutSec()) && RequestNotChecked))) {
        ForceACLCheck = false;
        RequestNotChecked = false;
        if (Auth.GetCredentialsCase() != NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET) {
            ACLCheckInProgress = true;
            auto* request = new TEvDescribeTopicsRequest({DiscoveryConverter});
            ctx.Send(SchemeCache, request);
        }
    }
    if ((!pqConfig.GetTopicsAreFirstClassCitizen() || pqConfig.GetUseSrcIdMetaMappingInFirstClass())
                                && !SourceIdUpdatesInflight
                                && ctx.Now() - LastSourceIdUpdate > SOURCEID_UPDATE_PERIOD
    ) {
        SourceIdUpdatesInflight++;
        Y_ABORT_UNLESS(FullConverter);
        auto ev = MakeUpdateSourceIdMetadataRequest(FullConverter->GetTopicForSrcId(), ctx);
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }
    if (ctx.Now() >= LogSessionDeadline) {
        LogSession(ctx);
    }
}

}
}
