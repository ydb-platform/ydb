
#include "pq_impl.h"
#include "pq_impl_types.h"
#include "fix_transaction_states.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include <ydb/core/persqueue/pqtablet/common/event_helpers.h>
#include <ydb/core/persqueue/pqtablet/cache/read.h>
#include <ydb/core/persqueue/pqtablet/readproxy/readproxy.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/persqueue/pqtablet/common/tracing_support.h>
#include <ydb/core/persqueue/pqtablet/partition/partition_factory.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/core/base/tx_processing.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/pqtablet/partition/sourceid.h>  // FIXME move sourceid
#include <ydb/core/persqueue/pqtablet/quota/quota.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/library/persqueue/topic_parser/counters.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/strbuf.h>

//TODO: move this code to vieiwer
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/escape.h>

#define PQ_LOG_ERROR_AND_DIE(expr) \
    PQ_LOG_ERROR(expr); \
    ctx.Send(ctx.SelfID, new TEvents::TEvPoison())

#define PQ_ENSURE(condition) AFL_ENSURE(condition)("tablet_id", TabletID())("path", TopicPath)("topic", TopicName)

namespace NKikimr::NPQ {

static constexpr TDuration TOTAL_TIMEOUT = TDuration::Seconds(120);
static constexpr ui32 CACHE_SIZE = 100_MB;
static constexpr ui32 MAX_BYTES = 25_MB;
static constexpr ui32 MAX_SOURCE_ID_LENGTH = 2048;
static constexpr ui32 MAX_HEARTBEAT_SIZE = 2_KB;
static constexpr ui32 MAX_TXS = 1000;

struct TChangeNotification {
    TChangeNotification(const TActorId& actor, const ui64 txId)
        : Actor(actor)
        , TxId(txId)
    {}

    operator size_t() const {
        return THash<TActorId>()(Actor) + TxId;
    }

    bool operator==(const TChangeNotification& b) {
        return b.Actor == Actor && b.TxId == TxId;
    }

    bool operator < (const TChangeNotification& req) const {
        return (Actor < req.Actor) || (Actor == req.Actor && TxId < req.TxId);
    }

    TActorId Actor;
    ui64 TxId;
};

static TMaybe<TPartitionKeyRange> GetPartitionKeyRange(const NKikimrPQ::TPQTabletConfig& config,
                                                       const NKikimrPQ::TPQTabletConfig::TPartition& proto) {
    if (!proto.HasKeyRange() || config.GetPartitionKeySchema().empty()) {
        return Nothing();
    }
    return TPartitionKeyRange::Parse(proto.GetKeyRange());
}

static bool IsDirectReadCmd(const auto& cmd) {
    return cmd.GetDirectReadId() != 0;
}


TEvPQ::TMessageGroupsPtr CreateExplicitMessageGroups(const NKikimrPQ::TBootstrapConfig& bootstrapCfg, const NKikimrPQ::TPartitions& partitionsData, const TPartitionGraph& graph, ui32 partitionId) {
    TEvPQ::TMessageGroupsPtr explicitMessageGroups = std::make_shared<TEvPQ::TMessageGroups>();

    for (const auto& mg : bootstrapCfg.GetExplicitMessageGroups()) {
        TPartitionKeyRange keyRange;
        if (mg.HasKeyRange()) {
            keyRange = TPartitionKeyRange::Parse(mg.GetKeyRange());
        }

        (*explicitMessageGroups)[mg.GetId()] = {0, std::move(keyRange)};
    }

    if (graph) {
        auto* node = graph.GetPartition(partitionId);
        Y_VERIFY_S(node, "Partition " << partitionId << " not found. Known partitions " << graph.DebugString());
        for (const auto& p : partitionsData.GetPartition()) {
            if (node->IsParent(p.GetPartitionId())) {
                for (const auto& g : p.GetMessageGroup()) {
                    auto& group = (*explicitMessageGroups)[g.GetId()];
                    group.SeqNo = std::max(group.SeqNo, g.GetSeqNo());
                }
            }
        }
    }

    return explicitMessageGroups;
}

/******************************************************* AnswerBuilderProxy *********************************************************/
class TResponseBuilder {
public:

    TResponseBuilder(const TActorId& sender, const TActorId& tablet, const TString& topicName, const ui32 partition, const ui64 messageNo,
                     const TString& reqId, const TMaybe<ui64> cookie, NMetrics::TResourceMetrics* resourceMetrics,
                     const TActorContext&)
    : Sender(sender)
    , Tablet(tablet)
    , TopicName(topicName)
    , Partition(partition)
    , MessageNo(messageNo)
    , CounterId(0)
    , Waiting(0)
    , ReqId(reqId)
    , Response(new TEvPersQueue::TEvResponse)
    , Timestamp(TAppData::TimeProvider->Now())
    , WasSplit(false)
    , Cookie(cookie)
    , ResourceMetrics(resourceMetrics)
    {
        if (cookie)
            Response->Record.MutablePartitionResponse()->SetCookie(*cookie);

        PQ_LOG_D("Handle TEvRequest topic: '" << TopicName << "' requestId: " << ReqId);

    }

    void SetWasSplit()
    {
        WasSplit = true;
    }

    void AddPartialReplyCount(const ui32 partialReplyCount)
    {
        Waiting += partialReplyCount;
    }

    void SetCounterId(const ui32 counterId)
    {
        CounterId = counterId;
    }

    bool HandleProxyResponse(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext& ctx)
    {
        AFL_ENSURE(Waiting);
        AFL_ENSURE(Response);
        --Waiting;
        bool skip = false;
        if (WasSplit && ev->Get()->Response->GetPartitionResponse().CmdWriteResultSize() == 1) { //megaqc - remove this
            const auto& x = ev->Get()->Response->GetPartitionResponse().GetCmdWriteResult(0);
            if (x.HasPartNo() && x.GetPartNo() > 0)
                skip = true;
        }
        if (!skip) //megaqc - remove this
            Response->Record.MergeFrom(*ev->Get()->Response);

        if (!Waiting) {
            PQ_LOG_D("Answer ok topic: '" << TopicName << "' partition: " << Partition
                        << " messageNo: " << MessageNo << "  requestId: " << ReqId << " cookie: " << (Cookie ? *Cookie : 0));

            if (ResourceMetrics) {
                ResourceMetrics->Network.Increment(Response->Record.ByteSizeLong());
                ResourceMetrics->TryUpdate(ctx);
            }

            ctx.Send(Sender, Response.Release());
            return true;
        }
        return false;
    }

    bool HandleError(TEvPQ::TEvError *ev, const TActorContext& ctx)
    {
        const auto errorCode = ev->ErrorCode;
        const auto priority = errorCode == NPersQueue::NErrorCode::OVERLOAD ? NActors::NLog::EPriority::PRI_INFO : NActors::NLog::EPriority::PRI_ERROR;
        PQ_LOG(priority, "Answer error topic: '" << TopicName << "'" <<
                 " partition: " << Partition <<
                 " messageNo: " << MessageNo <<
                 " requestId: " << ReqId <<
                 " error: " << ev->Error);

        Response->Record.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
        Response->Record.SetErrorCode(ev->ErrorCode);
        Response->Record.SetErrorReason(ev->Error);
        ctx.Send(Sender, Response.Release());
        return true;
    }

    const TActorId Sender;
    const TActorId Tablet;
    const TString TopicName;
    const ui32 Partition;
    const ui64 MessageNo;
    ui32 CounterId;
    ui32 Waiting;
    const TString ReqId;
    THolder<TEvPersQueue::TEvResponse> Response;
    TInstant Timestamp;
    bool WasSplit;
    TMaybe<ui64> Cookie;
    NMetrics::TResourceMetrics* ResourceMetrics;
};


TAutoPtr<TResponseBuilder> CreateResponseProxy(const TActorId& sender, const TActorId& tablet, const TString& topicName,
                                                    const ui32 partition, const ui64 messageNo, const TString& reqId, const TMaybe<ui64> cookie,
                                                    NMetrics::TResourceMetrics *resourceMetrics, const TActorContext& ctx)
{
    return new TResponseBuilder(sender, tablet, topicName, partition, messageNo, reqId, cookie, resourceMetrics, ctx);
}


/******************************************************* OffsetsBuilderProxy *********************************************************/

template <typename T, typename T2, typename T3>
class TBuilderProxy : public TBaseTabletActor<TBuilderProxy<T,T2,T3>> {
    typedef TBuilderProxy<T,T2,T3> TThis;

    friend class TActorBootstrapped<TThis>;
    typedef T TEvent;
    typedef typename TEvent::TPtr TTPtr;

    using TBase = TBaseTabletActor<TThis>;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_ANS_ACTOR;
    }

    TBuilderProxy(const ui64 tabletId, const TActorId& tabletActorId, const TActorId& sender, const ui32 count, const ui64 cookie)
    : TBaseTabletActor<TBuilderProxy<T,T2,T3>>(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
    , Sender(sender)
    , Waiting(count)
    , Result()
    , Cookie(cookie)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        TThis::Become(&TThis::StateFunc);
        if (!Waiting) {
            AnswerAndDie(ctx);
            return;
        }
        ctx.Schedule(TOTAL_TIMEOUT, new TEvents::TEvWakeup());
    }

    const TString& GetLogPrefix() const {
        static const TString LogPrefix = "[BuilderProxy]";
        return LogPrefix;
    }

private:

    void AnswerAndDie(const TActorContext& ctx)
    {
        std::sort(Result.begin(), Result.end(), [](const typename T2::TPartResult& a, const typename T2::TPartResult& b){
                                                    return a.GetPartition() < b.GetPartition();
                                                });
        THolder<T3> res = MakeHolder<T3>();
        auto& resp = res->Record;
        resp.SetTabletId(TBase::TabletId);
        for (const auto& p : Result) {
            resp.AddPartResult()->CopyFrom(p);
        }
        ctx.Send(Sender, res.Release(), 0, Cookie);
        TThis::Die(ctx);
    }

    void Handle(TTPtr& ev, const TActorContext& ctx)
    {
        Result.push_back(ev->Get()->PartResult);
        if (--Waiting == 0) {
            AnswerAndDie(ctx);
        }
    }

    void Wakeup(const TActorContext& ctx) {
        AnswerAndDie(ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvent, Handle);
            CFunc(TEvents::TSystem::Wakeup, Wakeup);
        default:
            break;
        };
    }

    TActorId Sender;
    ui32 Waiting;
    TVector<typename T2::TPartResult> Result;
    ui64 Cookie;
};


TActorId CreateOffsetsProxyActor(const ui64 tabletId, const TActorId& tabletActorId, const TActorId& sender, const ui32 count, const TActorContext& ctx)
{
    return ctx.Register(new TBuilderProxy<TEvPQ::TEvPartitionOffsetsResponse,
                                          NKikimrPQ::TOffsetsResponse,
                                          TEvPersQueue::TEvOffsetsResponse>(tabletId, tabletActorId, sender, count, 0));
}

/******************************************************* StatusProxy *********************************************************/


TActorId CreateStatusProxyActor(const ui64 tabletId, const TActorId& tabletActorId, const TActorId& sender, const ui32 count, const ui64 cookie, const TActorContext& ctx)
{
    return ctx.Register(new TBuilderProxy<TEvPQ::TEvPartitionStatusResponse,
                                          NKikimrPQ::TStatusResponse,
                                          TEvPersQueue::TEvStatusResponse>(tabletId, tabletActorId, sender, count, cookie));
}

/******************************************************* TPersQueue *********************************************************/

struct TPersQueue::TReplyToActor {
    using TEventBasePtr = std::unique_ptr<IEventBase>;

    TReplyToActor(const TActorId& actorId, TEventBasePtr event) :
        ActorId(actorId),
        Event(std::move(event))
    {
    }

    TActorId ActorId;
    TEventBasePtr Event;
};

void TPersQueue::ReplyError(const TActorContext& ctx, const ui64 responseCookie, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error)
{
    ReplyPersQueueError(
        ctx.SelfID, ctx, TabletID(), TopicName, Nothing(), *Counters, NKikimrServices::PERSQUEUE,
        responseCookie, errorCode, error
    );
}

void TPersQueue::ApplyNewConfigAndReply(const TActorContext& ctx)
{
    EnsurePartitionsAreNotDeleted(NewConfig);

    // in order to answer only after all parts are ready to work
    PQ_ENSURE(ConfigInited && AllOriginalPartitionsInited());

    ApplyNewConfig(NewConfig, ctx);
    ClearNewConfig();

    for (auto& p : Partitions) { //change config for already created partitions
        if (p.first.IsSupportivePartition()) {
            continue;
        }

        ctx.Send(p.second.Actor, new TEvPQ::TEvChangePartitionConfig(TopicConverter, Config));
    }
    ChangePartitionConfigInflight += Partitions.size();

    for (const auto& partition : Config.GetPartitions()) {
        const TPartitionId partitionId(partition.GetPartitionId());
        if (Partitions.find(partitionId) == Partitions.end()) {
            CreateOriginalPartition(Config,
                                    partition,
                                    TopicConverter,
                                    partitionId,
                                    true,
                                    ctx);
        }
    }

    TrySendUpdateConfigResponses(ctx);
}

void TPersQueue::ApplyNewConfig(const NKikimrPQ::TPQTabletConfig& newConfig,
                                const TActorContext& ctx)
{
    Config = newConfig;

    PQ_LOG_D("Apply new config " << Config.ShortDebugString());

    ui32 cacheSize = CACHE_SIZE;
    if (Config.HasCacheSize()) {
        cacheSize = Config.GetCacheSize();
    }

    if (!TopicConverter) { // it's the first time
        TopicName = Config.GetTopicName();
        TopicPath = Config.GetTopicPath();
        IsLocalDC = Config.GetLocalDC();

        CreateTopicConverter(Config,
                             TopicConverterFactory,
                             TopicConverter,
                             ctx);

        KeySchema.clear();
        KeySchema.reserve(Config.PartitionKeySchemaSize());
        for (const auto& component : Config.GetPartitionKeySchema()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(component.GetTypeId(),
                component.HasTypeInfo() ? &component.GetTypeInfo() : nullptr);
            KeySchema.push_back(typeInfoMod.TypeInfo);
        }

        PQ_ENSURE(TopicName.size())("description", "Need topic name here");
        ctx.Send(CacheActor, new TEvPQ::TEvChangeCacheConfig(TopicName, cacheSize));
    } else {
        //AFL_ENSURE(TopicName == Config.GetTopicName(), "Changing topic name is not supported");
        TopicPath = Config.GetTopicPath();
        ctx.Send(CacheActor, new TEvPQ::TEvChangeCacheConfig(cacheSize));
    }

    InitializeMeteringSink(ctx);
}

void TPersQueue::EndWriteConfig(const NKikimrClient::TResponse& resp, const TActorContext& ctx)
{
    if (resp.GetStatus() != NMsgBusProxy::MSTATUS_OK ||
        resp.WriteResultSize() < 1) {
        PQ_LOG_ERROR("Config write error: " << resp.DebugString() << " " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }
    for (const auto& res : resp.GetWriteResult()) {
        if (res.GetStatus() != NKikimrProto::OK) {
            PQ_LOG_ERROR("Config write error: " << resp.DebugString() << " " << ctx.SelfID);
                ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
            return;
        }
    }

    if (resp.WriteResultSize() > 1) {
        PQ_LOG_I("restarting - have some registering of message groups");
            ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    PQ_ENSURE(resp.WriteResultSize() >= 1);
    PQ_ENSURE(resp.GetWriteResult(0).GetStatus() == NKikimrProto::OK);
    if (ConfigInited && AllOriginalPartitionsInited()) //all partitions are working well - can apply new config
        ApplyNewConfigAndReply(ctx);
    else
        NewConfigShouldBeApplied = true; //when config will be inited with old value new config will be applied
}

void TPersQueue::HandleConfigReadResponse(NKikimrClient::TResponse&& resp, const TActorContext& ctx)
{
    bool ok =
        (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK) &&
        (resp.ReadResultSize() == 3) &&
        (resp.HasSetExecutorFastLogPolicyResult()) &&
        (resp.GetSetExecutorFastLogPolicyResult().GetStatus() == NKikimrProto::OK);
    if (!ok) {
        PQ_LOG_ERROR_AND_DIE("Config read error: " << resp.ShortDebugString());
        return;
    }

    ConfigReadResponse = std::move(resp);

    BeginInitTransactions();
    SendTransactionsReadRequest(GetTxKey(Min<ui64>()), true, ctx);
}

void TPersQueue::SendTransactionsReadRequest(const TString& fromKey, bool includeFrom,
                                             const TActorContext& ctx)
{
    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    request->Record.SetCookie(READ_TXS_COOKIE);

    AddCmdReadTransactionRange(*request, fromKey, includeFrom);

    request->Record.MutableCmdSetExecutorFastLogPolicy()
                ->SetIsAllowed(AppData(ctx)->PQConfig.GetTactic() == NKikimrClient::TKeyValueRequest::MIN_LATENCY);
    ctx.Send(ctx.SelfID, request.Release());
}

TString GetLastKey(const NKikimrClient::TKeyValueResponse::TReadRangeResult& result)
{
    if (!result.PairSize()) {
        return {};
    }

    return result.GetPair(result.PairSize() - 1).GetKey();
}

void TPersQueue::HandleTransactionsReadResponse(NKikimrClient::TResponse&& resp, const TActorContext& ctx)
{
    bool ok =
        (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK) &&
        (resp.ReadRangeResultSize() == 1) &&
        (resp.HasSetExecutorFastLogPolicyResult()) &&
        (resp.GetSetExecutorFastLogPolicyResult().GetStatus() == NKikimrProto::OK);
    if (!ok) {
        PQ_LOG_ERROR_AND_DIE("Transactions read error: " << resp.ShortDebugString());
        return;
    }

    const auto& result = resp.GetReadRangeResult(0);
    auto status = result.GetStatus();
    if (status != NKikimrProto::OVERRUN &&
        status != NKikimrProto::OK &&
        status != NKikimrProto::NODATA) {
        ok = false;
    }
    if (!ok) {
        PQ_LOG_ERROR_AND_DIE("Transactions read error: " << resp.ShortDebugString());
        return;
    }

    TransactionsReadResults.emplace_back(std::move(result));

    if (status == NKikimrProto::OVERRUN) {
        SendTransactionsReadRequest(GetLastKey(result), false, ctx);
        return;
    }

    ReadTxInfo(ConfigReadResponse.GetReadResult(2), ctx);
    ReadConfig(ConfigReadResponse.GetReadResult(0), TransactionsReadResults, ctx);
    ReadTxWrites(ConfigReadResponse.GetReadResult(2), ctx);
    ReadState(ConfigReadResponse.GetReadResult(1), ctx);
}

void TPersQueue::ReadTxInfo(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                            const TActorContext& ctx)
{
    PQ_ENSURE(read.HasStatus());
    if (read.GetStatus() != NKikimrProto::OK && read.GetStatus() != NKikimrProto::NODATA) {
        PQ_LOG_ERROR("tx info read error " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    switch (read.GetStatus()) {
    case NKikimrProto::OK: {
        PQ_LOG_TX_I("has a tx info");

        NKikimrPQ::TTabletTxInfo info;
        PQ_ENSURE(info.ParseFromString(read.GetValue()));

        InitPlanStep(info);

        break;
    }
    case NKikimrProto::NODATA: {
        PQ_LOG_TX_I("doesn't have tx info");

        InitPlanStep();

        break;
    }
    default:
        PQ_LOG_ERROR_AND_DIE("Unexpected tx info read status: " << read.GetStatus());
        return;
    }

    PQ_LOG_TX_I("PlanStep " << PlanStep << ", PlanTxId " << PlanTxId <<
             ", ExecStep " << ExecStep << ", ExecTxId " << ExecTxId);
}

void TPersQueue::InitPlanStep(const NKikimrPQ::TTabletTxInfo& info)
{
    PlanStep = info.GetPlanStep();
    PlanTxId = info.GetPlanTxId();

    ExecStep = info.GetExecStep();
    ExecTxId = info.GetExecTxId();
}

void TPersQueue::ReadTxWrites(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                              const TActorContext& ctx)
{
    PQ_ENSURE(read.HasStatus());
    if (read.GetStatus() != NKikimrProto::OK && read.GetStatus() != NKikimrProto::NODATA) {
        PQ_LOG_ERROR("tx writes read error " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    switch (read.GetStatus()) {
    case NKikimrProto::OK: {
        PQ_LOG_I("has a tx writes info");

        NKikimrPQ::TTabletTxInfo info;
        if (!info.ParseFromString(read.GetValue())) {
            PQ_LOG_ERROR_AND_DIE("tx writes read error");
            return;
        }

        InitTxWrites(info, ctx);

        break;
    }
    case NKikimrProto::NODATA: {
        PQ_LOG_I("doesn't have tx writes info");

        InitTxWrites({}, ctx);

        break;
    }
    default:
        PQ_LOG_ERROR_AND_DIE("Unexpected tx writes info read status: " << read.GetStatus());
        return;
    }
}

void TPersQueue::CreateOriginalPartition(const NKikimrPQ::TPQTabletConfig& config,
                                         const NKikimrPQ::TPQTabletConfig::TPartition& partition,
                                         NPersQueue::TTopicConverterPtr topicConverter,
                                         const TPartitionId& partitionId,
                                         bool newPartition,
                                         const TActorContext& ctx)
{
    TActorId actorId = ctx.RegisterWithSameMailbox(CreatePartitionActor(partitionId,
                                                         topicConverter,
                                                         config,
                                                         newPartition,
                                                         ctx));
    Partitions.emplace(std::piecewise_construct,
                       std::forward_as_tuple(partitionId),
                       std::forward_as_tuple(actorId,
                                             GetPartitionKeyRange(config, partition)));
    ++OriginalPartitionsCount;
}

void TPersQueue::MoveTopTxToCalculating(TDistributedTransaction& tx,
                                        const TActorContext& ctx)
{
    PQ_ENSURE(!TxQueue.empty());

    std::tie(ExecStep, ExecTxId) = TxQueue.front();
    PQ_LOG_TX_I("New ExecStep " << ExecStep << ", ExecTxId " << ExecTxId);

    switch (tx.Kind) {
    case NKikimrPQ::TTransaction::KIND_DATA:
        SendEvTxCalcPredicateToPartitions(ctx, tx);
        break;
    case NKikimrPQ::TTransaction::KIND_CONFIG: {
        NPersQueue::TConverterFactoryPtr converterFactory;
        CreateTopicConverter(tx.TabletConfig,
                             converterFactory,
                             tx.TopicConverter,
                             ctx);
        CreateNewPartitions(tx.TabletConfig,
                            tx.TopicConverter,
                            ctx);
        SendEvProposePartitionConfig(ctx, tx);
        break;
    }
    case NKikimrPQ::TTransaction::KIND_UNKNOWN:
        PQ_ENSURE(false);
    }

    TryChangeTxState(tx, NKikimrPQ::TTransaction::CALCULATING);
}

void TPersQueue::AddSupportivePartition(const TPartitionId& partitionId)
{
    Partitions.emplace(partitionId,
                       TPartitionInfo(TActorId(),
                                      {}));
    NewSupportivePartitions.insert(partitionId);
}

NKikimrPQ::TPQTabletConfig TPersQueue::MakeSupportivePartitionConfig() const
{
    NKikimrPQ::TPQTabletConfig partitionConfig = Config;
    partitionConfig.MutableConsumers()->Clear();
    return partitionConfig;
}

void TPersQueue::CreateSupportivePartitionActor(const TPartitionId& partitionId,
                                                const TActorContext& ctx)
{
    PQ_ENSURE(Partitions.contains(partitionId));

    TPartitionInfo& partition = Partitions.at(partitionId);
    partition.Actor = ctx.RegisterWithSameMailbox(CreatePartitionActor(partitionId,
                                                        TopicConverter,
                                                        MakeSupportivePartitionConfig(),
                                                        false,
                                                        ctx));
}

void TPersQueue::InitTxWrites(const NKikimrPQ::TTabletTxInfo& info,
                              const TActorContext& ctx)
{
    if (info.HasNextSupportivePartitionId()) {
        NextSupportivePartitionId = info.GetNextSupportivePartitionId();
    } else {
        NextSupportivePartitionId = 100'000;
    }

    for (size_t i = 0; i != info.TxWritesSize(); ++i) {
        auto& txWrite = info.GetTxWrites(i);
        const TWriteId writeId = GetWriteId(txWrite);

        TTxWriteInfo& writeInfo = TxWrites[writeId];
        if (txWrite.HasOriginalPartitionId()) {
            ui32 partitionId = txWrite.GetOriginalPartitionId();
            TPartitionId shadowPartitionId(partitionId, writeId, txWrite.GetInternalPartitionId());

            writeInfo.Partitions.emplace(partitionId, shadowPartitionId);

            AddSupportivePartition(shadowPartitionId);
            CreateSupportivePartitionActor(shadowPartitionId, ctx);

            NextSupportivePartitionId = Max(NextSupportivePartitionId, shadowPartitionId.InternalPartitionId + 1);
        }

        if (writeId.IsTopicApiTransaction()) {
            SubscribeWriteId(writeId, ctx);
        }

        // this branch will be executed only if EnableKafkaTransactions feature flag is enabled, cause
        // sending transactional requests through Kafka API is restricted by feature flag here: ydb/core/kafka_proxy/kafka_connection.cpp
        if (txWrite.GetKafkaTransaction() && txWrite.HasCreatedAt()) {
            writeInfo.KafkaTransaction = true;
            writeInfo.CreatedAt = TInstant::MilliSeconds(txWrite.GetCreatedAt());
        }
    }

    NewSupportivePartitions.clear();
}

void TPersQueue::ReadConfig(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                            const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges,
                            const TActorContext& ctx)
{
    PQ_ENSURE(read.HasStatus());
    if (read.GetStatus() != NKikimrProto::OK && read.GetStatus() != NKikimrProto::NODATA) {
        PQ_LOG_ERROR("Config read error " << ctx.SelfID <<
            " Error status code " << read.GetStatus());
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    PQ_ENSURE(!ConfigInited);

    if (read.GetStatus() == NKikimrProto::OK) {
        bool res = Config.ParseFromString(read.GetValue());
        PQ_ENSURE(res);

        Migrate(Config);

        TopicName = Config.GetTopicName();
        TopicPath = Config.GetTopicPath();
        IsLocalDC = Config.GetLocalDC();

        CreateTopicConverter(Config,
                             TopicConverterFactory,
                             TopicConverter,
                             ctx);

        KeySchema.clear();
        KeySchema.reserve(Config.PartitionKeySchemaSize());
        for (const auto& component : Config.GetPartitionKeySchema()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(component.GetTypeId(),
                component.HasTypeInfo() ? &component.GetTypeInfo() : nullptr);
            KeySchema.push_back(typeInfoMod.TypeInfo);
        }

        ui32 cacheSize = CACHE_SIZE;
        if (Config.HasCacheSize())
            cacheSize = Config.GetCacheSize();

        PQ_ENSURE(TopicName.size())("description", "Need topic name here");
        ctx.Send(CacheActor, new TEvPQ::TEvChangeCacheConfig(TopicName, cacheSize));
    } else if (read.GetStatus() == NKikimrProto::NODATA) {
        PQ_LOG_D("no config, start with empty partitions and default config");
    } else {
        PQ_LOG_ERROR_AND_DIE("Unexpected config read status: " << read.GetStatus());
        return;
    }

    TxWrites.clear();

    const auto txs = CollectTransactions(readRanges);
    for (const auto& [txId, tx] : txs) {
        if (tx.HasStep()) {
            if (tx.GetStep() > PlanStep) {
                PlanStep = tx.GetStep();
                PlanTxId = tx.GetTxId();
                PQ_LOG_TX_D("PlanStep " << PlanStep << ", PlanTxId " << PlanTxId);
            } else if (tx.GetStep() == PlanStep) {
                if (tx.GetTxId() > PlanTxId) {
                    PlanTxId = tx.GetTxId();
                    PQ_LOG_TX_D("PlanStep " << PlanStep << ", PlanTxId " << PlanTxId);
                }
            }
        }

        PQ_LOG_TX_I("Restore Tx. " <<
                    "TxId: " << tx.GetTxId() <<
                    ", Step: " << tx.GetStep() <<
                    ", State: " << NKikimrPQ::TTransaction_EState_Name(tx.GetState()) <<
                    ", Predicate: " << tx.GetPredicate() << " (" << (tx.HasPredicate() ? "+" : "-") << ")" <<
                    ", WriteId: " << tx.GetWriteId().ShortDebugString());

        Txs.emplace(tx.GetTxId(), tx);

        if (tx.HasWriteId()) {
            PQ_LOG_TX_I("Link TxId " << tx.GetTxId() << " with WriteId " << GetWriteId(tx));
            TxWrites[GetWriteId(tx)].TxId = tx.GetTxId();
        }
    }

    for (const auto& [txId, tx] : Txs) {
        PQ_LOG_D("TxId: " << txId <<
                 ", Step: " << tx.Step <<
                 ", State: " << NKikimrPQ::TTransaction_EState_Name(tx.State) <<
                 ", Decision: " << NKikimrTx::TReadSetData_EDecision_Name(tx.ParticipantsDecision));

        if (tx.Step != Max<ui64>()) {
            PlannedTxs.emplace_back(tx.Step, txId);
        }
    }

    // у таблетки 5 партиций. все участвуют в транзакции
    // в очереди 2 транзакции
    // первая транзакция выполнялась на stable-25-4 и только 4 партиции успели выполнить коммит
    // таблетка переехала на stable-26-1, для второй транзакции дошли все предикаты и уже обе транзакции пошли выполняться в партициях
    // оставшаяся партиция из первой транзакции и 5 партиций из второй транзакции записали субтранзакции
    // таблетка снова перезапустилась и пытается восстановить транзакций
    // у первой транзакции в очереди состояние PLANNED, а у второй - EXECUTED
    // надо подправить состояние транзакций. можно продвинуть вперёд PLANNED -> EXECUTED, а можно наоборот
    std::sort(PlannedTxs.begin(), PlannedTxs.end());
    for (size_t i = 1; i < PlannedTxs.size(); ++i) {
        auto& prevTx = Txs.at(PlannedTxs[i - 1].second);
        auto& currentTx = Txs.at(PlannedTxs[i].second);

        if (prevTx.State < currentTx.State) {
            currentTx.State = prevTx.State;
        }
    }

    EndInitTransactions();
    EndReadConfig(ctx);

    SetTxCounters();
}

void TPersQueue::EndReadConfig(const TActorContext& ctx)
{
    for (const auto& partition : Config.GetPartitions()) { // no partitions will be created with empty config
        const TPartitionId partitionId(partition.GetPartitionId());
        CreateOriginalPartition(Config,
                                partition,
                                TopicConverter,
                                partitionId,
                                false,
                                ctx);
    }

    ConfigInited = true;

    InitializeMeteringSink(ctx);

    PQ_ENSURE(!NewConfigShouldBeApplied);
    for (auto& req : UpdateConfigRequests) {
        ProcessUpdateConfigRequest(req.first, req.second, ctx);
    }
    UpdateConfigRequests.clear();

    for (auto& req : HasDataRequests) {
        const TPartitionId partitionId(req->Record.GetPartition());
        auto it = Partitions.find(partitionId);
        if (it != Partitions.end()) {
            if (ctx.Now().MilliSeconds() < req->Record.GetDeadline()) { //otherwise there is no need to send event - proxy will generate event itself
                ctx.Send(it->second.Actor, req.Release());
            }
        }
    }
    HasDataRequests.clear();

    if (Partitions.empty()) {
        OnInitComplete(ctx);
    }
}

void TPersQueue::ReadState(const NKikimrClient::TKeyValueResponse::TReadResult& read, const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    if (read.GetStatus() == NKikimrProto::OK) {
        NKikimrPQ::TTabletState stateProto;
        bool ok = stateProto.ParseFromString(read.GetValue());
        PQ_ENSURE(ok);
        PQ_ENSURE(stateProto.HasState());
        TabletState = stateProto.GetState();
    } else if (read.GetStatus() == NKikimrProto::NODATA) {
        TabletState = NKikimrPQ::ENormal;
    } else {
        PQ_LOG_ERROR_AND_DIE("Unexpected state read status: " << read.GetStatus());
        return;
    }
}

void TPersQueue::InitializeMeteringSink(const TActorContext& ctx) {
    TStringBuf stream;
    const auto streamName =
        TStringBuf(Config.GetTopicPath()).AfterPrefix(Config.GetYdbDatabasePath() + "/", stream)
        ? stream.data() : Config.GetTopicPath();
    const auto streamPath = Config.GetTopicPath();

    auto& pqConfig = AppData(ctx)->PQConfig;
    if (!pqConfig.GetBillingMeteringConfig().GetEnabled()) {
        PQ_LOG_NOTICE("disable metering"
            << ": reason# " << "billing is not enabled in BillingMeteringConfig");
        return;
    }

    TSet<EMeteringJson> whichToFlush{EMeteringJson::PutEventsV1,
                                     EMeteringJson::ResourcesReservedV1,
                                     EMeteringJson::UsedStorageV1};
    ui64 storageLimitBytes{Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() *
        Config.GetPartitionConfig().GetLifetimeSeconds()};

    if (Config.GetPartitionConfig().HasStorageLimitBytes()) {
        storageLimitBytes = Config.GetPartitionConfig().GetStorageLimitBytes();
        whichToFlush = TSet<EMeteringJson>{EMeteringJson::PutEventsV1,
                                           EMeteringJson::ThroughputV1,
                                           EMeteringJson::StorageV1,
                                           EMeteringJson::UsedStorageV1};
    }

    switch (Config.GetMeteringMode()) {
    case NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS:
        PQ_LOG_NOTICE("metering mode METERING_MODE_REQUEST_UNITS");
        whichToFlush = TSet<EMeteringJson>{EMeteringJson::UsedStorageV1};
        break;

    default:
        break;
    }

    auto countReadRulesWithPricing = [&](const TActorContext& ctx, const auto& config) {
        auto& defaultClientServiceType = AppData(ctx)->PQConfig.GetDefaultClientServiceType().GetName();

        ui32 result = 0;
        for (auto& consumer : config.GetConsumers()) {
            TString serviceType = consumer.GetServiceType();
            if (serviceType.empty() || serviceType == defaultClientServiceType)
                ++result;
        }
        return result;
    };

    MeteringSink.Create(ctx.Now(), {
            .FlushInterval  = TDuration::Seconds(pqConfig.GetBillingMeteringConfig().GetFlushIntervalSec()),
            .TabletId       = ToString(TabletID()),
            .YcCloudId      = Config.GetYcCloudId(),
            .YcFolderId     = Config.GetYcFolderId(),
            .YdbDatabaseId  = Config.GetYdbDatabaseId(),
            .StreamName     = streamName,
            .ResourceId     = streamPath,
            .PartitionsSize = CountActivePartitions(Config.GetPartitions()),
            .WriteQuota     = Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond(),
            .ReservedSpace  = storageLimitBytes,
            .ConsumersCount = countReadRulesWithPricing(ctx, Config),
        }, whichToFlush, std::bind(NMetering::SendMeteringJson, ctx, std::placeholders::_1));
}

void TPersQueue::ReturnTabletState(const TActorContext& ctx, const TChangeNotification& req, NKikimrProto::EReplyStatus status)
{
    THolder<TEvPersQueue::TEvDropTabletReply> event = MakeHolder<TEvPersQueue::TEvDropTabletReply>();
    event->Record.SetStatus(status);
    event->Record.SetTabletId(TabletID());
    event->Record.SetTxId(req.TxId);
    event->Record.SetActualState(TabletState);

    ctx.Send(req.Actor, event.Release());
}

void TPersQueue::TryReturnTabletStateAll(const TActorContext& ctx, NKikimrProto::EReplyStatus status)
{
    if (ReadyForDroppedReply()) {
        for (const auto& req : TabletStateRequests) {
            ReturnTabletState(ctx, req, status);
        }
        TabletStateRequests.clear();
    }
}

void TPersQueue::BeginWriteTabletState(const TActorContext& ctx, NKikimrPQ::ETabletState state)
{
    NKikimrPQ::TTabletState stateProto;
    stateProto.SetState(state);
    TString strState;
    bool ok = stateProto.SerializeToString(&strState);
    PQ_ENSURE(ok);

    TAutoPtr<TEvKeyValue::TEvRequest> kvRequest(new TEvKeyValue::TEvRequest);
    kvRequest->Record.SetCookie(WRITE_STATE_COOKIE);

    auto kvCmd = kvRequest->Record.AddCmdWrite();
    kvCmd->SetKey(KeyState());
    kvCmd->SetValue(strState);
    kvCmd->SetTactic(AppData(ctx)->PQConfig.GetTactic());
    kvCmd->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

    ctx.Send(ctx.SelfID, kvRequest.Release());
}

void TPersQueue::EndWriteTabletState(const NKikimrClient::TResponse& resp, const TActorContext& ctx)
{
    bool ok = (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK) &&
            (resp.WriteResultSize() == 1) &&
            (resp.GetWriteResult(0).GetStatus() == NKikimrProto::OK);
    if (!ok) {
        PQ_LOG_ERROR("SelfId " << ctx.SelfID << " State write error: " << resp.DebugString());

        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    TabletState = NKikimrPQ::EDropped;

    TryReturnTabletStateAll(ctx);
}

void TPersQueue::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx)
{
    auto& resp = ev->Get()->Record;

    switch (resp.GetCookie()) {
    case WRITE_CONFIG_COOKIE:
        EndWriteConfig(resp, ctx);
        break;
    case READ_CONFIG_COOKIE:
        // read is only for config - is signal to create interal actors
        HandleConfigReadResponse(std::move(resp), ctx);
        break;
    case READ_TXS_COOKIE:
        HandleTransactionsReadResponse(std::move(resp), ctx);
        break;
    case WRITE_STATE_COOKIE:
        EndWriteTabletState(resp, ctx);
        break;
    case WRITE_TX_COOKIE:
        PQ_LOG_D("Handle TEvKeyValue::TEvResponse (WRITE_TX_COOKIE)");
        EndWriteTxs(resp, ctx);
        // Завершилась операция с CmdWrite. Можно отправлять отложенные TEvReadSetAck
        SendDeferredReadSetAcks(ctx);
        break;
    default:
        PQ_LOG_ERROR("Unexpected KV response: " << ev->Get()->ToString() << " " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
    }
}

void TPersQueue::SetCacheCounters(TEvPQ::TEvTabletCacheCounters::TCacheCounters& cacheCounters)
{
    Counters->Simple()[COUNTER_PQ_TABLET_CACHE_SIZE] = cacheCounters.CacheSizeBytes;
    Counters->Simple()[COUNTER_PQ_TABLET_CACHE_COUNT] = cacheCounters.CacheSizeBlobs;
    Counters->Simple()[COUNTER_PQ_TABLET_CACHED_ON_READ] = cacheCounters.CachedOnRead;
    Counters->Simple()[COUNTER_PQ_TABLET_CACHED_ON_WRATE] = cacheCounters.CachedOnWrite;
    Counters->Simple()[COUNTER_PQ_TABLET_OPENED_PIPES] = PipesInfo.size();
}

void TPersQueue::Handle(TEvPQ::TEvMetering::TPtr& ev, const TActorContext&)
{
    MeteringSink.IncreaseQuantity(ev->Get()->Type, ev->Get()->Quantity);
}

TPartitionInfo& TPersQueue::GetPartitionInfo(const TPartitionId& partitionId)
{
    auto it = Partitions.find(partitionId);
    PQ_ENSURE(it != Partitions.end());
    return it->second;
}

void TPersQueue::Handle(TEvPQ::TEvPartitionCounters::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_T("Handle TEvPQ::TEvPartitionCounters" <<
             " PartitionId " << ev->Get()->Partition);

    auto& partitionId = ev->Get()->Partition;
    auto& partition = GetPartitionInfo(partitionId);

    auto& counters = ev->Get()->Counters;
    ui64 cpuUsage = counters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE].Get();
    ui64 networkBytesUsage = counters.Cumulative()[COUNTER_PQ_TABLET_NETWORK_BYTES_USAGE].Get();
    if (ResourceMetrics) {
        if (cpuUsage > 0) {
            ResourceMetrics->CPU.Increment(cpuUsage);
        }
        if (networkBytesUsage > 0) {
            ResourceMetrics->Network.Increment(networkBytesUsage);
        }
        if (cpuUsage > 0 || networkBytesUsage > 0) {
            ResourceMetrics->TryUpdate(ctx);
        }
    }
    Counters->Percentile().Populate(counters.Percentile());
    Counters->Cumulative().Populate(counters.Cumulative());

    partition.ReservedBytes = counters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Get();

    // restore cache's simple counters cleaned by partition's counters
    SetCacheCounters(CacheCounters);
    ui64 reservedSize = std::accumulate(Partitions.begin(), Partitions.end(), 0ul,
        [](ui64 sum, const auto& p) { return sum + p.second.ReservedBytes; });
    Counters->Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(reservedSize);

    // Features of the implementation of SimpleCounters. It is necessary to restore the value of
    // indicators for transactions.
    SetTxCounters();
}


void TPersQueue::AggregateAndSendLabeledCountersFor(const TString& group, const TActorContext& ctx)
{
    if (CounterEventsInflight[group].RefCount() <= 1) {
        if (CounterEventsInflight[group].RefCount() == 0) {
            CounterEventsInflight[group] = new TEvTabletCounters::TInFlightCookie;
        }

        const TMaybe<TString> dbPath = AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()
            ? TMaybe<TString>(Config.GetYdbDatabasePath())
            : Nothing();
        TAutoPtr<TTabletLabeledCountersBase> aggr(new TTabletLabeledCountersBase);
        aggr->SetDatabasePath(dbPath);
        for (auto& p : Partitions) {
            auto it = p.second.LabeledCounters.find(group);
            if (it != p.second.LabeledCounters.end()) {
                aggr->AggregateWith(it->second);
                if (it->second.GetDrop()) {
                    p.second.LabeledCounters.erase(it);
                }
            }
        }

        PQ_ENSURE(aggr->HasCounters());

        TActorId countersAggregator = MakeTabletCountersAggregatorID(ctx.SelfID.NodeId());

        ctx.Send(countersAggregator,
                 new TEvTabletCounters::TEvTabletAddLabeledCounters(
                     CounterEventsInflight[group], TabletID(), TTabletTypes::PersQueue, aggr));
    }
}

void TPersQueue::Handle(TEvPQ::TEvPartitionLabeledCounters::TPtr& ev, const TActorContext& ctx)
{
    const auto& partitionId = ev->Get()->Partition;
    if (partitionId.IsSupportivePartition()) {
        return;
    }
    auto& partition = GetPartitionInfo(partitionId);
    const TString& group = ev->Get()->LabeledCounters.GetGroup();
    partition.LabeledCounters[group] = ev->Get()->LabeledCounters;
    Y_UNUSED(ctx);
//  if uncommented, all changes will be reported immediatly
//    AggregateAndSendLabeledCountersFor(group, ctx);
}

void TPersQueue::Handle(TEvPQ::TEvPartitionLabeledCountersDrop::TPtr& ev, const TActorContext& ctx)
{
    const auto& partitionId = ev->Get()->Partition;
    if (partitionId.IsSupportivePartition()) {
        return;
    }
    auto& partition = GetPartitionInfo(partitionId);
    const TString& group = ev->Get()->Group;
    auto jt = partition.LabeledCounters.find(group);
    if (jt != partition.LabeledCounters.end())
        jt->second.SetDrop();
    Y_UNUSED(ctx);
//  if uncommented, all changes will be reported immediatly
//    AggregateAndSendLabeledCountersFor(group, ctx);

}



void TPersQueue::Handle(TEvPQ::TEvTabletCacheCounters::TPtr& ev, const TActorContext&)
{
    CacheCounters = ev->Get()->Counters;
    SetCacheCounters(CacheCounters);

    PQ_LOG_D("Topic '" << (TopicConverter ? TopicConverter->GetClientsideName() : "Undefined")
        << "' counters. CacheSize " << CacheCounters.CacheSizeBytes << " CachedBlobs " << CacheCounters.CacheSizeBlobs);
}

bool TPersQueue::AllOriginalPartitionsInited() const
{
    return PartitionsInited == OriginalPartitionsCount;
}

void TPersQueue::Handle(TEvPQ::TEvInitComplete::TPtr& ev, const TActorContext& ctx)
{
    const auto& partitionId = ev->Get()->Partition;
    auto& partition = GetPartitionInfo(partitionId);
    PQ_ENSURE(!partition.InitDone);
    partition.InitDone = true;

    if (partitionId.IsSupportivePartition()) {
        for (auto& request : partition.PendingRequests) {
            HandleEventForSupportivePartition(request.Cookie,
                                              std::move(ev->TraceId),
                                              request.Event->Record.GetPartitionRequest(),
                                              request.Sender,
                                              ctx);
        }
        partition.PendingRequests.clear();
    } else {
        ++PartitionsInited;
    }

    PQ_ENSURE(ConfigInited);//partitions are inited only after config

    auto allInitialized = AllOriginalPartitionsInited();
    if (!InitCompleted && allInitialized) {
        OnInitComplete(ctx);
    }

    if (NewConfigShouldBeApplied && allInitialized) {
        ApplyNewConfigAndReply(ctx);
    }

    if (!partitionId.IsSupportivePartition()) {
        ProcessCheckPartitionStatusRequests(partitionId);
    }

    if (allInitialized) {
        ProcessStatusRequests(ctx);
    }

    ProcessMLPQueue();
}

void TPersQueue::Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPQ::TEvError" <<
             " Cookie " << ev->Get()->Cookie <<
             ", Error " << ev->Get()->Error);

    auto it = ResponseProxy.find(ev->Get()->Cookie);
    if (it == ResponseProxy.end())
        return;
    bool res = it->second->HandleError(ev->Get(), ctx);
    if (res) {
        FinishResponse(it);
    }
}

void TPersQueue::Handle(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext& ctx)
{

    auto it = ResponseProxy.find(ev->Get()->Cookie);
    if (it == ResponseProxy.end()) { //response for already closed proxy
        return;
    }
    bool res = it->second->HandleProxyResponse(ev, ctx);
    if (res) {
        FinishResponse(it);
    }
}


void TPersQueue::FinishResponse(THashMap<ui64, TAutoPtr<TResponseBuilder>>::iterator it)
{
    //            ctx.Send(Tablet, new TEvPQ::TEvCompleteResponse(Sender, CounterId, , Response.Release()));
    Counters->Percentile()[it->second->CounterId].IncrementFor((TAppData::TimeProvider->Now() - it->second->Timestamp).MilliSeconds());
    ResponseProxy.erase(it);
    Counters->Simple()[COUNTER_PQ_TABLET_INFLIGHT].Set(ResponseProxy.size());
}


void TPersQueue::Handle(TEvPersQueue::TEvUpdateConfig::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPersQueue::TEvUpdateConfig");
    if (!ConfigInited) {
        UpdateConfigRequests.emplace_back(ev->Release(), ev->Sender);
        return;
    }
    ProcessUpdateConfigRequest(ev->Release(), ev->Sender, ctx);
}


void TPersQueue::Handle(TEvPQ::TEvPartitionConfigChanged::TPtr&, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPQ::TEvPartitionConfigChanged");

    PQ_ENSURE(ChangePartitionConfigInflight > 0);
    --ChangePartitionConfigInflight;

    TrySendUpdateConfigResponses(ctx);
}

void TPersQueue::TrySendUpdateConfigResponses(const TActorContext& ctx)
{
    if (ChangePartitionConfigInflight) {
        return;
    }

    for (auto& p : ChangeConfigNotification) {
        PQ_LOG_I("Config applied version " << Config.GetVersion() << " actor " << p.Actor
                << " txId " << p.TxId << " config:\n" << Config.DebugString());

        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::OK);
        res->Record.SetTxId(p.TxId);
        res->Record.SetOrigin(TabletID());
        ctx.Send(p.Actor, res.Release());
    }

    ChangeConfigNotification.clear();
}

void TPersQueue::CreateTopicConverter(const NKikimrPQ::TPQTabletConfig& config,
                                      NPersQueue::TConverterFactoryPtr& converterFactory,
                                      NPersQueue::TTopicConverterPtr& topicConverter,
                                      const TActorContext& ctx)
{
    auto& pqConfig = AppData(ctx)->PQConfig;
    converterFactory =
        std::make_shared<NPersQueue::TTopicNamesConverterFactory>(pqConfig,
                                                                  "",
                                                                  config.GetLocalDC());
    topicConverter = converterFactory->MakeTopicConverter(config);
    AFL_ENSURE(topicConverter);
    AFL_ENSURE(topicConverter->IsValid())("reason", topicConverter->GetReason());
}

void TPersQueue::UpdateConsumers(NKikimrPQ::TPQTabletConfig& cfg)
{
    PQ_ENSURE(cfg.HasVersion());
    const int curConfigVersion = cfg.GetVersion();

    THashMap<TString, NKikimrPQ::TPQTabletConfig::TConsumer> existedConsumers;
    for (const auto& c : Config.GetConsumers()) {
        existedConsumers[c.GetName()] = c;
    }

    for (auto& c : *cfg.MutableConsumers()) {
        auto it = existedConsumers.find(c.GetName());
        if (it != existedConsumers.end() && it->second.GetVersion() == c.GetVersion()) {
            c.SetGeneration(it->second.GetGeneration());
        } else {
            c.SetGeneration(curConfigVersion);
        }
    }
}

void TPersQueue::ProcessUpdateConfigRequest(TAutoPtr<TEvPersQueue::TEvUpdateConfig> ev, const TActorId& sender, const TActorContext& ctx)
{
    const auto& record = ev->GetRecord();

    int oldConfigVersion = Config.HasVersion() ? static_cast<int>(Config.GetVersion()) : -1;
    int newConfigVersion = NewConfig.HasVersion() ? static_cast<int>(NewConfig.GetVersion()) : oldConfigVersion;

    PQ_ENSURE(newConfigVersion >= oldConfigVersion);

    NKikimrPQ::TPQTabletConfig cfg = record.GetTabletConfig();

    PQ_ENSURE(cfg.HasVersion());
    const int curConfigVersion = cfg.GetVersion();

    if (curConfigVersion == oldConfigVersion) { //already applied
        PQ_LOG_I("Config already applied version " << Config.GetVersion() << " actor " << sender
                << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::OK);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(sender, res.Release());
        return;
    }
    if (curConfigVersion < newConfigVersion) { //Version must increase
        PQ_LOG_ERROR("Config has too small  version " << curConfigVersion << " actual " << newConfigVersion << " actor " << sender
                << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR_BAD_VERSION);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(sender, res.Release());
        return;
    }
    if (curConfigVersion == newConfigVersion) { //nothing to change, will be answered on cfg write from prev step
        PQ_LOG_I("Config update version " << newConfigVersion << " is already in progress actor " << sender
                    << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());
        ChangeConfigNotification.insert(TChangeNotification(sender, record.GetTxId()));
        return;
    }

    if (curConfigVersion > newConfigVersion && NewConfig.HasVersion()) { //already in progress with older version
        PQ_LOG_ERROR("Config version " << curConfigVersion << " is too big, applying right now version " << newConfigVersion
                    << " actor " << sender
                    << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR_UPDATE_IN_PROGRESS);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(sender, res.Release());
        return;
    }

    const auto& bootstrapCfg = record.GetBootstrapConfig();

    if (bootstrapCfg.ExplicitMessageGroupsSize() && !AppData(ctx)->PQConfig.GetEnableProtoSourceIdInfo()) {
        PQ_LOG_ERROR("cannot apply explicit message groups unless proto source id enabled"
                    << ", actor " << sender
                    << ", txId " << record.GetTxId());

        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(sender, res.Release());
        return;
    }

    for (const auto& mg : bootstrapCfg.GetExplicitMessageGroups()) {
        TString error;

        if (!mg.HasId() || mg.GetId().empty()) {
            error = "Empty Id";
        } else if (mg.GetId().size() > MAX_SOURCE_ID_LENGTH) {
            error = "Too long Id";
        } else if (mg.HasKeyRange() && !cfg.PartitionKeySchemaSize()) {
            error = "Missing KeySchema";
        }

        if (error) {
            PQ_LOG_ERROR("Cannot apply explicit message group: " << error
                        << " actor " << sender
                        << " txId " << record.GetTxId());

            THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
            res->Record.SetStatus(NKikimrPQ::ERROR);
            res->Record.SetTxId(record.GetTxId());
            res->Record.SetOrigin(TabletID());
            ctx.Send(sender, res.Release());
            return;
        }
    }

    ChangeConfigNotification.insert(TChangeNotification(sender, record.GetTxId()));

    if (!cfg.HasPartitionConfig())
        cfg.MutablePartitionConfig()->CopyFrom(Config.GetPartitionConfig());
    if (!cfg.HasCacheSize() && Config.HasCacheSize()) //if not set and it is alter - preserve old cache size
        cfg.SetCacheSize(Config.GetCacheSize());

    Migrate(cfg);

    UpdateConsumers(cfg);

    PQ_LOG_TX_D("Config update version " << cfg.GetVersion() << "(current " << Config.GetVersion() << ") received from actor " << sender
                << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

    TString str;
    PQ_ENSURE(CheckPersQueueConfig(cfg, true, &str))("error", str);

    BeginWriteConfig(cfg, bootstrapCfg, ctx);

    NewConfig = std::move(cfg);
}

void TPersQueue::BeginWriteConfig(const NKikimrPQ::TPQTabletConfig& cfg,
                                  const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                                  const TActorContext& ctx)
{
    TAutoPtr<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    request->Record.SetCookie(WRITE_CONFIG_COOKIE);

    AddCmdWriteConfig(request.Get(),
                      cfg,
                      bootstrapCfg,
                      NKikimrPQ::TPartitions(),
                      ctx);
    PQ_ENSURE((ui64)request->Record.GetCmdWrite().size() == (ui64)bootstrapCfg.GetExplicitMessageGroups().size() * cfg.PartitionsSize() + 1);

    ctx.Send(ctx.SelfID, request.Release());
}

void TPersQueue::AddCmdWriteConfig(TEvKeyValue::TEvRequest* request,
                                   const NKikimrPQ::TPQTabletConfig& cfg,
                                   const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                                   const NKikimrPQ::TPartitions& partitionsData,
                                   const TActorContext& ctx)
{
    PQ_ENSURE(request);

    TString str;
    PQ_ENSURE(cfg.SerializeToString(&str));

    auto write = request->Record.AddCmdWrite();
    write->SetKey(KeyConfig());
    write->SetValue(str);
    write->SetTactic(AppData(ctx)->PQConfig.GetTactic());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

    auto graph = MakePartitionGraph(cfg);
    for (const auto& partition : cfg.GetPartitions()) {
        auto explicitMessageGroups = CreateExplicitMessageGroups(bootstrapCfg, partitionsData, graph, partition.GetPartitionId());

        TSourceIdWriter sourceIdWriter(ESourceIdFormat::Proto);
        for (const auto& [id, mg] : *explicitMessageGroups) {
            sourceIdWriter.RegisterSourceId(id, mg.SeqNo, 0, ctx.Now(), std::move(mg.KeyRange), false);
        }

        sourceIdWriter.FillRequest(request, TPartitionId(partition.GetPartitionId()));
    }
}

void TPersQueue::ClearNewConfig()
{
    NewConfigShouldBeApplied = false;
    NewConfig.Clear();
}

void TPersQueue::Handle(TEvPersQueue::TEvDropTablet::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPersQueue::TEvDropTablet");

    const auto& record = ev->Get()->Record;
    ui64 txId = record.GetTxId();

    TChangeNotification stateRequest(ev->Sender, txId);

    NKikimrPQ::ETabletState reqState = record.GetRequestedState();

    if (reqState == NKikimrPQ::ENormal) {
        ReturnTabletState(ctx, stateRequest, NKikimrProto::ERROR);
        return;
    }

    PQ_ENSURE(reqState == NKikimrPQ::EDropped);

    TabletStateRequests.insert(stateRequest);
    if (TabletStateRequests.size() > 1) {
        return; // already writing, just enqueue
    }

    BeginWriteTabletState(ctx, reqState);
}

void TPersQueue::Handle(TEvPersQueue::TEvOffsets::TPtr& ev, const TActorContext& ctx)
{
    if (!ConfigInited) {
        THolder<TEvPersQueue::TEvOffsetsResponse> res = MakeHolder<TEvPersQueue::TEvOffsetsResponse>();
        auto& resp = res->Record;
        resp.SetTabletId(TabletID());

        ctx.Send(ev->Sender, res.Release());
        return;
    }
    ui32 cnt = 0;
    for (auto& p : Partitions) {
        if (p.first.IsSupportivePartition()) {
            continue;
        }

        cnt += p.second.InitDone;
    }
    TActorId ans = CreateOffsetsProxyActor(TabletID(), SelfId(), ev->Sender, cnt, ctx);

    for (auto& p : Partitions) {
        if (!p.second.InitDone || p.first.IsSupportivePartition()) {
            continue;
        }

        THolder<TEvPQ::TEvPartitionOffsets> event = MakeHolder<TEvPQ::TEvPartitionOffsets>(ans, ev->Get()->Record.HasClientId() ?
                                                                                        ev->Get()->Record.GetClientId() : "");
        ctx.Send(p.second.Actor, event.Release());
    }
}

void TPersQueue::Handle(TEvPersQueue::TEvHasDataInfo::TPtr& ev, const TActorContext& ctx)
{
    auto& record = ev->Get()->Record;
    ActorIdToProto(ev->Sender, record.MutableSender());
    if (!ConfigInited) {
        HasDataRequests.push_back(ev->Release());
    } else {
        auto it = Partitions.find(TPartitionId(record.GetPartition()));
        if (it != Partitions.end()) {
            ctx.Send(it->second.Actor, ev->Release().Release());
        }
    }
}


void TPersQueue::Handle(TEvPersQueue::TEvPartitionClientInfo::TPtr& ev, const TActorContext& ctx) {
    for (auto partition : ev->Get()->Record.GetPartitions()) {
        auto it = Partitions.find(TPartitionId(partition));
        if (it != Partitions.end()) {
            ctx.Send(it->second.Actor, new TEvPQ::TEvGetPartitionClientInfo(ev->Sender), 0, ev->Cookie);
        } else {
            THolder<TEvPersQueue::TEvPartitionClientInfoResponse> clientInfo = MakeHolder<TEvPersQueue::TEvPartitionClientInfoResponse>();
            clientInfo->Record.SetPartition(partition);
            ctx.Send(ev->Sender, clientInfo.Release(), 0, ev->Cookie);
        }
    }
}

void TPersQueue::ProcessStatusRequests(const TActorContext &ctx) {
    for (auto& ev : StatusRequests) {
        Handle(ev, ctx);
    }
    StatusRequests.clear();
}

void TPersQueue::Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_T("Handle TEvPersQueue::TEvStatus");

    if (!ConfigInited || !AllOriginalPartitionsInited()) {
        PQ_LOG_D("Postpone the request." <<
                 " ConfigInited " << static_cast<int>(ConfigInited) <<
                 ", PartitionsInited " << PartitionsInited <<
                 ", OriginalPartitionsCount " << OriginalPartitionsCount);
        StatusRequests.push_back(ev);
        return;
    }

    ui32 cnt = 0;
    for (auto& [partitionId, partitionInfo] : Partitions) {
        if (partitionId.IsSupportivePartition()) {
            continue;
        }

        cnt += partitionInfo.InitDone;
    }

    TActorId ans = CreateStatusProxyActor(TabletID(), SelfId(), ev->Sender, cnt, ev->Cookie, ctx);
    for (auto& p : Partitions) {
        if (!p.second.InitDone || p.first.IsSupportivePartition()) {
            continue;
        }

        THolder<TEvPQ::TEvPartitionStatus> event;
        if (ev->Get()->Record.GetConsumers().empty()) {
            event = MakeHolder<TEvPQ::TEvPartitionStatus>(ans, ev->Get()->Record.HasClientId() ? ev->Get()->Record.GetClientId() : "",
                ev->Get()->Record.HasGetStatForAllConsumers() ? ev->Get()->Record.GetGetStatForAllConsumers() : false);
        } else {
            TVector<TString> consumers;
            for (auto consumer : ev->Get()->Record.GetConsumers()) {
                consumers.emplace_back(consumer);
            }
            event = MakeHolder<TEvPQ::TEvPartitionStatus>(ans, consumers);
        }
        ctx.Send(p.second.Actor, event.Release());
    }
}


void TPersQueue::InitResponseBuilder(const ui64 responseCookie, const ui32 count, const ui32 counterId)
{
    auto it = ResponseProxy.find(responseCookie);
    PQ_ENSURE(it != ResponseProxy.end());
    it->second->AddPartialReplyCount(count);
    it->second->SetCounterId(counterId);
}

void TPersQueue::HandleGetMaxSeqNoRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    PQ_ENSURE(req.HasCmdGetMaxSeqNo());
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_MAX_SEQ_NO);
    const auto& cmd = req.GetCmdGetMaxSeqNo();
    TVector<TString> ids;
    ids.reserve(cmd.SourceIdSize());
    for (ui32 i = 0; i < cmd.SourceIdSize(); ++i)
        ids.push_back(cmd.GetSourceId(i));
    THolder<TEvPQ::TEvGetMaxSeqNoRequest> event = MakeHolder<TEvPQ::TEvGetMaxSeqNoRequest>(responseCookie, ids);
    ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
}

void TPersQueue::HandleDeleteSessionRequest(
        const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
        const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
        const TActorId& pipeClient, const TActorId&
)
{
    PQ_ENSURE(req.HasCmdDeleteSession());
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_DELETE_SESSION);
    const auto& cmd = req.GetCmdDeleteSession();
    //To do : priority
    PQ_LOG_I("Got cmd delete session: " << cmd.DebugString());

    if (!cmd.HasClientId()){
        return ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no clientId in DeleteSession request: " << ToString(req).data());
    } else if (!cmd.HasSessionId()) {
        return ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "not sessionId in DeleteSession request: " << ToString(req).data());
    } else {
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(
                responseCookie, cmd.GetClientId(), 0, cmd.GetSessionId(), 0, 0, 0, pipeClient,
                TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION
        );
        ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
    }
    auto pipe = PipesInfo.find(pipeClient);
    if (!pipe.IsEnd()) {
        DestroySession(pipe->second);
    }
}

void TPersQueue::HandleCreateSessionRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                            const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
                                            const TActorId& pipeClient, const TActorId&
) {
    PQ_ENSURE(req.HasCmdCreateSession());
    const auto& cmd = req.GetCmdCreateSession();

    if (!cmd.HasClientId()){
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no clientId in CreateSession request: " << ToString(req).data());
    } else if (!cmd.HasSessionId()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "not sessionId in CreateSession request: " << ToString(req).data());
    } else if (!cmd.HasGeneration()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "not geneartion in CreateSession request: " << ToString(req).data());
    } else if (!cmd.HasStep()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "not step in CreateSession request: " << ToString(req).data());
    } else {
        bool isDirectRead = cmd.GetPartitionSessionId() > 0;
        InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_CREATE_SESSION);
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(
                responseCookie, cmd.GetClientId(), 0, cmd.GetSessionId(), cmd.GetPartitionSessionId(), cmd.GetGeneration(), cmd.GetStep(),
                pipeClient, TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION, 0, false
        );

        if (isDirectRead) {
            auto pipeIter = PipesInfo.find(pipeClient);
            if (pipeIter.IsEnd()) {
                ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::ERROR,
                        TStringBuilder() << "Internal error - server pipe " << pipeClient.ToString() << " not found");
                return;
            }

            pipeIter->second.ClientId = cmd.GetClientId();
            pipeIter->second.SessionId = cmd.GetSessionId();
            pipeIter->second.PartitionSessionId = cmd.GetPartitionSessionId();
            PQ_LOG_D("Created session " << cmd.GetSessionId() << " on pipe: " << pipeIter->first.ToString());
            ctx.Send(MakePQDReadCacheServiceActorId(),
                new TEvPQ::TEvRegisterDirectReadSession(
                    TReadSessionKey{cmd.GetSessionId(), cmd.GetPartitionSessionId()},
                    GetGeneration()
                )
            );

        }
        if (cmd.GetRestoreSession()) {
            PQ_ENSURE(isDirectRead);
            auto fakeResponse = MakeHolder<TEvPQ::TEvProxyResponse>(responseCookie, false);
            auto& record = *fakeResponse->Response;
            record.SetStatus(NMsgBusProxy::MSTATUS_OK);
            auto& partResponse = *record.MutablePartitionResponse();
            partResponse.MutableCmdRestoreDirectReadResult();

	    Send(SelfId(), fakeResponse.Release());
        } else {
            ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
        }
    }
}

void TPersQueue::HandleSetClientOffsetRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    PQ_ENSURE(req.HasCmdSetClientOffset());
    const auto& cmd = req.GetCmdSetClientOffset();

    if (!cmd.HasClientId()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no clientId in SetClientOffset request: " << ToString(req).data());
    } else if (!cmd.HasOffset()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no offset in SetClientOffset request: " << ToString(req).data());
    } else if (cmd.GetOffset() < 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "negative offset in SetClientOffset request: " << ToString(req).data());
    } else {
        InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_SET_OFFSET);
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(
            responseCookie, cmd.GetClientId(), cmd.GetOffset(), cmd.HasSessionId() ? cmd.GetSessionId() : "", 0, 0, 0,
            TActorId{}, TEvPQ::TEvSetClientInfo::ESCI_OFFSET, 0, cmd.GetStrict(),
            cmd.HasCommittedMetadata() ? static_cast<std::optional<TString>>(cmd.GetCommittedMetadata()) : std::nullopt
        );
        ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
    }
}

void TPersQueue::HandleGetClientOffsetRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    PQ_ENSURE(req.HasCmdGetClientOffset());
    const auto& cmd = req.GetCmdGetClientOffset();
    if (!cmd.HasClientId() || cmd.GetClientId().empty()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no clientId in GetClientOffset request: " << ToString(req).data());
    } else {
        InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_OFFSET);
        THolder<TEvPQ::TEvGetClientOffset> event = MakeHolder<TEvPQ::TEvGetClientOffset>(responseCookie, cmd.GetClientId());
        ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
    }
}

void TPersQueue::HandleUpdateWriteTimestampRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    PQ_ENSURE(req.HasCmdUpdateWriteTimestamp());
    const auto& cmd = req.GetCmdUpdateWriteTimestamp();
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_OFFSET);
    THolder<TEvPQ::TEvUpdateWriteTimestamp> event = MakeHolder<TEvPQ::TEvUpdateWriteTimestamp>(responseCookie, cmd.GetWriteTimeMS());
    ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
}

void TPersQueue::HandleWriteRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    auto& pqConfig = AppData(ctx)->PQConfig;

    PQ_ENSURE(req.CmdWriteSize());
    MeteringSink.MayFlush(ctx.Now()); // To ensure hours' border;
    if (Config.GetMeteringMode() != NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS) {
        MeteringSink.IncreaseQuantity(EMeteringJson::PutEventsV1, req.HasPutUnitsSize() ? req.GetPutUnitsSize() : 0);
    }

    TVector<TEvPQ::TEvWrite::TMsg> msgs;

    bool mirroredPartition = MirroringEnabled(Config);

    if (!req.GetIsDirectWrite()) {
        if (!req.HasMessageNo()) {
            ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
                       "MessageNo must be set for writes");
            return;
        }

        if (!mirroredPartition && !req.HasOwnerCookie()) {
            ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
                       "OwnerCookie must be set for writes");
            return;
        }
    }

    if (req.HasCmdWriteOffset() && req.GetCmdWriteOffset() < 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
                   "CmdWriteOffset can't be negative");
        return;
    }

    if (req.HasWriteId()) {
        PQ_LOG_TX_D("Write in transaction." <<
                    " Partition: " << req.GetPartition() <<
                    ", WriteId: " << req.GetWriteId() <<
                    ", NeedSupportivePartition: " << req.GetNeedSupportivePartition());
    }

    for (ui32 i = 0; i < req.CmdWriteSize(); ++i) {
        const auto& cmd = req.GetCmdWrite(i);

        if (AppData(ctx)->Counters && !AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            auto counters = AppData(ctx)->Counters;
            TString clientDC = to_lower(cmd.HasClientDC() ? cmd.GetClientDC() : "unknown");
            clientDC.to_title();
            auto it = BytesWrittenFromDC.find(clientDC);
            if (it == BytesWrittenFromDC.end()) {

                auto labels = NPersQueue::GetLabelsForCustomCluster(TopicConverter, clientDC);
                if (!labels.empty()) {
                    labels.pop_back();
                }
                it = BytesWrittenFromDC.emplace(clientDC, NKikimr::NPQ::TMultiCounter(GetServiceCounters(counters, "pqproxy|writeSession"),
                            labels, {{"ClientDC", clientDC}}, {"BytesWrittenFromDC"}, true)).first;
            }
            if (it != BytesWrittenFromDC.end())
                it->second.Inc(cmd.ByteSize());
        }

        ui64 createTimestampMs = 0, writeTimestampMs = 0;
        NKikimrPQClient::TDataChunk proto;
        std::optional<TString> deduplicationId;

        TString errorStr = "";
        if (!cmd.HasSeqNo() && !req.GetIsDirectWrite()) {
            errorStr = "no SeqNo";
        } else if (cmd.HasData() && cmd.HasHeartbeat()) {
            errorStr = "Data and Heartbeat are mutually exclusive";
        } else if (cmd.GetData().empty() && cmd.GetHeartbeat().GetData().empty()) {
            errorStr = "empty Data";
        } else if ((!cmd.HasSourceId() || cmd.GetSourceId().empty()) && !req.GetIsDirectWrite() && !cmd.GetDisableDeduplication()) {
            errorStr = "empty SourceId";
        } else if (cmd.GetPartitionKey().size() > 256) {
            errorStr = "too long partition key";
        } else if (cmd.GetSeqNo() < 0) {
            errorStr = "SeqNo must be >= 0";
        } else if (cmd.HasPartNo() && (cmd.GetPartNo() < 0 || cmd.GetPartNo() >= Max<ui16>())) {
            errorStr = "PartNo must be >= 0 and < 65535";
        } else if (cmd.HasPartNo() != cmd.HasTotalParts()) {
            errorStr = "PartNo and TotalParts must be filled together";
        } else if (cmd.HasTotalParts() && (cmd.GetTotalParts() <= cmd.GetPartNo() || cmd.GetTotalParts() <= 1 || cmd.GetTotalParts() > Max<ui16>())) {
            errorStr = "TotalParts must be > PartNo and > 1 and < 65536";
        } else if (cmd.HasPartNo() && cmd.GetPartNo() == 0 && !cmd.HasTotalSize()) {
            errorStr = "TotalSize must be filled for first part";
        } else if (cmd.HasTotalSize() && static_cast<size_t>(cmd.GetTotalSize()) <= cmd.GetData().size()) { // TotalSize must be > size of each part
            errorStr = "TotalSize is incorrect";
        } else if (cmd.HasSourceId() && !cmd.GetSourceId().empty() && NPQ::NSourceIdEncoding::Decode(cmd.GetSourceId()).length() > MAX_SOURCE_ID_LENGTH) {
            errorStr = "Too big SourceId";
        } else if (mirroredPartition && !cmd.GetDisableDeduplication()) {
            errorStr = "Write to mirrored topic is forbiden";
        } else if (cmd.HasHeartbeat() && cmd.GetHeartbeat().GetData().size() > MAX_HEARTBEAT_SIZE) {
            errorStr = "Too big Heartbeat";
        } else if (cmd.HasHeartbeat() && cmd.HasTotalParts() && cmd.GetTotalParts() != 1) {
            errorStr = "Heartbeat must be a single-part message";
        } else if (cmd.GetData().size() > pqConfig.GetMaxMessageSizeBytes()) {
            errorStr = TStringBuilder() << "Too big message. Max message size is " << pqConfig.GetMaxMessageSizeBytes()
                << " bytes, but got " << cmd.GetData().size() << " bytes";
        } else {
            if (cmd.HasCreateTimeMS() && cmd.GetCreateTimeMS() >= 0) {
                createTimestampMs = cmd.GetCreateTimeMS();
            }
            if (cmd.HasWriteTimeMS() && cmd.GetWriteTimeMS() > 0) {
                writeTimestampMs = cmd.GetWriteTimeMS();
                if (!cmd.GetDisableDeduplication()) {
                    errorStr = "WriteTimestamp avail only without deduplication";
                }
            }
        }

        if (!errorStr.empty()) {
            ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, errorStr);
            return;
        }

        if (proto.ParseFromString(cmd.GetData())) {
            for (auto& attr : *proto.MutableMessageMeta()) {
                if (attr.key() == MESSAGE_ATTRIBUTE_DEDUPLICATION_ID) {
                    deduplicationId = attr.value();
                    break;
                }
            }
        }

        ui32 mSize = MAX_BLOB_PART_SIZE - cmd.GetSourceId().size() - sizeof(ui32) - TClientBlob::OVERHEAD; //megaqc - remove this
        PQ_ENSURE(mSize > 204800)("mSize", mSize);
        ui64 receiveTimestampMs = TAppData::TimeProvider->Now().MilliSeconds();
        bool disableDeduplication = cmd.GetDisableDeduplication();

        std::optional<TRowVersion> heartbeatVersion;
        if (cmd.HasHeartbeat()) {
            heartbeatVersion.emplace(cmd.GetHeartbeat().GetStep(), cmd.GetHeartbeat().GetTxId());
        }

        if (cmd.GetData().size() > mSize) {
            if (cmd.HasPartNo()) {
                ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
                            TStringBuilder() << "Too big message while using PartNo; must be at most " << mSize << ", but got " << cmd.GetData().size());
                return;
            }
            auto it = ResponseProxy.find(responseCookie);
            PQ_ENSURE(it != ResponseProxy.end());
            it->second->SetWasSplit();
            ui32 pos = 0;
            ui16 partNo = 0;
            ui32 totalSize = cmd.GetData().size();
            ui16 totalParts = (totalSize - 1) / mSize + 1;
            ui32 diff = 0;
            ui32 lastPartSize = (totalSize - 1) % mSize + 1; // mSize for x*mSize , x for x (x < mSize)
            ui32 uncompressedSize = cmd.HasUncompressedSize() ? cmd.GetUncompressedSize() : 0;
            if (lastPartSize < 100) { //size of first part will be reduced by diff, => size of last part will be increased by diff => = 100 bytes
                diff = 100 - lastPartSize;
            }
            PQ_ENSURE(!cmd.HasTotalParts())("description", "too big part"); //change this verify for errorStr, when LB will be ready
            while (pos < totalSize) {
                TString data = cmd.GetData().substr(pos, mSize - diff);
                pos += mSize - diff;
                diff = 0;

                msgs.push_back({
                    .SourceId = cmd.GetSourceId(),
                    .SeqNo = static_cast<ui64>(cmd.GetSeqNo()),
                    .PartNo = partNo,
                    .TotalParts = totalParts,
                    .TotalSize = totalSize,
                    .CreateTimestamp = createTimestampMs,
                    .ReceiveTimestamp = receiveTimestampMs,
                    .DisableDeduplication = disableDeduplication,
                    .WriteTimestamp = writeTimestampMs,
                    .Data = data,
                    .UncompressedSize = uncompressedSize,
                    .PartitionKey = cmd.GetPartitionKey(),
                    .ExplicitHashKey = cmd.GetExplicitHash(),
                    .External = cmd.GetExternalOperation(),
                    .IgnoreQuotaDeadline = cmd.GetIgnoreQuotaDeadline(),
                    .HeartbeatVersion = heartbeatVersion,
                    .EnableKafkaDeduplication = cmd.GetEnableKafkaDeduplication(),
                    .ProducerEpoch = (cmd.HasProducerEpoch() ? TMaybe<i32>(cmd.GetProducerEpoch()) : Nothing()),
                    .MessageDeduplicationId = deduplicationId
                });

                partNo++;
                uncompressedSize = 0;
                PQ_LOG_D("got client PART message topic: " << (TopicConverter ? TopicConverter->GetClientsideName() : "Undefined") << " partition: " << req.GetPartition()
                        << " SourceId: \'" << EscapeC(msgs.back().SourceId) << "\' SeqNo: "
                        << msgs.back().SeqNo << " partNo : " << msgs.back().PartNo
                        << " messageNo: " << req.GetMessageNo() << " size: " << data.size()
                );
            }
            PQ_ENSURE(partNo == totalParts);
        } else if (cmd.GetHeartbeat().GetData().size() > mSize) {
            Y_DEBUG_ABORT("This should never happen");
            ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, TStringBuilder()
                << "Too big heartbeat message, must be at most " << mSize << ", but got " << cmd.GetHeartbeat().GetData().size());
            return;
        } else {
            ui32 totalSize = cmd.GetData().size();
            if (cmd.HasHeartbeat()) {
                totalSize = cmd.GetHeartbeat().GetData().size();
            }
            if (cmd.HasTotalSize()) {
                totalSize = cmd.GetTotalSize();
            }

            const auto& data = cmd.HasHeartbeat()
                ? cmd.GetHeartbeat().GetData()
                : cmd.GetData();

            msgs.push_back({
                .SourceId = cmd.GetSourceId(),
                .SeqNo =  static_cast<ui64>(cmd.GetSeqNo()),
                .PartNo = static_cast<ui16>(cmd.HasPartNo() ? cmd.GetPartNo() : 0),
                .TotalParts = static_cast<ui16>(cmd.HasPartNo() ? cmd.GetTotalParts() : 1),
                .TotalSize = totalSize,
                .CreateTimestamp = createTimestampMs,
                .ReceiveTimestamp = receiveTimestampMs,
                .DisableDeduplication = disableDeduplication,
                .WriteTimestamp = writeTimestampMs,
                .Data = data,
                .UncompressedSize = cmd.HasUncompressedSize() ? cmd.GetUncompressedSize() : 0u,
                .PartitionKey = cmd.GetPartitionKey(),
                .ExplicitHashKey = cmd.GetExplicitHash(),
                .External = cmd.GetExternalOperation(),
                .IgnoreQuotaDeadline = cmd.GetIgnoreQuotaDeadline(),
                .HeartbeatVersion = heartbeatVersion,
                .EnableKafkaDeduplication = cmd.GetEnableKafkaDeduplication(),
                .ProducerEpoch = (cmd.HasProducerEpoch() ? TMaybe<i32>(cmd.GetProducerEpoch()) : Nothing()),
                .MessageDeduplicationId = std::move(deduplicationId)
            });
        }
        PQ_LOG_D("got client message topic: " << (TopicConverter ? TopicConverter->GetClientsideName() : "Undefined") <<
                 " partition: " << req.GetPartition() <<
                 " SourceId: \'" << EscapeC(msgs.back().SourceId) <<
                 "\' SeqNo: " << msgs.back().SeqNo << " partNo : " << msgs.back().PartNo <<
                 " messageNo: " << req.GetMessageNo() << " size " << msgs.back().Data.size() <<
                 " offset: " << (req.HasCmdWriteOffset() ? (req.GetCmdWriteOffset() + i) : -1));
    }
    InitResponseBuilder(responseCookie, msgs.size(), COUNTER_LATENCY_PQ_WRITE);
    std::optional<ui64> initialSeqNo;
    if (req.HasInitialSeqNo()) {
        initialSeqNo = req.GetInitialSeqNo();
    }
    THolder<TEvPQ::TEvWrite> event =
        MakeHolder<TEvPQ::TEvWrite>(responseCookie, req.GetMessageNo(),
                                    req.HasOwnerCookie() ? req.GetOwnerCookie() : "",
                                    req.HasCmdWriteOffset() ? req.GetCmdWriteOffset() : TMaybe<ui64>(),
                                    std::move(msgs), req.GetIsDirectWrite(), initialSeqNo);
    ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
}


void TPersQueue::HandleReserveBytesRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
                                          const TActorId& pipeClient, const TActorId&)
{
    PQ_ENSURE(req.HasCmdReserveBytes());

    auto it = PipesInfo.find(pipeClient);
    if (it == PipesInfo.end()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::ERROR,
            TStringBuilder() << "pipe already dead: " << pipeClient);
        return;
    }

    if (!req.HasMessageNo()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "MessageNo must be set for ReserveBytes request");
        return;
    }
    if (!req.HasOwnerCookie()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "OwnerCookie must be set for ReserveBytes request");
        return;
    }

    if (req.HasWriteId()) {
        PQ_LOG_D("Reserve bytes in transaction." <<
                 " Partition: " << req.GetPartition() <<
                 ", WriteId: " << req.GetWriteId() <<
                 ", NeedSupportivePartition: " << req.GetNeedSupportivePartition());
    }

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_RESERVE_BYTES);
    THolder<TEvPQ::TEvReserveBytes> event = MakeHolder<TEvPQ::TEvReserveBytes>(responseCookie, req.GetCmdReserveBytes().GetSize(),
                                                                        req.GetOwnerCookie(), req.GetMessageNo(), req.GetCmdReserveBytes().GetLastRequest());
    ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
}


void TPersQueue::HandleGetOwnershipRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
                                          const TActorId& pipeClient, const TActorId& sender)
{
    PQ_ENSURE(req.HasCmdGetOwnership());

    const TString& owner = req.GetCmdGetOwnership().GetOwner();
    if (owner.empty()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "empty owner in CmdGetOwnership request");
        return;
    }
    PQ_ENSURE(pipeClient != TActorId());
    auto it = PipesInfo.find(pipeClient);
    if (it == PipesInfo.end()) { //do nothing. this could not be happen, just in tests
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "request via dead pipe");
        return;
    }

    it->second = TPipeInfo::ForOwner(partActor, owner, it->second.ServerActors);

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_OWNERSHIP);
    THolder<TEvPQ::TEvChangeOwner> event = MakeHolder<TEvPQ::TEvChangeOwner>(responseCookie, owner, pipeClient, sender,
            req.GetCmdGetOwnership().GetForce(), req.GetCmdGetOwnership().GetRegisterIfNotExists());
    ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
}


void TPersQueue::HandleReadRequest(
        const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
        const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
        const TActorId& pipeClient, const TActorId&
) {
    PQ_ENSURE(req.HasCmdRead());

    auto cmd = req.GetCmdRead();
    if (!cmd.HasOffset()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no offset in read request: " << ToString(req).data());
    } else if (!cmd.HasClientId()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no clientId in read request: " << ToString(req).data());
    } else if (cmd.HasCount() && cmd.GetCount() <= 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "invalid count in read request: " << ToString(req).data());
    } else if (!cmd.HasOffset() || cmd.GetOffset() < 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "invalid offset in read request: " << ToString(req).data());
    } else if (cmd.HasBytes() && cmd.GetBytes() <= 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "invalid bytes in read request: " << ToString(req).data());
    } else if (cmd.HasTimeoutMs() && cmd.GetTimeoutMs() < 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "invalid timeout in read request: " << ToString(req).data());
    } else if (cmd.HasTimeoutMs() && cmd.GetTimeoutMs() > 120000) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "invalid timeout in read request, must be less than 120 secs: " << ToString(req).data());
    } else if (cmd.HasPartNo() && cmd.GetPartNo() < 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "invalid partNo in read request: " << ToString(req).data());
    } else if (cmd.HasMaxTimeLagMs() && cmd.GetMaxTimeLagMs() < 0) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "invalid maxTimeLagMs in read request: " << ToString(req).data());

    } else if (cmd.GetClientId() == NPQ::CLIENTID_COMPACTION_CONSUMER) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "cannot read with internal clinetId: " << cmd.GetClientId());
    } else {
        InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_READ);
        ui32 count = cmd.HasCount() ? cmd.GetCount() : Max<ui32>();
        ui32 bytes = Min<ui32>(MAX_BYTES, cmd.HasBytes() ? cmd.GetBytes() : MAX_BYTES);
        auto clientDC = cmd.HasClientDC() ? to_lower(cmd.GetClientDC()) : "unknown";
        clientDC.to_title();
        if (IsDirectReadCmd(cmd)) {
            auto pipeIter = PipesInfo.find(pipeClient);
            if (pipeIter.IsEnd()) {
                ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::READ_ERROR_NO_SESSION,
                           TStringBuilder() << "Read prepare request from unknown(old?) pipe: " << pipeClient.ToString());
                return;
            } else if (cmd.GetSessionId().empty()) {
                ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::READ_ERROR_NO_SESSION,
                           TStringBuilder() << "Read prepare request with empty session id");
                return;
            } else if (pipeIter->second.SessionId != cmd.GetSessionId()) {
                ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::READ_ERROR_NO_SESSION,
                           TStringBuilder() << "Read prepare request with unknown(old?) session id " << cmd.GetSessionId());
                return;
            }
        }

        THolder<TEvPQ::TEvRead> event =
            MakeHolder<TEvPQ::TEvRead>(responseCookie, cmd.GetOffset(), cmd.GetLastOffset(),
                                       cmd.HasPartNo() ? cmd.GetPartNo() : 0,
                                       count,
                                       cmd.HasSessionId() ? cmd.GetSessionId() : "",
                                       cmd.GetClientId(),
                                       cmd.HasTimeoutMs() ? cmd.GetTimeoutMs() : 0,
                                       bytes,
                                       cmd.HasMaxTimeLagMs() ? cmd.GetMaxTimeLagMs() : 0,
                                       cmd.HasReadTimestampMs() ? cmd.GetReadTimestampMs() : 0,
                                       clientDC,
                                       cmd.GetExternalOperation(),
                                       pipeClient);

        ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
    }
}

template<class TRequest>
bool ValidateDirectReadRequestBase(
        const TRequest& cmd, const THashMap<TActorId, TPersQueue::TPipeInfo>::iterator& pipeIter,
        TStringBuilder& error, TDirectReadKey& key
) {
    key = TDirectReadKey{cmd.GetSessionKey().GetSessionId(), cmd.GetSessionKey().GetPartitionSessionId(), cmd.GetDirectReadId()};
    if (key.SessionId.empty()) {
        error << "no session id in publish read request: ";
        return false;
    } else if (key.PartitionSessionId == 0) {
        error << "No or zero partition session id in publish read request: ";
        return false;
    } else if (key.ReadId == 0) {
        error << "No or zero ReadId in publish read request: ";
        return false;
    }
    if (pipeIter.IsEnd()) {
        error << "Read prepare request from unknown(old?) pipe";
        return false;
    } else if (pipeIter->second.SessionId != key.SessionId) {
        error << "Read prepare request with unknown(old?) session id " << key.SessionId;
        return false;
    }
    return true;
}

void TPersQueue::HandlePublishReadRequest(
        const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId&,
        const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
        const TActorId& pipeClient, const TActorId&
) {
    auto cmd = req.GetCmdPublishRead();
    TDirectReadKey key;
    TStringBuilder error;

    if (!ValidateDirectReadRequestBase(cmd, PipesInfo.find(pipeClient), error, key)) {
        error << req.DebugString();
        return ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, error);
    }
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_PUBLISH_READ);
    THolder<TEvPQ::TEvProxyResponse> publishDoneEvent = MakeHolder<TEvPQ::TEvProxyResponse>(responseCookie, false);
    publishDoneEvent->Response->SetStatus(NMsgBusProxy::MSTATUS_OK);
    publishDoneEvent->Response->SetErrorCode(NPersQueue::NErrorCode::OK);

    auto* publishRes = publishDoneEvent->Response->MutablePartitionResponse()->MutableCmdPublishReadResult();
    publishRes->SetDirectReadId(key.ReadId);
    ctx.Send(SelfId(), publishDoneEvent.Release());

    PQ_LOG_D("Publish direct read id " << key.ReadId << " for session " << key.SessionId);
    ctx.Send(
            MakePQDReadCacheServiceActorId(),
            new TEvPQ::TEvPublishDirectRead(key, GetGeneration()),
            0, 0, std::move(traceId)
    );

}

void TPersQueue::HandleForgetReadRequest(
        const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& ,
        const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
        const TActorId& pipeClient, const TActorId&
) {
    auto cmd = req.GetCmdForgetRead();
    TDirectReadKey key;
    TStringBuilder error;
    if (!ValidateDirectReadRequestBase(cmd, PipesInfo.find(pipeClient), error, key)) {
        error << req.DebugString();
        return ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, error);
    }
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_FORGET_READ);
    THolder<TEvPQ::TEvProxyResponse> forgetDoneEvent = MakeHolder<TEvPQ::TEvProxyResponse>(responseCookie, false);
    forgetDoneEvent->Response->SetStatus(NMsgBusProxy::MSTATUS_OK);
    forgetDoneEvent->Response->SetErrorCode(NPersQueue::NErrorCode::OK);
    forgetDoneEvent->Response->MutablePartitionResponse()->MutableCmdForgetReadResult()->SetDirectReadId(key.ReadId);
    ctx.Send(SelfId(), forgetDoneEvent.Release());

    PQ_LOG_D("Forget direct read id " << key.ReadId << " for session " << key.SessionId);
    ctx.Send(
            MakePQDReadCacheServiceActorId(),
            new TEvPQ::TEvForgetDirectRead(key, GetGeneration()),
            0, 0, std::move(traceId)
    );

}

void TPersQueue::DestroySession(TPipeInfo& pipeInfo) {
    PQ_LOG_D("Destroy direct read session " << pipeInfo.SessionId);
    if (pipeInfo.SessionId.empty())
        return;
    ActorContext().Send(
            MakePQDReadCacheServiceActorId(),
            new TEvPQ::TEvDeregisterDirectReadSession(
                TReadSessionKey{pipeInfo.SessionId, pipeInfo.PartitionSessionId},
                GetGeneration()
            )
    );
    pipeInfo.SessionId = TString{};
}

TMaybe<TEvPQ::TEvRegisterMessageGroup::TBody> TPersQueue::MakeRegisterMessageGroup(
        const NKikimrClient::TPersQueuePartitionRequest::TCmdRegisterMessageGroup& cmd,
        NPersQueue::NErrorCode::EErrorCode& code, TString& error) const
{
    if (!cmd.HasId() || cmd.GetId().empty()) {
        code = NPersQueue::NErrorCode::BAD_REQUEST;
        error = "Empty Id";
        return Nothing();
    } else if (cmd.GetId().size() > MAX_SOURCE_ID_LENGTH) {
        code = NPersQueue::NErrorCode::BAD_REQUEST;
        error = "Too long Id";
        return Nothing();
    } else if (cmd.HasPartitionKeyRange() && !Config.PartitionKeySchemaSize()) {
        code = NPersQueue::NErrorCode::BAD_REQUEST;
        error = "Missing KeySchema";
        return Nothing();
    }

    TMaybe<NKikimrPQ::TPartitionKeyRange> keyRange;
    if (cmd.HasPartitionKeyRange()) {
        keyRange = cmd.GetPartitionKeyRange();
    }

    return TEvPQ::TEvRegisterMessageGroup::TBody(cmd.GetId(), std::move(keyRange), cmd.GetStartingSeqNo(), cmd.GetAfterSplit());
}

TMaybe<TEvPQ::TEvDeregisterMessageGroup::TBody> TPersQueue::MakeDeregisterMessageGroup(
        const NKikimrClient::TPersQueuePartitionRequest::TCmdDeregisterMessageGroup& cmd,
        NPersQueue::NErrorCode::EErrorCode& code, TString& error) const
{
    if (!cmd.HasId() || cmd.GetId().empty()) {
        code = NPersQueue::NErrorCode::BAD_REQUEST;
        error = "Empty Id";
        return Nothing();
    } else if (cmd.GetId().size() > MAX_SOURCE_ID_LENGTH) {
        code = NPersQueue::NErrorCode::BAD_REQUEST;
        error = "Too long Id";
        return Nothing();
    }

    return TEvPQ::TEvDeregisterMessageGroup::TBody(cmd.GetId());
}

void TPersQueue::HandleRegisterMessageGroupRequest(ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    PQ_ENSURE(req.HasCmdRegisterMessageGroup());

    NPersQueue::NErrorCode::EErrorCode code;
    TString error;
    auto body = MakeRegisterMessageGroup(req.GetCmdRegisterMessageGroup(), code, error);

    if (!body) {
        return ReplyError(ctx, responseCookie, code, error);
    }

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_REGISTER_MESSAGE_GROUP);
    ctx.Send(partActor, new TEvPQ::TEvRegisterMessageGroup(responseCookie, std::move(body.GetRef())), 0, 0, std::move(traceId));
}

void TPersQueue::HandleDeregisterMessageGroupRequest(ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    PQ_ENSURE(req.HasCmdDeregisterMessageGroup());

    NPersQueue::NErrorCode::EErrorCode code;
    TString error;
    auto body = MakeDeregisterMessageGroup(req.GetCmdDeregisterMessageGroup(), code, error);

    if (!body) {
        return ReplyError(ctx, responseCookie, code, error);
    }

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_DEREGISTER_MESSAGE_GROUP);
    ctx.Send(partActor, new TEvPQ::TEvDeregisterMessageGroup(responseCookie, std::move(body.GetRef())), 0, 0, std::move(traceId));
}

void TPersQueue::HandleUpdateReadMetricsRequest(ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    AFL_VERIFY_DEBUG(req.HasCmdUpdateReadMetrics());

    if (!req.GetCmdUpdateReadMetrics().HasClientId()) {
        return ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "source_id is required");
    }

    auto clientId = req.GetCmdUpdateReadMetrics().GetClientId();
    TDuration inFlightLimitReachedDuration = TDuration::Zero();
    if (req.GetCmdUpdateReadMetrics().HasInFlightLimitReachedDurationMs()) {
        inFlightLimitReachedDuration = TDuration::MilliSeconds(req.GetCmdUpdateReadMetrics().GetInFlightLimitReachedDurationMs());
    }
    ctx.Send(partActor, new TEvPQ::TEvUpdateReadMetrics(clientId, inFlightLimitReachedDuration), 0, 0, std::move(traceId));
}

void TPersQueue::HandleSplitMessageGroupRequest(ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    PQ_ENSURE(req.HasCmdSplitMessageGroup());
    const auto& cmd = req.GetCmdSplitMessageGroup();

    NPersQueue::NErrorCode::EErrorCode code;
    TString error;

    TVector<TEvPQ::TEvDeregisterMessageGroup::TBody> deregistrations;
    for (const auto& group : cmd.GetDeregisterGroups()) {
        auto body = MakeDeregisterMessageGroup(group, code, error);
        if (!body) {
            return ReplyError(ctx, responseCookie, code, error);
        }

        deregistrations.push_back(std::move(body.GetRef()));
    }

    TVector<TEvPQ::TEvRegisterMessageGroup::TBody> registrations;
    for (const auto& group : cmd.GetRegisterGroups()) {
        if (group.GetAfterSplit()) {
            return ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "AfterSplit cannot be set");
        }

        auto body = MakeRegisterMessageGroup(group, code, error);
        if (!body) {
            return ReplyError(ctx, responseCookie, code, error);
        }

        registrations.push_back(std::move(body.GetRef()));
    }

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_SPLIT_MESSAGE_GROUP);
    ctx.Send(partActor, new TEvPQ::TEvSplitMessageGroup(responseCookie, std::move(deregistrations), std::move(registrations)), 0, 0, std::move(traceId));
}

const TPartitionInfo& TPersQueue::GetPartitionInfo(const NKikimrClient::TPersQueuePartitionRequest& req) const
{
    PQ_ENSURE(req.HasWriteId());

    const TWriteId writeId = GetWriteId(req);
    ui32 originalPartitionId = req.GetPartition();

    PQ_ENSURE(TxWrites.contains(writeId) && TxWrites.at(writeId).Partitions.contains(originalPartitionId))
        ("WriteId", writeId.ToString())("partition_id", originalPartitionId);

    const TPartitionId& partitionId = TxWrites.at(writeId).Partitions.at(originalPartitionId);
    PQ_ENSURE(Partitions.contains(partitionId));
    const TPartitionInfo& partition = Partitions.at(partitionId);
    PQ_ENSURE(partition.InitDone);

    return partition;
}

void TPersQueue::HandleGetOwnershipRequestForSupportivePartition(const ui64 responseCookie,
                                                                 NWilson::TTraceId traceId,
                                                                 const NKikimrClient::TPersQueuePartitionRequest& req,
                                                                 const TActorId& sender,
                                                                 const TActorContext& ctx)
{
    const TPartitionInfo& partition = GetPartitionInfo(req);
    const TActorId& actorId = partition.Actor;
    TActorId pipeClient = ActorIdFromProto(req.GetPipeClient());

    HandleGetOwnershipRequest(responseCookie, std::move(traceId), actorId, req, ctx, pipeClient, sender);
}

void TPersQueue::HandleReserveBytesRequestForSupportivePartition(const ui64 responseCookie,
                                                                 NWilson::TTraceId traceId,
                                                                 const NKikimrClient::TPersQueuePartitionRequest& req,
                                                                 const TActorId& sender,
                                                                 const TActorContext& ctx)
{
    const TPartitionInfo& partition = GetPartitionInfo(req);
    const TActorId& actorId = partition.Actor;
    TActorId pipeClient = ActorIdFromProto(req.GetPipeClient());

    HandleReserveBytesRequest(responseCookie, std::move(traceId), actorId, req, ctx, pipeClient, sender);
}

void TPersQueue::HandleWriteRequestForSupportivePartition(const ui64 responseCookie,
                                                          NWilson::TTraceId traceId,
                                                          const NKikimrClient::TPersQueuePartitionRequest& req,
                                                          const TActorContext& ctx)
{
    PQ_ENSURE(req.HasWriteId());

    const TPartitionInfo& partition = GetPartitionInfo(req);
    const TActorId& actorId = partition.Actor;

    HandleWriteRequest(responseCookie, std::move(traceId), actorId, req, ctx);
}

void TPersQueue::HandleEventForSupportivePartition(const ui64 responseCookie,
                                                   NWilson::TTraceId traceId,
                                                   const NKikimrClient::TPersQueuePartitionRequest& req,
                                                   const TActorId& sender,
                                                   const TActorContext& ctx)
{
    if (req.HasCmdGetOwnership()) {
        HandleGetOwnershipRequestForSupportivePartition(responseCookie, std::move(traceId), req, sender, ctx);
    } else if (req.HasCmdReserveBytes()) {
        HandleReserveBytesRequestForSupportivePartition(responseCookie, std::move(traceId), req, sender, ctx);
    } else if (req.CmdWriteSize()) {
        HandleWriteRequestForSupportivePartition(responseCookie, std::move(traceId), req, ctx);
    } else {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "CmdGetOwnership, CmdReserveBytes or CmdWrite expected");
    }
}

void TPersQueue::HandleEventForSupportivePartition(const ui64 responseCookie,
                                                   TEvPersQueue::TEvRequest::TPtr& event,
                                                   const TActorId& sender,
                                                   const TActorContext& ctx)
{
    const NKikimrClient::TPersQueuePartitionRequest& req =
        event->Get()->Record.GetPartitionRequest();

    PQ_ENSURE(req.HasWriteId());

    bool isValid =
        req.HasCmdGetOwnership()
        || req.HasCmdReserveBytes()
        || req.CmdWriteSize()
        ;
    if (!isValid) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "CmdGetOwnership, CmdReserveBytes or CmdWrite expected");
        return;
    }

    const TWriteId writeId = GetWriteId(req);
    ui32 originalPartitionId = req.GetPartition();
    if (writeId.IsKafkaApiTransaction() && TxWrites.contains(writeId) && TxWrites.at(writeId).Deleting) {
        // This branch happens when previous Kafka transaction has committed and we receive write for next one
        // after PQ has deleted supportive partition and before it has deleted writeId from TxWrites (tx has not transaitioned to DELETED state)
        PQ_LOG_D("GetOwnership request for the next Kafka transaction while previous is being deleted. Saving it till the complete delete of the previous tx.%01");
        KafkaNextTransactionRequests[writeId.KafkaProducerInstanceId].push_back(event);
        return;
    } else if (TxWrites.contains(writeId) && TxWrites.at(writeId).Partitions.contains(originalPartitionId)) {
        //
        // - вспомогательная партиция уже существует
        // - если партиция инициализирована, то
        // -     отправить ей сообщение
        // - иначе
        // -     добавить сообщение в очередь для партиции
        //
        const TTxWriteInfo& writeInfo = TxWrites.at(writeId);
        if (writeInfo.TxId.Defined() && writeId.IsTopicApiTransaction()) {
            ReplyError(ctx,
                       responseCookie,
                       NPersQueue::NErrorCode::BAD_REQUEST,
                       "it is forbidden to write after a commit");
            return;
        } else if (writeInfo.TxId.Defined() && writeId.IsKafkaApiTransaction()) {
            // This branch happens when previous Kafka transaction has committed and we receive write for next one
            // before PQ has deleted supportive partition for previous transaction
            PQ_LOG_D("GetOwnership request for the next Kafka transaction while previous is being deleted. Saving it till the complete delete of the previous tx.%02");
            KafkaNextTransactionRequests[writeId.KafkaProducerInstanceId].push_back(event);
            return;
        }

        const TPartitionId& partitionId = writeInfo.Partitions.at(originalPartitionId);
        PQ_ENSURE(Partitions.contains(partitionId));
        TPartitionInfo& partition = Partitions.at(partitionId);

        if (partition.InitDone) {
            HandleEventForSupportivePartition(responseCookie, std::move(event->TraceId), req, sender, ctx);
        } else {
            partition.PendingRequests.emplace_back(responseCookie,
                                                   std::shared_ptr<TEvPersQueue::TEvRequest>(event->Release().Release()),
                                                   sender);
        }
    } else {
        if (!req.GetNeedSupportivePartition()) {
            // missing supportivce partition in kafka transaction means that we already committed and deleted transaction for current producerId + producerEpoch
            NPersQueue::NErrorCode::EErrorCode errorCode = writeId.KafkaApiTransaction ?
                NPersQueue::NErrorCode::KAFKA_TRANSACTION_MISSING_SUPPORTIVE_PARTITION :
                NPersQueue::NErrorCode::PRECONDITION_FAILED;
            TString error = writeId.KafkaApiTransaction ?
                "Kafka transaction and there is no supportive partition for current producerId and producerEpoch. It means GetOwnership request was not called from TPartitionWriter" :
                "lost messages";

            ReplyError(ctx,
                       responseCookie,
                       errorCode,
                       error);
            return;
        }

        //
        // этап 1:
        // - создать запись в TxWrites
        // - создать вспомогательную партицию
        // - добавить сообщение в очередь для партиции
        // - отправить на запись
        //
        // этап 2:
        // - после записи запустить актор партиции
        //
        // этап 3:
        // - когда партиция будет готова отправить ей накопленные сообщения
        //
        TTxWriteInfo& writeInfo = TxWrites[writeId];
        TPartitionId partitionId(originalPartitionId, writeId, NextSupportivePartitionId++);
        PQ_LOG_TX_I("partition " << partitionId << " for WriteId " << writeId);

        writeInfo.Partitions.emplace(originalPartitionId, partitionId);
        TxWritesChanged = true;
        AddSupportivePartition(partitionId);

        PQ_ENSURE(Partitions.contains(partitionId));

        TPartitionInfo& partition = Partitions.at(partitionId);
        partition.PendingRequests.emplace_back(responseCookie,
                                               std::shared_ptr<TEvPersQueue::TEvRequest>(event->Release().Release()),
                                               sender);

        if (writeId.IsTopicApiTransaction() && writeInfo.LongTxSubscriptionStatus == NKikimrLongTxService::TEvLockStatus::STATUS_UNSPECIFIED) {
            SubscribeWriteId(writeId, ctx);
        }

        if (writeId.KafkaApiTransaction) {
            writeInfo.KafkaTransaction = true;
            writeInfo.CreatedAt = TAppData::TimeProvider->Now();
        }

        TryWriteTxs(ctx);
    }
}

void TPersQueue::Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx)
{
    NKikimrClient::TPersQueueRequest& request = ev->Get()->Record;
    TString s = request.HasRequestId() ? request.GetRequestId() : "<none>";
    ui32 p = request.HasPartitionRequest() && request.GetPartitionRequest().HasPartition() ? request.GetPartitionRequest().GetPartition() : 0;
    ui64 m = request.HasPartitionRequest() && request.GetPartitionRequest().HasMessageNo() ? request.GetPartitionRequest().GetMessageNo() : 0;
    TMaybe<ui64> c;
    if (request.HasPartitionRequest() && request.GetPartitionRequest().HasCookie())
        c = request.GetPartitionRequest().GetCookie();
    TAutoPtr<TResponseBuilder> ans;
    ui64 responseCookie = ++NextResponseCookie;

    auto& req = request.GetPartitionRequest();
    TActorId pipeClient = ActorIdFromProto(req.GetPipeClient());

    if (request.GetPartitionRequest().HasCmdRead() && s != TMP_REQUEST_MARKER) {
        auto pipeIter = PipesInfo.find(pipeClient);
        TDirectReadKey directKey{};
        if (!pipeIter.IsEnd()) {
            directKey.SessionId = pipeIter->second.SessionId;
            directKey.PartitionSessionId = pipeIter->second.PartitionSessionId;
        }
        TActorId rr = ctx.RegisterWithSameMailbox(CreateReadProxy(ev->Sender, TabletID(), ctx.SelfID, GetGeneration(), directKey, request));
        ans = CreateResponseProxy(rr, ctx.SelfID, TopicName, p, m, s, c, ResourceMetrics, ctx);
    } else {
        ans = CreateResponseProxy(ev->Sender, ctx.SelfID, TopicName, p, m, s, c, ResourceMetrics, ctx);
    }

    ResponseProxy[responseCookie] = ans;
    Counters->Simple()[COUNTER_PQ_TABLET_INFLIGHT].Set(ResponseProxy.size());

    if (!ConfigInited) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::INITIALIZING, "tablet is not ready");
        return;
    }

    if (TabletState == NKikimrPQ::EDropped) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::TABLET_IS_DROPPED, "tablet is dropped");
        return;
    }

    if (!request.HasPartitionRequest()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "no partition request");
        return;
    }


    if (!req.HasPartition()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "no partition number");
        return;
    }

    TPartitionId partition(req.GetPartition());
    auto it = Partitions.find(partition);

    PQ_LOG_D("got client message batch for topic '"
            << (TopicConverter ? TopicConverter->GetClientsideName() : "Undefined") << "' partition " << partition);

    if (it == Partitions.end()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::WRONG_PARTITION_NUMBER,
            TStringBuilder() << "wrong partition number " << partition);
        return;
    }

    if (!it->second.InitDone) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::INITIALIZING,
            TStringBuilder() << "partition " << partition << " is not ready");
        return;
    }

    ui32 count = req.HasCmdGetMaxSeqNo()
        + req.HasCmdDeleteSession()
        + req.HasCmdCreateSession()
        + req.HasCmdSetClientOffset()
        + req.HasCmdGetClientOffset()
        + req.HasCmdRead()
        + req.HasCmdGetOwnership()
        + (req.CmdWriteSize() > 0 ? 1 : 0)
        + req.HasCmdReserveBytes()
        + req.HasCmdUpdateWriteTimestamp()
        + req.HasCmdRegisterMessageGroup()
        + req.HasCmdDeregisterMessageGroup()
        + req.HasCmdSplitMessageGroup()
        + req.HasCmdPublishRead()
        + req.HasCmdForgetRead()
        + req.HasCmdUpdateReadMetrics();

    if (count != 1) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "multiple commands in request: " << count);
        return;
    }

    if (req.HasWriteId()) {
        HandleEventForSupportivePartition(responseCookie, ev, ev->Sender, ctx);
        return;
    }

    const TActorId& partActor = it->second.Actor;
    if (req.HasCmdGetMaxSeqNo()) {
        HandleGetMaxSeqNoRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.HasCmdDeleteSession()) {
        HandleDeleteSessionRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdCreateSession()) {
        HandleCreateSessionRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdSetClientOffset()) {
        HandleSetClientOffsetRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.HasCmdGetClientOffset()) {
        HandleGetClientOffsetRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.CmdWriteSize()) {
        HandleWriteRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.HasCmdUpdateWriteTimestamp()) {
        HandleUpdateWriteTimestampRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.HasCmdRead()) {
        HandleReadRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdPublishRead()) {
        HandlePublishReadRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdForgetRead()) {
        HandleForgetReadRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdGetOwnership()) {
        HandleGetOwnershipRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdReserveBytes()) {
        HandleReserveBytesRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdRegisterMessageGroup()) {
        HandleRegisterMessageGroupRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.HasCmdDeregisterMessageGroup()) {
        HandleDeregisterMessageGroupRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.HasCmdSplitMessageGroup()) {
        HandleSplitMessageGroupRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else if (req.HasCmdUpdateReadMetrics()) {
        HandleUpdateReadMetricsRequest(responseCookie, NWilson::TTraceId(ev->TraceId), partActor, req, ctx);
    } else {
        PQ_LOG_ERROR_AND_DIE("unknown or empty command");
        return;
    }
}


void TPersQueue::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&)
{
    PQ_LOG_T("Handle TEvTabletPipe::TEvServerConnected");

    auto it = PipesInfo.insert({ev->Get()->ClientId, {}}).first;
    it->second.ServerActors++;
    PQ_LOG_D("server connected, pipe "
                << ev->Get()->ClientId.ToString() << ", now have " << it->second.ServerActors << " active actors on pipe");

    Counters->Simple()[COUNTER_PQ_TABLET_OPENED_PIPES] = PipesInfo.size();
}


void TPersQueue::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_T("Handle TEvTabletPipe::TEvServerDisconnected");

    //inform partition if needed;
    auto it = PipesInfo.find(ev->Get()->ClientId);
    if (it != PipesInfo.end()) {
        if(--(it->second.ServerActors) > 0) {
            return;
        }
        if (it->second.PartActor != TActorId()) {
            ctx.Send(it->second.PartActor, new TEvPQ::TEvPipeDisconnected(
                    it->second.Owner, it->first
            ));
        }
        if (!it->second.SessionId.empty()) {
            DestroySession(it->second);
        }
        PQ_LOG_D("server disconnected, pipe "
                << ev->Get()->ClientId.ToString() << " destroyed");
        PipesInfo.erase(it);
        Counters->Simple()[COUNTER_PQ_TABLET_OPENED_PIPES] = PipesInfo.size();
    }
}

void TPersQueue::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvTabletPipe::TEvClientConnected");

    PQ_ENSURE(ev->Get()->Leader)("description", TStringBuilder() << "Unexpectedly connected to follower of tablet " << ev->Get()->TabletId);

    if (PipeClientCache->OnConnect(ev)) {
        PQ_LOG_D("Connected to tablet " << ev->Get()->TabletId);
        return;
    }

    if (ev->Get()->Dead) {
        AckReadSetsToTablet(ev->Get()->TabletId, ctx);
        return;
    }

    RestartPipe(ev->Get()->TabletId, ctx);
}

void TPersQueue::AckReadSetsToTablet(ui64 tabletId, const TActorContext& ctx)
{
    THashSet<TDistributedTransaction*> txs;
    TVector<std::pair<ui64, ui64>> bindings;



    for (ui64 txId : GetBindedTxs(tabletId)) {
        PQ_LOG_TX_I("Assume tablet " << tabletId << " dead, sending read set acks for tx " << txId);

        auto* tx = GetTransaction(ctx, txId);
        if (!tx) {
            continue;
        }

        tx->OnReadSetAck(tabletId);
        tx->UnbindMsgsFromPipe(tabletId);

        bindings.emplace_back(tabletId, tx->TxId);

        txs.insert(tx);
    }

    if (txs.empty()) {
        return;
    }

    for (const auto& [tabletId, txId] : bindings) {
        UnbindTxFromPipe(tabletId, txId);
    }

    for (auto* tx : txs) {
        TryExecuteTxs(ctx, *tx);
    }

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvTabletPipe::TEvClientDestroyed");
    PQ_LOG_D("Client pipe to tablet " << ev->Get()->TabletId << " is reset");

    PipeClientCache->OnDisconnect(ev);

    RestartPipe(ev->Get()->TabletId, ctx);
}

void TPersQueue::RestartPipe(ui64 tabletId, const TActorContext& ctx)
{
    for (ui64 txId : GetBindedTxs(tabletId)) {
        auto* tx = GetTransaction(ctx, txId);
        if (!tx) {
            continue;
        }

        for (const auto& message : tx->GetBindedMsgs(tabletId)) {
            auto event = std::make_unique<TEvTxProcessing::TEvReadSet>();
            event->Record = message;
            PipeClientCache->Send(ctx, tabletId, event.release());
        }
    }
}

void TPersQueue::HandleDie(const TActorContext& ctx)
{
    MeteringSink.MayFlushForcibly(ctx.Now());

    for (const auto& p : Partitions) {
        ctx.Send(p.second.Actor, new TEvents::TEvPoisonPill());
    }
    ctx.Send(CacheActor, new TEvents::TEvPoisonPill());

    for (auto& pipe : PipesInfo) {
        if (!pipe.second.SessionId.empty()) {
            DestroySession(pipe.second);
        }
    }
    for (const auto& p : ResponseProxy) {
        THolder<TEvPQ::TEvError> ev = MakeHolder<TEvPQ::TEvError>(NPersQueue::NErrorCode::INITIALIZING, "tablet will be restarted right now", p.first);
        bool res = p.second->HandleError(ev.Get(), ctx);
        PQ_ENSURE(res);
    }
    ResponseProxy.clear();

    StopWatchingTenantPathId(ctx);
    MediatorTimeCastUnregisterTablet(ctx);

    NKeyValue::TKeyValueFlat::HandleDie(ctx);
}

void TPersQueue::InitPipeClientCache()
{
    PipeClientCacheConfig = MakeIntrusive<NTabletPipe::TBoundedClientCacheConfig>();
    PipeClientCacheConfig->ClientPoolLimit = 2000;

    PipeClientCache.Reset(NTabletPipe::CreateBoundedClientCache(PipeClientCacheConfig, GetPipeClientConfig()));
}

TPersQueue::TPersQueue(const TActorId& tablet, TTabletStorageInfo *info)
    : TKeyValueFlat(tablet, info)
    , ConfigInited(false)
    , PartitionsInited(0)
    , OriginalPartitionsCount(0)
    , NewConfigShouldBeApplied(false)
    , TabletState(NKikimrPQ::ENormal)
    , NextResponseCookie(0)
    , ResourceMetrics(nullptr)
{
    // Override to persqueue activity type
    SetActivityType(ActorActivityType());

    InitPipeClientCache();

    typedef TProtobufTabletCounters<
        NKeyValue::ESimpleCounters_descriptor,
        NKeyValue::ECumulativeCounters_descriptor,
        NKeyValue::EPercentileCounters_descriptor,
        NKeyValue::ETxTypes_descriptor> TKeyValueCounters;
    typedef TAppProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor> TPersQueueCounters;
    typedef TProtobufTabletCountersPair<TKeyValueCounters, TPersQueueCounters> TCounters;

    TAutoPtr<TCounters> counters(new TCounters());
    Counters.reset(counters->GetSecondTabletCounters().Release());

    State.SetupTabletCounters(counters->GetFirstTabletCounters().Release()); //FirstTabletCounters is of good type and contains all counters
    State.Clear();
}

void TPersQueue::CreatedHook(const TActorContext& ctx)
{
    IsServerless = AppData(ctx)->FeatureFlags.GetEnableDbCounters(); //TODO: find out it via describe
    CacheActor = ctx.RegisterWithSameMailbox(new TPQCacheProxy(ctx.SelfID, TabletID()));

    SamplingControl = AppData(ctx)->TracingConfigurator->GetControl();

    ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(ctx.SelfID.NodeId()));
    InitProcessingParams(ctx);
}

void TPersQueue::StartWatchingTenantPathId(const TActorContext& ctx)
{
    ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(Info()->TenantPathId));
}

void TPersQueue::StopWatchingTenantPathId(const TActorContext& ctx)
{
    ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
}

void TPersQueue::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx)
{
    const auto& result = ev->Get()->Result;
    ProcessingParams = result->GetPathDescription().GetDomainDescription().GetProcessingParams();

    InitMediatorTimeCast(ctx);
}

void TPersQueue::MediatorTimeCastRegisterTablet(const TActorContext& ctx)
{
    PQ_ENSURE(ProcessingParams);
    ctx.Send(MakeMediatorTimecastProxyID(),
             new TEvMediatorTimecast::TEvRegisterTablet(TabletID(), *ProcessingParams));
}

void TPersQueue::MediatorTimeCastUnregisterTablet(const TActorContext& ctx)
{
    ctx.Send(MakeMediatorTimecastProxyID(),
             new TEvMediatorTimecast::TEvUnregisterTablet(TabletID()));
}

void TPersQueue::Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx)
{
    const auto* message = ev->Get();
    PQ_ENSURE(message->TabletId == TabletID());

    MediatorTimeCastEntry = message->Entry;
    PQ_ENSURE(MediatorTimeCastEntry);

    PQ_LOG_TX_D("Registered with mediator time cast");

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvInterconnect::TEvNodeInfo");

    PQ_ENSURE(ev->Get()->Node);
    DCId = ev->Get()->Node->Location.GetDataCenterId();
    ResourceMetrics = Executor()->GetResourceMetrics();

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    request->Record.SetCookie(READ_CONFIG_COOKIE);

    request->Record.AddCmdRead()->SetKey(KeyConfig());
    request->Record.AddCmdRead()->SetKey(KeyState());
    request->Record.AddCmdRead()->SetKey(KeyTxInfo());

    request->Record.MutableCmdSetExecutorFastLogPolicy()
                ->SetIsAllowed(AppData(ctx)->PQConfig.GetTactic() == NKikimrClient::TKeyValueRequest::MIN_LATENCY);
    ctx.Send(ctx.SelfID, request.Release());

    ctx.Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
}

void TPersQueue::AddCmdReadTransactionRange(TEvKeyValue::TEvRequest& request,
                                            const TString& fromKey, bool includeFrom)
{
    auto cmd = request.Record.AddCmdReadRange();
    cmd->MutableRange()->SetFrom(fromKey);
    cmd->MutableRange()->SetIncludeFrom(includeFrom);
    cmd->MutableRange()->SetTo(GetTxKey(Max<ui64>()));
    cmd->MutableRange()->SetIncludeTo(true);
    cmd->SetIncludeData(true);

    PQ_LOG_D("Transactions request." <<
             " From " << cmd->MutableRange()->GetFrom() <<
             ", To " << cmd->MutableRange()->GetTo());
}

void TPersQueue::HandleWakeup(const TActorContext& ctx) {
    THashSet<TString> groups;
    for (auto& p : Partitions) {
        for (auto& m : p.second.LabeledCounters) {
            groups.insert(m.first);
        }
    }
    for (auto& g : groups) {
        AggregateAndSendLabeledCountersFor(g, ctx);
    }
    MeteringSink.MayFlush(ctx.Now());
    DeleteExpiredTransactions(ctx);
    SetTxCounters();
    ctx.Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
}

void TPersQueue::DeleteExpiredTransactions(const TActorContext& ctx)
{
    ScheduleDeleteExpiredKafkaTransactions();

    if (!MediatorTimeCastEntry) {
        return;
    }

    ui64 step = MediatorTimeCastEntry->Get(TabletID());

    for (auto& [txId, tx] : Txs) {
        if ((tx.MaxStep < step) && (tx.State <= NKikimrPQ::TTransaction::PREPARED)) {
            BeginDeleteTransaction(ctx, tx, NKikimrPQ::TTransaction::EXPIRED);
        }
    }

    TryWriteTxs(ctx);
}

void TPersQueue::ScheduleDeleteExpiredKafkaTransactions() {
    TDuration kafkaTxnTimeout = TDuration::MilliSeconds(
        AppData()->KafkaProxyConfig.GetTransactionTimeoutMs() + KAFKA_TRANSACTION_DELETE_DELAY_MS);

    auto txnExpired = [kafkaTxnTimeout](const TTxWriteInfo& txWriteInfo) {
        return txWriteInfo.KafkaTransaction && txWriteInfo.CreatedAt + kafkaTxnTimeout < TAppData::TimeProvider->Now();
    };

    for (auto& pair : TxWrites) {
        if (txnExpired(pair.second)) {
            PQ_LOG_D("Transaction for Kafka producer " << pair.first.KafkaProducerInstanceId << " is expired");
            BeginDeletePartitions(pair.first, pair.second);
        }
    }
}

void TPersQueue::TryContinueKafkaWrites(const TMaybe<TWriteId> writeId, const TActorContext& ctx) {
    if (writeId.Defined() && writeId->IsKafkaApiTransaction()) {
        auto it = KafkaNextTransactionRequests.find(writeId->KafkaProducerInstanceId);
        if (it != KafkaNextTransactionRequests.end()) {
            for (auto& request : it->second) {
                Handle(request, ctx);
            }
            KafkaNextTransactionRequests.erase(it);
        }
    }
}

void TPersQueue::SetTxCounters()
{
    SetTxCompleteLagCounter();
    SetTxInFlyCounter();
}

void TPersQueue::SetTxCompleteLagCounter()
{
    ui64 lag = 0;

    if (!TxQueue.empty()) {
        ui64 firstTxStep = TxQueue.front().first;
        ui64 currentStep = TAppData::TimeProvider->Now().MilliSeconds();

        if (currentStep > firstTxStep) {
            lag = currentStep - firstTxStep;
        }
    }

    Counters->Simple()[COUNTER_PQ_TABLET_TX_COMPLETE_LAG] = lag;
}

void TPersQueue::SetTxInFlyCounter()
{
    Counters->Simple()[COUNTER_PQ_TABLET_TX_IN_FLY] = Txs.size();
}

void TPersQueue::BeginDeleteTransaction(const TActorContext& ctx,
                                        TDistributedTransaction& tx,
                                        NKikimrPQ::TTransaction::EState state)
{
    BeginDeletePartitions(tx);
    ChangeTxState(tx, state);
    CheckTxState(ctx, tx);
}

void TPersQueue::Handle(TEvPersQueue::TEvCancelTransactionProposal::TPtr& ev, const TActorContext& ctx)
{
    if (!InitCompleted) {
        AddPendingEvent(ev.Release());
        return;
    }

    NKikimrPQ::TEvCancelTransactionProposal& event = ev->Get()->Record;
    PQ_ENSURE(event.HasTxId());

    PQ_LOG_TX_W("Handle TEvPersQueue::TEvCancelTransactionProposal for TxId " << event.GetTxId());

    if (auto tx = GetTransaction(ctx, event.GetTxId()); tx) {
        PQ_ENSURE(tx->State <= NKikimrPQ::TTransaction::PREPARED);

        BeginDeleteTransaction(ctx, *tx, NKikimrPQ::TTransaction::CANCELED);

        TryWriteTxs(ctx);
    }
}

void TPersQueue::Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx)
{
    if (!InitCompleted) {
        AddPendingEvent(ev.Release());
        return;
    }

    const NKikimrPQ::TEvProposeTransaction& event = ev->Get()->GetRecord();
    PQ_LOG_TX_D("Handle TEvPersQueue::TEvProposeTransaction " << event.ShortDebugString());

    auto span = GenerateSpan("Topic.Transaction", *SamplingControl, std::move(ev->TraceId));
    span.Attribute("TxId", static_cast<i64>(event.GetTxId()));
    span.Attribute("TabletId", static_cast<i64>(TabletID()));
    span.Attribute("database", Config.GetYdbDatabasePath());

    ev->Get()->ExecuteSpan = std::move(span);

    switch (event.GetTxBodyCase()) {
    case NKikimrPQ::TEvProposeTransaction::kData:
        HandleDataTransaction(ev->Release(), ctx);
        break;
    case NKikimrPQ::TEvProposeTransaction::kConfig:
        HandleConfigTransaction(ev->Release(), ctx);
        break;
    case NKikimrPQ::TEvProposeTransaction::TXBODY_NOT_SET:
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::ERROR,
                                    "missing TxBody",
                                    ctx);
        break;
    }

}

bool TPersQueue::CheckTxWriteOperation(const NKikimrPQ::TPartitionOperation& operation,
                                       const TWriteId& writeId) const
{
    TPartitionId partitionId;
    if (operation.GetKafkaTransaction()) {
        auto txWriteInfoIt = TxWrites.find(writeId);
        if (txWriteInfoIt == TxWrites.end()) {
            return false;
        }
        auto it = txWriteInfoIt->second.Partitions.find(operation.GetPartitionId());
        if (it == txWriteInfoIt->second.Partitions.end()) {
            return false;
        } else {
            partitionId = it->second;
        }
    } else {
        partitionId = TPartitionId{operation.GetPartitionId(),
                                 writeId,
                                 operation.GetSupportivePartition()};
    }
    PQ_LOG_TX_D("PartitionId " << partitionId << " for WriteId " << writeId);
    return Partitions.contains(partitionId);
}

static bool IsWriteTxOperation(const NKikimrPQ::TPartitionOperation& operation) {
    bool isRead = operation.HasCommitOffsetsBegin() || (operation.GetKafkaTransaction() && operation.HasCommitOffsetsEnd());
    return !isRead;
}

bool TPersQueue::CheckTxWriteOperations(const NKikimrPQ::TDataTransaction& txBody) const
{
    if (!txBody.HasWriteId()) {
        return true;
    }

    const TWriteId writeId = GetWriteId(txBody);

    for (auto& operation : txBody.GetOperations()) {
        if (IsWriteTxOperation(operation)) {
            if (!CheckTxWriteOperation(operation, writeId)) {
                return false;
            }
        }
    }

    return true;
}

void TPersQueue::HandleDataTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> ev,
                                       const TActorContext& ctx)
{
    auto span = ev->ExecuteSpan.CreateChild(TWilsonTopic::TopicTopLevel,
                                            "Topic.Transaction.HandleDataTransaction",
                                            NWilson::EFlags::AUTO_END);

    NKikimrPQ::TEvProposeTransaction& event = *ev->MutableRecord();
    PQ_ENSURE(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    PQ_ENSURE(event.HasData());
    const NKikimrPQ::TDataTransaction& txBody = event.GetData();

    if (TabletState != NKikimrPQ::ENormal) {
        PQ_LOG_TX_W("TxId " << event.GetTxId() << " invalid PQ tablet state (" << NKikimrPQ::ETabletState_Name(TabletState) << ")");
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::ERROR,
                                    "invalid PQ tablet state",
                                    ctx);
        return;
    }

    if (txBody.OperationsSize() <= 0) {
        PQ_LOG_TX_W("TxId " << event.GetTxId() << " empty list of operations");
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::BAD_REQUEST,
                                    "empty list of operations",
                                    ctx);
        return;
    }

    if (!CheckTxWriteOperations(txBody)) {
        PQ_LOG_TX_W("TxId " << event.GetTxId() << " invalid WriteId " << txBody.GetWriteId());
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::BAD_REQUEST,
                                    "invalid WriteId",
                                    ctx);
        return;
    }

    if (txBody.HasWriteId()) {
        const TWriteId writeId = GetWriteId(txBody);
        if (!TxWrites.contains(writeId)) {
            PQ_LOG_TX_W("TxId " << event.GetTxId() << " unknown WriteId " << writeId);
            SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                        event.GetTxId(),
                                        NKikimrPQ::TError::BAD_REQUEST,
                                        "unknown WriteId",
                                        ctx);
            return;
        }

        TTxWriteInfo& writeInfo = TxWrites.at(writeId);
        if (writeInfo.Deleting) {
            PQ_LOG_TX_W("TxId " << event.GetTxId() << " WriteId " << writeId << " will be deleted");
            SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                        event.GetTxId(),
                                        NKikimrPQ::TError::BAD_REQUEST,
                                        "WriteId will be deleted",
                                        ctx);
            return;
        }

        writeInfo.TxId = event.GetTxId();
        PQ_LOG_TX_I("TxId " << event.GetTxId() << " has WriteId " << writeId);
    }

    TMaybe<TPartitionId> partitionId = FindPartitionId(txBody);
    if (!partitionId.Defined()) {
        PQ_LOG_TX_W("TxId " << event.GetTxId() << " unknown partition for WriteId " << txBody.GetWriteId());
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::INTERNAL,
                                    "unknown supportive partition",
                                    ctx);
        return;
    }

    if (!Partitions.contains(*partitionId)) {
        PQ_LOG_TX_W("unknown partition " << *partitionId);
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::INTERNAL,
                                    "unknown supportive partition",
                                    ctx);
        return;
    }

    if (txBody.GetImmediate()) {
        PQ_LOG_TX_D("immediate transaction");
        TPartitionId originalPartitionId(txBody.GetOperations(0).GetPartitionId());
        const TPartitionInfo& partition = Partitions.at(originalPartitionId);

        if (txBody.HasWriteId()) {
            // the condition `Partition.contains(*partitioned)` is checked above
            const TPartitionInfo& partition = Partitions.at(*partitionId);
            ActorIdToProto(partition.Actor, event.MutableSupportivePartitionActor());
        }

        ctx.Send(partition.Actor, ev.Release());
    } else {
        if ((EvProposeTransactionQueue.size() + Txs.size()) >= MAX_TXS) {
            SendProposeTransactionOverloaded(ActorIdFromProto(event.GetSourceActor()),
                                             event.GetTxId(),
                                             NKikimrPQ::TError::ERROR,
                                             "too many transactions",
                                             ctx);
            return;
        }

        PQ_LOG_TX_D("distributed transaction");
        EvProposeTransactionQueue.emplace_back(ev.Release());

        TryWriteTxs(ctx);
    }
}

void TPersQueue::HandleConfigTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> ev,
                                         const TActorContext& ctx)
{
    const NKikimrPQ::TEvProposeTransaction& event = ev->GetRecord();
    PQ_ENSURE(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kConfig);
    PQ_ENSURE(event.HasConfig());

    EvProposeTransactionQueue.emplace_back(ev.Release());

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx)
{
    if (!InitCompleted) {
        AddPendingEvent(ev.Release());
        return;
    }

    PQ_LOG_TX_D("Handle TEvTxProcessing::TEvPlanStep " << ev->Get()->Record.ShortDebugString());

    const TActorId sender = ev->Sender;
    std::unique_ptr<TEvTxProcessing::TEvPlanStep> event{ev->Release().Release()};

    ProcessPlanStep(sender, std::move(event), ctx);
}

void TPersQueue::Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx)
{
    if (!InitCompleted) {
        AddPendingEvent(ev.Release());
        return;
    }

    PQ_LOG_TX_D("Handle TEvTxProcessing::TEvReadSet " << ev->Get()->Record.ShortDebugString());

    NKikimrTx::TEvReadSet& event = ev->Get()->Record;
    PQ_ENSURE(event.HasTxId());

    std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack;
    if (!(event.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_ACK)) {
        ack = std::make_unique<TEvTxProcessing::TEvReadSetAck>(*ev->Get(), TabletID());
    }

    PQ_LOG_TX_I("Handle TEvTxProcessing::TEvReadSet tx " << event.GetTxId() << " tabletProducer " << event.GetTabletProducer());

    if (auto tx = GetTransaction(ctx, event.GetTxId()); tx && tx->PredicatesReceived.contains(event.GetTabletProducer())) {
        tx->SetLastTabletSentByRS(event.GetTabletProducer());

        if ((tx->State > NKikimrPQ::TTransaction::EXECUTED) ||
            ((tx->State == NKikimrPQ::TTransaction::EXECUTED) && !tx->WriteInProgress)) {
            if (ack) {
                PQ_LOG_TX_I("send TEvReadSetAck to " << event.GetTabletProducer() << " for TxId " << event.GetTxId());
                ctx.Send(ev->Sender, ack.release());
                return;
            }
        }

        tx->OnReadSet(event, ev->Sender, std::move(ack));

        if (tx->HaveParticipantsDecision()) {
            tx->EndWaitRSSpan();
        }

        if (tx->State == NKikimrPQ::TTransaction::WAIT_RS) {
            TryExecuteTxs(ctx, *tx);

            TryWriteTxs(ctx);
        }
    } else if (ack) {
        PQ_LOG_TX_I("a TEvReadSetAck message to " << event.GetTabletProducer() <<
                    " for TxId " << event.GetTxId() <<
                    " will be sent later");

        AddPendingDeferredReadSetAck({.Sender = ev->Sender, .Ack = std::move(ack)});
    }
}

void TPersQueue::MovePendingDeferredReadSetAcks()
{
    DeferredReadSetAcks = std::move(PendingDeferredReadSetAcks);
    PendingDeferredReadSetAcks.clear();
}

void TPersQueue::AddPendingDeferredReadSetAck(TDeferredReadSetAck&& ack)
{
    PendingDeferredReadSetAcks.push_back(std::move(ack));
}

void TPersQueue::SendDeferredReadSetAcks(const TActorContext& ctx)
{
    for (auto& e : DeferredReadSetAcks) {
        PQ_LOG_TX_I("send TEvReadSetAck to " << e.Ack->Record.GetTabletSource() <<
                    " for TxId " << e.Ack->Record.GetTxId());
        ctx.Send(e.Sender, e.Ack.release());
    }

    DeferredReadSetAcks.clear();
}

void TPersQueue::Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_TX_I("Handle TEvTxProcessing::TEvReadSetAck " << ev->Get()->Record.ShortDebugString());

    NKikimrTx::TEvReadSetAck& event = ev->Get()->Record;
    PQ_ENSURE(event.HasTxId());

    auto tx = GetTransaction(ctx, event.GetTxId());
    if (!tx) {
        return;
    }

    tx->OnReadSetAck(event);
    tx->UnbindMsgsFromPipe(event.GetTabletConsumer());
    UnbindTxFromPipe(event.GetTabletConsumer(), event.GetTxId());

    if (tx->State == NKikimrPQ::TTransaction::WAIT_RS_ACKS) {
        TryExecuteTxs(ctx, *tx);

        TryWriteTxs(ctx);
    }
}

void TPersQueue::Handle(TEvPQ::TEvTxCalcPredicateResult::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvTxCalcPredicateResult& event = *ev->Get();

    PQ_LOG_TX_D("Handle TEvPQ::TEvTxCalcPredicateResult" <<
             " Step " << event.Step <<
             ", TxId " << event.TxId <<
             ", Partition " << event.Partition <<
             ", Predicate " << event.Predicate);

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    PQ_ENSURE(tx->State == NKikimrPQ::TTransaction::CALCULATING);

    tx->OnTxCalcPredicateResult(event);

    TryExecuteTxs(ctx, *tx);

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPQ::TEvProposePartitionConfigResult::TPtr& ev, const TActorContext& ctx)
{
    TEvPQ::TEvProposePartitionConfigResult& event = *ev->Get();

    PQ_LOG_TX_D("Handle TEvPQ::TEvProposePartitionConfigResult" <<
             " Step " << event.Step <<
             ", TxId " << event.TxId <<
             ", Partition " << event.Partition);

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    PQ_ENSURE(tx->State == NKikimrPQ::TTransaction::CALCULATING);

    tx->OnProposePartitionConfigResult(event);

    TryExecuteTxs(ctx, *tx);

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPQ::TEvTxDone::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvTxDone& event = *ev->Get();

    PQ_LOG_TX_I("Handle TEvPQ::TEvTxDone" <<
             " Step " << event.Step <<
             ", TxId " << event.TxId <<
             ", Partition " << event.Partition);

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    PQ_ENSURE(tx->State == NKikimrPQ::TTransaction::EXECUTING)("State", NKikimrPQ::TTransaction_EState_Name(tx->State));

    tx->OnTxDone(event);

    TryExecuteTxs(ctx, *tx);

    TryWriteTxs(ctx);
}

bool TPersQueue::CanProcessProposeTransactionQueue() const
{
    return
        !EvProposeTransactionQueue.empty()
        && ProcessingParams
        && (!UseMediatorTimeCast || MediatorTimeCastEntry);
}

bool TPersQueue::CanProcessWriteTxs() const
{
    return !WriteTxs.empty();
}

bool TPersQueue::CanProcessTxWrites() const
{
    return !NewSupportivePartitions.empty();
}

void TPersQueue::SubscribeWriteId(const TWriteId& writeId,
                                  const TActorContext& ctx)
{
    PQ_LOG_TX_D("send TEvSubscribeLock for WriteId " << writeId);
    ctx.Send(NLongTxService::MakeLongTxServiceID(writeId.NodeId),
             new NLongTxService::TEvLongTxService::TEvSubscribeLock(writeId.KeyId, writeId.NodeId));
}

void TPersQueue::UnsubscribeWriteId(const TWriteId& writeId,
                                    const TActorContext& ctx)
{
    PQ_LOG_TX_D("send TEvUnsubscribeLock for WriteId " << writeId);
    ctx.Send(NLongTxService::MakeLongTxServiceID(writeId.NodeId),
             new NLongTxService::TEvLongTxService::TEvUnsubscribeLock(writeId.KeyId, writeId.NodeId));
}

void TPersQueue::CreateSupportivePartitionActors(const TActorContext& ctx)
{
    for (const auto& partitionId : PendingSupportivePartitions) {
        CreateSupportivePartitionActor(partitionId, ctx);
    }

    PendingSupportivePartitions.clear();
}

void TPersQueue::BeginWriteTxs(const TActorContext& ctx)
{
    PQ_ENSURE(!WriteTxsInProgress);

    bool canProcess =
        CanProcessProposeTransactionQueue() ||
        CanProcessWriteTxs() ||
        CanProcessTxWrites() ||
        TxWritesChanged ||
        DeleteTxsContainsKafkaTxs
        ;
    if (!canProcess) {
        return;
    }

    auto request = MakeHolder<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(WRITE_TX_COOKIE);

    ProcessProposeTransactionQueue(ctx, request->Record);
    ProcessWriteTxs(ctx, request->Record);
    AddCmdWriteTabletTxInfo(request->Record);

    MovePendingDeferredReadSetAcks();

    WriteTxsInProgress = true;

    PendingSupportivePartitions = std::move(NewSupportivePartitions);
    NewSupportivePartitions.clear();

    PQ_LOG_D("Send TEvKeyValue::TEvRequest (WRITE_TX_COOKIE)");
    ctx.Send(ctx.SelfID, request.Release(),
             0, 0,
             WriteTxsSpan.GetTraceId());

    TryReturnTabletStateAll(ctx);
}

void TPersQueue::EndWriteTxs(const NKikimrClient::TResponse& resp,
                             const TActorContext& ctx)
{
    PQ_ENSURE(WriteTxsInProgress);

    bool ok = (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK);
    if (ok) {
        for (auto& result : resp.GetWriteResult()) {
            if (result.GetStatus() != NKikimrProto::OK) {
                ok = false;
                break;
            }
        }
    }

    if (!ok) {
        PQ_LOG_ERROR("SelfId " << ctx.SelfID << " TxInfo write error: " << resp.DebugString());

        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    TxWritesChanged = false;

    SendReplies(ctx);
    CheckChangedTxStates(ctx);
    CreateSupportivePartitionActors(ctx);

    WriteTxsInProgress = false;

    TryWriteTxs(ctx);
}

void TPersQueue::TryWriteTxs(const TActorContext& ctx)
{
    if (!WriteTxsInProgress) {
        BeginWriteTxs(ctx);
    }
}

void TPersQueue::ProcessProposeTransactionQueue(const TActorContext& ctx,
                                                NKikimrClient::TKeyValueRequest& request)
{
    PQ_ENSURE(!WriteTxsInProgress);

    if (CanProcessProposeTransactionQueue() || DeleteTxsContainsKafkaTxs) {
        ProcessDeleteTxs(ctx, request);
    }

    while (CanProcessProposeTransactionQueue()) {
        const auto front = std::move(EvProposeTransactionQueue.front());
        EvProposeTransactionQueue.pop_front();

        auto span = front->ExecuteSpan.CreateChild(TWilsonTopic::TopicTopLevel,
                                                   "Topic.Transaction.ProcessProposeTransactionQueue",
                                                   NWilson::EFlags::AUTO_END);

        const NKikimrPQ::TEvProposeTransaction& event = front->GetRecord();
        TDistributedTransaction& tx = Txs[event.GetTxId()];
        SetTxInFlyCounter();

        tx.SetExecuteSpan(std::move(front->ExecuteSpan));

        switch (tx.State) {
        case NKikimrPQ::TTransaction::UNKNOWN:
            tx.OnProposeTransaction(event, GetAllowedStep(),
                                    TabletID());
            PQ_LOG_TX_I("Propose TxId " << tx.TxId << ", WriteId " << tx.WriteId);

            if (tx.Kind == NKikimrPQ::TTransaction::KIND_CONFIG) {
                UpdateConsumers(tx.TabletConfig);
            }

            if (tx.WriteId.Defined()) {
                const TWriteId& writeId = *tx.WriteId;
                PQ_ENSURE(TxWrites.contains(writeId))("TxId", tx.TxId)("WriteId", writeId.ToString());
                TTxWriteInfo& writeInfo = TxWrites.at(writeId);
                PQ_LOG_TX_D("Link TxId " << tx.TxId << " with WriteId " << writeId);
                writeInfo.TxId = tx.TxId;
            }

            TryExecuteTxs(ctx, tx);
            break;
        case NKikimrPQ::TTransaction::PREPARING:
        case NKikimrPQ::TTransaction::PREPARED:
            //
            // the sender re-sent the TEvProposeTransaction. the actor ID could have changed. you need to
            // update the ID in the transaction and schedule a response to the new actor
            //
            tx.SourceActor = ActorIdFromProto(event.GetSourceActor());
            ScheduleProposeTransactionResult(tx);
            break;
        default:
            PQ_LOG_ERROR_AND_DIE("Unknown tx state: " << static_cast<int>(tx.State));
            return;
        }
    }
}

void TPersQueue::ProcessPlanStep(const TActorId& sender, std::unique_ptr<TEvTxProcessing::TEvPlanStep>&& ev,
                                 const TActorContext& ctx)
{
    const NKikimrTx::TEvMediatorPlanStep& event = ev->Record;
    const ui64 step = event.GetStep();
    TMaybe<ui64> lastPlannedTxId;

    for (const auto& tx : event.GetTransactions()) {
        PQ_ENSURE(tx.HasTxId());
        const ui64 txId = tx.GetTxId();
        PQ_ENSURE(!lastPlannedTxId.Defined() || (*lastPlannedTxId < txId));

        if (auto p = Txs.find(txId); p != Txs.end()) {
            TDistributedTransaction& tx = p->second;

            PQ_ENSURE(tx.MaxStep >= step);

            if (tx.Step == Max<ui64>()) {
                auto span = tx.CreatePlanStepSpan(TabletID(), step);
                tx.BeginWaitRSSpan(TabletID());

                PQ_ENSURE(TxQueue.empty() || (TxQueue.back() < std::make_pair(step, txId)));

                TxQueue.emplace_back(step, txId);
                SetTxCompleteLagCounter();

                tx.OnPlanStep(step);
                TryExecuteTxs(ctx, tx);
            } else {
                PQ_LOG_TX_W("Transaction already planned for step " << tx.Step <<
                            ", Step: " << step <<
                            ", TxId: " << txId);
            }

            lastPlannedTxId = txId;
        } else {
            PQ_LOG_TX_W("Unknown transaction  TxId " << txId << ". Step " << step);
        }
    }

    if ((step > PlanStep) && lastPlannedTxId.Defined()) {
        // если это план из будущего, то надо запомнить, последнюю запланированную транзакцию
        PlanStep = step;
        PlanTxId = *lastPlannedTxId;
    }

    if (lastPlannedTxId.Defined()) {
        // эту транзакцию ещё не удалили
        auto p = Txs.find(*lastPlannedTxId);
        TDistributedTransaction& tx = p->second;

        // таблетка координатора могла перезапуститься надо обновить информацию
        tx.SendPlanStepAcksAfterCompletion(sender, std::move(ev));

        if (tx.State >= NKikimrPQ::TTransaction::EXECUTED) {
            // таблетка PQ могла отправить подтвержение, но координатор перезапустился и его не получил
            SendPlanStepAcks(ctx, tx);
        }
    } else {
        // таблетка PQ успела выполнить и удалить все транзакции этого шага. надо отправить подтверждение
        SendPlanStepAcks(ctx, sender, *ev);
    }

    PQ_LOG_TX_D("PlanStep " << PlanStep << ", PlanTxId " << PlanTxId);
}

void TPersQueue::SendPlanStepAcks(const TActorContext& ctx,
                                  const TDistributedTransaction& tx)
{
    if (!tx.PlanStepSender) {
        return;
    }

    SendPlanStepAcks(ctx, tx.PlanStepSender, *tx.PlanStepEvent);
}

void TPersQueue::SendPlanStepAcks(const TActorContext& ctx,
                                  const TActorId& receiver,
                                  const TEvTxProcessing::TEvPlanStep& ev)
{
    THashMap<TActorId, TVector<ui64>> txAcks;

    for (auto& m : ev.Record.GetTransactions()) {
        PQ_ENSURE(m.HasTxId());

        if (m.HasAckTo()) {
            TActorId txOwner = ActorIdFromProto(m.GetAckTo());
            if (txOwner) {
                txAcks[txOwner].push_back(m.GetTxId());
            }
        }
    }

    const ui64 step = ev.Record.GetStep();

    SendPlanStepAck(ctx, step, txAcks);
    SendPlanStepAccepted(ctx, receiver, step);
}

void TPersQueue::SendPlanStepAck(const TActorContext& ctx,
                                 ui64 step,
                                 const THashMap<TActorId, TVector<ui64>>& txAcks)
{
    for (auto& [actorId, txIds] : txAcks) {
        auto event = std::make_unique<TEvTxProcessing::TEvPlanStepAck>(TabletID(),
                                                                       step,
                                                                       txIds.begin(), txIds.end());
        ctx.Send(actorId, event.release());
    }
}

void TPersQueue::SendPlanStepAccepted(const TActorContext& ctx,
                                      const TActorId& actorId,
                                      ui64 step)
{
    auto event = std::make_unique<TEvTxProcessing::TEvPlanStepAccepted>(TabletID(), step);

    ctx.Send(actorId, event.release());
}

void TPersQueue::ProcessWriteTxs(const TActorContext& ctx,
                                 NKikimrClient::TKeyValueRequest& request)
{
    PQ_ENSURE(!WriteTxsInProgress);

    if (!WriteTxs.empty() && HasTxPersistSpan && !WriteTxsSpan) {
        WriteTxsSpan = GenerateSpan("Topic.Transaction.WriteTxs", WriteTxsSpanVerbosity);
    }

    for (auto& [txId, state] : WriteTxs) {
        // There may be cases when in one iteration of a record we change the state of a transaction and delete it
        auto tx = GetTransaction(ctx, txId);
        if (tx) {
            // таблетка PQ сохраняет только после TEvProposeTransaction
            PQ_ENSURE(state == NKikimrPQ::TTransaction::PREPARED)("TxId", txId)("State", NKikimrPQ::TTransaction_EState_Name(state));

            PQ_LOG_TX_D("Persist state " << NKikimrPQ::TTransaction_EState_Name(state) << " for TxId " << txId);
            tx->AddCmdWrite(request, state);

            ChangedTxs.emplace(tx->Step, txId);

            tx->BeginPersistSpan(TabletID(), state, WriteTxsSpan.GetTraceId());
        }
    }

    WriteTxs.clear();
}

void TPersQueue::ProcessDeleteTxs(const TActorContext& ctx,
                                  NKikimrClient::TKeyValueRequest& request)
{
    PQ_ENSURE(!WriteTxsInProgress);

    if (!DeleteTxs.empty() && HasTxDeleteSpan && !WriteTxsSpan) {
        WriteTxsSpan = GenerateSpan("Topic.Transaction.WriteTxs", WriteTxsSpanVerbosity);
    }

    for (ui64 txId : DeleteTxs) {
        PQ_LOG_TX_D("Delete key for TxId " << txId);
        AddCmdDeleteTx(request, txId);

        auto tx = GetTransaction(ctx, txId);
        if (tx) {
            tx->BeginDeleteSpan(TabletID(), WriteTxsSpan.GetTraceId());

            ChangedTxs.emplace(tx->Step, txId);
        }
    }

    DeleteTxs.clear();
    DeleteTxsContainsKafkaTxs = false;
}

void TPersQueue::AddCmdDeleteTx(NKikimrClient::TKeyValueRequest& request,
                                ui64 txId)
{
    auto range = request.AddCmdDeleteRange()->MutableRange();
    range->SetFrom(GetTxKey(txId));
    range->SetIncludeFrom(true);
    range->SetTo(GetTxKey(txId + 1));
    range->SetIncludeTo(false);
}

void TPersQueue::ProcessConfigTx(const TActorContext& ctx,
                                 TEvKeyValue::TEvRequest* request)
{
    PQ_ENSURE(!WriteTxsInProgress);

    if (!TabletConfigTx.Defined()) {
        return;
    }

    AddCmdWriteConfig(request,
                      *TabletConfigTx,
                      *BootstrapConfigTx,
                      *PartitionsDataConfigTx,
                      ctx);

    TabletConfigTx = Nothing();
    BootstrapConfigTx = Nothing();
    PartitionsDataConfigTx = Nothing();
}

void TPersQueue::AddCmdWriteTabletTxInfo(NKikimrClient::TKeyValueRequest& request)
{
    NKikimrPQ::TTabletTxInfo info;
    SavePlanStep(info);
    SaveTxWrites(info);

    TString value;
    PQ_ENSURE(info.SerializeToString(&value));

    auto command = request.AddCmdWrite();
    command->SetKey(KeyTxInfo());
    command->SetValue(value);
    command->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

void TPersQueue::SavePlanStep(NKikimrPQ::TTabletTxInfo& info)
{
    info.SetPlanStep(PlanStep);
    info.SetPlanTxId(PlanTxId);

    info.SetExecStep(ExecStep);
    info.SetExecTxId(ExecTxId);
}

void TPersQueue::SaveTxWrites(NKikimrPQ::TTabletTxInfo& info)
{
    auto setKafkaTxnTimeout = [](const TTxWriteInfo& txWriteInfo, NKikimrPQ::TTabletTxInfo::TTxWriteInfo& infoToPersist) {
        if (txWriteInfo.KafkaTransaction) {
            infoToPersist.SetKafkaTransaction(true);
            infoToPersist.SetCreatedAt(txWriteInfo.CreatedAt.MilliSeconds());
        }
    };

    for (auto& [writeId, write] : TxWrites) {
        if (write.Partitions.empty()) {
            auto* txWrite = info.MutableTxWrites()->Add();
            SetWriteId(*txWrite, writeId);
            setKafkaTxnTimeout(write, *txWrite);
        } else {
            for (auto [partitionId, shadowPartitionId] : write.Partitions) {
                auto* txWrite = info.MutableTxWrites()->Add();
                SetWriteId(*txWrite, writeId);
                setKafkaTxnTimeout(write, *txWrite);
                txWrite->SetOriginalPartitionId(partitionId);
                txWrite->SetInternalPartitionId(shadowPartitionId.InternalPartitionId);
            }
        }
    }

    info.SetNextSupportivePartitionId(NextSupportivePartitionId);
}

void TPersQueue::ScheduleProposeTransactionResult(const TDistributedTransaction& tx)
{
    PQ_LOG_TX_D("schedule TEvProposeTransactionResult(PREPARED)");
    auto event = std::make_unique<TEvPersQueue::TEvProposeTransactionResult>();

    event->Record.SetOrigin(TabletID());
    event->Record.SetStatus(NKikimrPQ::TEvProposeTransactionResult::PREPARED);
    event->Record.SetTxId(tx.TxId);
    event->Record.SetMinStep(tx.MinStep);
    event->Record.SetMaxStep(tx.MaxStep);

    if (ProcessingParams) {
        event->Record.MutableDomainCoordinators()->CopyFrom(ProcessingParams->GetCoordinators());
    }

    RepliesToActor.emplace_back(tx.SourceActor, std::move(event));
}

void TPersQueue::SendEvReadSetToReceivers(const TActorContext& ctx,
                                          TDistributedTransaction& tx)
{
    NKikimrTx::TReadSetData data;
    data.SetDecision(tx.SelfDecision);
    if (tx.PartitionsData.PartitionSize()) {
        data.MutableData()->PackFrom(tx.PartitionsData);
    }

    TString body;
    PQ_ENSURE(data.SerializeToString(&body));

    PQ_LOG_TX_I("Send TEvTxProcessing::TEvReadSet to " << tx.PredicateRecipients.size() << " receivers. Wait TEvTxProcessing::TEvReadSet from " << tx.PredicatesReceived.size() << " senders.");
    for (auto& [receiverId, _] : tx.PredicateRecipients) {
        if (receiverId != TabletID()) {
            auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(tx.Step,
                                                                       tx.TxId,
                                                                       TabletID(),
                                                                       receiverId,
                                                                       TabletID(),
                                                                       body,
                                                                       0);
            PQ_LOG_TX_I("Send TEvReadSet to tablet " << receiverId << " tx " << tx.TxId);
            SendToPipe(receiverId, tx, std::move(event), ctx);
        }
    }
}

void TPersQueue::SendEvReadSetAckToSenders(const TActorContext& ctx,
                                           TDistributedTransaction& tx)
{
    PQ_LOG_TX_D("TPersQueue::SendEvReadSetAckToSenders");
    for (auto& [target, event] : tx.ReadSetAcks) {
        PQ_LOG_TX_I("Send TEvTxProcessing::TEvReadSetAck " << event->ToString());
        ctx.Send(target, event.release());
    }
}

TMaybe<TPartitionId> TPersQueue::FindPartitionId(const NKikimrPQ::TDataTransaction& txBody) const
{
    auto hasWriteOperation = [](const auto& txBody) {
        for (const auto& o : txBody.GetOperations()) {
            if (IsWriteTxOperation(o)) {
                return true;
            }
        }
        return false;
    };

    ui32 partitionId = txBody.GetOperations(0).GetPartitionId();

    if (txBody.HasWriteId() && hasWriteOperation(txBody)) {
        const TWriteId writeId = GetWriteId(txBody);
        if (!TxWrites.contains(writeId)) {
            PQ_LOG_TX_W("unknown WriteId " << writeId);
            return Nothing();
        }

        const TTxWriteInfo& writeInfo = TxWrites.at(writeId);
        if (!writeInfo.Partitions.contains(partitionId)) {
            PQ_LOG_TX_W("unknown partition " << partitionId << " for WriteId " << writeId);
            return Nothing();
        }

        return writeInfo.Partitions.at(partitionId);
    } else {
        return TPartitionId(partitionId);
    }
}

void TPersQueue::SendEvTxCalcPredicateToPartitions(const TActorContext& ctx,
                                                   TDistributedTransaction& tx)
{
    auto OriginalPartitionExists = [this](ui32 partitionId) {
        return Partitions.contains(TPartitionId(partitionId));
    };

    // if the predicate is violated, the transaction will end with the ABORTED code
    bool forcePredicateFalse = false;
    THashMap<ui32, std::unique_ptr<TEvPQ::TEvTxCalcPredicate>> events;

    for (auto& operation : tx.Operations) {
        ui32 originalPartitionId = operation.GetPartitionId();

        if (!OriginalPartitionExists(originalPartitionId)) {
            forcePredicateFalse = true;
            continue;
        }

        auto& event = events[originalPartitionId];
        if (!event) {
            event = std::make_unique<TEvPQ::TEvTxCalcPredicate>(tx.Step, tx.TxId);
        }

        if (operation.HasCommitOffsetsBegin()) {
            event->AddOperation(operation.GetConsumer(),
                                operation.GetCommitOffsetsBegin(),
                                operation.GetCommitOffsetsEnd(),
                                operation.HasForceCommit() ? operation.GetForceCommit() : false,
                                operation.HasKillReadSession() ? operation.GetKillReadSession() : false,
                                operation.HasOnlyCheckCommitedToFinish() ? operation.GetOnlyCheckCommitedToFinish() : false,
                                operation.HasReadSessionId() ? operation.GetReadSessionId() : "");
        }
        if (operation.GetKafkaTransaction() && operation.HasCommitOffsetsEnd()) {
            event->AddKafkaOffsetCommitOperation(operation.GetConsumer(), operation.GetCommitOffsetsEnd());
        }
    }

    if (tx.WriteId.Defined()) {
        const TWriteId& writeId = *tx.WriteId;
        if (TxWrites.contains(writeId)) {
            const TTxWriteInfo& writeInfo = TxWrites.at(writeId);

            for (auto& [originalPartitionId, partitionId] : writeInfo.Partitions) {
                if (!OriginalPartitionExists(originalPartitionId)) {
                    PQ_LOG_TX_W("Unknown partition " << originalPartitionId << " for TxId " << tx.TxId);
                    forcePredicateFalse = true;
                    continue;
                }

                auto& event = events[originalPartitionId];
                if (!event) {
                    event = std::make_unique<TEvPQ::TEvTxCalcPredicate>(tx.Step, tx.TxId);
                }

                if (!Partitions.contains(partitionId)) {
                    PQ_LOG_TX_W("Unknown partition " << partitionId << " for TxId " << tx.TxId);
                    forcePredicateFalse = true;
                    continue;
                }

                const TPartitionInfo& partition = Partitions.at(partitionId);

                event->SupportivePartitionActor = partition.Actor;
            }
        } else {
            PQ_LOG_TX_W("Unknown WriteId " << writeId << " for TxId " << tx.TxId);
            forcePredicateFalse = true;
        }
    }

    for (auto& [originalPartitionId, event] : events) {
        TPartitionId partitionId(originalPartitionId);
        const TPartitionInfo& partition = Partitions.at(partitionId);

        event->ForcePredicateFalse = forcePredicateFalse;

        ctx.Send(partition.Actor, event.release(),
                 0, 0,
                 tx.GetExecuteSpanTraceId());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = events.size();
}

void TPersQueue::SendEvTxCommitToPartitions(const TActorContext& ctx,
                                            TDistributedTransaction& tx)
{
    PQ_LOG_T("Commit TxId " << tx.TxId);

    const auto serializedTx = tx.Serialize(NKikimrPQ::TTransaction::EXECUTED);
    TMaybe<NKikimrPQ::TPQTabletConfig> tabletConfig;

    if (tx.Kind == NKikimrPQ::TTransaction::KIND_CONFIG) {
        tabletConfig = tx.TabletConfig;
    }

    auto graph = MakePartitionGraph(tx.TabletConfig);
    for (const ui32 partitionId : tx.Partitions) {
        auto explicitMessageGroups = CreateExplicitMessageGroups(tx.BootstrapConfig, tx.PartitionsData, graph, partitionId);
        auto event = std::make_unique<TEvPQ::TEvTxCommit>(tx.Step, tx.TxId, explicitMessageGroups);

        event->SerializedTx = serializedTx;

        if (tabletConfig.Defined()) {
            event->TabletConfig = std::move(*tabletConfig);
            event->BootstrapConfig = tx.BootstrapConfig;
            event->PartitionsData = tx.PartitionsData;

            tabletConfig = Nothing();
        }

        auto p = Partitions.find(TPartitionId(partitionId));
        PQ_ENSURE(p != Partitions.end())("partition_id", partitionId)("TxId", tx.TxId);

        ctx.Send(p->second.Actor, event.release(),
                 0, 0,
                 tx.GetExecuteSpanTraceId());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = tx.Partitions.size();
}

void TPersQueue::SendEvTxRollbackToPartitions(const TActorContext& ctx,
                                              TDistributedTransaction& tx)
{
    PQ_LOG_T("Rollback TxId " << tx.TxId);

    TMaybe<NKikimrPQ::TTransaction> serializedTx = tx.Serialize(NKikimrPQ::TTransaction::EXECUTED);

    for (ui32 partitionId : tx.Partitions) {
        auto event = std::make_unique<TEvPQ::TEvTxRollback>(tx.Step, tx.TxId);

        if (serializedTx.Defined()) {
            event->SerializedTx = std::move(*serializedTx);
            serializedTx = Nothing();
        }

        auto p = Partitions.find(TPartitionId(partitionId));
        PQ_ENSURE(p != Partitions.end())("partition_id", partitionId)("TxId", tx.TxId);

        ctx.Send(p->second.Actor, event.release(),
                 0, 0,
                 tx.GetExecuteSpanTraceId());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = tx.Partitions.size();
}

void TPersQueue::SendEvProposeTransactionResult(const TActorContext& ctx,
                                                TDistributedTransaction& tx)
{
    auto result = std::make_unique<TEvPersQueue::TEvProposeTransactionResult>();
    auto status =
        (tx.GetDecision() == NKikimrTx::TReadSetData::DECISION_COMMIT) ? NKikimrPQ::TEvProposeTransactionResult::COMPLETE : NKikimrPQ::TEvProposeTransactionResult::ABORTED;

    result->Record.SetOrigin(TabletID());
    result->Record.SetStatus(status);
    result->Record.SetTxId(tx.TxId);
    result->Record.SetStep(tx.Step);

    if (tx.Error.Defined() && tx.Error->GetKind() != NKikimrPQ::TError::OK) {
        auto* error = result->Record.MutableErrors()->Add();
        *error = *tx.Error;
    }

    PQ_LOG_TX_D("TxId: " << tx.TxId <<
             " send TEvPersQueue::TEvProposeTransactionResult(" <<
             NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(result->Record.GetStatus()) <<
             ")");
    ctx.Send(tx.SourceActor, std::move(result));
}

void TPersQueue::SendToPipe(ui64 tabletId,
                            TDistributedTransaction& tx,
                            std::unique_ptr<TEvTxProcessing::TEvReadSet> event,
                            const TActorContext& ctx)
{
    PQ_ENSURE(event);

    BindTxToPipe(tabletId, tx.TxId);
    tx.BindMsgToPipe(tabletId, *event);
    PipeClientCache->Send(ctx, tabletId, event.release());
}

void TPersQueue::BindTxToPipe(ui64 tabletId, ui64 txId)
{
    BindedTxs[tabletId].insert(txId);
}

void TPersQueue::UnbindTxFromPipe(ui64 tabletId, ui64 txId)
{
    if (auto p = BindedTxs.find(tabletId); p != BindedTxs.end()) {
        p->second.erase(txId);

        if (p->second.empty()) {
            BindedTxs.erase(p);
        }
    }
}

const THashSet<ui64>& TPersQueue::GetBindedTxs(ui64 tabletId)
{
    if (auto p = BindedTxs.find(tabletId); p != BindedTxs.end()) {
        return p->second;
    }

    static THashSet<ui64> empty;

    return empty;
}

TDistributedTransaction* TPersQueue::GetTransaction(const TActorContext& ctx,
                                                    ui64 txId)
{
    Y_UNUSED(ctx);
    auto p = Txs.find(txId);
    if (p == Txs.end()) {
        PQ_LOG_TX_W("Unknown transaction " << txId);
        return nullptr;
    }
    return &p->second;
}

void TPersQueue::PushTxInQueue(TDistributedTransaction& tx, TDistributedTransaction::EState state)
{
    auto& txQueue = TxsOrder[state];
    txQueue.push_back(tx.TxId);
    tx.Pending = txQueue.size() > 1;
}

void TPersQueue::ChangeTxState(TDistributedTransaction& tx,
                               TDistributedTransaction::EState newState)
{
    PQ_LOG_TX_I("TxId " << tx.TxId << " moved from " <<
             NKikimrPQ::TTransaction_EState_Name(tx.State) <<
             " to " <<
             NKikimrPQ::TTransaction_EState_Name(newState));

    tx.State = newState;
}

bool TPersQueue::TryChangeTxState(TDistributedTransaction& tx,
                                  TDistributedTransaction::EState newState)
{
    auto oldState = tx.State;
    PQ_ENSURE(TxsOrder.contains(oldState) || (oldState == NKikimrPQ::TTransaction::PLANNING))
        ("TxId", tx.TxId)("State", NKikimrPQ::TTransaction_EState_Name(oldState));

    if (oldState != NKikimrPQ::TTransaction::PLANNING) {
        PQ_ENSURE(TxsOrder.contains(oldState))
            ("TxId", tx.TxId)("State", NKikimrPQ::TTransaction_EState_Name(oldState));
        PQ_ENSURE(TxsOrder[oldState].front() == tx.TxId)
            ("TxId", tx.TxId)("State", NKikimrPQ::TTransaction_EState_Name(oldState))("FrontTxId", TxsOrder[oldState].front());
    }

    ChangeTxState(tx, newState);

    if (oldState != NKikimrPQ::TTransaction::PLANNING) {
        TxsOrder[oldState].pop_front();
    }
    if (TxsOrder.contains(newState)) {
        PushTxInQueue(tx, newState);
    }

    return true;
}

bool TPersQueue::CanExecute(const TDistributedTransaction& tx)
{
    if (tx.Pending) {
        return false;
    }

    if (!TxsOrder.contains(tx.State)) {
        return true;
    }

    auto& txQueue = TxsOrder[tx.State];
    PQ_ENSURE(!txQueue.empty())("TxId", tx.TxId)("State", NKikimrPQ::TTransaction_EState_Name(tx.State));

    PQ_LOG_TX_D("TxId " << tx.TxId <<
             " State " << NKikimrPQ::TTransaction_EState_Name(tx.State) <<
             " FrontTxId " << txQueue.front());

    return txQueue.front() == tx.TxId;
}

void TPersQueue::TryExecuteTxs(const TActorContext& ctx,
                               TDistributedTransaction& tx)
{
    PQ_LOG_TX_D("Try execute txs with state " << NKikimrPQ::TTransaction_EState_Name(tx.State));

    TDistributedTransaction::EState oldState = tx.State;

    CheckTxState(ctx, tx);

    if (!TxsOrder.contains(oldState)) {
        // This transaction is either not scheduled or has already been completed.
        return;
    }

    if (oldState == tx.State) {
        // The transaction status has not changed. There is no point in watching the transactions behind her.
        PQ_LOG_TX_D("TxId " << tx.TxId << " status has not changed");
        return;
    }

    auto& txQueue = TxsOrder[oldState];
    while (!txQueue.empty()) {
        PQ_LOG_TX_D("There are " << txQueue.size() << " txs in the queue " << NKikimrPQ::TTransaction_EState_Name(oldState));
        ui64 txId = txQueue.front();
        PQ_ENSURE(Txs.contains(txId))("unknown TxId", txId);
        auto& tx = Txs.at(txId);
        PQ_LOG_TX_D("Try execute TxId " << tx.TxId << " Pending " << tx.Pending);

        if (!tx.Pending) {
            // The transaction was not postponed for execution.
            break;
        }
        tx.Pending = false;

        CheckTxState(ctx, tx);

        if (oldState == tx.State) {
            // The transaction status has not changed. There is no point in watching the transactions behind her.
            PQ_LOG_TX_D("TxId " << tx.TxId << " status has not changed");
            break;
        }
    }
}

void TPersQueue::CheckTxState(const TActorContext& ctx,
                              TDistributedTransaction& tx)
{
    PQ_LOG_TX_D("TxId " << tx.TxId <<
             ", State " << NKikimrPQ::TTransaction_EState_Name(tx.State));

    if (!CanExecute(tx)) {
        PQ_LOG_TX_D("Can't execute TxId " << tx.TxId << " Pending " << tx.Pending);
        tx.Pending = true;
        PQ_LOG_TX_D("Wait for TxId " << tx.TxId);
        return;
    }
    if (tx.WriteInProgress) {
        if (tx.State == NKikimrPQ::TTransaction::EXECUTED) {
            PQ_LOG_TX_I("You cannot send TEvReadSetAck for TxId: " << tx.TxId << " until the EXECUTED state is saved");
        }
        return;
    }

    switch (tx.State) {
    case NKikimrPQ::TTransaction::UNKNOWN:
        PQ_ENSURE(tx.TxId != Max<ui64>())("TxId", tx.TxId);

        WriteTx(tx, NKikimrPQ::TTransaction::PREPARED);
        ScheduleProposeTransactionResult(tx);

        ChangeTxState(tx, NKikimrPQ::TTransaction::PREPARING);

        break;

    case NKikimrPQ::TTransaction::PREPARING:
        PQ_ENSURE(!tx.WriteInProgress)("TxId", tx.TxId);

        // scheduled events will be sent to EndWriteTxs

        ChangeTxState(tx, NKikimrPQ::TTransaction::PREPARED);

        break;

    case NKikimrPQ::TTransaction::PREPARED:
        PQ_ENSURE(tx.Step != Max<ui64>())("TxId", tx.TxId);

        ChangeTxState(tx, NKikimrPQ::TTransaction::PLANNING);

        [[fallthrough]];

    case NKikimrPQ::TTransaction::PLANNING:
        PQ_ENSURE(!tx.WriteInProgress)("TxId", tx.TxId);

        // scheduled events will be sent to EndWriteTxs

        TryChangeTxState(tx, NKikimrPQ::TTransaction::PLANNED);

        if (tx.TxId != TxsOrder[tx.State].front()) {
            break;
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::PLANNED:
        PQ_ENSURE(tx.TxId == TxsOrder[tx.State].front())("TxId", tx.TxId)("FrontTxId", TxsOrder[tx.State].front());
        PQ_LOG_TX_D("TxQueue.size " << TxQueue.size());

        MoveTopTxToCalculating(tx, ctx);

        break;

    case NKikimrPQ::TTransaction::CALCULATING:
        PQ_ENSURE(tx.TxId == TxsOrder[tx.State].front())("TxId", tx.TxId)("FrontTxId", TxsOrder[tx.State].front());
        PQ_ENSURE(tx.PartitionRepliesCount <= tx.PartitionRepliesExpected)("TxId", tx.TxId)
            ("PartitionRepliesCount", tx.PartitionRepliesCount)("PartitionRepliesExpected", tx.PartitionRepliesExpected);

        PQ_LOG_TX_D("Responses received from the partitions " << tx.PartitionRepliesCount << "/" << tx.PartitionRepliesExpected);

        if (tx.PartitionRepliesCount != tx.PartitionRepliesExpected) {
            break;
        }

        switch (tx.Kind) {
        case NKikimrPQ::TTransaction::KIND_DATA:
        case NKikimrPQ::TTransaction::KIND_CONFIG:
            TryChangeTxState(tx, NKikimrPQ::TTransaction::CALCULATED);

            break;

        case NKikimrPQ::TTransaction::KIND_UNKNOWN:
            PQ_ENSURE(false);
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::CALCULATED:
        PQ_ENSURE(tx.TxId == TxsOrder[tx.State].front())("TxId", tx.TxId)("FrontTxId", TxsOrder[tx.State].front());

        TryChangeTxState(tx, NKikimrPQ::TTransaction::WAIT_RS);

        tx.BeginWaitRSAcksSpan(TabletID());

        SendEvReadSetToReceivers(ctx, tx);

        if (tx.TxId != TxsOrder[tx.State].front()) {
            break;
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::WAIT_RS:
        PQ_ENSURE(tx.TxId == TxsOrder[tx.State].front())("TxId", tx.TxId)("FrontTxId", TxsOrder[tx.State].front());

        PQ_LOG_TX_D("HaveParticipantsDecision " << tx.HaveParticipantsDecision());

        if (tx.HaveParticipantsDecision()) {
            tx.EndWaitRSSpan();

            if (tx.GetDecision() == NKikimrTx::TReadSetData::DECISION_COMMIT) {
                SendEvTxCommitToPartitions(ctx, tx);
            } else {
                SendEvTxRollbackToPartitions(ctx, tx);
            }

            TryChangeTxState(tx, NKikimrPQ::TTransaction::EXECUTING);
        } else {
            break;
        }

        if (tx.TxId != TxsOrder[tx.State].front()) {
            break;
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::EXECUTING:
        PQ_ENSURE(tx.TxId == TxsOrder[tx.State].front())("TxId", tx.TxId)("FrontTxId", TxsOrder[tx.State].front());
        PQ_ENSURE(tx.PartitionRepliesCount <= tx.PartitionRepliesExpected)("TxId", tx.TxId)
            ("PartitionRepliesCount", tx.PartitionRepliesCount)("PartitionRepliesExpected", tx.PartitionRepliesExpected);

        PQ_LOG_TX_D("Responses received from the partitions " << tx.PartitionRepliesCount << "/" << tx.PartitionRepliesExpected);

        if (tx.PartitionRepliesCount != tx.PartitionRepliesExpected) {
            break;
        }

        tx.EndExecuteSpan();

        SendEvProposeTransactionResult(ctx, tx);
        PQ_LOG_TX_I("Complete TxId " << tx.TxId);

        switch (tx.Kind) {
        case NKikimrPQ::TTransaction::KIND_DATA:
            break;
        case NKikimrPQ::TTransaction::KIND_CONFIG:
            ApplyNewConfig(tx.TabletConfig, ctx);
            TabletConfigTx = tx.TabletConfig;
            BootstrapConfigTx = tx.BootstrapConfig;
            PartitionsDataConfigTx = tx.PartitionsData;

            break;
        case NKikimrPQ::TTransaction::KIND_UNKNOWN:
            PQ_ENSURE(false);
        }

        TryChangeTxState(tx, NKikimrPQ::TTransaction::EXECUTED);

        [[fallthrough]];

    case NKikimrPQ::TTransaction::EXECUTED:
        PQ_ENSURE(tx.TxId == TxsOrder[tx.State].front())("TxId", tx.TxId)("FrontTxId", TxsOrder[tx.State].front());
        PQ_ENSURE(tx.TxId == TxQueue.front().second)("TxId", tx.TxId)("FrontTxId", TxQueue.front().second);
        PQ_ENSURE(!tx.WriteInProgress)("TxId", tx.TxId);

        TxQueue.pop_front();
        SetTxCompleteLagCounter();

        SendPlanStepAcks(ctx, tx);
        SendEvReadSetAckToSenders(ctx, tx);
        TryReturnTabletStateAll(ctx);

        PQ_LOG_TX_I("delete partitions for TxId " << tx.TxId);
        BeginDeletePartitions(tx);

        TryChangeTxState(tx, NKikimrPQ::TTransaction::WAIT_RS_ACKS);

        [[fallthrough]];

    case NKikimrPQ::TTransaction::WAIT_RS_ACKS:
        PQ_LOG_TX_D("HaveAllRecipientsReceive " << tx.HaveAllRecipientsReceive() <<
                 ", AllSupportivePartitionsHaveBeenDeleted " << AllSupportivePartitionsHaveBeenDeleted(tx.WriteId));
        if (tx.HaveAllRecipientsReceive() && AllSupportivePartitionsHaveBeenDeleted(tx.WriteId)) {
            tx.EndWaitRSAcksSpan();

            DeleteTx(tx);
            // implicitly switch to the state DELETING
        }

        break;

    case NKikimrPQ::TTransaction::EXPIRED:
    case NKikimrPQ::TTransaction::CANCELED:
        PQ_LOG_TX_D("AllSupportivePartitionsHaveBeenDeleted " << AllSupportivePartitionsHaveBeenDeleted(tx.WriteId));
        if (AllSupportivePartitionsHaveBeenDeleted(tx.WriteId)) {
            DeleteTx(tx);
            // implicitly switch to the state DELETING
        }

        break;

    case NKikimrPQ::TTransaction::DELETING:
        // The PQ tablet has persisted its state. Now she can delete the transaction and take the next one.
        TMaybe<TWriteId> writeId = tx.WriteId; // copy writeId to save for kafka transaction after erase
        DeleteWriteId(writeId);
        PQ_LOG_TX_I("delete TxId " << tx.TxId);
        Txs.erase(tx.TxId);
        SetTxInFlyCounter();

        // If this was the last transaction, then you need to send responses to messages about changes
        // in the status of the PQ tablet (if they came)
        TryReturnTabletStateAll(ctx);

        TryContinueKafkaWrites(writeId, ctx);
        break;
    }
}

bool TPersQueue::AllSupportivePartitionsHaveBeenDeleted(const TMaybe<TWriteId>& writeId) const
{
    if (!writeId.Defined()) {
        return true;
    }

    PQ_ENSURE(TxWrites.contains(*writeId))("WriteId", writeId->ToString());
    const TTxWriteInfo& writeInfo = TxWrites.at(*writeId);

    PQ_LOG_TX_D("WriteId " << *writeId <<
             " Partitions.size=" << writeInfo.Partitions.size());
    bool deleted =
        writeInfo.Partitions.empty()
        ;

    return deleted;
}

void TPersQueue::DeleteWriteId(const TMaybe<TWriteId>& writeId)
{
    if (!writeId.Defined() || !TxWrites.contains(*writeId)) {
        return;
    }

    PQ_LOG_TX_I("delete WriteId " << *writeId);
    TxWrites.erase(*writeId);
}

void TPersQueue::WriteTx(TDistributedTransaction& tx, NKikimrPQ::TTransaction::EState state)
{
    WriteTxs[tx.TxId] = state;

    if (auto traceId = tx.GetExecuteSpanTraceId(); traceId) {
        HasTxPersistSpan = true;
        WriteTxsSpanVerbosity = traceId.GetVerbosity();
    }

    tx.WriteInProgress = true;
}

void TPersQueue::DeleteTx(TDistributedTransaction& tx)
{
    PQ_LOG_TX_D("add an TxId " << tx.TxId << " to the list for deletion");

    DeleteTxs.insert(tx.TxId);
    DeleteTxsContainsKafkaTxs |= (tx.WriteId.Defined() && tx.WriteId->IsKafkaApiTransaction());

    if (auto traceId = tx.GetExecuteSpanTraceId(); traceId) {
        HasTxDeleteSpan = true;
        WriteTxsSpanVerbosity = traceId.GetVerbosity();
    }

    ChangeTxState(tx, NKikimrPQ::TTransaction::DELETING);

    tx.WriteInProgress = true;
}

void TPersQueue::SendReplies(const TActorContext& ctx)
{
    for (auto& [actorId, event] : RepliesToActor) {
        ctx.Send(actorId, event.release());
    }
    RepliesToActor.clear();
}

void TPersQueue::CheckChangedTxStates(const TActorContext& ctx)
{
    for (const auto& p : ChangedTxs) {
        ui64 txId = p.second;
        auto tx = GetTransaction(ctx, txId);
        PQ_ENSURE(tx)("TxId", txId);

        tx->WriteInProgress = false;

        tx->EndPersistSpan();
        tx->EndDeleteSpan();

        TryExecuteTxs(ctx, *tx);
    }

    HasTxPersistSpan = false;
    HasTxDeleteSpan = false;

    if (WriteTxsSpan) {
        WriteTxsSpan.End();
        WriteTxsSpan = {};
    }

    ChangedTxs.clear();
}

void TPersQueue::InitProcessingParams(const TActorContext& ctx)
{
    if (Info()->TenantPathId) {
        UseMediatorTimeCast = true;
        StartWatchingTenantPathId(ctx);
        return;
    }

    ProcessingParams = ExtractProcessingParams(*AppData(ctx)->DomainsInfo->GetDomain());

    InitMediatorTimeCast(ctx);
}

void TPersQueue::InitMediatorTimeCast(const TActorContext& ctx)
{
    PQ_ENSURE(ProcessingParams);

    if (ProcessingParams->MediatorsSize()) {
        UseMediatorTimeCast = true;
        MediatorTimeCastRegisterTablet(ctx);
    } else {
        UseMediatorTimeCast = false;

        TryWriteTxs(ctx);
    }
}

bool TPersQueue::ReadyForDroppedReply() const 
{
    if (TabletState != NKikimrPQ::EDropped) {
        return false;
    }

    for (const auto& [_, tx] : Txs) {
        switch (tx.Kind) {
        case NKikimrPQ::TTransaction::KIND_DATA:
            if (tx.State < NKikimrPQ::TTransaction::EXECUTED) {
                return false;
            }
            break;
        case NKikimrPQ::TTransaction::KIND_CONFIG:
            if ((tx.State < NKikimrPQ::TTransaction::EXECUTED) &&
                (tx.Step != Max<ui64>())) {
                return false;
            }
            break;
        case NKikimrPQ::TTransaction::KIND_UNKNOWN:
            PQ_ENSURE(false);
        }
    }

    return EvProposeTransactionQueue.empty();
}

void TPersQueue::SendProposeTransactionResult(const TActorId& target,
                                              ui64 txId,
                                              NKikimrPQ::TEvProposeTransactionResult::EStatus status,
                                              NKikimrPQ::TError::EKind kind,
                                              const TString& reason,
                                              const TActorContext& ctx)
{
    auto event = std::make_unique<TEvPersQueue::TEvProposeTransactionResult>();

    event->Record.SetOrigin(TabletID());
    event->Record.SetStatus(status);
    event->Record.SetTxId(txId);

    if (kind != NKikimrPQ::TError::OK) {
        auto* error = event->Record.MutableErrors()->Add();
        error->SetKind(kind);
        error->SetReason(reason);
    }

    PQ_LOG_TX_I("TxId: " << txId <<
             " send TEvPersQueue::TEvProposeTransactionResult(" <<
             NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event->Record.GetStatus()) <<
             ")");
    ctx.Send(target, std::move(event));
}

void TPersQueue::SendProposeTransactionAbort(const TActorId& target,
                                             ui64 txId,
                                             NKikimrPQ::TError::EKind kind,
                                             const TString& reason,
                                             const TActorContext& ctx)
{
    SendProposeTransactionResult(target,
                                 txId,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                                 kind,
                                 reason,
                                 ctx);
}

void TPersQueue::SendProposeTransactionOverloaded(const TActorId& target,
                                                  ui64 txId,
                                                  NKikimrPQ::TError::EKind kind,
                                                  const TString& reason,
                                                  const TActorContext& ctx)
{
    SendProposeTransactionResult(target,
                                 txId,
                                 NKikimrPQ::TEvProposeTransactionResult::OVERLOADED,
                                 kind,
                                 reason,
                                 ctx);
}

void TPersQueue::SendEvProposePartitionConfig(const TActorContext& ctx,
                                              TDistributedTransaction& tx)
{
    auto graph = MakePartitionGraph(tx.TabletConfig);

    for (auto& [partitionId, partition] : Partitions) {
        if (partitionId.IsSupportivePartition()) {
            continue;
        }

        auto explicitMessageGroups = CreateExplicitMessageGroups(tx.BootstrapConfig, tx.PartitionsData, graph, partitionId.OriginalPartitionId);
        auto event = std::make_unique<TEvPQ::TEvProposePartitionConfig>(tx.Step, tx.TxId);

        event->TopicConverter = tx.TopicConverter;
        event->Config = tx.TabletConfig;

        ctx.Send(partition.Actor, std::move(event));
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = OriginalPartitionsCount;
}

TActorId TPersQueue::GetPartitionQuoter(const TPartitionId& partition) {
    if (!AppData()->PQConfig.GetQuotingConfig().GetEnableQuoting())
        return TActorId{};

    auto& quoterId = PartitionWriteQuoters[partition.OriginalPartitionId];
    if (!quoterId) {
        quoterId = RegisterWithSameMailbox(CreateWriteQuoter(
            AppData()->PQConfig,
            TopicConverter,
            Config,
            partition,
            SelfId(),
            TabletID(),
            Counters
        ));
    }
    return quoterId;
}

IActor* TPersQueue::CreatePartitionActor(const TPartitionId& partitionId,
                                             const NPersQueue::TTopicConverterPtr topicConverter,
                                             const NKikimrPQ::TPQTabletConfig& config,
                                             bool newPartition,
                                             const TActorContext& ctx)
{
    int channels = Info()->Channels.size() - NKeyValue::BLOB_CHANNEL; // channels 0,1 are reserved in tablet
    PQ_ENSURE(channels > 0);

    return NPQ::CreatePartitionActor(TabletID(),
                          partitionId,
                          ctx.SelfID,
                          GetGeneration(),
                          CacheActor,
                          topicConverter,
                          DCId,
                          IsServerless,
                          config,
                          Counters,
                          SubDomainOutOfSpace,
                          (ui32)channels,
                          GetPartitionQuoter(partitionId),
                          SamplingControl,
                          newPartition);
}

void TPersQueue::CreateNewPartitions(NKikimrPQ::TPQTabletConfig& config,
                                     NPersQueue::TTopicConverterPtr topicConverter,
                                     const TActorContext& ctx)
{
    EnsurePartitionsAreNotDeleted(config);

    PQ_ENSURE(ConfigInited)("ConfigInited", static_cast<int>(ConfigInited));

    for (const auto& partition : config.GetPartitions()) {
        const TPartitionId partitionId(partition.GetPartitionId());
        if (Partitions.contains(partitionId)) {
            continue;
        }

        CreateOriginalPartition(config,
                                partition,
                                topicConverter,
                                partitionId,
                                true,
                                ctx);
    }
}

void TPersQueue::EnsurePartitionsAreNotDeleted(const NKikimrPQ::TPQTabletConfig& config) const
{
    THashSet<ui32> was;
    for (const auto& partition : config.GetPartitions()) {
        was.insert(partition.GetPartitionId());
    }

    for (const auto& partition : Config.GetPartitions()) {
        Y_VERIFY_S(was.contains(partition.GetPartitionId()), "New config is bad, missing partition " << partition.GetPartitionId());
    }
}

void TPersQueue::BeginInitTransactions()
{
    Txs.clear();
    TxQueue.clear();

    InitTxsOrder();

    PlannedTxs.clear();
}

std::array<NKikimrPQ::TTransaction::EState, 6> GetTxsStatesDirectOrder()
{
    return {
        NKikimrPQ::TTransaction::PLANNED,
        NKikimrPQ::TTransaction::CALCULATING,
        NKikimrPQ::TTransaction::CALCULATED,
        NKikimrPQ::TTransaction::WAIT_RS,
        NKikimrPQ::TTransaction::EXECUTING,
        NKikimrPQ::TTransaction::EXECUTED
    };
}

std::array<NKikimrPQ::TTransaction::EState, 6> GetTxsStatesReverseOrder()
{
    auto states = GetTxsStatesDirectOrder();
    std::reverse(states.begin(), states.end());
    return states;
}

void TPersQueue::InitTxsOrder()
{
    TxsOrder.clear();

    static const auto txStates = GetTxsStatesDirectOrder();

    for (auto state : txStates) {
        TxsOrder[state].clear();
    }
}

void TPersQueue::EndInitTransactions()
{
    PQ_LOG_TX_D("Txs.size=" << Txs.size() << ", PlannedTxs.size=" << PlannedTxs.size());

    std::sort(PlannedTxs.begin(), PlannedTxs.end());
    for (auto& item : PlannedTxs) {
        TxQueue.push_back(item);
    }

    if (!TxQueue.empty()) {
        PQ_LOG_TX_D("top tx queue (" << TxQueue.front().first << ", " << TxQueue.front().second << ")");
    }

    for (const auto& [_, txId] : TxQueue) {
        PQ_ENSURE(Txs.contains(txId))("unknown TxId", txId);
        auto& tx = Txs.at(txId);

        PQ_ENSURE(txId == tx.TxId);

        if (!TxsOrder.contains(tx.State)) {
            PQ_LOG_TX_D("TxsOrder: " <<
                        tx.Step << " " << txId << " " << NKikimrPQ::TTransaction_EState_Name(tx.State) << " skip");
            continue;
        }

        PushTxInQueue(tx, tx.State);

        PQ_LOG_TX_D("TxsOrder: " <<
                    tx.Step << " " << txId << " " << NKikimrPQ::TTransaction_EState_Name(tx.State) << " " << tx.Pending);
    }
}

void TPersQueue::TryStartTransaction(const TActorContext& ctx)
{
    static const auto txStates = GetTxsStatesReverseOrder();

    ResendEvReadSetToReceivers(ctx);
    DeleteSupportivePartitions(ctx);

    for (auto state : txStates) {
        const auto& txQueue = TxsOrder[state];
        if (txQueue.empty()) {
            continue;
        }

        auto next = GetTransaction(ctx, txQueue.front());
        PQ_ENSURE(next);

        TryExecuteTxs(ctx, *next);

        TryWriteTxs(ctx);
    }
}

void TPersQueue::ResendEvReadSetToReceivers(const TActorContext& ctx)
{
    ResendEvReadSetToReceiversForState(ctx, NKikimrPQ::TTransaction::CALCULATED);
    ResendEvReadSetToReceiversForState(ctx, NKikimrPQ::TTransaction::EXECUTED);
}

void TPersQueue::ResendEvReadSetToReceiversForState(const TActorContext& ctx, NKikimrPQ::TTransaction::EState state)
{
    for (ui64 txId : TxsOrder[state]) {
        auto tx = GetTransaction(ctx, txId);
        PQ_ENSURE(tx)("unknown TxId", txId);

        SendEvReadSetToReceivers(ctx, *tx);
    }
}

void TPersQueue::DeleteSupportivePartitions(const TActorContext& ctx)
{
    for (ui64 txId : TxsOrder[NKikimrPQ::TTransaction::EXECUTED]) {
        auto tx = GetTransaction(ctx, txId);
        PQ_ENSURE(tx)("unknown TxId", txId);

        BeginDeletePartitions(*tx);
    }
}

void TPersQueue::OnInitComplete(const TActorContext& ctx)
{
    SignalTabletActive(ctx);
    InitCompleted = true;
    ProcessPendingEvents();
    TryStartTransaction(ctx);
}

ui64 TPersQueue::GetAllowedStep() const
{
    return Max(PlanStep + 1,
               MediatorTimeCastEntry ? MediatorTimeCastEntry->Get(TabletID()) : TAppData::TimeProvider->Now().MilliSeconds());
}

NTabletPipe::TClientConfig TPersQueue::GetPipeClientConfig()
{
    NTabletPipe::TClientConfig config;
    config.CheckAliveness = true;
    config.RetryPolicy = {
        .RetryLimitCount = 30,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(500),
        .BackoffMultiplier = 2,
    };
    return config;
}

void TPersQueue::Handle(TEvPQ::TEvSubDomainStatus::TPtr& ev, const TActorContext& ctx)
{
    ReadBalancerActorId = ev->Sender;

    const TEvPQ::TEvSubDomainStatus& event = *ev->Get();
    SubDomainOutOfSpace = event.SubDomainOutOfSpace();

    for (auto& p : Partitions) {
        ctx.Send(p.second.Actor, new TEvPQ::TEvSubDomainStatus(event.SubDomainOutOfSpace()));
    }
}

void TPersQueue::Handle(TEvPersQueue::TEvProposeTransactionAttach::TPtr &ev, const TActorContext &ctx)
{
    if (!InitCompleted) {
        AddPendingEvent(ev.Release());
        return;
    }

    PQ_LOG_TX_D("Handle TEvPersQueue::TEvProposeTransactionAttach " << ev->Get()->Record.ShortDebugString());

    const ui64 txId = ev->Get()->Record.GetTxId();
    NKikimrProto::EReplyStatus status = NKikimrProto::NODATA;

    auto tx = GetTransaction(ctx, txId);
    if (tx) {
        //
        // the actor's ID could have changed from the moment he sent the TEvProposeTransaction. you need to
        // update the actor ID in the transaction
        //
        // if the transaction has progressed beyond EXECUTED, then a response has been sent to the sender
        //
        status = NKikimrProto::OK;

        tx->SourceActor = ev->Sender;
        if (tx->State >= NKikimrPQ::TTransaction::EXECUTED) {
            SendEvProposeTransactionResult(ctx, *tx);
        }
    }

    ctx.Send(ev->Sender, new TEvPersQueue::TEvProposeTransactionAttachResult(TabletID(), txId, status), 0, ev->Cookie);
}

void TPersQueue::Handle(TEvPQ::TEvCheckPartitionStatusRequest::TPtr& ev, const TActorContext&)
{
    auto& record = ev->Get()->Record;
    auto it = Partitions.find(TPartitionId(TPartitionId(record.GetPartition())));
    if (InitCompleted && it == Partitions.end()) {
        PQ_LOG_I("Unknown partition " << record.GetPartition());

        auto response = MakeHolder<TEvPQ::TEvCheckPartitionStatusResponse>();
        response->Record.SetStatus(NKikimrPQ::ETopicPartitionStatus::Deleted);
        Send(ev->Sender, response.Release());

        return;
    }

    if (it != Partitions.end() && it->second.InitDone) {
        Forward(ev, it->second.Actor);
    } else {
        CheckPartitionStatusRequests[record.GetPartition()].push_back(ev);
    }
}

void TPersQueue::ProcessCheckPartitionStatusRequests(const TPartitionId& partitionId)
{
    PQ_ENSURE(!partitionId.WriteId.Defined());
    ui32 originalPartitionId = partitionId.OriginalPartitionId;
    auto sit = CheckPartitionStatusRequests.find(originalPartitionId);
    if (sit != CheckPartitionStatusRequests.end()) {
        auto it = Partitions.find(partitionId);
        for (auto& r : sit->second) {
            Forward(r, it->second.Actor);
        }
        CheckPartitionStatusRequests.erase(originalPartitionId);
    }
}

void TPersQueue::Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev)
{
    PQ_LOG_TX_D("Handle TEvLongTxService::TEvLockStatus " << ev->Get()->Record.ShortDebugString());

    auto& record = ev->Get()->Record;
    const TWriteId writeId(record.GetLockNode(), record.GetLockId());

    if (!TxWrites.contains(writeId)) {
        // the transaction has already been completed
        PQ_LOG_TX_W("unknown WriteId " << writeId);
        return;
    }

    TTxWriteInfo& writeInfo = TxWrites.at(writeId);
    PQ_LOG_TX_D("TxWriteInfo: " <<
             "WriteId " << writeId <<
             ", TxId " << writeInfo.TxId <<
             ", Status " << NKikimrLongTxService::TEvLockStatus_EStatus_Name(writeInfo.LongTxSubscriptionStatus));
    writeInfo.LongTxSubscriptionStatus = record.GetStatus();

    if (writeInfo.LongTxSubscriptionStatus == NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED) {
        PQ_LOG_TX_D("subscribed WriteId " << writeId);
        return;
    }

    if (writeInfo.TxId.Defined()) {
        // the message `TEvProposeTransaction` has already arrived
        PQ_LOG_TX_D("there is already a transaction TxId " << writeInfo.TxId << " for WriteId " << writeId);
        return;
    }

    PQ_LOG_TX_I("delete partitions for WriteId " << writeId << " (longTxService lost tx)");
    BeginDeletePartitions(writeId, writeInfo);
}

void TPersQueue::Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx)
{
    if (ReadBalancerActorId) {
        ctx.Send(ReadBalancerActorId, ev->Release().Release());
    }
}

void TPersQueue::Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx)
{
    const NKikimrPQ::TEvPartitionScaleStatusChanged& record = ev->Get()->Record;
    if (MirroringEnabled(Config) && record.HasParticipatingPartitions()) {
        PQ_LOG_I("Got mirrorer split merge request" << ev->ToString());
    }

    if (ReadBalancerActorId) {
        ctx.Send(ReadBalancerActorId, ev->Release().Release());
    }
}

void TPersQueue::Handle(TEvPQ::TBroadcastPartitionError::TPtr& ev, const TActorContext& ctx) {
    const TEvPQ::TBroadcastPartitionError& event = *ev->Get();
    for (auto& [partitionId, partitionInfo] : Partitions) {
        if (partitionId.IsSupportivePartition()) {
            continue;
        }
        THolder error = MakeHolder<TEvPQ::TBroadcastPartitionError>();
        error->Record.CopyFrom(event.Record);
        ctx.Send(partitionInfo.Actor, std::move(error));
    }
}

void TPersQueue::DeletePartition(const TPartitionId& partitionId, const TActorContext& ctx)
{
    auto p = Partitions.find(partitionId);
    if (p == Partitions.end()) {
        return;
    }

    const TPartitionInfo& partition = p->second;
    ctx.Send(partition.Actor, new TEvents::TEvPoisonPill());

    PQ_LOG_D("DeletePartition " << partitionId);
    Partitions.erase(partitionId);
}

void TPersQueue::Handle(TEvPQ::TEvDeletePartitionDone::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_TX_D("Handle TEvPQ::TEvDeletePartitionDone " << ev->Get()->PartitionId);

    auto* event = ev->Get();
    PQ_ENSURE(event->PartitionId.WriteId.Defined());
    const TWriteId& writeId = *event->PartitionId.WriteId;
    PQ_ENSURE(TxWrites.contains(writeId))("WriteId", writeId.ToString());
    TTxWriteInfo& writeInfo = TxWrites.at(writeId);
    PQ_ENSURE(writeInfo.Partitions.contains(event->PartitionId.OriginalPartitionId));
    const TPartitionId& partitionId = writeInfo.Partitions.at(event->PartitionId.OriginalPartitionId);
    PQ_ENSURE(partitionId == event->PartitionId);
    PQ_ENSURE(partitionId.IsSupportivePartition());
    PQ_ENSURE(Partitions.contains(partitionId));

    DeletePartition(partitionId, ctx);

    writeInfo.Partitions.erase(partitionId.OriginalPartitionId);
    TryDeleteWriteId(writeId, writeInfo, ctx);
    TxWritesChanged = true;

    TryWriteTxs(ctx);
}

void TPersQueue::TryDeleteWriteId(const TWriteId& writeId, const TTxWriteInfo& writeInfo, const TActorContext& ctx)
{
    if (writeInfo.Partitions.empty()) {
        if (!writeInfo.KafkaTransaction) {
            UnsubscribeWriteId(writeId, ctx);
        }
        if (writeInfo.TxId.Defined()) {
            if (auto tx = GetTransaction(ctx, *writeInfo.TxId); tx) {
                if ((tx->State == NKikimrPQ::TTransaction::WAIT_RS_ACKS) ||
                    (tx->State == NKikimrPQ::TTransaction::EXPIRED) ||
                    (tx->State == NKikimrPQ::TTransaction::CANCELED)) {
                    TryExecuteTxs(ctx, *tx);
                }
            } else {
                // if the transaction is not in Txs, then it is an immediate transaction
                DeleteWriteId(writeId);
            }
        } else {
            // this is kafka transaction or immediate transaction
            DeleteWriteId(writeId);
        }
    }
}

void TPersQueue::Handle(TEvPQ::TEvTransactionCompleted::TPtr& ev, const TActorContext&)
{
    PQ_LOG_TX_I("Handle TEvPQ::TEvTransactionCompleted" <<
             " WriteId " << ev->Get()->WriteId);

    auto* event = ev->Get();
    if (!event->WriteId.Defined()) {
        return;
    }

    const TWriteId& writeId = *event->WriteId;
    PQ_ENSURE(TxWrites.contains(writeId))("WriteId", writeId.ToString());
    TTxWriteInfo& writeInfo = TxWrites.at(writeId);
    PQ_ENSURE(writeInfo.Partitions.size() == 1);

    BeginDeletePartitions(writeId, writeInfo);
}

void TPersQueue::BeginDeletePartitions(const TWriteId& writeId, TTxWriteInfo& writeInfo)
{
    if (writeInfo.Deleting) {
        PQ_LOG_TX_D("Already deleting WriteInfo");
        return;
    }
    writeInfo.Deleting = true;
    if (writeInfo.Partitions.empty()) {
        TryDeleteWriteId(writeId, writeInfo, ActorContext());
        TxWritesChanged = true;
    } else {
        for (auto& [_, partitionId] : writeInfo.Partitions) {
            PQ_ENSURE(Partitions.contains(partitionId));
            const TPartitionInfo& partition = Partitions.at(partitionId);
            PQ_LOG_TX_D("send TEvPQ::TEvDeletePartition to partition " << partitionId);
            Send(partition.Actor, new TEvPQ::TEvDeletePartition);
        }
    }
}

void TPersQueue::BeginDeletePartitions(const TDistributedTransaction& tx)
{
    if (!tx.WriteId.Defined() || !TxWrites.contains(*tx.WriteId)) {
        return;
    }

    TTxWriteInfo& writeInfo = TxWrites.at(*tx.WriteId);
    BeginDeletePartitions(*tx.WriteId, writeInfo);
}

TString TPersQueue::LogPrefix() const {
    return TStringBuilder() << "[PQ: " << TabletID() << "] ";
}

ui64 TPersQueue::GetGeneration() {
    if (!TabletGeneration.Defined()) {
        TabletGeneration = Executor()->Generation();
    }
    return *TabletGeneration;
}

void TPersQueue::AddPendingEvent(IEventHandle* ev)
{
    PendingEvents.emplace_back(ev);
}

void TPersQueue::ProcessPendingEvents()
{
    auto events = std::move(PendingEvents);
    PendingEvents.clear();

    for (auto& ev : events) {
        HandleHook(ev);
    }
}

void TPersQueue::Handle(TEvPQ::TEvForceCompaction::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("TPersQueue::Handle(TEvPQ::TEvForceCompaction)");

    const auto& event = *ev->Get();
    const TPartitionId partitionId(event.PartitionId);

    if (!Partitions.contains(partitionId)) {
        PQ_LOG_D("Unknown partition id " << event.PartitionId);
        return;
    }

    auto p = Partitions.find(partitionId);
    ctx.Send(p->second.Actor,
             new TEvPQ::TEvForceCompaction(event.PartitionId));
}

void TPersQueue::Handle(TEvPQ::TEvMLPReadRequest::TPtr& ev) {
    ForwardToPartition(ev->Get()->GetPartitionId(), ev);
}

void TPersQueue::Handle(TEvPQ::TEvMLPCommitRequest::TPtr& ev) {
    ForwardToPartition(ev->Get()->GetPartitionId(), ev);
}

void TPersQueue::Handle(TEvPQ::TEvMLPUnlockRequest::TPtr& ev) {
    ForwardToPartition(ev->Get()->GetPartitionId(), ev);
}

void TPersQueue::Handle(TEvPQ::TEvMLPChangeMessageDeadlineRequest::TPtr& ev) {
    ForwardToPartition(ev->Get()->GetPartitionId(), ev);
}

void TPersQueue::Handle(TEvPQ::TEvMLPPurgeRequest::TPtr& ev) {
    ForwardToPartition(ev->Get()->GetPartitionId(), ev);
}

void TPersQueue::Handle(TEvPQ::TEvGetMLPConsumerStateRequest::TPtr& ev) {
    ForwardToPartition(ev->Get()->GetPartitionId(), ev);
}

void TPersQueue::Handle(TEvPQ::TEvMLPConsumerStatus::TPtr& ev) {
    auto& record = ev->Get()->Record;
    PQ_LOG_D("Handle TEvPQ::TEvMLPConsumerStatus " << record.ShortDebugString());
    Forward(ev, ReadBalancerActorId);
}

template<typename TEventHandle>
bool TPersQueue::ForwardToPartition(ui32 partitionId, TAutoPtr<TEventHandle>& ev) {
    auto it = Partitions.find(TPartitionId{partitionId});
    if (it == Partitions.end()) {
        Send(ev->Sender, new TEvPQ::TEvMLPErrorResponse(partitionId, Ydb::StatusIds::SCHEME_ERROR,
            TStringBuilder() <<"Partition " << partitionId << " not found"), 0, ev->Cookie);
        return true;
    }

    auto& partitionInfo = it->second;
    if (!partitionInfo.InitDone) {
        MLPRequests.emplace_back(std::move(ev));
        return false;
    }
    Forward(ev, partitionInfo.Actor);
    return true;
}

void TPersQueue::ProcessMLPQueue() {
    auto queue = std::exchange(MLPRequests, {});
    while(!queue.empty()) {
        auto ev = std::move(queue.front());
        queue.pop_front();

        auto result = std::visit([&, this](auto& ev) {
            return ForwardToPartition(ev->Get()->GetPartitionId(), ev);
        }, ev);
        if (!result) {
            return;
        }
    }
}

bool TPersQueue::HandleHook(STFUNC_SIG)
{
    TRACE_EVENT(NKikimrServices::PERSQUEUE);
    switch(ev->GetTypeRewrite())
    {
        HFuncTraced(TEvInterconnect::TEvNodeInfo, Handle);
        HFuncTraced(TEvPersQueue::TEvRequest, Handle);
        HFuncTraced(TEvPersQueue::TEvUpdateConfig, Handle);
        HFuncTraced(TEvPersQueue::TEvOffsets, Handle);
        HFuncTraced(TEvPersQueue::TEvHasDataInfo, Handle);
        HFuncTraced(TEvPersQueue::TEvStatus, Handle);
        HFuncTraced(TEvPersQueue::TEvPartitionClientInfo, Handle);
        HFuncTraced(TEvKeyValue::TEvResponse, Handle);
        HFuncTraced(TEvPQ::TEvInitComplete, Handle);
        HFuncTraced(TEvPQ::TEvPartitionCounters, Handle);
        HFuncTraced(TEvPQ::TEvMetering, Handle);
        HFuncTraced(TEvPQ::TEvPartitionLabeledCounters, Handle);
        HFuncTraced(TEvPQ::TEvPartitionLabeledCountersDrop, Handle);
        HFuncTraced(TEvPQ::TEvTabletCacheCounters, Handle);
        HFuncTraced(TEvPersQueue::TEvDropTablet, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvClientConnected, Handle);
        HFuncTraced(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFuncTraced(TEvPQ::TEvError, Handle);
        HFuncTraced(TEvPQ::TEvProxyResponse, Handle);
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        HFuncTraced(TEvPersQueue::TEvProposeTransaction, Handle);
        HFuncTraced(TEvPQ::TEvPartitionConfigChanged, Handle);
        HFuncTraced(TEvTxProcessing::TEvPlanStep, Handle);
        HFuncTraced(TEvTxProcessing::TEvReadSet, Handle);
        HFuncTraced(TEvTxProcessing::TEvReadSetAck, Handle);
        HFuncTraced(TEvPQ::TEvTxCalcPredicateResult, Handle);
        HFuncTraced(TEvPQ::TEvProposePartitionConfigResult, Handle);
        HFuncTraced(TEvPQ::TEvTxDone, Handle);
        HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);
        HFuncTraced(TEvPersQueue::TEvProposeTransactionAttach, Handle);
        HFuncTraced(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        HFuncTraced(TEvPersQueue::TEvCancelTransactionProposal, Handle);
        HFuncTraced(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
        HFuncTraced(TEvPQ::TEvCheckPartitionStatusRequest, Handle);
        HFuncTraced(TEvPQ::TEvPartitionScaleStatusChanged, Handle);
        HFuncTraced(TEvPQ::TBroadcastPartitionError, Handle);
        hFuncTraced(NLongTxService::TEvLongTxService::TEvLockStatus, Handle);
        HFuncTraced(TEvPQ::TEvReadingPartitionStatusRequest, Handle);
        HFuncTraced(TEvPQ::TEvDeletePartitionDone, Handle);
        HFuncTraced(TEvPQ::TEvTransactionCompleted, Handle);
        HFuncTraced(TEvPQ::TEvForceCompaction, Handle);
        hFuncTraced(TEvPQ::TEvMLPReadRequest, Handle);
        hFuncTraced(TEvPQ::TEvMLPCommitRequest, Handle);
        hFuncTraced(TEvPQ::TEvMLPUnlockRequest, Handle);
        hFuncTraced(TEvPQ::TEvMLPChangeMessageDeadlineRequest, Handle);
        hFuncTraced(TEvPQ::TEvMLPPurgeRequest, Handle);
        hFuncTraced(TEvPQ::TEvGetMLPConsumerStateRequest, Handle);
        hFuncTraced(TEvPQ::TEvMLPConsumerStatus, Handle);
        default:
            return false;
    }
    return true;
}

} // namespace NKikimr::NPQ

namespace NKikimr {

IActor* CreatePersQueue(const TActorId& tablet, TTabletStorageInfo *info) {
    return new NPQ::TPersQueue(tablet, info);
}

} // namespace NKikimr
