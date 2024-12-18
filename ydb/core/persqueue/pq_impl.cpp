
#include "pq_impl.h"
#include "pq_impl_types.h"
#include "event_helpers.h"
#include "partition_log.h"
#include "partition.h"
#include "read.h"
#include "utils.h"

#include <ydb/core/base/tx_processing.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/strbuf.h>

//TODO: move this code to vieiwer
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/escape.h>

#define PQ_LOG_ERROR_AND_DIE(expr) \
    PQ_LOG_ERROR(expr); \
    ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill())

namespace NKikimr::NPQ {

static constexpr TDuration TOTAL_TIMEOUT = TDuration::Seconds(120);
static constexpr char TMP_REQUEST_MARKER[] = "__TMP__REQUEST__MARKER__";
static constexpr ui32 CACHE_SIZE = 100_MB;
static constexpr ui32 MAX_BYTES = 25_MB;
static constexpr ui32 MAX_SOURCE_ID_LENGTH = 2048;
static constexpr ui32 MAX_HEARTBEAT_SIZE = 2_KB;

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

/******************************************************* ReadProxy *********************************************************/
//megaqc - remove it when LB will be ready
class TReadProxy : public TActorBootstrapped<TReadProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_ANS_ACTOR;
    }

    TReadProxy(const TActorId& sender, const TActorId& tablet, ui64 tabletGeneration,
               const TDirectReadKey& directReadKey, const NKikimrClient::TPersQueueRequest& request)
        : Sender(sender)
        , Tablet(tablet)
        , TabletGeneration(tabletGeneration)
        , Request(request)
        , Response(new TEvPersQueue::TEvResponse)
        , DirectReadKey(directReadKey)
    {
        Y_ABORT_UNLESS(Request.HasPartitionRequest() && Request.GetPartitionRequest().HasCmdRead());
        Y_ABORT_UNLESS(Request.GetPartitionRequest().GetCmdRead().GetPartNo() == 0); //partial request are not allowed, otherwise remove ReadProxy
        Y_ABORT_UNLESS(!Response->Record.HasPartitionResponse());
        if (!directReadKey.SessionId.empty()) {
            DirectReadKey.ReadId = Request.GetPartitionRequest().GetCmdRead().GetDirectReadId();
        }
    }

    void Bootstrap(const TActorContext&)
    {
        Become(&TThis::StateFunc);
    }

private:
    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx)
    {
        Y_ABORT_UNLESS(Response);
        const auto& record = ev->Get()->Record;
        if (!record.HasPartitionResponse() || !record.GetPartitionResponse().HasCmdReadResult() ||
            record.GetStatus() != NMsgBusProxy::MSTATUS_OK || record.GetErrorCode() != NPersQueue::NErrorCode::OK ||
            record.GetPartitionResponse().GetCmdReadResult().ResultSize() == 0) {

            Response->Record.CopyFrom(record);
            ctx.Send(Sender, Response.Release());
            Die(ctx);
            return;
        }
        Y_ABORT_UNLESS(record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult());
        const auto& readResult = record.GetPartitionResponse().GetCmdReadResult();
        auto isDirectRead = DirectReadKey.ReadId != 0;
        if (isDirectRead) {
            if (!PreparedResponse) {
                PreparedResponse = std::make_shared<NKikimrClient::TResponse>();
            }
        }

        auto& responseRecord = isDirectRead ? *PreparedResponse : Response->Record;
        responseRecord.SetStatus(NMsgBusProxy::MSTATUS_OK);
        responseRecord.SetErrorCode(NPersQueue::NErrorCode::OK);

        Y_ABORT_UNLESS(readResult.ResultSize() > 0);
        bool isStart = false;
        if (!responseRecord.HasPartitionResponse()) {
            Y_ABORT_UNLESS(!readResult.GetResult(0).HasPartNo() || readResult.GetResult(0).GetPartNo() == 0); //starts from begin of record
            auto partResp = responseRecord.MutablePartitionResponse();
            auto readRes = partResp->MutableCmdReadResult();
            readRes->SetBlobsFromDisk(readRes->GetBlobsFromDisk() + readResult.GetBlobsFromDisk());
            readRes->SetBlobsFromCache(readRes->GetBlobsFromCache() + readResult.GetBlobsFromCache());
            isStart = true;
        }
        ui64 readFromTimestampMs = AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()
                                    ? (isStart ? readResult.GetReadFromTimestampMs()
                                                : responseRecord.GetPartitionResponse().GetCmdReadResult().GetReadFromTimestampMs())
                                    : 0;

        if (record.GetPartitionResponse().HasCookie())
            responseRecord.MutablePartitionResponse()->SetCookie(record.GetPartitionResponse().GetCookie());

        auto partResp = responseRecord.MutablePartitionResponse()->MutableCmdReadResult();

        partResp->SetMaxOffset(readResult.GetMaxOffset());
        partResp->SetSizeLag(readResult.GetSizeLag());
        partResp->SetWaitQuotaTimeMs(partResp->GetWaitQuotaTimeMs() + readResult.GetWaitQuotaTimeMs());

        partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), readResult.GetRealReadOffset()));

        for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
            bool isNewMsg = !readResult.GetResult(i).HasPartNo() || readResult.GetResult(i).GetPartNo() == 0;
            if (!isStart) {
                Y_ABORT_UNLESS(partResp->ResultSize() > 0);
                auto& back = partResp->GetResult(partResp->ResultSize() - 1);
                bool lastMsgIsNotFull = back.GetPartNo() + 1 < back.GetTotalParts();
                bool trancate = lastMsgIsNotFull && isNewMsg;
                if (trancate) {
                    partResp->MutableResult()->RemoveLast();
                    if (partResp->GetResult().empty()) isStart = false;
                }
            }

            if (isNewMsg) {
                if (!isStart && readResult.GetResult(i).HasTotalParts()
                             && readResult.GetResult(i).GetTotalParts() + i > readResult.ResultSize()) //last blob is not full
                    break;
                partResp->AddResult()->CopyFrom(readResult.GetResult(i));
                isStart = false;
            } else { //glue to last res
                auto rr = partResp->MutableResult(partResp->ResultSize() - 1);
                if (rr->GetSeqNo() != readResult.GetResult(i).GetSeqNo() || rr->GetPartNo() + 1 != readResult.GetResult(i).GetPartNo()) {
                    PQ_LOG_CRIT("Handle TEvRead last read pos (seqno/parno): " << rr->GetSeqNo() << "," << rr->GetPartNo() << " readed now "
                                    << readResult.GetResult(i).GetSeqNo() << ", " << readResult.GetResult(i).GetPartNo()
                                    << " full request(now): " << Request);
                }
                Y_ABORT_UNLESS(rr->GetSeqNo() == readResult.GetResult(i).GetSeqNo());
                (*rr->MutableData()) += readResult.GetResult(i).GetData();
                rr->SetPartitionKey(readResult.GetResult(i).GetPartitionKey());
                rr->SetExplicitHash(readResult.GetResult(i).GetExplicitHash());
                rr->SetPartNo(readResult.GetResult(i).GetPartNo());
                rr->SetUncompressedSize(rr->GetUncompressedSize() + readResult.GetResult(i).GetUncompressedSize());
                if (readResult.GetResult(i).GetPartNo() + 1 == readResult.GetResult(i).GetTotalParts()) {
                    Y_ABORT_UNLESS((ui32)rr->GetTotalSize() == (ui32)rr->GetData().size());
                }
            }
        }
        if (!partResp->GetResult().empty()) {
            const auto& lastRes = partResp->GetResult(partResp->GetResult().size() - 1);
            if (lastRes.HasPartNo() && lastRes.GetPartNo() + 1 < lastRes.GetTotalParts()) { //last res is not full
                Request.SetRequestId(TMP_REQUEST_MARKER);

                auto read = Request.MutablePartitionRequest()->MutableCmdRead();
                read->SetOffset(lastRes.GetOffset());
                read->SetPartNo(lastRes.GetPartNo() + 1);
                read->SetCount(1);
                read->ClearBytes();
                read->ClearTimeoutMs();
                read->ClearMaxTimeLagMs();
                read->SetReadTimestampMs(readFromTimestampMs);

                THolder<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
                req->Record = Request;
                ctx.Send(Tablet, req.Release());
                return;
            }
        }
        //filter old messages
        ::google::protobuf::RepeatedPtrField<NKikimrClient::TCmdReadResult::TResult> records;
        records.Swap(partResp->MutableResult());
        partResp->ClearResult();
        for (auto & rec : records) {
            partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), rec.GetOffset()));
            if (rec.GetWriteTimestampMS() >= readFromTimestampMs) {
                auto result = partResp->AddResult();
                result->CopyFrom(rec);
            }
        }
        if (isDirectRead) {
            auto* prepareResponse = Response->Record.MutablePartitionResponse()->MutableCmdPrepareReadResult();
            prepareResponse->SetBytesSizeEstimate(readResult.GetSizeEstimate());
            prepareResponse->SetDirectReadId(DirectReadKey.ReadId);
            prepareResponse->SetReadOffset(readResult.GetRealReadOffset());
            prepareResponse->SetLastOffset(readResult.GetLastOffset());
            prepareResponse->SetEndOffset(readResult.GetEndOffset());

            prepareResponse->SetSizeLag(readResult.GetSizeLag());
            Response->Record.MutablePartitionResponse()->SetCookie(record.GetPartitionResponse().GetCookie());
            if (readResult.ResultSize()) {
                prepareResponse->SetWriteTimestampMS(readResult.GetResult(readResult.ResultSize() - 1).GetWriteTimestampMS());
            }
            Response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
            Response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
            ctx.Send(
                MakePQDReadCacheServiceActorId(),
                new TEvPQ::TEvStageDirectReadData(DirectReadKey, TabletGeneration, PreparedResponse)
            );
            ctx.Send(Sender, Response.Release());
        } else {
            ctx.Send(Sender, Response.Release());
        }
        Die(ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, Handle);
        default:
            break;
        };
    }

    const TActorId Sender;
    const TActorId Tablet;
    ui32 TabletGeneration;
    NKikimrClient::TPersQueueRequest Request;
    THolder<TEvPersQueue::TEvResponse> Response;
    std::shared_ptr<NKikimrClient::TResponse> PreparedResponse;
    TDirectReadKey DirectReadKey;

};


TActorId CreateReadProxy(const TActorId& sender, const TActorId& tablet, ui32 tabletGeneration,
                         const TDirectReadKey& directReadKey, const NKikimrClient::TPersQueueRequest& request,
                         const TActorContext& ctx)
{
    return ctx.Register(new TReadProxy(sender, tablet, tabletGeneration, directReadKey, request));
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
        Y_ABORT_UNLESS(Waiting);
        Y_ABORT_UNLESS(Response);
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
        PQ_LOG_D("Answer error topic: '" << TopicName << "'" <<
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
class TBuilderProxy : public TActorBootstrapped<TBuilderProxy<T,T2,T3>> {
    typedef TBuilderProxy<T,T2,T3> TThis;

    friend class TActorBootstrapped<TThis>;
    typedef T TEvent;
    typedef typename TEvent::TPtr TTPtr;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_ANS_ACTOR;
    }

    TBuilderProxy(const ui64 tabletId, const TActorId& sender, const ui32 count, const ui64 cookie)
    : TabletId(tabletId)
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

private:

    void AnswerAndDie(const TActorContext& ctx)
    {
        std::sort(Result.begin(), Result.end(), [](const typename T2::TPartResult& a, const typename T2::TPartResult& b){
                                                    return a.GetPartition() < b.GetPartition();
                                                });
        THolder<T3> res = MakeHolder<T3>();
        auto& resp = res->Record;
        resp.SetTabletId(TabletId);
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

    ui64 TabletId;
    TActorId Sender;
    ui32 Waiting;
    TVector<typename T2::TPartResult> Result;
    ui64 Cookie;
};


TActorId CreateOffsetsProxyActor(const ui64 tabletId, const TActorId& sender, const ui32 count, const TActorContext& ctx)
{
    return ctx.Register(new TBuilderProxy<TEvPQ::TEvPartitionOffsetsResponse,
                                          NKikimrPQ::TOffsetsResponse,
                                          TEvPersQueue::TEvOffsetsResponse>(tabletId, sender, count, 0));
}

/******************************************************* StatusProxy *********************************************************/


TActorId CreateStatusProxyActor(const ui64 tabletId, const TActorId& sender, const ui32 count, const ui64 cookie, const TActorContext& ctx)
{
    return ctx.Register(new TBuilderProxy<TEvPQ::TEvPartitionStatusResponse,
                                          NKikimrPQ::TStatusResponse,
                                          TEvPersQueue::TEvStatusResponse>(tabletId, sender, count, cookie));
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
    Y_ABORT_UNLESS(ConfigInited && AllOriginalPartitionsInited());

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

        Y_ABORT_UNLESS(TopicName.size(), "Need topic name here");
        ctx.Send(CacheActor, new TEvPQ::TEvChangeCacheConfig(TopicName, cacheSize));
    } else {
        //Y_ABORT_UNLESS(TopicName == Config.GetTopicName(), "Changing topic name is not supported");
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

    Y_ABORT_UNLESS(resp.WriteResultSize() >= 1);
    Y_ABORT_UNLESS(resp.GetWriteResult(0).GetStatus() == NKikimrProto::OK);
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
    Y_ABORT_UNLESS(read.HasStatus());
    if (read.GetStatus() != NKikimrProto::OK && read.GetStatus() != NKikimrProto::NODATA) {
        PQ_LOG_ERROR("tx info read error " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    switch (read.GetStatus()) {
    case NKikimrProto::OK: {
        PQ_LOG_D("has a tx info");

        NKikimrPQ::TTabletTxInfo info;
        Y_ABORT_UNLESS(info.ParseFromString(read.GetValue()));

        InitPlanStep(info);

        break;
    }
    case NKikimrProto::NODATA: {
        PQ_LOG_D("doesn't have tx info");

        InitPlanStep();

        break;
    }
    default:
        PQ_LOG_ERROR_AND_DIE("Unexpected tx info read status: " << read.GetStatus());
        return;
    }

    PQ_LOG_D("PlanStep " << PlanStep << ", PlanTxId " << PlanTxId <<
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
    Y_ABORT_UNLESS(read.HasStatus());
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
    TActorId actorId = ctx.Register(CreatePartitionActor(partitionId,
                                                         topicConverter,
                                                         config,
                                                         newPartition,
                                                         ctx));
    Partitions.emplace(std::piecewise_construct,
                       std::forward_as_tuple(partitionId),
                       std::forward_as_tuple(actorId,
                                             GetPartitionKeyRange(config, partition),
                                             *Counters));
    ++OriginalPartitionsCount;
}

void TPersQueue::MoveTopTxToCalculating(TDistributedTransaction& tx,
                                        const TActorContext& ctx)
{
    std::tie(ExecStep, ExecTxId) = TxQueue.front();
    PQ_LOG_D("New ExecStep " << ExecStep << ", ExecTxId " << ExecTxId);

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
        Y_ABORT_UNLESS(false);
    }

    TryChangeTxState(tx, NKikimrPQ::TTransaction::CALCULATING);
}

void TPersQueue::AddSupportivePartition(const TPartitionId& partitionId)
{
    Partitions.emplace(partitionId,
                       TPartitionInfo(TActorId(),
                                      {},
                                      *Counters));
    NewSupportivePartitions.insert(partitionId);
}

NKikimrPQ::TPQTabletConfig TPersQueue::MakeSupportivePartitionConfig() const
{
    NKikimrPQ::TPQTabletConfig partitionConfig = Config;

    partitionConfig.MutableReadRules()->Clear();
    partitionConfig.MutableReadFromTimestampsMs()->Clear();
    partitionConfig.MutableConsumerFormatVersions()->Clear();
    partitionConfig.MutableConsumerCodecs()->Clear();
    partitionConfig.MutableReadRuleServiceTypes()->Clear();
    partitionConfig.MutableReadRuleVersions()->Clear();
    partitionConfig.MutableReadRuleGenerations()->Clear();

    return partitionConfig;
}

void TPersQueue::CreateSupportivePartitionActor(const TPartitionId& partitionId,
                                                const TActorContext& ctx)
{
    Y_ABORT_UNLESS(Partitions.contains(partitionId));

    TPartitionInfo& partition = Partitions.at(partitionId);
    partition.Actor = ctx.Register(CreatePartitionActor(partitionId,
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

        SubscribeWriteId(writeId, ctx);
    }

    NewSupportivePartitions.clear();
}

void TPersQueue::ReadConfig(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                            const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges,
                            const TActorContext& ctx)
{
    Y_ABORT_UNLESS(read.HasStatus());
    if (read.GetStatus() != NKikimrProto::OK && read.GetStatus() != NKikimrProto::NODATA) {
        PQ_LOG_ERROR("Config read error " << ctx.SelfID <<
            " Error status code " << read.GetStatus());
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    Y_ABORT_UNLESS(!ConfigInited);

    if (read.GetStatus() == NKikimrProto::OK) {
        bool res = Config.ParseFromString(read.GetValue());
        Y_ABORT_UNLESS(res);

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

        Y_ABORT_UNLESS(TopicName.size(), "Need topic name here");
        ctx.Send(CacheActor, new TEvPQ::TEvChangeCacheConfig(TopicName, cacheSize));
    } else if (read.GetStatus() == NKikimrProto::NODATA) {
        PQ_LOG_D("no config, start with empty partitions and default config");
    } else {
        PQ_LOG_ERROR_AND_DIE("Unexpected config read status: " << read.GetStatus());
        return;
    }

    TxWrites.clear();

    for (const auto& readRange : readRanges) {
        Y_ABORT_UNLESS(readRange.HasStatus());
        if (readRange.GetStatus() != NKikimrProto::OK &&
            readRange.GetStatus() != NKikimrProto::OVERRUN &&
            readRange.GetStatus() != NKikimrProto::NODATA) {
            PQ_LOG_ERROR_AND_DIE("Transactions read error: " << readRange.GetStatus());
            return;
        }

        for (size_t i = 0; i < readRange.PairSize(); ++i) {
            const auto& pair = readRange.GetPair(i);

            PQ_LOG_D("ReadRange pair." <<
                     " Key " << (pair.HasKey() ? pair.GetKey() : "unknown") <<
                     ", Status " << pair.GetStatus());

            NKikimrPQ::TTransaction tx;
            Y_ABORT_UNLESS(tx.ParseFromString(pair.GetValue()));

            PQ_LOG_D("Restore Tx. " <<
                     "TxId: " << tx.GetTxId() <<
                     ", Step: " << tx.GetStep() <<
                     ", State: " << NKikimrPQ::TTransaction_EState_Name(tx.GetState()) <<
                     ", WriteId: " << tx.GetWriteId().ShortDebugString());

            if (tx.GetState() == NKikimrPQ::TTransaction::CALCULATED) {
                PQ_LOG_D("Fix tx state");
                tx.SetState(NKikimrPQ::TTransaction::PLANNED);
            }

            Txs.emplace(tx.GetTxId(), tx);

            if (tx.HasStep()) {
                if (std::make_pair(tx.GetStep(), tx.GetTxId()) >= std::make_pair(ExecStep, ExecTxId)) {
                    PlannedTxs.emplace_back(tx.GetStep(), tx.GetTxId());
                }
            }

            if (tx.HasWriteId()) {
                PQ_LOG_D("Link TxId " << tx.GetTxId() << " with WriteId " << GetWriteId(tx));
                TxWrites[GetWriteId(tx)].TxId = tx.GetTxId();
            }
        }
    }

    EndInitTransactions();
    EndReadConfig(ctx);
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

    Y_ABORT_UNLESS(!NewConfigShouldBeApplied);
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
        Y_ABORT_UNLESS(ok);
        Y_ABORT_UNLESS(stateProto.HasState());
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
    if (AllTransactionsHaveBeenProcessed() && (TabletState == NKikimrPQ::EDropped)) {
        for (auto req : TabletStateRequests) {
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
    Y_ABORT_UNLESS(ok);

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
    Y_ABORT_UNLESS(it != Partitions.end());
    return it->second;
}

void TPersQueue::Handle(TEvPQ::TEvPartitionCounters::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_T("Handle TEvPQ::TEvPartitionCounters" <<
             " PartitionId " << ev->Get()->Partition);

    const auto& partitionId = ev->Get()->Partition;
    auto& partition = GetPartitionInfo(partitionId);
    auto diff = ev->Get()->Counters.MakeDiffForAggr(partition.Baseline);
    ui64 cpuUsage = diff->Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE].Get();
    ui64 networkBytesUsage = diff->Cumulative()[COUNTER_PQ_TABLET_NETWORK_BYTES_USAGE].Get();
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

    Counters->Populate(*diff.Get());
    ev->Get()->Counters.RememberCurrentStateAsBaseline(partition.Baseline);

    // restore cache's simple counters cleaned by partition's counters
    SetCacheCounters(CacheCounters);
    ui64 reservedSize = 0;
    for (auto& p : Partitions) {
        if (p.second.Baseline.Simple().Size() > 0) //there could be no counters from this partition yet
            reservedSize += p.second.Baseline.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Get();
    }
    Counters->Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(reservedSize);
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

        Y_ABORT_UNLESS(aggr->HasCounters());

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

    PQ_LOG_D("topic '" << (TopicConverter ? TopicConverter->GetClientsideName() : "Undefined")
        << "Counters. CacheSize " << CacheCounters.CacheSizeBytes << " CachedBlobs " << CacheCounters.CacheSizeBlobs);
}

bool TPersQueue::AllOriginalPartitionsInited() const
{
    return PartitionsInited == OriginalPartitionsCount;
}

void TPersQueue::Handle(TEvPQ::TEvInitComplete::TPtr& ev, const TActorContext& ctx)
{
    const auto& partitionId = ev->Get()->Partition;
    auto& partition = GetPartitionInfo(partitionId);
    Y_ABORT_UNLESS(!partition.InitDone);
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

    Y_ABORT_UNLESS(ConfigInited);//partitions are inited only after config

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

    Y_ABORT_UNLESS(ChangePartitionConfigInflight > 0);
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
    Y_ABORT_UNLESS(topicConverter);
    Y_ABORT_UNLESS(topicConverter->IsValid(), "%s", topicConverter->GetReason().c_str());
}

void TPersQueue::UpdateReadRuleGenerations(NKikimrPQ::TPQTabletConfig& cfg) const
{
    Y_ABORT_UNLESS(cfg.HasVersion());
    const int curConfigVersion = cfg.GetVersion();

    // set rr generation for provided read rules
    THashMap<TString, std::pair<ui64, ui64>> existed; // map name -> rrVersion, rrGeneration
    for (const auto& c : Config.GetConsumers()) {
        existed[c.GetName()] = std::make_pair(c.GetVersion(), c.GetGeneration());
    }

    for (auto& c : *cfg.MutableConsumers()) {
        auto it = existed.find(c.GetName());
        ui64 generation = 0;
        if (it != existed.end() && it->second.first == c.GetVersion()) {
            generation = it->second.second;
        } else {
            generation = curConfigVersion;
        }
        c.SetGeneration(generation);
        cfg.AddReadRuleGenerations(generation);
    }
}

void TPersQueue::ProcessUpdateConfigRequest(TAutoPtr<TEvPersQueue::TEvUpdateConfig> ev, const TActorId& sender, const TActorContext& ctx)
{
    const auto& record = ev->GetRecord();

    int oldConfigVersion = Config.HasVersion() ? static_cast<int>(Config.GetVersion()) : -1;
    int newConfigVersion = NewConfig.HasVersion() ? static_cast<int>(NewConfig.GetVersion()) : oldConfigVersion;

    Y_ABORT_UNLESS(newConfigVersion >= oldConfigVersion);

    NKikimrPQ::TPQTabletConfig cfg = record.GetTabletConfig();

    Y_ABORT_UNLESS(cfg.HasVersion());
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

    UpdateReadRuleGenerations(cfg);

    PQ_LOG_D("Config update version " << cfg.GetVersion() << "(current " << Config.GetVersion() << ") received from actor " << sender
            << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

    TString str;
    Y_ABORT_UNLESS(CheckPersQueueConfig(cfg, true, &str), "%s", str.c_str());

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
    Y_ABORT_UNLESS((ui64)request->Record.GetCmdWrite().size() == (ui64)bootstrapCfg.GetExplicitMessageGroups().size() * cfg.PartitionsSize() + 1);

    ctx.Send(ctx.SelfID, request.Release());
}

void TPersQueue::AddCmdWriteConfig(TEvKeyValue::TEvRequest* request,
                                   const NKikimrPQ::TPQTabletConfig& cfg,
                                   const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                                   const NKikimrPQ::TPartitions& partitionsData,
                                   const TActorContext& ctx)
{
    Y_ABORT_UNLESS(request);

    TString str;
    Y_ABORT_UNLESS(cfg.SerializeToString(&str));

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

    auto& record = ev->Get()->Record;
    ui64 txId = record.GetTxId();

    TChangeNotification stateRequest(ev->Sender, txId);

    NKikimrPQ::ETabletState reqState = record.GetRequestedState();

    if (reqState == NKikimrPQ::ENormal) {
        ReturnTabletState(ctx, stateRequest, NKikimrProto::ERROR);
        return;
    }

    Y_ABORT_UNLESS(reqState == NKikimrPQ::EDropped);

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
    TActorId ans = CreateOffsetsProxyActor(TabletID(), ev->Sender, cnt, ctx);

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

    ReadBalancerActorId = ev->Sender;

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

    TActorId ans = CreateStatusProxyActor(TabletID(), ev->Sender, cnt, ev->Cookie, ctx);
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
    Y_ABORT_UNLESS(it != ResponseProxy.end());
    it->second->AddPartialReplyCount(count);
    it->second->SetCounterId(counterId);
}

void TPersQueue::HandleGetMaxSeqNoRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(req.HasCmdGetMaxSeqNo());
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
    Y_ABORT_UNLESS(req.HasCmdDeleteSession());
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
    Y_ABORT_UNLESS(req.HasCmdCreateSession());
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
            Y_ABORT_UNLESS(isDirectRead);
            auto fakeResponse = MakeHolder<TEvPQ::TEvProxyResponse>(responseCookie);
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
    Y_ABORT_UNLESS(req.HasCmdSetClientOffset());
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
            TActorId{}, TEvPQ::TEvSetClientInfo::ESCI_OFFSET, 0, cmd.GetStrict()
        );
        ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
    }
}

void TPersQueue::HandleGetClientOffsetRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(req.HasCmdGetClientOffset());
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
    Y_ABORT_UNLESS(req.HasCmdUpdateWriteTimestamp());
    const auto& cmd = req.GetCmdUpdateWriteTimestamp();
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_OFFSET);
    THolder<TEvPQ::TEvUpdateWriteTimestamp> event = MakeHolder<TEvPQ::TEvUpdateWriteTimestamp>(responseCookie, cmd.GetWriteTimeMS());
    ctx.Send(partActor, event.Release(), 0, 0, std::move(traceId));
}

void TPersQueue::HandleWriteRequest(const ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
                                    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(req.CmdWriteSize());
    MeteringSink.MayFlush(ctx.Now()); // To ensure hours' border;
    if (Config.GetMeteringMode() != NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS) {
        MeteringSink.IncreaseQuantity(EMeteringJson::PutEventsV1, req.HasPutUnitsSize() ? req.GetPutUnitsSize() : 0);
    }

    TVector<TEvPQ::TEvWrite::TMsg> msgs;

    bool mirroredPartition = Config.GetPartitionConfig().HasMirrorFrom();

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
        PQ_LOG_D("Write in transaction." <<
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
        }
        ui64 createTimestampMs = 0, writeTimestampMs = 0;
        if (cmd.HasCreateTimeMS() && cmd.GetCreateTimeMS() >= 0)
            createTimestampMs = cmd.GetCreateTimeMS();
        if (cmd.HasWriteTimeMS() && cmd.GetWriteTimeMS() > 0) {
            writeTimestampMs = cmd.GetWriteTimeMS();
            if (!cmd.GetDisableDeduplication()) {
                errorStr = "WriteTimestamp avail only without deduplication";
            }
        }

        if (!errorStr.empty()) {
            ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, errorStr);
            return;
        }
        ui32 mSize = MAX_BLOB_PART_SIZE - cmd.GetSourceId().size() - sizeof(ui32) - TClientBlob::OVERHEAD; //megaqc - remove this
        Y_ABORT_UNLESS(mSize > 204800);
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
            Y_ABORT_UNLESS(it != ResponseProxy.end());
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
            Y_ABORT_UNLESS(!cmd.HasTotalParts(), "too big part"); //change this verify for errorStr, when LB will be ready
            while (pos < totalSize) {
                TString data = cmd.GetData().substr(pos, mSize - diff);
                pos += mSize - diff;
                diff = 0;
                msgs.push_back({cmd.GetSourceId(), static_cast<ui64>(cmd.GetSeqNo()), partNo,
                    totalParts, totalSize, createTimestampMs, receiveTimestampMs,
                    disableDeduplication, writeTimestampMs, data, uncompressedSize,
                    cmd.GetPartitionKey(), cmd.GetExplicitHash(), cmd.GetExternalOperation(),
                    cmd.GetIgnoreQuotaDeadline(), heartbeatVersion
                });
                partNo++;
                uncompressedSize = 0;
                PQ_LOG_D("got client PART message topic: " << (TopicConverter ? TopicConverter->GetClientsideName() : "Undefined") << " partition: " << req.GetPartition()
                        << " SourceId: \'" << EscapeC(msgs.back().SourceId) << "\' SeqNo: "
                        << msgs.back().SeqNo << " partNo : " << msgs.back().PartNo
                        << " messageNo: " << req.GetMessageNo() << " size: " << data.size()
                );
            }
            Y_ABORT_UNLESS(partNo == totalParts);
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

            msgs.push_back({cmd.GetSourceId(), static_cast<ui64>(cmd.GetSeqNo()), static_cast<ui16>(cmd.HasPartNo() ? cmd.GetPartNo() : 0),
                static_cast<ui16>(cmd.HasPartNo() ? cmd.GetTotalParts() : 1), totalSize,
                createTimestampMs, receiveTimestampMs, disableDeduplication, writeTimestampMs, data,
                cmd.HasUncompressedSize() ? cmd.GetUncompressedSize() : 0u, cmd.GetPartitionKey(), cmd.GetExplicitHash(),
                cmd.GetExternalOperation(), cmd.GetIgnoreQuotaDeadline(), heartbeatVersion
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
    Y_ABORT_UNLESS(req.HasCmdReserveBytes());

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
    Y_ABORT_UNLESS(req.HasCmdGetOwnership());

    const TString& owner = req.GetCmdGetOwnership().GetOwner();
    if (owner.empty()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "empty owner in CmdGetOwnership request");
        return;
    }
    Y_ABORT_UNLESS(pipeClient != TActorId());
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
    Y_ABORT_UNLESS(req.HasCmdRead());

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
                                       cmd.HasTimeoutMs() ? cmd.GetTimeoutMs() : 0, bytes,
                                       cmd.HasMaxTimeLagMs() ? cmd.GetMaxTimeLagMs() : 0,
                                       cmd.HasReadTimestampMs() ? cmd.GetReadTimestampMs() : 0, clientDC,
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
    THolder<TEvPQ::TEvProxyResponse> publishDoneEvent = MakeHolder<TEvPQ::TEvProxyResponse>(responseCookie);
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
    THolder<TEvPQ::TEvProxyResponse> forgetDoneEvent = MakeHolder<TEvPQ::TEvProxyResponse>(responseCookie);
    forgetDoneEvent->Response->SetStatus(NMsgBusProxy::MSTATUS_OK);
    forgetDoneEvent->Response->SetErrorCode(NPersQueue::NErrorCode::OK);

    forgetDoneEvent->Response->MutablePartitionResponse()->MutableCmdForgetReadResult();
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
    Y_ABORT_UNLESS(req.HasCmdRegisterMessageGroup());

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
    Y_ABORT_UNLESS(req.HasCmdDeregisterMessageGroup());

    NPersQueue::NErrorCode::EErrorCode code;
    TString error;
    auto body = MakeDeregisterMessageGroup(req.GetCmdDeregisterMessageGroup(), code, error);

    if (!body) {
        return ReplyError(ctx, responseCookie, code, error);
    }

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_DEREGISTER_MESSAGE_GROUP);
    ctx.Send(partActor, new TEvPQ::TEvDeregisterMessageGroup(responseCookie, std::move(body.GetRef())), 0, 0, std::move(traceId));
}

void TPersQueue::HandleSplitMessageGroupRequest(ui64 responseCookie, NWilson::TTraceId traceId, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(req.HasCmdSplitMessageGroup());
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
    Y_ABORT_UNLESS(req.HasWriteId());

    const TWriteId writeId = GetWriteId(req);
    ui32 originalPartitionId = req.GetPartition();

    Y_ABORT_UNLESS(TxWrites.contains(writeId) && TxWrites.at(writeId).Partitions.contains(originalPartitionId),
                   "PQ %" PRIu64 ", WriteId {%" PRIu64 ", %" PRIu64 "}, Partition %" PRIu32,
                   TabletID(), writeId.NodeId, writeId.KeyId, originalPartitionId);

    const TPartitionId& partitionId = TxWrites.at(writeId).Partitions.at(originalPartitionId);
    Y_ABORT_UNLESS(Partitions.contains(partitionId));
    const TPartitionInfo& partition = Partitions.at(partitionId);
    Y_ABORT_UNLESS(partition.InitDone);

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
    Y_ABORT_UNLESS(req.HasWriteId());

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

    Y_ABORT_UNLESS(req.HasWriteId());

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

    if (TxWrites.contains(writeId) && TxWrites.at(writeId).Partitions.contains(originalPartitionId)) {
        //
        // - вспомогательная партиция уже существует
        // - если партиция инициализирована, то
        // -     отправить ей сообщение
        // - иначе
        // -     добавить сообщение в очередь для партиции
        //
        const TTxWriteInfo& writeInfo = TxWrites.at(writeId);
        if (writeInfo.TxId.Defined()) {
            ReplyError(ctx,
                       responseCookie,
                       NPersQueue::NErrorCode::BAD_REQUEST,
                       "it is forbidden to write after a commit");
            return;
        }

        const TPartitionId& partitionId = writeInfo.Partitions.at(originalPartitionId);
        Y_ABORT_UNLESS(Partitions.contains(partitionId));
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
            ReplyError(ctx,
                       responseCookie,
                       NPersQueue::NErrorCode::PRECONDITION_FAILED,
                       "lost messages");
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
        PQ_LOG_D("partition " << partitionId << " for WriteId " << writeId);

        writeInfo.Partitions.emplace(originalPartitionId, partitionId);
        TxWritesChanged = true;
        AddSupportivePartition(partitionId);

        Y_ABORT_UNLESS(Partitions.contains(partitionId));

        TPartitionInfo& partition = Partitions.at(partitionId);
        partition.PendingRequests.emplace_back(responseCookie,
                                               std::shared_ptr<TEvPersQueue::TEvRequest>(event->Release().Release()),
                                               sender);

        if (writeInfo.LongTxSubscriptionStatus == NKikimrLongTxService::TEvLockStatus::STATUS_UNSPECIFIED) {
            SubscribeWriteId(writeId, ctx);
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
        TStringBuilder log; log << "PQ - create read proxy" << Endl;
        TActorId rr = CreateReadProxy(ev->Sender, ctx.SelfID, GetGeneration(), directKey, request, ctx);
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
        + req.HasCmdForgetRead();

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

    Y_ABORT_UNLESS(ev->Get()->Leader, "Unexpectedly connected to follower of tablet %" PRIu64, ev->Get()->TabletId);

    if (PipeClientCache->OnConnect(ev)) {
        PQ_LOG_D("Connected to tablet " << ev->Get()->TabletId);
        return;
    }

    RestartPipe(ev->Get()->TabletId, ctx);
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
    for (auto& txId: GetBindedTxs(tabletId)) {
        auto* tx = GetTransaction(ctx, txId);
        if (!tx) {
            continue;
        }

        for (auto& message : tx->GetBindedMsgs(tabletId)) {
            PipeClientCache->Send(ctx, tabletId, message.Type, message.Data);
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
        Y_ABORT_UNLESS(res);
    }
    ResponseProxy.clear();

    StopWatchingTenantPathId(ctx);
    MediatorTimeCastUnregisterTablet(ctx);

    NKeyValue::TKeyValueFlat::HandleDie(ctx);
}


TPersQueue::TPersQueue(const TActorId& tablet, TTabletStorageInfo *info)
    : TKeyValueFlat(tablet, info)
    , ConfigInited(false)
    , PartitionsInited(0)
    , OriginalPartitionsCount(0)
    , NewConfigShouldBeApplied(false)
    , TabletState(NKikimrPQ::ENormal)
    , Counters(nullptr)
    , NextResponseCookie(0)
    , ResourceMetrics(nullptr)
    , PipeClientCacheConfig(new NTabletPipe::TBoundedClientCacheConfig())
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(PipeClientCacheConfig, GetPipeClientConfig()))
{
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
    Counters = (counters->GetSecondTabletCounters()).Release();

    State.SetupTabletCounters(counters->GetFirstTabletCounters().Release()); //FirstTabletCounters is of good type and contains all counters
    State.Clear();
}

void TPersQueue::CreatedHook(const TActorContext& ctx)
{
    IsServerless = AppData(ctx)->FeatureFlags.GetEnableDbCounters(); //TODO: find out it via describe
    CacheActor = ctx.Register(new TPQCacheProxy(ctx.SelfID, TabletID()));

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
    Y_ABORT_UNLESS(ProcessingParams);
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
    Y_ABORT_UNLESS(message->TabletId == TabletID());

    MediatorTimeCastEntry = message->Entry;
    Y_ABORT_UNLESS(MediatorTimeCastEntry);

    PQ_LOG_D("Registered with mediator time cast");

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvInterconnect::TEvNodeInfo");

    Y_ABORT_UNLESS(ev->Get()->Node);
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
    ctx.Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
}

void TPersQueue::DeleteExpiredTransactions(const TActorContext& ctx)
{
    if (!MediatorTimeCastEntry) {
        return;
    }

    ui64 step = MediatorTimeCastEntry->Get(TabletID());

    for (auto& [txId, tx] : Txs) {
        if ((tx.MaxStep < step) && (tx.State <= NKikimrPQ::TTransaction::PREPARED)) {
            DeleteTx(tx);
        }
    }

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPersQueue::TEvCancelTransactionProposal::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPersQueue::TEvCancelTransactionProposal");

    NKikimrPQ::TEvCancelTransactionProposal& event = ev->Get()->Record;
    Y_ABORT_UNLESS(event.HasTxId());

    if (auto tx = GetTransaction(ctx, event.GetTxId()); tx) {
        Y_ABORT_UNLESS(tx->State <= NKikimrPQ::TTransaction::PREPARED);

        DeleteTx(*tx);

        TryWriteTxs(ctx);
    }
}

void TPersQueue::Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx)
{
    const NKikimrPQ::TEvProposeTransaction& event = ev->Get()->GetRecord();
    PQ_LOG_D("Handle TEvPersQueue::TEvProposeTransaction " << event.ShortDebugString());

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
    TPartitionId partitionId(operation.GetPartitionId(),
                             writeId,
                             operation.GetSupportivePartition());
    PQ_LOG_D("PartitionId " << partitionId << " for WriteId " << writeId);
    return Partitions.contains(partitionId);
}

bool TPersQueue::CheckTxWriteOperations(const NKikimrPQ::TDataTransaction& txBody) const
{
    if (!txBody.HasWriteId()) {
        return true;
    }

    const TWriteId writeId = GetWriteId(txBody);

    for (auto& operation : txBody.GetOperations()) {
        auto isWrite = [](const NKikimrPQ::TPartitionOperation& o) {
            return !o.HasBegin();
        };

        if (isWrite(operation)) {
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
    NKikimrPQ::TEvProposeTransaction& event = *ev->MutableRecord();
    Y_ABORT_UNLESS(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_ABORT_UNLESS(event.HasData());
    const NKikimrPQ::TDataTransaction& txBody = event.GetData();

    if (TabletState != NKikimrPQ::ENormal) {
        PQ_LOG_D("TxId " << event.GetTxId() << " invalid PQ tablet state (" << NKikimrPQ::ETabletState_Name(TabletState) << ")");
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::ERROR,
                                    "invalid PQ tablet state",
                                    ctx);
        return;
    }

    //
    // TODO(abcdef): сохранить пока инициализируемся. TEvPersQueue::TEvHasDataInfo::TPtr как образец. не только конфиг. Inited==true
    //

    if (txBody.OperationsSize() <= 0) {
        PQ_LOG_D("TxId " << event.GetTxId() << " empty list of operations");
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::BAD_REQUEST,
                                    "empty list of operations",
                                    ctx);
        return;
    }

    if (!CheckTxWriteOperations(txBody)) {
        PQ_LOG_D("TxId " << event.GetTxId() << " invalid WriteId " << txBody.GetWriteId());
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
            PQ_LOG_D("TxId " << event.GetTxId() << " unknown WriteId " << writeId);
            SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                        event.GetTxId(),
                                        NKikimrPQ::TError::BAD_REQUEST,
                                        "unknown WriteId",
                                        ctx);
            return;
        }

        TTxWriteInfo& writeInfo = TxWrites.at(writeId);
        if (writeInfo.Deleting) {
            PQ_LOG_W("TxId " << event.GetTxId() << " WriteId " << writeId << " will be deleted");
            SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                        event.GetTxId(),
                                        NKikimrPQ::TError::BAD_REQUEST,
                                        "WriteId will be deleted",
                                        ctx);
            return;
        }

        writeInfo.TxId = event.GetTxId();
        PQ_LOG_D("TxId " << event.GetTxId() << " has WriteId " << writeId);
    }

    TMaybe<TPartitionId> partitionId = FindPartitionId(txBody);
    if (!partitionId.Defined()) {
        PQ_LOG_W("TxId " << event.GetTxId() << " unknown partition for WriteId " << txBody.GetWriteId());
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::INTERNAL,
                                    "unknown supportive partition",
                                    ctx);
        return;
    }

    if (!Partitions.contains(*partitionId)) {
        PQ_LOG_D("unknown partition " << *partitionId);
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    NKikimrPQ::TError::INTERNAL,
                                    "unknown supportive partition",
                                    ctx);
        return;
    }


    if (txBody.GetImmediate()) {
        PQ_LOG_D("immediate transaction");
        TPartitionId originalPartitionId(txBody.GetOperations(0).GetPartitionId());
        const TPartitionInfo& partition = Partitions.at(originalPartitionId);

        if (txBody.HasWriteId()) {
            // the condition `Partition.contains(*partitioned)` is checked above
            const TPartitionInfo& partition = Partitions.at(*partitionId);
            ActorIdToProto(partition.Actor, event.MutableSupportivePartitionActor());
        }

        ctx.Send(partition.Actor, ev.Release());
    } else {
        PQ_LOG_D("distributed transaction");
        EvProposeTransactionQueue.emplace_back(ev.Release());

        TryWriteTxs(ctx);
    }
}

void TPersQueue::HandleConfigTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> ev,
                                         const TActorContext& ctx)
{
    const NKikimrPQ::TEvProposeTransaction& event = ev->GetRecord();
    Y_ABORT_UNLESS(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kConfig);
    Y_ABORT_UNLESS(event.HasConfig());

    EvProposeTransactionQueue.emplace_back(ev.Release());

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvTxProcessing::TEvPlanStep " << ev->Get()->Record.ShortDebugString());

    EvPlanStepQueue.emplace_back(ev->Sender, ev->Release().Release());

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvTxProcessing::TEvReadSet " << ev->Get()->Record.ShortDebugString());

    NKikimrTx::TEvReadSet& event = ev->Get()->Record;
    Y_ABORT_UNLESS(event.HasTxId());

    std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack;
    if (!(event.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_ACK)) {
        ack = std::make_unique<TEvTxProcessing::TEvReadSetAck>(*ev->Get(), TabletID());
    }

    if (auto tx = GetTransaction(ctx, event.GetTxId()); tx && tx->PredicatesReceived.contains(event.GetTabletProducer())) {
        if (tx->State >= NKikimrPQ::TTransaction::EXECUTED) {
            if (ack) {
                PQ_LOG_D("send TEvReadSetAck to " << event.GetTabletProducer());
                ctx.Send(ev->Sender, ack.release());
                return;
            }
        }

        tx->OnReadSet(event, ev->Sender, std::move(ack));

        if (tx->State == NKikimrPQ::TTransaction::WAIT_RS) {
            TryExecuteTxs(ctx, *tx);

            TryWriteTxs(ctx);
        }
    } else if (ack) {
        PQ_LOG_D("send TEvReadSetAck to " << event.GetTabletProducer());
        //
        // для неизвестных транзакций подтверждение отправляется сразу
        //
        ctx.Send(ev->Sender, ack.release());
    }
}

void TPersQueue::Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvTxProcessing::TEvReadSetAck " << ev->Get()->Record.ShortDebugString());

    NKikimrTx::TEvReadSetAck& event = ev->Get()->Record;
    Y_ABORT_UNLESS(event.HasTxId());

    auto tx = GetTransaction(ctx, event.GetTxId());
    if (!tx) {
        return;
    }

    tx->OnReadSetAck(event);
    tx->UnbindMsgsFromPipe(event.GetTabletConsumer());

    if (tx->State == NKikimrPQ::TTransaction::WAIT_RS_ACKS) {
        TryExecuteTxs(ctx, *tx);

        TryWriteTxs(ctx);
    }
}

void TPersQueue::Handle(TEvPQ::TEvTxCalcPredicateResult::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvTxCalcPredicateResult& event = *ev->Get();

    PQ_LOG_D("Handle TEvPQ::TEvTxCalcPredicateResult" <<
             " Step " << event.Step <<
             ", TxId " << event.TxId <<
             ", Partition " << event.Partition <<
             ", Predicate " << event.Predicate);

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    Y_ABORT_UNLESS(tx->State == NKikimrPQ::TTransaction::CALCULATING);

    tx->OnTxCalcPredicateResult(event);

    TryExecuteTxs(ctx, *tx);

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPQ::TEvProposePartitionConfigResult::TPtr& ev, const TActorContext& ctx)
{
    TEvPQ::TEvProposePartitionConfigResult& event = *ev->Get();

    PQ_LOG_D("Handle TEvPQ::TEvProposePartitionConfigResult" <<
             " Step " << event.Step <<
             ", TxId " << event.TxId <<
             ", Partition " << event.Partition);

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    Y_ABORT_UNLESS(tx->State == NKikimrPQ::TTransaction::CALCULATING);

    tx->OnProposePartitionConfigResult(event);

    TryExecuteTxs(ctx, *tx);

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPQ::TEvTxCommitDone::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvTxCommitDone& event = *ev->Get();

    PQ_LOG_D("Handle TEvPQ::TEvTxCommitDone" <<
             " Step " << event.Step <<
             ", TxId " << event.TxId <<
             ", Partition " << event.Partition);

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    Y_ABORT_UNLESS(tx->State == NKikimrPQ::TTransaction::EXECUTING);

    tx->OnTxCommitDone(event);

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

bool TPersQueue::CanProcessPlanStepQueue() const
{
    return !EvPlanStepQueue.empty();
}

bool TPersQueue::CanProcessWriteTxs() const
{
    return !WriteTxs.empty();
}

bool TPersQueue::CanProcessDeleteTxs() const
{
    return !DeleteTxs.empty();
}

bool TPersQueue::CanProcessTxWrites() const
{
    return !NewSupportivePartitions.empty();
}

void TPersQueue::SubscribeWriteId(const TWriteId& writeId,
                                  const TActorContext& ctx)
{
    PQ_LOG_D("send TEvSubscribeLock for WriteId " << writeId);
    ctx.Send(NLongTxService::MakeLongTxServiceID(writeId.NodeId),
             new NLongTxService::TEvLongTxService::TEvSubscribeLock(writeId.KeyId, writeId.NodeId));
}

void TPersQueue::UnsubscribeWriteId(const TWriteId& writeId,
                                    const TActorContext& ctx)
{
    PQ_LOG_D("send TEvUnsubscribeLock for WriteId " << writeId);
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
    Y_ABORT_UNLESS(!WriteTxsInProgress);

    bool canProcess =
        CanProcessProposeTransactionQueue() ||
        CanProcessPlanStepQueue() ||
        CanProcessWriteTxs() ||
        CanProcessDeleteTxs() ||
        CanProcessTxWrites() ||
        TxWritesChanged
        ;
    if (!canProcess) {
        return;
    }

    auto request = MakeHolder<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(WRITE_TX_COOKIE);

    ProcessProposeTransactionQueue(ctx);
    ProcessPlanStepQueue(ctx);
    ProcessWriteTxs(ctx, request->Record);
    ProcessDeleteTxs(ctx, request->Record);
    AddCmdWriteTabletTxInfo(request->Record);
    ProcessConfigTx(ctx, request.Get());

    WriteTxsInProgress = true;

    PendingSupportivePartitions = std::move(NewSupportivePartitions);
    NewSupportivePartitions.clear();

    PQ_LOG_D("Send TEvKeyValue::TEvRequest (WRITE_TX_COOKIE)");
    ctx.Send(ctx.SelfID, request.Release());

    TryReturnTabletStateAll(ctx);
}

void TPersQueue::EndWriteTxs(const NKikimrClient::TResponse& resp,
                             const TActorContext& ctx)
{
    Y_ABORT_UNLESS(WriteTxsInProgress);

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

void TPersQueue::ProcessProposeTransactionQueue(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(!WriteTxsInProgress);

    while (CanProcessProposeTransactionQueue()) {
        const auto front = std::move(EvProposeTransactionQueue.front());
        EvProposeTransactionQueue.pop_front();

        const NKikimrPQ::TEvProposeTransaction& event = front->GetRecord();
        TDistributedTransaction& tx = Txs[event.GetTxId()];

        switch (tx.State) {
        case NKikimrPQ::TTransaction::UNKNOWN:
            tx.OnProposeTransaction(event, GetAllowedStep(),
                                    TabletID());
            PQ_LOG_D("Propose TxId " << tx.TxId << ", WriteId " << tx.WriteId);

            if (tx.Kind == NKikimrPQ::TTransaction::KIND_CONFIG) {
                UpdateReadRuleGenerations(tx.TabletConfig);
            }

            if (tx.WriteId.Defined()) {
                const TWriteId& writeId = *tx.WriteId;
                Y_ABORT_UNLESS(TxWrites.contains(writeId),
                               "PQ %" PRIu64 ", TxId %" PRIu64 ", WriteId {%" PRIu64 ", %" PRIu64 "}",
                               TabletID(), tx.TxId, writeId.NodeId, writeId.KeyId);
                TTxWriteInfo& writeInfo = TxWrites.at(writeId);
                PQ_LOG_D("Link TxId " << tx.TxId << " with WriteId " << writeId);
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

void TPersQueue::ProcessPlanStepQueue(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(!WriteTxsInProgress);

    while (CanProcessPlanStepQueue()) {
        const auto front = std::move(EvPlanStepQueue.front());
        EvPlanStepQueue.pop_front();

        const TActorId& sender = front.first;
        const NKikimrTx::TEvMediatorPlanStep& event = front.second->Record;

        ui64 step = event.GetStep();

        TVector<ui64> txIds;
        THashMap<TActorId, TVector<ui64>> txAcks;

        for (auto& tx : event.GetTransactions()) {
            Y_ABORT_UNLESS(tx.HasTxId());
            Y_ABORT_UNLESS(tx.HasAckTo());

            txIds.push_back(tx.GetTxId());
            txAcks[ActorIdFromProto(tx.GetAckTo())].push_back(tx.GetTxId());
        }

        if (step >= PlanStep) {
            ui64 lastPlannedTxId = 0;

            for (ui64 txId : txIds) {
                Y_ABORT_UNLESS(lastPlannedTxId < txId);

                if (auto p = Txs.find(txId); p != Txs.end()) {
                    TDistributedTransaction& tx = p->second;

                    Y_ABORT_UNLESS(tx.MaxStep >= step);

                    if (tx.Step == Max<ui64>()) {
                        Y_ABORT_UNLESS(TxQueue.empty() || (TxQueue.back() < std::make_pair(step, txId)));

                        tx.OnPlanStep(step);
                        TryExecuteTxs(ctx, tx);

                        TxQueue.emplace_back(step, txId);
                    } else {
                        PQ_LOG_W("Transaction already planned for step " << tx.Step <<
                                 ", Step: " << step <<
                                 ", TxId: " << txId);
                    }
                } else {
                    PQ_LOG_W("Unknown transaction " << txId <<
                             ", Step: " << step);
                }

                lastPlannedTxId = txId;
            }

            PlanStep = step;
            PlanTxId = lastPlannedTxId;

            PQ_LOG_D("PlanStep " << PlanStep << ", PlanTxId " << PlanTxId);
        } else {
            PQ_LOG_ERROR("Old plan step " << step <<
                        ", PlanStep: " << PlanStep);
        }

        SchedulePlanStepAck(step, txAcks);
        SchedulePlanStepAccepted(sender, step);
    }
}

void TPersQueue::ProcessWriteTxs(const TActorContext& ctx,
                                 NKikimrClient::TKeyValueRequest& request)
{
    Y_ABORT_UNLESS(!WriteTxsInProgress);

    for (auto& [txId, state] : WriteTxs) {
        auto tx = GetTransaction(ctx, txId);
        Y_ABORT_UNLESS(tx);

        tx->AddCmdWrite(request, state);

        ChangedTxs.emplace(tx->Step, txId);
    }

    WriteTxs.clear();
}

void TPersQueue::ProcessDeleteTxs(const TActorContext& ctx,
                                  NKikimrClient::TKeyValueRequest& request)
{
    Y_ABORT_UNLESS(!WriteTxsInProgress,
                   "PQ %" PRIu64,
                   TabletID());

    for (ui64 txId : DeleteTxs) {
        PQ_LOG_D("delete key for TxId " << txId);
        AddCmdDeleteTx(request, txId);

        auto tx = GetTransaction(ctx, txId);
        if (tx) {
            ChangedTxs.emplace(tx->Step, txId);
        }
    }

    DeleteTxs.clear();
}

void TPersQueue::AddCmdDeleteTx(NKikimrClient::TKeyValueRequest& request,
                                ui64 txId)
{
    TString key = GetTxKey(txId);
    auto range = request.AddCmdDeleteRange()->MutableRange();
    range->SetFrom(key);
    range->SetIncludeFrom(true);
    range->SetTo(key);
    range->SetIncludeTo(true);
}

void TPersQueue::ProcessConfigTx(const TActorContext& ctx,
                                 TEvKeyValue::TEvRequest* request)
{
    Y_ABORT_UNLESS(!WriteTxsInProgress);

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
    Y_ABORT_UNLESS(info.SerializeToString(&value));

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
    for (auto& [writeId, write] : TxWrites) {
        if (write.Partitions.empty()) {
            auto* txWrite = info.MutableTxWrites()->Add();
            SetWriteId(*txWrite, writeId);
        } else {
            for (auto [partitionId, shadowPartitionId] : write.Partitions) {
                auto* txWrite = info.MutableTxWrites()->Add();
                SetWriteId(*txWrite, writeId);
                txWrite->SetOriginalPartitionId(partitionId);
                txWrite->SetInternalPartitionId(shadowPartitionId.InternalPartitionId);
            }
        }
    }

    info.SetNextSupportivePartitionId(NextSupportivePartitionId);
}

void TPersQueue::ScheduleProposeTransactionResult(const TDistributedTransaction& tx)
{
    PQ_LOG_D("schedule TEvProposeTransactionResult(PREPARED)");
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

void TPersQueue::SchedulePlanStepAck(ui64 step,
                                     const THashMap<TActorId, TVector<ui64>>& txAcks)
{
    for (auto& [actorId, txIds] : txAcks) {
        auto event = std::make_unique<TEvTxProcessing::TEvPlanStepAck>(TabletID(),
                                                                       step,
                                                                       txIds.begin(), txIds.end());

        RepliesToActor.emplace_back(actorId, std::move(event));
    }
}

void TPersQueue::SchedulePlanStepAccepted(const TActorId& actorId,
                                          ui64 step)
{
    auto event = std::make_unique<TEvTxProcessing::TEvPlanStepAccepted>(TabletID(), step);

    RepliesToActor.emplace_back(actorId, std::move(event));
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
    Y_ABORT_UNLESS(data.SerializeToString(&body));

    PQ_LOG_D("Send TEvTxProcessing::TEvReadSet to " << tx.PredicateRecipients.size() << " receivers. Wait TEvTxProcessing::TEvReadSet from " << tx.PredicatesReceived.size() << " senders.");
    for (auto& [receiverId, _] : tx.PredicateRecipients) {
        if (receiverId != TabletID()) {
            auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(tx.Step,
                                                                       tx.TxId,
                                                                       TabletID(),
                                                                       receiverId,
                                                                       TabletID(),
                                                                       body,
                                                                       0);
            PQ_LOG_D("Send TEvReadSet to tablet " << receiverId);
            SendToPipe(receiverId, tx, std::move(event), ctx);
        }
    }
}

void TPersQueue::SendEvReadSetAckToSenders(const TActorContext& ctx,
                                           TDistributedTransaction& tx)
{
    PQ_LOG_D("TPersQueue::SendEvReadSetAckToSenders");
    for (auto& [target, event] : tx.ReadSetAcks) {
        PQ_LOG_D("Send TEvTxProcessing::TEvReadSetAck " << event->ToString());
        ctx.Send(target, event.release());
    }
}

TMaybe<TPartitionId> TPersQueue::FindPartitionId(const NKikimrPQ::TDataTransaction& txBody) const
{
    auto hasWriteOperation = [](const auto& txBody) {
        for (const auto& o : txBody.GetOperations()) {
            if (!o.HasBegin()) {
                return true;
            }
        }
        return false;
    };

    ui32 partitionId = txBody.GetOperations(0).GetPartitionId();

    if (txBody.HasWriteId() && hasWriteOperation(txBody)) {
        const TWriteId writeId = GetWriteId(txBody);
        if (!TxWrites.contains(writeId)) {
            PQ_LOG_D("unknown WriteId " << writeId);
            return Nothing();
        }

        const TTxWriteInfo& writeInfo = TxWrites.at(writeId);
        if (!writeInfo.Partitions.contains(partitionId)) {
            PQ_LOG_D("unknown partition " << partitionId << " for WriteId " << writeId);
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
    THashMap<ui32, std::unique_ptr<TEvPQ::TEvTxCalcPredicate>> events;

    for (auto& operation : tx.Operations) {
        ui32 originalPartitionId = operation.GetPartitionId();
        auto& event = events[originalPartitionId];
        if (!event) {
            event = std::make_unique<TEvPQ::TEvTxCalcPredicate>(tx.Step, tx.TxId);
        }

        if (operation.HasBegin()) {
            event->AddOperation(operation.GetConsumer(),
                                operation.GetBegin(),
                                operation.GetEnd());
        }
    }

    if (tx.WriteId.Defined()) {
        const TWriteId& writeId = *tx.WriteId;
        Y_ABORT_UNLESS(TxWrites.contains(writeId),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", WriteId {%" PRIu64 ", %" PRIu64 "}",
                       TabletID(), tx.TxId, writeId.NodeId, writeId.KeyId);
        const TTxWriteInfo& writeInfo = TxWrites.at(writeId);

        for (auto& [originalPartitionId, partitionId] : writeInfo.Partitions) {
            Y_ABORT_UNLESS(Partitions.contains(partitionId));
            const TPartitionInfo& partition = Partitions.at(partitionId);

            auto& event = events[originalPartitionId];
            if (!event) {
                event = std::make_unique<TEvPQ::TEvTxCalcPredicate>(tx.Step, tx.TxId);
            }

            event->SupportivePartitionActor = partition.Actor;
        }
    }

    for (auto& [originalPartitionId, event] : events) {
        TPartitionId partitionId(originalPartitionId);
        Y_ABORT_UNLESS(Partitions.contains(partitionId));
        const TPartitionInfo& partition = Partitions.at(partitionId);

        ctx.Send(partition.Actor, event.release());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = events.size();
}

void TPersQueue::SendEvTxCommitToPartitions(const TActorContext& ctx,
                                            TDistributedTransaction& tx)
{
    PQ_LOG_T("Commit tx " << tx.TxId);

    auto graph = MakePartitionGraph(tx.TabletConfig);
    for (ui32 partitionId : tx.Partitions) {
        auto explicitMessageGroups = CreateExplicitMessageGroups(tx.BootstrapConfig, tx.PartitionsData, graph, partitionId);
        auto event = std::make_unique<TEvPQ::TEvTxCommit>(tx.Step, tx.TxId, explicitMessageGroups);

        auto p = Partitions.find(TPartitionId(partitionId));
        Y_ABORT_UNLESS(p != Partitions.end(),
                       "PQ %" PRIu64 ", Partition %" PRIu32 ", TxId %" PRIu64,
                       TabletID(), partitionId, tx.TxId);

        ctx.Send(p->second.Actor, event.release());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = tx.Partitions.size();
}

void TPersQueue::SendEvTxRollbackToPartitions(const TActorContext& ctx,
                                              TDistributedTransaction& tx)
{
    PQ_LOG_T("Rollback tx " << tx.TxId);

    for (ui32 partitionId : tx.Partitions) {
        auto event = std::make_unique<TEvPQ::TEvTxRollback>(tx.Step, tx.TxId);

        auto p = Partitions.find(TPartitionId(partitionId));
        Y_ABORT_UNLESS(p != Partitions.end());

        ctx.Send(p->second.Actor, event.release());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = 0;
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

    PQ_LOG_D("TxId: " << tx.TxId <<
             " send TEvPersQueue::TEvProposeTransactionResult(" <<
             NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(result->Record.GetStatus()) <<
             ")");
    ctx.Send(tx.SourceActor, std::move(result));
}

void TPersQueue::SendToPipe(ui64 tabletId,
                            TDistributedTransaction& tx,
                            std::unique_ptr<IEventBase> event,
                            const TActorContext& ctx)
{
    Y_ABORT_UNLESS(event);

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
        PQ_LOG_W("Unknown transaction " << txId);
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
    tx.State = newState;

    PQ_LOG_D("TxId " << tx.TxId <<
             ", NewState " << NKikimrPQ::TTransaction_EState_Name(tx.State));
}

bool TPersQueue::TryChangeTxState(TDistributedTransaction& tx,
                                  TDistributedTransaction::EState newState)
{
    auto oldState = tx.State;
    Y_ABORT_UNLESS(TxsOrder.contains(oldState) || (oldState == NKikimrPQ::TTransaction::PLANNING),
                   "PQ %" PRIu64 ", TxId %" PRIu64 ", State %s",
                   TabletID(), tx.TxId, NKikimrPQ::TTransaction_EState_Name(oldState).data());

    if (oldState != NKikimrPQ::TTransaction::PLANNING) {
        Y_ABORT_UNLESS(TxsOrder.contains(oldState),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", State %s",
                       TabletID(), tx.TxId,
                       NKikimrPQ::TTransaction_EState_Name(oldState).data());
        Y_ABORT_UNLESS(TxsOrder[oldState].front() == tx.TxId,
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", State %s, FrontTxId %" PRIu64,
                       TabletID(), tx.TxId,
                       NKikimrPQ::TTransaction_EState_Name(oldState).data(),
                       TxsOrder[oldState].front());
    }

    ChangeTxState(tx, newState);

    if (oldState != NKikimrPQ::TTransaction::PLANNING) {
        TxsOrder[oldState].pop_front();
    }
    if (TxsOrder.contains(newState)) {
        PushTxInQueue(tx, newState);
    }

    PQ_LOG_D("TxId " << tx.TxId << " moved from " <<
             NKikimrPQ::TTransaction_EState_Name(oldState) <<
             " to " <<
             NKikimrPQ::TTransaction_EState_Name(newState));

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
    Y_ABORT_UNLESS(!txQueue.empty(),
                   "PQ %" PRIu64 ", TxId %" PRIu64 ", State %s",
                   TabletID(), tx.TxId, NKikimrPQ::TTransaction_EState_Name(tx.State).data());

    PQ_LOG_D("TxId " << tx.TxId <<
             " State " << NKikimrPQ::TTransaction_EState_Name(tx.State) <<
             " FrontTxId " << txQueue.front());

    return txQueue.front() == tx.TxId;
}

void TPersQueue::TryExecuteTxs(const TActorContext& ctx,
                               TDistributedTransaction& tx)
{
    PQ_LOG_D("Try execute txs with state " << NKikimrPQ::TTransaction_EState_Name(tx.State));

    TDistributedTransaction::EState oldState = tx.State;

    CheckTxState(ctx, tx);

    if (!TxsOrder.contains(oldState)) {
        // This transaction is either not scheduled or has already been completed.
        return;
    }

    if (oldState == tx.State) {
        // The transaction status has not changed. There is no point in watching the transactions behind her.
        PQ_LOG_D("TxId " << tx.TxId << " status has not changed");
        return;
    }

    auto& txQueue = TxsOrder[oldState];
    while (!txQueue.empty()) {
        PQ_LOG_D("There are " << txQueue.size() << " txs in the queue " << NKikimrPQ::TTransaction_EState_Name(oldState));
        ui64 txId = txQueue.front();
        Y_ABORT_UNLESS(Txs.contains(txId), "unknown TxId %" PRIu64, txId);
        auto& tx = Txs.at(txId);
        PQ_LOG_D("Try execute TxId " << tx.TxId << " Pending " << tx.Pending);

        if (!tx.Pending) {
            // The transaction was not postponed for execution.
            break;
        }
        tx.Pending = false;

        CheckTxState(ctx, tx);

        if (oldState == tx.State) {
            // The transaction status has not changed. There is no point in watching the transactions behind her.
            PQ_LOG_D("TxId " << tx.TxId << " status has not changed");
            break;
        }
    }
}

void TPersQueue::CheckTxState(const TActorContext& ctx,
                              TDistributedTransaction& tx)
{
    PQ_LOG_D("TxId " << tx.TxId <<
             ", State " << NKikimrPQ::TTransaction_EState_Name(tx.State));

    if (!CanExecute(tx)) {
        PQ_LOG_D("Can't execute TxId " << tx.TxId << " Pending " << tx.Pending);
        tx.Pending = true;
        PQ_LOG_D("Wait for TxId " << tx.TxId);
        return;
    }

    switch (tx.State) {
    case NKikimrPQ::TTransaction::UNKNOWN:
        Y_ABORT_UNLESS(tx.TxId != Max<ui64>(),
                       "PQ %" PRIu64 ", TxId %" PRIu64,
                       TabletID(), tx.TxId);

        WriteTx(tx, NKikimrPQ::TTransaction::PREPARED);
        ScheduleProposeTransactionResult(tx);

        ChangeTxState(tx, NKikimrPQ::TTransaction::PREPARING);

        break;

    case NKikimrPQ::TTransaction::PREPARING:
        Y_ABORT_UNLESS(tx.WriteInProgress,
                       "PQ %" PRIu64 ", TxId %" PRIu64,
                       TabletID(), tx.TxId);

        tx.WriteInProgress = false;

        // scheduled events will be sent to EndWriteTxs

        ChangeTxState(tx, NKikimrPQ::TTransaction::PREPARED);

        break;

    case NKikimrPQ::TTransaction::PREPARED:
        Y_ABORT_UNLESS(tx.Step != Max<ui64>(),
                       "PQ %" PRIu64 ", TxId %" PRIu64,
                       TabletID(), tx.TxId);

        WriteTx(tx, NKikimrPQ::TTransaction::PLANNED);

        ChangeTxState(tx, NKikimrPQ::TTransaction::PLANNING);

        break;

    case NKikimrPQ::TTransaction::PLANNING:
        Y_ABORT_UNLESS(tx.WriteInProgress,
                       "PQ %" PRIu64 ", TxId %" PRIu64,
                       TabletID(), tx.TxId);

        tx.WriteInProgress = false;

        // scheduled events will be sent to EndWriteTxs

        TryChangeTxState(tx, NKikimrPQ::TTransaction::PLANNED);

        if (tx.TxId != TxsOrder[tx.State].front()) {
            break;
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::PLANNED:
        Y_ABORT_UNLESS(tx.TxId == TxsOrder[tx.State].front(),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", FrontTxId %" PRIu64,
                       TabletID(), tx.TxId, TxsOrder[tx.State].front());

        PQ_LOG_D("TxQueue.size " << TxQueue.size());

        MoveTopTxToCalculating(tx, ctx);

        break;

    case NKikimrPQ::TTransaction::CALCULATING:
        Y_ABORT_UNLESS(tx.TxId == TxsOrder[tx.State].front(),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", FrontTxId %" PRIu64,
                       TabletID(), tx.TxId, TxsOrder[tx.State].front());
        Y_ABORT_UNLESS(tx.PartitionRepliesCount <= tx.PartitionRepliesExpected,
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", PartitionRepliesCount %" PRISZT ", PartitionRepliesExpected %" PRISZT,
                       TabletID(), tx.TxId,
                       tx.PartitionRepliesCount, tx.PartitionRepliesExpected);

        PQ_LOG_D("Received " << tx.PartitionRepliesCount <<
                 ", Expected " << tx.PartitionRepliesExpected);

        if (tx.PartitionRepliesCount == tx.PartitionRepliesExpected) {
            switch (tx.Kind) {
            case NKikimrPQ::TTransaction::KIND_DATA:
            case NKikimrPQ::TTransaction::KIND_CONFIG:
                WriteTx(tx, NKikimrPQ::TTransaction::CALCULATED);

                TryChangeTxState(tx, NKikimrPQ::TTransaction::CALCULATED);

                break;

            case NKikimrPQ::TTransaction::KIND_UNKNOWN:
                Y_ABORT_UNLESS(false);
            }
        }

        break;

    case NKikimrPQ::TTransaction::CALCULATED:
        Y_ABORT_UNLESS(tx.TxId == TxsOrder[tx.State].front(),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", FrontTxId %" PRIu64,
                       TabletID(), tx.TxId, TxsOrder[tx.State].front());

        TryChangeTxState(tx, NKikimrPQ::TTransaction::WAIT_RS);

        //
        // the number of TEvReadSetAck sent should not be greater than the number of senders
        // from TEvProposeTransaction
        //
        Y_ABORT_UNLESS(tx.ReadSetAcks.size() <= tx.PredicatesReceived.size(),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", ReadSetAcks.size %" PRISZT ", PredicatesReceived.size %" PRISZT,
                       TabletID(), tx.TxId,
                       tx.ReadSetAcks.size(), tx.PredicatesReceived.size());

        SendEvReadSetToReceivers(ctx, tx);

        if (tx.TxId != TxsOrder[tx.State].front()) {
            break;
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::WAIT_RS:
        Y_ABORT_UNLESS(tx.TxId == TxsOrder[tx.State].front(),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", FrontTxId %" PRIu64,
                       TabletID(), tx.TxId, TxsOrder[tx.State].front());

        PQ_LOG_D("HaveParticipantsDecision " << tx.HaveParticipantsDecision());

        if (tx.HaveParticipantsDecision()) {
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
        Y_ABORT_UNLESS(tx.TxId == TxsOrder[tx.State].front(),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", FrontTxId %" PRIu64,
                       TabletID(), tx.TxId, TxsOrder[tx.State].front());
        Y_ABORT_UNLESS(tx.PartitionRepliesCount <= tx.PartitionRepliesExpected,
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", PartitionRepliesCount %" PRISZT ", PartitionRepliesExpected %" PRISZT,
                       TabletID(), tx.TxId,
                       tx.PartitionRepliesCount, tx.PartitionRepliesExpected);

        PQ_LOG_D("Received " << tx.PartitionRepliesCount <<
                 ", Expected " << tx.PartitionRepliesExpected);

        if (tx.PartitionRepliesCount == tx.PartitionRepliesExpected) {
            SendEvProposeTransactionResult(ctx, tx);
            PQ_LOG_D("complete TxId " << tx.TxId);

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
                Y_ABORT_UNLESS(false);
            }

            WriteTx(tx, NKikimrPQ::TTransaction::EXECUTED);

            PQ_LOG_D("delete partitions for TxId " << tx.TxId);
            BeginDeletePartitions(tx);

            TryChangeTxState(tx, NKikimrPQ::TTransaction::EXECUTED);
        }

        break;

    case NKikimrPQ::TTransaction::EXECUTED:
        Y_ABORT_UNLESS(tx.TxId == TxsOrder[tx.State].front(),
                       "PQ %" PRIu64 ", TxId %" PRIu64 ", FrontTxId %" PRIu64,
                       TabletID(), tx.TxId, TxsOrder[tx.State].front());

        SendEvReadSetAckToSenders(ctx, tx);

        TryChangeTxState(tx, NKikimrPQ::TTransaction::WAIT_RS_ACKS);

        [[fallthrough]];

    case NKikimrPQ::TTransaction::WAIT_RS_ACKS:
        PQ_LOG_D("HaveAllRecipientsReceive " << tx.HaveAllRecipientsReceive() <<
                 ", AllSupportivePartitionsHaveBeenDeleted " << AllSupportivePartitionsHaveBeenDeleted(tx.WriteId));
        if (tx.HaveAllRecipientsReceive() && AllSupportivePartitionsHaveBeenDeleted(tx.WriteId)) {
            DeleteTx(tx);
            // implicitly switch to the state DELETING
        }

        break;

    case NKikimrPQ::TTransaction::DELETING:
        // The PQ tablet has persisted its state. Now she can delete the transaction and take the next one.
        if (!TxQueue.empty() && (TxQueue.front().second == tx.TxId)) {
            TxQueue.pop_front();
        }

        DeleteWriteId(tx.WriteId);
        PQ_LOG_D("delete TxId " << tx.TxId);
        Txs.erase(tx.TxId);

        // If this was the last transaction, then you need to send responses to messages about changes
        // in the status of the PQ tablet (if they came)
        TryReturnTabletStateAll(ctx);
        break;
    }
}

bool TPersQueue::AllSupportivePartitionsHaveBeenDeleted(const TMaybe<TWriteId>& writeId) const
{
    if (!writeId.Defined()) {
        return true;
    }

    Y_ABORT_UNLESS(TxWrites.contains(*writeId),
                   "PQ %" PRIu64 ", WriteId {%" PRIu64 ", %" PRIu64 "}",
                   TabletID(), writeId->NodeId, writeId->KeyId);
    const TTxWriteInfo& writeInfo = TxWrites.at(*writeId);

    PQ_LOG_D("WriteId " << *writeId <<
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

    PQ_LOG_D("delete WriteId " << *writeId);
    TxWrites.erase(*writeId);
}

void TPersQueue::WriteTx(TDistributedTransaction& tx, NKikimrPQ::TTransaction::EState state)
{
    WriteTxs[tx.TxId] = state;

    tx.WriteInProgress = true;
}

void TPersQueue::DeleteTx(TDistributedTransaction& tx)
{
    PQ_LOG_D("add an TxId " << tx.TxId << " to the list for deletion");

    DeleteTxs.insert(tx.TxId);

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
        Y_ABORT_UNLESS(tx,
                       "PQ %" PRIu64 ", TxId %" PRIu64,
                       TabletID(), txId);

        TryExecuteTxs(ctx, *tx);
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
    Y_ABORT_UNLESS(ProcessingParams);

    if (ProcessingParams->MediatorsSize()) {
        UseMediatorTimeCast = true;
        MediatorTimeCastRegisterTablet(ctx);
    } else {
        UseMediatorTimeCast = false;

        TryWriteTxs(ctx);
    }
}

bool TPersQueue::AllTransactionsHaveBeenProcessed() const
{
    return EvProposeTransactionQueue.empty() && Txs.empty();
}

void TPersQueue::SendProposeTransactionAbort(const TActorId& target,
                                             ui64 txId,
                                             NKikimrPQ::TError::EKind kind,
                                             const TString& reason,
                                             const TActorContext& ctx)
{
    auto event = std::make_unique<TEvPersQueue::TEvProposeTransactionResult>();

    event->Record.SetOrigin(TabletID());
    event->Record.SetStatus(NKikimrPQ::TEvProposeTransactionResult::ABORTED);
    event->Record.SetTxId(txId);

    if (kind != NKikimrPQ::TError::OK) {
        auto* error = event->Record.MutableErrors()->Add();
        error->SetKind(kind);
        error->SetReason(reason);
    }

    PQ_LOG_D("TxId: " << txId <<
             " send TEvPersQueue::TEvProposeTransactionResult(" <<
             NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event->Record.GetStatus()) <<
             ")");
    ctx.Send(target, std::move(event));
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
        quoterId = Register(new TWriteQuoter(
            TopicConverter,
            Config,
            AppData()->PQConfig,
            partition,
            SelfId(),
            TabletID(),
            IsLocalDC,
            *Counters
        ));
    }
    return quoterId;
}

TPartition* TPersQueue::CreatePartitionActor(const TPartitionId& partitionId,
                                             const NPersQueue::TTopicConverterPtr topicConverter,
                                             const NKikimrPQ::TPQTabletConfig& config,
                                             bool newPartition,
                                             const TActorContext& ctx)
{
    int channels = Info()->Channels.size() - NKeyValue::BLOB_CHANNEL; // channels 0,1 are reserved in tablet
    Y_ABORT_UNLESS(channels > 0);

    return new TPartition(TabletID(),
                          partitionId,
                          ctx.SelfID,
                          GetGeneration(),
                          CacheActor,
                          topicConverter,
                          DCId,
                          IsServerless,
                          config,
                          *Counters,
                          SubDomainOutOfSpace,
                          (ui32)channels,
                          GetPartitionQuoter(partitionId),
                          newPartition);
}

void TPersQueue::CreateNewPartitions(NKikimrPQ::TPQTabletConfig& config,
                                     NPersQueue::TTopicConverterPtr topicConverter,
                                     const TActorContext& ctx)
{
    EnsurePartitionsAreNotDeleted(config);

    Y_ABORT_UNLESS(ConfigInited,
                   "ConfigInited=%d",
                   static_cast<int>(ConfigInited));

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
    PQ_LOG_D("Txs.size=" << Txs.size() << ", PlannedTxs.size=" << PlannedTxs.size());

    std::sort(PlannedTxs.begin(), PlannedTxs.end());
    for (auto& item : PlannedTxs) {
        TxQueue.push_back(item);
    }

    if (!TxQueue.empty()) {
        PQ_LOG_D("top tx queue (" << TxQueue.front().first << ", " << TxQueue.front().second << ")");
    }

    for (const auto& [_, txId] : TxQueue) {
        Y_ABORT_UNLESS(Txs.contains(txId),
                       "PQ %" PRIu64 ", unknown TxId %" PRIu64,
                       TabletID(), txId);
        auto& tx = Txs.at(txId);

        Y_ABORT_UNLESS(txId == tx.TxId);

        if (!TxsOrder.contains(tx.State)) {
            PQ_LOG_D("TxsOrder: " <<
                     txId << " " << NKikimrPQ::TTransaction_EState_Name(tx.State) << " skip");
            continue;
        }

        PushTxInQueue(tx, tx.State);

        PQ_LOG_D("TxsOrder: " <<
                 txId << " " << NKikimrPQ::TTransaction_EState_Name(tx.State) << " " << tx.Pending);
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
        Y_ABORT_UNLESS(next);

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
        Y_ABORT_UNLESS(tx, "unknown TxId %" PRIu64, txId);

        SendEvReadSetToReceivers(ctx, *tx);
    }
}

void TPersQueue::DeleteSupportivePartitions(const TActorContext& ctx)
{
    for (ui64 txId : TxsOrder[NKikimrPQ::TTransaction::EXECUTED]) {
        auto tx = GetTransaction(ctx, txId);
        Y_ABORT_UNLESS(tx, "unknown TxId %" PRIu64, txId);

        BeginDeletePartitions(*tx);
    }
}

void TPersQueue::OnInitComplete(const TActorContext& ctx)
{
    SignalTabletActive(ctx);
    TryStartTransaction(ctx);
    InitCompleted = true;
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
    PQ_LOG_D("Handle TEvPersQueue::TEvProposeTransactionAttach " << ev->Get()->Record.ShortDebugString());

    const ui64 txId = ev->Get()->Record.GetTxId();
    NKikimrProto::EReplyStatus status = NKikimrProto::NODATA;

    auto tx = GetTransaction(ctx, txId);
    if (tx) {
        //
        // the actor's ID could have changed from the moment he sent the TEvProposeTransaction. you need to
        // update the actor ID in the transaction
        //
        // if the transaction has progressed beyond WAIT_RS, then a response has been sent to the sender
        //
        tx->SourceActor = ev->Sender;
        if (tx->State <= NKikimrPQ::TTransaction::WAIT_RS) {
            status = NKikimrProto::OK;
        }
    }

    ctx.Send(ev->Sender, new TEvDataShard::TEvProposeTransactionAttachResult(TabletID(), txId, status), 0, ev->Cookie);
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
    Y_ABORT_UNLESS(!partitionId.WriteId.Defined());
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
    PQ_LOG_D("Handle TEvLongTxService::TEvLockStatus " << ev->Get()->Record.ShortDebugString());

    auto& record = ev->Get()->Record;
    const TWriteId writeId(record.GetLockNode(), record.GetLockId());

    if (!TxWrites.contains(writeId)) {
        // the transaction has already been completed
        PQ_LOG_D("unknown WriteId " << writeId);
        return;
    }

    TTxWriteInfo& writeInfo = TxWrites.at(writeId);
    PQ_LOG_D("TxWriteInfo: " <<
             "WriteId " << writeId <<
             ", TxId " << writeInfo.TxId <<
             ", Status " << NKikimrLongTxService::TEvLockStatus_EStatus_Name(writeInfo.LongTxSubscriptionStatus));
    writeInfo.LongTxSubscriptionStatus = record.GetStatus();

    if (writeInfo.LongTxSubscriptionStatus == NKikimrLongTxService::TEvLockStatus::STATUS_SUBSCRIBED) {
        PQ_LOG_D("subscribed WriteId " << writeId);
        return;
    }

    if (writeInfo.TxId.Defined()) {
        // the message `TEvProposeTransaction` has already arrived
        PQ_LOG_D("there is already a transaction TxId " << writeInfo.TxId << " for WriteId " << writeId);
        return;
    }

    PQ_LOG_D("delete partitions for WriteId " << writeId);
    BeginDeletePartitions(writeInfo);
}

void TPersQueue::Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx)
{
    if (ReadBalancerActorId) {
        ctx.Send(ReadBalancerActorId, ev->Release().Release());
    }
}

void TPersQueue::Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx)
{
    if (ReadBalancerActorId) {
        ctx.Send(ReadBalancerActorId, ev->Release().Release());
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

    Partitions.erase(partitionId);
}

void TPersQueue::Handle(TEvPQ::TEvDeletePartitionDone::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPQ::TEvDeletePartitionDone " << ev->Get()->PartitionId);

    auto* event = ev->Get();
    Y_ABORT_UNLESS(event->PartitionId.WriteId.Defined());
    const TWriteId& writeId = *event->PartitionId.WriteId;
    Y_ABORT_UNLESS(TxWrites.contains(writeId),
                   "PQ %" PRIu64 ", WriteId {%" PRIu64 ", %" PRIu64 "}",
                   TabletID(), writeId.NodeId, writeId.KeyId);
    TTxWriteInfo& writeInfo = TxWrites.at(writeId);
    Y_ABORT_UNLESS(writeInfo.Partitions.contains(event->PartitionId.OriginalPartitionId));
    const TPartitionId& partitionId = writeInfo.Partitions.at(event->PartitionId.OriginalPartitionId);
    Y_ABORT_UNLESS(partitionId == event->PartitionId);
    Y_ABORT_UNLESS(partitionId.IsSupportivePartition());
    Y_ABORT_UNLESS(Partitions.contains(partitionId));

    DeletePartition(partitionId, ctx);

    writeInfo.Partitions.erase(partitionId.OriginalPartitionId);
    if (writeInfo.Partitions.empty()) {
        UnsubscribeWriteId(writeId, ctx);
        if (writeInfo.TxId.Defined()) {
            if (auto tx = GetTransaction(ctx, *writeInfo.TxId); tx) {
                if (tx->State == NKikimrPQ::TTransaction::WAIT_RS_ACKS) {
                    TryExecuteTxs(ctx, *tx);
                }
            }
        }
    }
    TxWritesChanged = true;

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPQ::TEvTransactionCompleted::TPtr& ev, const TActorContext&)
{
    PQ_LOG_D("Handle TEvPQ::TEvTransactionCompleted" <<
             " WriteId " << ev->Get()->WriteId);

    auto* event = ev->Get();
    if (!event->WriteId.Defined()) {
        return;
    }

    const TWriteId& writeId = *event->WriteId;
    Y_ABORT_UNLESS(TxWrites.contains(writeId),
                   "PQ %" PRIu64 ", WriteId {%" PRIu64 ", %" PRIu64 "}",
                   TabletID(), writeId.NodeId, writeId.KeyId);
    TTxWriteInfo& writeInfo = TxWrites.at(writeId);
    Y_ABORT_UNLESS(writeInfo.Partitions.size() == 1);

    BeginDeletePartitions(writeInfo);
}

void TPersQueue::BeginDeletePartitions(TTxWriteInfo& writeInfo)
{
    if (writeInfo.Deleting) {
        PQ_LOG_D("Already deleting WriteInfo");
        return;
    }
    for (auto& [_, partitionId] : writeInfo.Partitions) {
        Y_ABORT_UNLESS(Partitions.contains(partitionId));
        const TPartitionInfo& partition = Partitions.at(partitionId);
        PQ_LOG_D("send TEvPQ::TEvDeletePartition to partition " << partitionId);
        Send(partition.Actor, new TEvPQ::TEvDeletePartition);
    }
    writeInfo.Deleting = true;
}

void TPersQueue::BeginDeletePartitions(const TDistributedTransaction& tx)
{
    if (!tx.WriteId.Defined() || !TxWrites.contains(*tx.WriteId)) {
        return;
    }

    TTxWriteInfo& writeInfo = TxWrites.at(*tx.WriteId);
    BeginDeletePartitions(writeInfo);
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

bool TPersQueue::HandleHook(STFUNC_SIG)
{
    SetActivityType(NKikimrServices::TActivity::PERSQUEUE_ACTOR);
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
        HFuncTraced(TEvPQ::TEvTxCommitDone, Handle);
        HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);
        HFuncTraced(TEvPersQueue::TEvProposeTransactionAttach, Handle);
        HFuncTraced(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        HFuncTraced(TEvPersQueue::TEvCancelTransactionProposal, Handle);
        HFuncTraced(TEvMediatorTimecast::TEvRegisterTabletResult, Handle);
        HFuncTraced(TEvPQ::TEvCheckPartitionStatusRequest, Handle);
        HFuncTraced(TEvPQ::TEvPartitionScaleStatusChanged, Handle);
        hFuncTraced(NLongTxService::TEvLongTxService::TEvLockStatus, Handle);
        HFuncTraced(TEvPQ::TEvReadingPartitionStatusRequest, Handle);
        HFuncTraced(TEvPQ::TEvDeletePartitionDone, Handle);
        HFuncTraced(TEvPQ::TEvTransactionCompleted, Handle);
        default:
            return false;
    }
    return true;
}

} // namespace NKikimr::NPQ
