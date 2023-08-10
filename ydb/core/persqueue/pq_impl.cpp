
#include "pq_impl.h"
#include "event_helpers.h"
#include "partition.h"
#include "read.h"
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/persqueue/config/config.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
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

namespace NKikimr::NPQ {

static constexpr TDuration TOTAL_TIMEOUT = TDuration::Seconds(120);
static constexpr char TMP_REQUEST_MARKER[] = "__TMP__REQUEST__MARKER__";
static constexpr ui32 CACHE_SIZE = 100_MB;
static constexpr ui32 MAX_BYTES = 25_MB;
static constexpr ui32 MAX_SOURCE_ID_LENGTH = 10_KB;

struct TPartitionInfo {
    TPartitionInfo(const TActorId& actor, TMaybe<TPartitionKeyRange>&& keyRange,
            const bool initDone, const TTabletCountersBase& baseline)
        : Actor(actor)
        , KeyRange(std::move(keyRange))
        , InitDone(initDone)
    {
        Baseline.Populate(baseline);
    }

    TPartitionInfo(const TPartitionInfo& info)
        : Actor(info.Actor)
        , KeyRange(info.KeyRange)
        , InitDone(info.InitDone)
    {
        Baseline.Populate(info.Baseline);
    }

    TActorId Actor;
    TMaybe<TPartitionKeyRange> KeyRange;
    bool InitDone;
    TTabletCountersBase Baseline;
    THashMap<TString, TTabletLabeledCountersBase> LabeledCounters;
};

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

/******************************************************* ReadProxy *********************************************************/
//megaqc - remove it when LB will be ready
class TReadProxy : public TActorBootstrapped<TReadProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_ANS_ACTOR;
    }

    TReadProxy(const TActorId& sender, const TActorId& tablet, const NKikimrClient::TPersQueueRequest& request)
    : Sender(sender)
    , Tablet(tablet)
    , Request(request)
    , Response(new TEvPersQueue::TEvResponse)
    {
        Y_VERIFY(Request.HasPartitionRequest() && Request.GetPartitionRequest().HasCmdRead());
        Y_VERIFY(Request.GetPartitionRequest().GetCmdRead().GetPartNo() == 0); //partial request are not allowed, otherwise remove ReadProxy
        Y_VERIFY(!Response->Record.HasPartitionResponse());
    }

    void Bootstrap(const TActorContext&)
    {
        Become(&TThis::StateFunc);
    }

private:

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx)
    {
        Y_VERIFY(Response);
        const auto& record = ev->Get()->Record;
        if (!record.HasPartitionResponse() || !record.GetPartitionResponse().HasCmdReadResult() ||
            record.GetStatus() != NMsgBusProxy::MSTATUS_OK || record.GetErrorCode() != NPersQueue::NErrorCode::OK ||
            record.GetPartitionResponse().GetCmdReadResult().ResultSize() == 0) {
            Response->Record.CopyFrom(record);
            ctx.Send(Sender, Response.Release());
            Die(ctx);
            return;
        }


        Y_VERIFY(record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult());

        const auto& res = record.GetPartitionResponse().GetCmdReadResult();

        Response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        Response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

        Y_VERIFY(res.ResultSize() > 0);
        bool isStart = false;
        if (!Response->Record.HasPartitionResponse()) {
            Y_VERIFY(!res.GetResult(0).HasPartNo() || res.GetResult(0).GetPartNo() == 0); //starts from begin of record
            auto partResp = Response->Record.MutablePartitionResponse();
            auto readRes = partResp->MutableCmdReadResult();
            readRes->SetBlobsFromDisk(readRes->GetBlobsFromDisk() + res.GetBlobsFromDisk());
            readRes->SetBlobsFromCache(readRes->GetBlobsFromCache() + res.GetBlobsFromCache());
            isStart = true;
        }
        ui64 readFromTimestampMs = AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()
                                    ? (isStart ? res.GetReadFromTimestampMs()
                                                : Response->Record.GetPartitionResponse().GetCmdReadResult().GetReadFromTimestampMs())
                                    : 0;

        if (record.GetPartitionResponse().HasCookie())
            Response->Record.MutablePartitionResponse()->SetCookie(record.GetPartitionResponse().GetCookie());

        auto partResp = Response->Record.MutablePartitionResponse()->MutableCmdReadResult();

        partResp->SetMaxOffset(res.GetMaxOffset());
        partResp->SetSizeLag(res.GetSizeLag());
        partResp->SetWaitQuotaTimeMs(partResp->GetWaitQuotaTimeMs() + res.GetWaitQuotaTimeMs());

        partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), res.GetRealReadOffset()));

        for (ui32 i = 0; i < res.ResultSize(); ++i) {
            bool isNewMsg = !res.GetResult(i).HasPartNo() || res.GetResult(i).GetPartNo() == 0;
            if (!isStart) {
                Y_VERIFY(partResp->ResultSize() > 0);
                auto& back = partResp->GetResult(partResp->ResultSize() - 1);
                bool lastMsgIsNotFull = back.GetPartNo() + 1 < back.GetTotalParts();
                bool trancate = lastMsgIsNotFull && isNewMsg;
                if (trancate) {
                    partResp->MutableResult()->RemoveLast();
                    if (partResp->GetResult().empty()) isStart = false;
                }
            }

            if (isNewMsg) {
                if (!isStart && res.GetResult(i).HasTotalParts() && res.GetResult(i).GetTotalParts() + i > res.ResultSize()) //last blob is not full
                    break;
                partResp->AddResult()->CopyFrom(res.GetResult(i));
                isStart = false;
            } else { //glue to last res
                auto rr = partResp->MutableResult(partResp->ResultSize() - 1);
                if (rr->GetSeqNo() != res.GetResult(i).GetSeqNo() || rr->GetPartNo() + 1 != res.GetResult(i).GetPartNo()) {
                    LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE, "Handle TEvRead tablet: " << Tablet
                                    << " last read pos (seqno/parno): " << rr->GetSeqNo() << "," << rr->GetPartNo() << " readed now "
                                    << res.GetResult(i).GetSeqNo() << ", " << res.GetResult(i).GetPartNo()
                                    << " full request(now): " << Request);
                }
                Y_VERIFY(rr->GetSeqNo() == res.GetResult(i).GetSeqNo());
                (*rr->MutableData()) += res.GetResult(i).GetData();
                rr->SetPartitionKey(res.GetResult(i).GetPartitionKey());
                rr->SetExplicitHash(res.GetResult(i).GetExplicitHash());
                rr->SetPartNo(res.GetResult(i).GetPartNo());
                rr->SetUncompressedSize(rr->GetUncompressedSize() + res.GetResult(i).GetUncompressedSize());
                if (res.GetResult(i).GetPartNo() + 1 == res.GetResult(i).GetTotalParts()) {
                    Y_VERIFY((ui32)rr->GetTotalSize() == (ui32)rr->GetData().size());
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

        ctx.Send(Sender, Response.Release());
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
    NKikimrClient::TPersQueueRequest Request;
    THolder<TEvPersQueue::TEvResponse> Response;
};


TActorId CreateReadProxy(const TActorId& sender, const TActorId& tablet, const NKikimrClient::TPersQueueRequest& request,
                         const TActorContext& ctx)
{
    return ctx.Register(new TReadProxy(sender, tablet, request));
}

/******************************************************* AnswerBuilderProxy *********************************************************/
class TResponseBuilder {
public:

    TResponseBuilder(const TActorId& sender, const TActorId& tablet, const TString& topicName, const ui32 partition, const ui64 messageNo,
                     const TString& reqId, const TMaybe<ui64> cookie, NMetrics::TResourceMetrics* resourceMetrics, const TActorContext& ctx)
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

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Handle TEvRequest topic: '" << TopicName << "' requestId: " << ReqId);

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
        Y_VERIFY(Waiting);
        Y_VERIFY(Response);
        --Waiting;
        bool skip = false;
        if (WasSplit && ev->Get()->Response.GetPartitionResponse().CmdWriteResultSize() == 1) { //megaqc - remove this
            const auto& x = ev->Get()->Response.GetPartitionResponse().GetCmdWriteResult(0);
            if (x.HasPartNo() && x.GetPartNo() > 0)
                skip = true;
        }
        if (!skip) //megaqc - remove this
            Response->Record.MergeFrom(ev->Get()->Response);

        if (!Waiting) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Answer ok topic: '" << TopicName << "' partition: " << Partition
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
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                   "Answer error topic: '" << TopicName << "'" <<
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

/******************************************************* MonitoringProxy *********************************************************/


class TMonitoringProxy : public TActorBootstrapped<TMonitoringProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_MON_ACTOR;
    }

    TMonitoringProxy(const TActorId& sender, const TString& query, const TMap<ui32, TActorId>& partitions, const TActorId& cache,
                     const TString& topicName, ui64 tabletId, ui32 inflight)
    : Sender(sender)
    , Query(query)
    , Partitions(partitions)
    , Cache(cache)
    , TotalRequests(partitions.size() + 1)
    , TotalResponses(0)
    , TopicName(topicName)
    , TabletID(tabletId)
    , Inflight(inflight)
    {
        for (auto& p: Partitions) {
            Results[p.first].push_back(Sprintf("Partition %u: NO DATA", p.first));
        }
    }

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateFunc);
        ctx.Send(Cache, new TEvPQ::TEvMonRequest(Sender, Query));
        for (auto& p : Partitions) {
            ctx.Send(p.second, new TEvPQ::TEvMonRequest(Sender, Query));
        }
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

private:

    void Reply(const TActorContext& ctx) {
        TStringStream str;
        ui32 mx = 0;
        for (auto& r: Results) mx = Max<ui32>(mx, r.second.size());

        HTML(str) {
            TAG(TH2) {str << "PersQueue Tablet";}
            TAG(TH3) {str << "Topic: " << TopicName;}
            TAG(TH4) {str << "inflight: " << Inflight;}
            UL_CLASS("nav nav-tabs") {
                LI_CLASS("active") {
                    str << "<a href=\"#main\" data-toggle=\"tab\">main</a>";
                }
                LI() {
                    str << "<a href=\"#cache\" data-toggle=\"tab\">cache</a>";
                }
                for (auto& r: Results) {
                    LI() {
                        str << "<a href=\"#partition_" << r.first << "\" data-toggle=\"tab\">" << r.first << "</a>";
                    }
                }
            }
            DIV_CLASS("tab-content") {
                DIV_CLASS_ID("tab-pane fade in active", "main") {
                    TABLE() {
                        for (ui32 i = 0; i < mx; ++i) {
                            TABLER() {
                                for (auto& r : Results) {
                                    TABLED() {
                                        if (r.second.size() > i)
                                            str << r.second[i];
                                    }
                                }
                            }
                        }
                    }
                }
                for (auto& s: Str) {
                    str << s;
                }
            }
            TAG(TH3) {str << "<a href=\"app?TabletID=" << TabletID << "&kv=1\">KV-tablet internals</a>";}
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Answer TEvRemoteHttpInfoRes: to " << Sender << " self " << ctx.SelfID);
        ctx.Send(Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        Die(ctx);
    }

    void Wakeup(const TActorContext& ctx) {
        Reply(ctx);
    }

    void Handle(TEvPQ::TEvMonResponse::TPtr& ev, const TActorContext& ctx)
    {
        if (ev->Get()->Partition != Max<ui32>()) {
            Results[ev->Get()->Partition] = ev->Get()->Res;
        } else {
            Y_VERIFY(ev->Get()->Partition == Max<ui32>());
        }
        Str.push_back(ev->Get()->Str);
        if(++TotalResponses == TotalRequests) {
            Reply(ctx);
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, Wakeup);
            HFunc(TEvPQ::TEvMonResponse, Handle);
        default:
            break;
        };
    }

    TActorId Sender;
    TString Query;
    TMap<ui32, TVector<TString>> Results;
    TVector<TString> Str;
    TMap<ui32, TActorId> Partitions;
    TActorId Cache;
    ui32 TotalRequests;
    ui32 TotalResponses;
    TString TopicName;
    ui64 TabletID;
    ui32 Inflight;
};


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
    THashSet<ui32> was;
    if (NewConfig.PartitionsSize()) {
        for (const auto& partition : NewConfig.GetPartitions()) {
            was.insert(partition.GetPartitionId());
        }
    } else {
        for (const auto partitionId : NewConfig.GetPartitionIds()) {
            was.insert(partitionId);
        }
    }
    for (const auto& partition : Config.GetPartitions()) {
        Y_VERIFY_S(was.contains(partition.GetPartitionId()), "New config is bad, missing partition " << partition.GetPartitionId());
    }

    // in order to answer only after all parts are ready to work
    Y_VERIFY(ConfigInited && PartitionsInited == Partitions.size());

    ApplyNewConfig(NewConfig, ctx);
    ClearNewConfig();

    for (auto& p : Partitions) { //change config for already created partitions
        ctx.Send(p.second.Actor, new TEvPQ::TEvChangePartitionConfig(TopicConverter, Config));
    }
    ChangePartitionConfigInflight += Partitions.size();

    for (const auto& partition : Config.GetPartitions()) {
        const auto partitionId = partition.GetPartitionId();
        if (Partitions.find(partitionId) == Partitions.end()) {
            Partitions.emplace(partitionId,
                TPartitionInfo(ctx.Register(CreatePartitionActor(partitionId, TopicConverter, Config, true, ctx)),
                GetPartitionKeyRange(Config, partition),
                true,
                *Counters
            ));

            // InitCompleted is true because this partition is empty
            ++PartitionsInited; //newly created partition is empty and ready to work
        }
    }

    TrySendUpdateConfigResponses(ctx);
}

void TPersQueue::ApplyNewConfig(const NKikimrPQ::TPQTabletConfig& newConfig,
                                const TActorContext& ctx)
{
    Config = newConfig;

    if (!Config.PartitionsSize()) {
        for (const auto partitionId : Config.GetPartitionIds()) {
            Config.AddPartitions()->SetPartitionId(partitionId);
        }
    }

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

        Y_VERIFY(TopicName.size(), "Need topic name here");
        CacheActor = ctx.Register(new TPQCacheProxy(ctx.SelfID, TopicName, TabletID(),
                                                    cacheSize));
    } else {
        //Y_VERIFY(TopicName == Config.GetTopicName(), "Changing topic name is not supported");
        TopicPath = Config.GetTopicPath();
        ctx.Send(CacheActor, new TEvPQ::TEvChangeCacheConfig(cacheSize));
    }

    InitializeMeteringSink(ctx);
}

void TPersQueue::EndWriteConfig(const NKikimrClient::TResponse& resp, const TActorContext& ctx)
{
    if (resp.GetStatus() != NMsgBusProxy::MSTATUS_OK ||
        resp.WriteResultSize() < 1) {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " Config write error: " << resp.DebugString() << " " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }
    for (const auto& res : resp.GetWriteResult()) {
        if (res.GetStatus() != NKikimrProto::OK) {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                        << " Config write error: " << resp.DebugString() << " " << ctx.SelfID);
                ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
            return;
        }
    }

    if (resp.WriteResultSize() > 1) {
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " restarting - have some registering of message groups");
            ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    Y_VERIFY(resp.WriteResultSize() >= 1);
    Y_VERIFY(resp.GetWriteResult(0).GetStatus() == NKikimrProto::OK);
    if (ConfigInited && PartitionsInited == Partitions.size()) //all partitions are working well - can apply new config
        ApplyNewConfigAndReply(ctx);
    else
        NewConfigShouldBeApplied = true; //when config will be inited with old value new config will be applied
}

void TPersQueue::HandleConfigReadResponse(const NKikimrClient::TResponse& resp, const TActorContext& ctx)
{
    bool ok =
        (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK) &&
        (resp.ReadResultSize() == 3) &&
        (resp.HasSetExecutorFastLogPolicyResult()) &&
        (resp.GetSetExecutorFastLogPolicyResult().GetStatus() == NKikimrProto::OK);
    if (!ok) {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
            "Tablet " << TabletID() << " Config read error: " << resp.DebugString() << " " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    ReadTxInfo(resp.GetReadResult(2), ctx);
    ReadConfig(resp.GetReadResult(0), resp.GetReadRangeResult(0), ctx);
    ReadState(resp.GetReadResult(1), ctx);
}

void TPersQueue::ReadTxInfo(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                            const TActorContext& ctx)
{
    Y_VERIFY(read.HasStatus());
    if (read.GetStatus() != NKikimrProto::OK && read.GetStatus() != NKikimrProto::NODATA) {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
            "Tablet " << TabletID() << " tx info read error " << ctx.SelfID);
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    switch (read.GetStatus()) {
    case NKikimrProto::OK: {
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " has a tx info");

        NKikimrPQ::TTabletTxInfo info;
        Y_VERIFY(info.ParseFromString(read.GetValue()));

        LastStep = info.GetLastStep();
        LastTxId = info.GetLastTxId();

        break;
    }
    case NKikimrProto::NODATA: {
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " doesn't have tx info");

        LastStep = 0;
        LastTxId = 0;

        break;
    }
    default:
        Y_FAIL("Unexpected tx info read status: %d", read.GetStatus());
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " LastStep " << LastStep << " LastTxId " << LastTxId);
}

void TPersQueue::ReadConfig(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                            const NKikimrClient::TKeyValueResponse::TReadRangeResult& readRange,
                            const TActorContext& ctx)
{
    Y_VERIFY(read.HasStatus());
    if (read.GetStatus() != NKikimrProto::OK && read.GetStatus() != NKikimrProto::NODATA) {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
            "Tablet " << TabletID() << " Config read error " << ctx.SelfID <<
            " Error status code " << read.GetStatus());
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }
 
    Y_VERIFY(readRange.HasStatus());
    if (readRange.GetStatus() != NKikimrProto::OK && readRange.GetStatus() != NKikimrProto::NODATA) {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
            "Tablet " << TabletID() << " Transactions read error " << ctx.SelfID <<
            " Error status code " << readRange.GetStatus());
        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    Y_VERIFY(!ConfigInited);

    if (read.GetStatus() == NKikimrProto::OK) {
        bool res = Config.ParseFromString(read.GetValue());
        Y_VERIFY(res);

        if (!Config.PartitionsSize()) {
            for (const auto partitionId : Config.GetPartitionIds()) {
                Config.AddPartitions()->SetPartitionId(partitionId);
            }
        }

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

        Y_VERIFY(TopicName.size(), "Need topic name here");
        CacheActor = ctx.Register(new TPQCacheProxy(ctx.SelfID, TopicName, TabletID(), cacheSize));
    } else if (read.GetStatus() == NKikimrProto::NODATA) {
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " no config, start with empty partitions and default config");
    } else {
        Y_FAIL("Unexpected config read status: %d", read.GetStatus());
    }

    THashMap<ui32, TVector<TTransaction>> partitionTxs;
    InitTransactions(readRange, partitionTxs);

    for (const auto& partition : Config.GetPartitions()) { // no partitions will be created with empty config
        const auto partitionId = partition.GetPartitionId();
        Partitions.emplace(partitionId, TPartitionInfo(
            ctx.Register(CreatePartitionActor(partitionId, TopicConverter, Config, false, ctx)),
            GetPartitionKeyRange(Config, partition),
            false,
            *Counters
        ));
    }

    ConfigInited = true;

    InitProcessingParams(ctx);
    InitializeMeteringSink(ctx);

    Y_VERIFY(!NewConfigShouldBeApplied);
    for (auto& req : UpdateConfigRequests) {
        ProcessUpdateConfigRequest(req.first, req.second, ctx);
    }
    UpdateConfigRequests.clear();

    for (auto& req : HasDataRequests) {
        auto it = Partitions.find(req->Record.GetPartition());
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
        Y_VERIFY(ok);
        Y_VERIFY(stateProto.HasState());
        TabletState = stateProto.GetState();
    } else if (read.GetStatus() == NKikimrProto::NODATA) {
        TabletState = NKikimrPQ::ENormal;
    } else {
        Y_FAIL("Unexpected state read status: %d", read.GetStatus());
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
        LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " disable metering"
            << ": reason# " << "billing is not enabled in BillingMeteringConfig");
        return;
    }

    TSet<EMeteringJson> whichToFlush{EMeteringJson::PutEventsV1, EMeteringJson::ResourcesReservedV1};
    ui64 storageLimitBytes{Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() *
        Config.GetPartitionConfig().GetLifetimeSeconds()};

    if (Config.GetPartitionConfig().HasStorageLimitBytes()) {
        storageLimitBytes = Config.GetPartitionConfig().GetStorageLimitBytes();
        whichToFlush = TSet<EMeteringJson>{EMeteringJson::PutEventsV1,
                                           EMeteringJson::ThroughputV1,
                                           EMeteringJson::StorageV1};
    }

    switch (Config.GetMeteringMode()) {
    case NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS:
        LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " metering mode METERING_MODE_REQUEST_UNITS");
        whichToFlush = TSet<EMeteringJson>{EMeteringJson::UsedStorageV1};
        break;

    default:
        break;
    }

    auto countReadRulesWithPricing = [&](const TActorContext& ctx, const auto& config) {
        ui32 result = 0;
        for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
            TString rrServiceType = config.ReadRuleServiceTypesSize() <= i ? "" : config.GetReadRuleServiceTypes(i);
            if (rrServiceType.empty() || rrServiceType == AppData(ctx)->PQConfig.GetDefaultClientServiceType().GetName())
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
            .PartitionsSize = Config.PartitionsSize(),
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
    Y_VERIFY(ok);

    TAutoPtr<TEvKeyValue::TEvRequest> kvRequest(new TEvKeyValue::TEvRequest);
    kvRequest->Record.SetCookie(WRITE_STATE_COOKIE);

    auto kvCmd = kvRequest->Record.AddCmdWrite();
    kvCmd->SetKey(KeyState());
    kvCmd->SetValue(strState);
    kvCmd->SetTactic(AppData(ctx)->PQConfig.GetTactic());

    ctx.Send(ctx.SelfID, kvRequest.Release());
}

void TPersQueue::EndWriteTabletState(const NKikimrClient::TResponse& resp, const TActorContext& ctx)
{
    bool ok = (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK) &&
            (resp.WriteResultSize() == 1) &&
            (resp.GetWriteResult(0).GetStatus() == NKikimrProto::OK);
    if (!ok) {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
            "Tablet " << TabletID() << " SelfId " << ctx.SelfID << " State write error: " << resp.DebugString());

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
        HandleConfigReadResponse(resp, ctx);
        break;
    case WRITE_STATE_COOKIE:
        EndWriteTabletState(resp, ctx);
        break;
    case WRITE_TX_COOKIE:
        EndWriteTxs(resp, ctx);
        break;
    default:
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " Unexpected KV response: " << ev->Get()->ToString() << " " << ctx.SelfID);
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


void TPersQueue::Handle(TEvPQ::TEvPartitionCounters::TPtr& ev, const TActorContext& ctx)
{
    auto it = Partitions.find(ev->Get()->Partition);
    Y_VERIFY(it != Partitions.end());
    auto diff = ev->Get()->Counters.MakeDiffForAggr(it->second.Baseline);
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
    ev->Get()->Counters.RememberCurrentStateAsBaseline(it->second.Baseline);

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

        Y_VERIFY(aggr->HasCounters());

        TActorId countersAggregator = MakeTabletCountersAggregatorID(ctx.SelfID.NodeId());

        ctx.Send(countersAggregator,
                 new TEvTabletCounters::TEvTabletAddLabeledCounters(
                     CounterEventsInflight[group], TabletID(), TTabletTypes::PersQueue, aggr));
    }
}

void TPersQueue::Handle(TEvPQ::TEvPartitionLabeledCounters::TPtr& ev, const TActorContext& ctx)
{
    auto it = Partitions.find(ev->Get()->Partition);
    Y_VERIFY(it != Partitions.end());
    const TString& group = ev->Get()->LabeledCounters.GetGroup();
    it->second.LabeledCounters[group] = ev->Get()->LabeledCounters;
    Y_UNUSED(ctx);
//  if uncommented, all changes will be reported immediatly
//    AggregateAndSendLabeledCountersFor(group, ctx);
}

void TPersQueue::Handle(TEvPQ::TEvPartitionLabeledCountersDrop::TPtr& ev, const TActorContext& ctx)
{
    auto it = Partitions.find(ev->Get()->Partition);
    Y_VERIFY(it != Partitions.end());
    const TString& group = ev->Get()->Group;
    auto jt = it->second.LabeledCounters.find(group);
    if (jt != it->second.LabeledCounters.end())
        jt->second.SetDrop();
    Y_UNUSED(ctx);
//  if uncommented, all changes will be reported immediatly
//    AggregateAndSendLabeledCountersFor(group, ctx);

}



void TPersQueue::Handle(TEvPQ::TEvTabletCacheCounters::TPtr& ev, const TActorContext& ctx)
{
    CacheCounters = ev->Get()->Counters;
    SetCacheCounters(CacheCounters);

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " topic '" << TopicConverter->GetClientsideName()
        << "Counters. CacheSize " << CacheCounters.CacheSizeBytes << " CachedBlobs " << CacheCounters.CacheSizeBlobs);
}

void TPersQueue::Handle(TEvPQ::TEvInitComplete::TPtr& ev, const TActorContext& ctx)
{
    auto it = Partitions.find(ev->Get()->Partition);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(!it->second.InitDone);
    it->second.InitDone = true;
    ++PartitionsInited;
    Y_VERIFY(ConfigInited);//partitions are inited only after config

    if (PartitionsInited == Partitions.size()) {
        OnInitComplete(ctx);
    }

    if (NewConfigShouldBeApplied && PartitionsInited == Partitions.size()) {
        ApplyNewConfigAndReply(ctx);
    }
}

void TPersQueue::Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext& ctx)
{

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
    if (!ConfigInited) {
        UpdateConfigRequests.emplace_back(ev->Release(), ev->Sender);
        return;
    }
    ProcessUpdateConfigRequest(ev->Release(), ev->Sender, ctx);
}


void TPersQueue::Handle(TEvPQ::TEvPartitionConfigChanged::TPtr&, const TActorContext& ctx)
{
    Y_VERIFY(ChangePartitionConfigInflight > 0);
    --ChangePartitionConfigInflight;

    TrySendUpdateConfigResponses(ctx);
}

void TPersQueue::TrySendUpdateConfigResponses(const TActorContext& ctx)
{
    if (ChangePartitionConfigInflight) {
        return;
    }

    for (auto& p : ChangeConfigNotification) {
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " Config applied version " << Config.GetVersion() << " actor " << p.Actor
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
    Y_VERIFY(topicConverter);
    Y_VERIFY(topicConverter->IsValid(), "%s", topicConverter->GetReason().c_str());
}

void TPersQueue::ProcessUpdateConfigRequest(TAutoPtr<TEvPersQueue::TEvUpdateConfig> ev, const TActorId& sender, const TActorContext& ctx)
{
    auto& record = ev->Record;

    int oldConfigVersion = Config.HasVersion() ? Config.GetVersion() : -1;
    int newConfigVersion = NewConfig.HasVersion() ? NewConfig.GetVersion() : oldConfigVersion;

    Y_VERIFY(newConfigVersion >= oldConfigVersion);

    NKikimrPQ::TPQTabletConfig cfg = record.GetTabletConfig();

    Y_VERIFY(cfg.HasVersion());
    int curConfigVersion = cfg.GetVersion();

    if (curConfigVersion == oldConfigVersion) { //already applied
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " Config already applied version " << Config.GetVersion() << " actor " << sender
                    << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::OK);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(sender, res.Release());
        return;
    }
    if (curConfigVersion < newConfigVersion) { //Version must increase
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " Config has too small  version " << curConfigVersion << " actual " << newConfigVersion << " actor " << sender
                    << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR_BAD_VERSION);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(sender, res.Release());
        return;
    }
    if (curConfigVersion == newConfigVersion) { //nothing to change, will be answered on cfg write from prev step
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " Config update version " << newConfigVersion << " is already in progress actor " << sender
                    << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());
        ChangeConfigNotification.insert(TChangeNotification(sender, record.GetTxId()));
        return;
    }

    if (curConfigVersion > newConfigVersion && NewConfig.HasVersion()) { //already in progress with older version
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << " Config version " << curConfigVersion << " is too big, applying right now version " << newConfigVersion
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
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                    << ": cannot apply explicit message groups unless proto source id enabled"
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
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                        << "Cannot apply explicit message group: " << error
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

    // set rr generation for provided read rules
    {
        THashMap<TString, std::pair<ui64, ui64>> existed; // map name -> rrVersion, rrGeneration
        for (ui32 i = 0; i < Config.ReadRulesSize(); ++i) {
            auto version = i < Config.ReadRuleVersionsSize() ? Config.GetReadRuleVersions(i) : 0;
            auto generation = i < Config.ReadRuleGenerationsSize() ? Config.GetReadRuleGenerations(i) : 0;
            existed[Config.GetReadRules(i)] = std::make_pair(version, generation);
        }
        for (ui32 i = 0; i < cfg.ReadRulesSize(); ++i) {
            auto version = i < cfg.ReadRuleVersionsSize() ? cfg.GetReadRuleVersions(i) : 0;
            auto it = existed.find(cfg.GetReadRules(i));
            ui64 generation = 0;
            if (it != existed.end() && it->second.first == version) {
                generation = it->second.second;
            } else {
                generation = curConfigVersion;
            }
            cfg.AddReadRuleGenerations(generation);
        }
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID()
                << " Config update version " << cfg.GetVersion() << "(current " << Config.GetVersion() << ") received from actor " << sender
                << " txId " << record.GetTxId() << " config:\n" << cfg.DebugString());

    TString str;
    Y_VERIFY(CheckPersQueueConfig(cfg, true, &str), "%s", str.c_str());

    BeginWriteConfig(cfg, bootstrapCfg, ctx);

    NewConfig = cfg;
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
                      ctx);
    Y_VERIFY((ui64)request->Record.GetCmdWrite().size() == (ui64)bootstrapCfg.GetExplicitMessageGroups().size() * cfg.PartitionsSize() + 1);

    ctx.Send(ctx.SelfID, request.Release());
}

void TPersQueue::AddCmdWriteConfig(TEvKeyValue::TEvRequest* request,
                                   const NKikimrPQ::TPQTabletConfig& cfg,
                                   const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                                   const TActorContext& ctx)
{
    Y_VERIFY(request);

    TString str;
    Y_VERIFY(cfg.SerializeToString(&str));

    auto write = request->Record.AddCmdWrite();
    write->SetKey(KeyConfig());
    write->SetValue(str);
    write->SetTactic(AppData(ctx)->PQConfig.GetTactic());

    TSourceIdWriter sourceIdWriter(ESourceIdFormat::Proto);
    for (const auto& mg : bootstrapCfg.GetExplicitMessageGroups()) {
        TMaybe<TPartitionKeyRange> keyRange;
        if (mg.HasKeyRange()) {
            keyRange = TPartitionKeyRange::Parse(mg.GetKeyRange());
        }

        sourceIdWriter.RegisterSourceId(mg.GetId(), 0, 0, ctx.Now(), std::move(keyRange));
    }

    for (const auto& partition : cfg.GetPartitions()) {
        sourceIdWriter.FillRequest(request, partition.GetPartitionId());
    }
}

void TPersQueue::ClearNewConfig()
{
    NewConfigShouldBeApplied = false;
    NewConfig.Clear();
}

void TPersQueue::Handle(TEvPersQueue::TEvDropTablet::TPtr& ev, const TActorContext& ctx)
{
    auto& record = ev->Get()->Record;
    ui64 txId = record.GetTxId();

    TChangeNotification stateRequest(ev->Sender, txId);

    NKikimrPQ::ETabletState reqState = record.GetRequestedState();

    if (reqState == NKikimrPQ::ENormal) {
        ReturnTabletState(ctx, stateRequest, NKikimrProto::ERROR);
        return;
    }

    Y_VERIFY(reqState == NKikimrPQ::EDropped);

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
         cnt += p.second.InitDone;
    }
    TActorId ans = CreateOffsetsProxyActor(TabletID(), ev->Sender, cnt, ctx);

    for (auto& p : Partitions) {
        if (!p.second.InitDone)
            continue;
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
        auto it = Partitions.find(record.GetPartition());
        if (it != Partitions.end()) {
            ctx.Send(it->second.Actor, ev->Release().Release());
        }
    }
}


void TPersQueue::Handle(TEvPersQueue::TEvPartitionClientInfo::TPtr& ev, const TActorContext& ctx) {
    for (auto partition : ev->Get()->Record.GetPartitions()) {
        auto it = Partitions.find(partition);
        if (it != Partitions.end()) {
            ctx.Send(it->second.Actor, new TEvPQ::TEvGetPartitionClientInfo(ev->Sender), 0, ev->Cookie);
        } else {
            THolder<TEvPersQueue::TEvPartitionClientInfoResponse> clientInfo = MakeHolder<TEvPersQueue::TEvPartitionClientInfoResponse>();
            clientInfo->Record.SetPartition(partition);
            ctx.Send(ev->Sender, clientInfo.Release(), 0, ev->Cookie);
        }
    }
}


void TPersQueue::Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext& ctx)
{
    if (!ConfigInited) {
        THolder<TEvPersQueue::TEvStatusResponse> res = MakeHolder<TEvPersQueue::TEvStatusResponse>();
        auto& resp = res->Record;
        resp.SetTabletId(TabletID());

        ctx.Send(ev->Sender, res.Release());
        return;
    }

    ui32 cnt = 0;
    for (auto& p : Partitions) {
         cnt += p.second.InitDone;
    }

    TActorId ans = CreateStatusProxyActor(TabletID(), ev->Sender, cnt, ev->Cookie, ctx);
    for (auto& p : Partitions) {
        if (!p.second.InitDone)
            continue;
        THolder<TEvPQ::TEvPartitionStatus> event = MakeHolder<TEvPQ::TEvPartitionStatus>(ans, ev->Get()->Record.HasClientId() ? ev->Get()->Record.GetClientId() : "",
                                                    ev->Get()->Record.HasGetStatForAllConsumers() ? ev->Get()->Record.GetGetStatForAllConsumers() : false);
        ctx.Send(p.second.Actor, event.Release());
    }
}


void TPersQueue::InitResponseBuilder(const ui64 responseCookie, const ui32 count, const ui32 counterId)
{
    auto it = ResponseProxy.find(responseCookie);
    Y_VERIFY(it != ResponseProxy.end());
    it->second->AddPartialReplyCount(count);
    it->second->SetCounterId(counterId);
}

void TPersQueue::HandleGetMaxSeqNoRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdGetMaxSeqNo());
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_MAX_SEQ_NO);
    const auto& cmd = req.GetCmdGetMaxSeqNo();
    TVector<TString> ids;
    ids.reserve(cmd.SourceIdSize());
    for (ui32 i = 0; i < cmd.SourceIdSize(); ++i)
        ids.push_back(cmd.GetSourceId(i));
    THolder<TEvPQ::TEvGetMaxSeqNoRequest> event = MakeHolder<TEvPQ::TEvGetMaxSeqNoRequest>(responseCookie, ids);
    ctx.Send(partActor, event.Release());
}

void TPersQueue::HandleDeleteSessionRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdDeleteSession());
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_DELETE_SESSION);
    const auto& cmd = req.GetCmdDeleteSession();

    if (!cmd.HasClientId()){
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no clientId in DeleteSession request: " << ToString(req).data());
    } else if (!cmd.HasSessionId()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "not sessionId in DeleteSession request: " << ToString(req).data());
    } else {
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(responseCookie, cmd.GetClientId(),
                                                                0, cmd.GetSessionId(), 0, 0, TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION);
        ctx.Send(partActor, event.Release());
    }
}

void TPersQueue::HandleCreateSessionRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdCreateSession());
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
        InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_CREATE_SESSION);
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(responseCookie, cmd.GetClientId(),
                                                                0, cmd.GetSessionId(), cmd.GetGeneration(), cmd.GetStep(), TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION);
        ctx.Send(partActor, event.Release());
    }
}

void TPersQueue::HandleSetClientOffsetRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdSetClientOffset());
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
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(responseCookie, cmd.GetClientId(),
                                                                cmd.GetOffset(),
                                                                cmd.HasSessionId() ? cmd.GetSessionId() : "", 0, 0,
                                                                TEvPQ::TEvSetClientInfo::ESCI_OFFSET, 0, cmd.GetStrict());
        ctx.Send(partActor, event.Release());
    }
}

void TPersQueue::HandleGetClientOffsetRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdGetClientOffset());
    const auto& cmd = req.GetCmdGetClientOffset();
    if (!cmd.HasClientId() || cmd.GetClientId().empty()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "no clientId in GetClientOffset request: " << ToString(req).data());
    } else {
        InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_OFFSET);
        THolder<TEvPQ::TEvGetClientOffset> event = MakeHolder<TEvPQ::TEvGetClientOffset>(responseCookie, cmd.GetClientId());
        ctx.Send(partActor, event.Release());
    }
}

void TPersQueue::HandleUpdateWriteTimestampRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdUpdateWriteTimestamp());
    const auto& cmd = req.GetCmdUpdateWriteTimestamp();
    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_OFFSET);
    THolder<TEvPQ::TEvUpdateWriteTimestamp> event = MakeHolder<TEvPQ::TEvUpdateWriteTimestamp>(responseCookie, cmd.GetWriteTimeMS());
    ctx.Send(partActor, event.Release());
}

void TPersQueue::HandleWriteRequest(const ui64 responseCookie, const TActorId& partActor,
                                    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.CmdWriteSize());
    MeteringSink.MayFlush(ctx.Now()); // To ensure hours' border;
    if (Config.GetMeteringMode() != NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS) {
        MeteringSink.IncreaseQuantity(EMeteringJson::PutEventsV1, req.HasPutUnitsSize() ? req.GetPutUnitsSize() : 0);
    }

    TVector <TEvPQ::TEvWrite::TMsg> msgs;

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
        } else if (!cmd.HasData() || cmd.GetData().empty()){
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
        } else if (cmd.GetSourceId().size() > MAX_SOURCE_ID_LENGTH) {
            errorStr = "Too big SourceId";
        } else if (mirroredPartition && !cmd.GetDisableDeduplication()) {
            errorStr = "Write to mirrored topic is forbiden";
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
        Y_VERIFY(mSize > 204800);
        ui64 receiveTimestampMs = TAppData::TimeProvider->Now().MilliSeconds();
        bool disableDeduplication = cmd.GetDisableDeduplication();
        if (cmd.GetData().size() > mSize) {
            if (cmd.HasPartNo()) {
                ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
                            TStringBuilder() << "Too big message while using PartNo; must be at most " << mSize << ", but got " << cmd.GetData().size());
                return;
            }
            auto it = ResponseProxy.find(responseCookie);
            Y_VERIFY(it != ResponseProxy.end());
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
            Y_VERIFY(!cmd.HasTotalParts(), "too big part"); //change this verify for errorStr, when LB will be ready
            while (pos < totalSize) {
                TString data = cmd.GetData().substr(pos, mSize - diff);
                pos += mSize - diff;
                diff = 0;
                msgs.push_back({cmd.GetSourceId(), static_cast<ui64>(cmd.GetSeqNo()), partNo,
                    totalParts, totalSize, createTimestampMs, receiveTimestampMs,
                    disableDeduplication, writeTimestampMs, data, uncompressedSize,
                    cmd.GetPartitionKey(), cmd.GetExplicitHash(), cmd.GetExternalOperation(),
                    cmd.GetIgnoreQuotaDeadline()
                });
                partNo++;
                uncompressedSize = 0;
                LOG_DEBUG_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "Tablet " << TabletID() <<
                        " got client PART message topic: " << TopicConverter->GetClientsideName() << " partition: " << req.GetPartition()
                            << " SourceId: \'" << EscapeC(msgs.back().SourceId) << "\' SeqNo: "
                            << msgs.back().SeqNo << " partNo : " << msgs.back().PartNo
                            << " messageNo: " << req.GetMessageNo() << " size: " << data.size()
                );
            }
            Y_VERIFY(partNo == totalParts);
        } else {
            msgs.push_back({cmd.GetSourceId(), static_cast<ui64>(cmd.GetSeqNo()), static_cast<ui16>(cmd.HasPartNo() ? cmd.GetPartNo() : 0),
                static_cast<ui16>(cmd.HasPartNo() ? cmd.GetTotalParts() : 1),
                static_cast<ui32>(cmd.HasTotalSize() ? cmd.GetTotalSize() : cmd.GetData().Size()),
                createTimestampMs, receiveTimestampMs, disableDeduplication, writeTimestampMs, cmd.GetData(),
                cmd.HasUncompressedSize() ? cmd.GetUncompressedSize() : 0u, cmd.GetPartitionKey(), cmd.GetExplicitHash(),
                cmd.GetExternalOperation(), cmd.GetIgnoreQuotaDeadline()
            });
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                    "Tablet " << TabletID() <<
                    " got client message topic: " << TopicConverter->GetClientsideName() <<
                    " partition: " << req.GetPartition() <<
                    " SourceId: \'" << EscapeC(msgs.back().SourceId) <<
                    "\' SeqNo: " << msgs.back().SeqNo << " partNo : " << msgs.back().PartNo <<
                    " messageNo: " << req.GetMessageNo() << " size " << msgs.back().Data.size() <<
                    " offset: " << (req.HasCmdWriteOffset() ? (req.GetCmdWriteOffset() + i) : -1));
    }
    InitResponseBuilder(responseCookie, msgs.size(), COUNTER_LATENCY_PQ_WRITE);
    THolder<TEvPQ::TEvWrite> event =
        MakeHolder<TEvPQ::TEvWrite>(responseCookie, req.GetMessageNo(),
                                    req.HasOwnerCookie() ? req.GetOwnerCookie() : "",
                                    req.HasCmdWriteOffset() ? req.GetCmdWriteOffset() : TMaybe<ui64>(),
                                    std::move(msgs), req.GetIsDirectWrite());
    ctx.Send(partActor, event.Release());
}


void TPersQueue::HandleReserveBytesRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
                                          const TActorId& pipeClient, const TActorId&)
{
    Y_VERIFY(req.HasCmdReserveBytes());

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

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_RESERVE_BYTES);
    THolder<TEvPQ::TEvReserveBytes> event = MakeHolder<TEvPQ::TEvReserveBytes>(responseCookie, req.GetCmdReserveBytes().GetSize(),
                                                                        req.GetOwnerCookie(), req.GetMessageNo(), req.GetCmdReserveBytes().GetLastRequest());
    ctx.Send(partActor, event.Release());
}


void TPersQueue::HandleGetOwnershipRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,
                                          const TActorId& pipeClient, const TActorId& sender)
{
    Y_VERIFY(req.HasCmdGetOwnership());

    const TString& owner = req.GetCmdGetOwnership().GetOwner();
    if (owner.empty()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "empty owner in CmdGetOwnership request");
        return;
    }
    Y_VERIFY(pipeClient != TActorId());
    auto it = PipesInfo.find(pipeClient);
    if (it == PipesInfo.end()) { //do nothing. this could not be happen, just in tests
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "request via dead pipe");
        return;
    }

    it->second = {partActor, owner, it->second.ServerActors};

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_GET_OWNERSHIP);
    THolder<TEvPQ::TEvChangeOwner> event = MakeHolder<TEvPQ::TEvChangeOwner>(responseCookie, owner, pipeClient, sender, req.GetCmdGetOwnership().GetForce());
    ctx.Send(partActor, event.Release());
}


void TPersQueue::HandleReadRequest(const ui64 responseCookie, const TActorId& partActor,
                                          const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdRead());

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
        THolder<TEvPQ::TEvRead> event =
            MakeHolder<TEvPQ::TEvRead>(responseCookie, cmd.GetOffset(),
                                       cmd.HasPartNo() ? cmd.GetPartNo() : 0,
                                       count,
                                       cmd.HasSessionId() ? cmd.GetSessionId() : "",
                                       cmd.GetClientId(),
                                       cmd.HasTimeoutMs() ? cmd.GetTimeoutMs() : 0, bytes,
                                       cmd.HasMaxTimeLagMs() ? cmd.GetMaxTimeLagMs() : 0,
                                       cmd.HasReadTimestampMs() ? cmd.GetReadTimestampMs() : 0, clientDC,
                                       cmd.GetExternalOperation());
        ctx.Send(partActor, event.Release());
    }
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

void TPersQueue::HandleRegisterMessageGroupRequest(ui64 responseCookie, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdRegisterMessageGroup());

    NPersQueue::NErrorCode::EErrorCode code;
    TString error;
    auto body = MakeRegisterMessageGroup(req.GetCmdRegisterMessageGroup(), code, error);

    if (!body) {
        return ReplyError(ctx, responseCookie, code, error);
    }

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_REGISTER_MESSAGE_GROUP);
    ctx.Send(partActor, new TEvPQ::TEvRegisterMessageGroup(responseCookie, std::move(body.GetRef())));
}

void TPersQueue::HandleDeregisterMessageGroupRequest(ui64 responseCookie, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdDeregisterMessageGroup());

    NPersQueue::NErrorCode::EErrorCode code;
    TString error;
    auto body = MakeDeregisterMessageGroup(req.GetCmdDeregisterMessageGroup(), code, error);

    if (!body) {
        return ReplyError(ctx, responseCookie, code, error);
    }

    InitResponseBuilder(responseCookie, 1, COUNTER_LATENCY_PQ_DEREGISTER_MESSAGE_GROUP);
    ctx.Send(partActor, new TEvPQ::TEvDeregisterMessageGroup(responseCookie, std::move(body.GetRef())));
}

void TPersQueue::HandleSplitMessageGroupRequest(ui64 responseCookie, const TActorId& partActor,
    const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx)
{
    Y_VERIFY(req.HasCmdSplitMessageGroup());
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
    ctx.Send(partActor, new TEvPQ::TEvSplitMessageGroup(responseCookie, std::move(deregistrations), std::move(registrations)));
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
    if (request.HasPartitionRequest() && request.GetPartitionRequest().HasCmdRead() && s != TMP_REQUEST_MARKER) {
        TActorId rr = CreateReadProxy(ev->Sender, ctx.SelfID, request, ctx);
        ans = CreateResponseProxy(rr, ctx.SelfID, TopicName, p, m, s, c, ResourceMetrics, ctx);
    } else {
        ans = CreateResponseProxy(ev->Sender, ctx.SelfID, TopicName, p, m, s, c, ResourceMetrics, ctx);
    }
    ui64 responseCookie = ++NextResponseCookie;
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

    auto& req = request.GetPartitionRequest();

    if (!req.HasPartition()) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST, "no partition number");
        return;
    }

    ui32 partition = req.GetPartition();
    auto it = Partitions.find(partition);

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " got client message batch for topic " << TopicConverter->GetClientsideName() << " partition " << partition);

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
        + req.HasCmdSplitMessageGroup();

    if (count != 1) {
        ReplyError(ctx, responseCookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "multiple commands in request: " << count);
        return;
    }

    const TActorId& partActor = it->second.Actor;

    TActorId pipeClient = ActorIdFromProto(req.GetPipeClient());

    if (req.HasCmdGetMaxSeqNo()) {
        HandleGetMaxSeqNoRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdDeleteSession()) {
        HandleDeleteSessionRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdCreateSession()) {
        HandleCreateSessionRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdSetClientOffset()) {
        HandleSetClientOffsetRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdGetClientOffset()) {
        HandleGetClientOffsetRequest(responseCookie, partActor, req, ctx);
    } else if (req.CmdWriteSize()) {
        HandleWriteRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdUpdateWriteTimestamp()) {
        HandleUpdateWriteTimestampRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdRead()) {
        HandleReadRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdGetOwnership()) {
        HandleGetOwnershipRequest(responseCookie, partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdReserveBytes()) {
        HandleReserveBytesRequest(responseCookie, partActor, req, ctx, pipeClient, ev->Sender);
    } else if (req.HasCmdRegisterMessageGroup()) {
        HandleRegisterMessageGroupRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdDeregisterMessageGroup()) {
        HandleDeregisterMessageGroupRequest(responseCookie, partActor, req, ctx);
    } else if (req.HasCmdSplitMessageGroup()) {
        HandleSplitMessageGroupRequest(responseCookie, partActor, req, ctx);
    } else Y_FAIL("unknown or empty command");
}


void TPersQueue::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&)
{
    auto it = PipesInfo.find(ev->Get()->ClientId);

    if (it == PipesInfo.end()) {
        PipesInfo.insert({ev->Get()->ClientId, {TActorId(), "", 1}});
    } else {
        it->second.ServerActors++;
    }

    Counters->Simple()[COUNTER_PQ_TABLET_OPENED_PIPES] = PipesInfo.size();
}


void TPersQueue::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx)
{
    //inform partition if needed;
    auto it = PipesInfo.find(ev->Get()->ClientId);
    if (it != PipesInfo.end()) {
        if(--(it->second.ServerActors) > 0) {
            return;
        }
        if (it->second.PartActor != TActorId()) {
            ctx.Send(it->second.PartActor, new TEvPQ::TEvPipeDisconnected(it->second.Owner, it->first));
        }
        PipesInfo.erase(it);
        Counters->Simple()[COUNTER_PQ_TABLET_OPENED_PIPES] = PipesInfo.size();
    }
}

void TPersQueue::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx)
{
    Y_VERIFY(ev->Get()->Leader, "Unexpectedly connected to follower of tablet %" PRIu64, ev->Get()->TabletId);

    if (PipeClientCache->OnConnect(ev)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                    "Tablet " << TabletID() <<
                    " Connected to tablet " << ev->Get()->TabletId);
        return;
    }

    RestartPipe(ev->Get()->TabletId, ctx);
}

void TPersQueue::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                "Tablet " << TabletID() <<
                " Client pipe to tablet " << ev->Get()->TabletId << " is reset");

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

bool TPersQueue::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx)
{
    if (!ev)
        return true;

    if (ev->Get()->Cgi().Has("kv")) {
        return TKeyValueFlat::OnRenderAppHtmlPage(ev, ctx);
    }
    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " Handle TEvRemoteHttpInfo: " << ev->Get()->Query);
    TMap<ui32, TActorId> res;
    for (auto& p : Partitions) {
        res.insert({p.first, p.second.Actor});
    }
    ctx.Register(new TMonitoringProxy(ev->Sender, ev->Get()->Query, res, CacheActor, TopicName, TabletID(), ResponseProxy.size()));
    return true;
}


void TPersQueue::HandleDie(const TActorContext& ctx)
{
    MeteringSink.MayFlushForcibly(ctx.Now());

    for (const auto& p : Partitions) {
        ctx.Send(p.second.Actor, new TEvents::TEvPoisonPill());
    }
    ctx.Send(CacheActor, new TEvents::TEvPoisonPill());


    for (const auto& p : ResponseProxy) {
        THolder<TEvPQ::TEvError> ev = MakeHolder<TEvPQ::TEvError>(NPersQueue::NErrorCode::INITIALIZING, "tablet will be restarted right now", p.first);
        bool res = p.second->HandleError(ev.Get(), ctx);
        Y_VERIFY(res);
    }
    ResponseProxy.clear();
    NKeyValue::TKeyValueFlat::HandleDie(ctx);
}


TPersQueue::TPersQueue(const TActorId& tablet, TTabletStorageInfo *info)
    : TKeyValueFlat(tablet, info)
    , ConfigInited(false)
    , PartitionsInited(0)
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

    ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(ctx.SelfID.NodeId()));
}

void TPersQueue::Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev, const TActorContext& ctx)
{
    Y_VERIFY(ev->Get()->Node);
    DCId = ev->Get()->Node->Location.GetDataCenterId();
    ResourceMetrics = Executor()->GetResourceMetrics();

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    request->Record.SetCookie(READ_CONFIG_COOKIE);

    request->Record.AddCmdRead()->SetKey(KeyConfig());
    request->Record.AddCmdRead()->SetKey(KeyState());
    request->Record.AddCmdRead()->SetKey(KeyTxInfo());

    auto cmd = request->Record.AddCmdReadRange();
    cmd->MutableRange()->SetFrom(GetTxKey(Min<ui64>()));
    cmd->MutableRange()->SetIncludeFrom(true);
    cmd->MutableRange()->SetTo(GetTxKey(Max<ui64>()));
    cmd->MutableRange()->SetIncludeTo(true);
    cmd->SetIncludeData(true);

    request->Record.MutableCmdSetExecutorFastLogPolicy()
                ->SetIsAllowed(AppData(ctx)->PQConfig.GetTactic() == NKikimrClient::TKeyValueRequest::MIN_LATENCY);
    ctx.Send(ctx.SelfID, request.Release());
    ctx.Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
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
    ctx.Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
}

void TPersQueue::Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " Handle TEvPersQueue::TEvProposeTransaction");

    NKikimrPQ::TEvProposeTransaction& event = ev->Get()->Record;
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
                                    ctx);
        break;
    }

}

void TPersQueue::HandleDataTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> ev,
                                       const TActorContext& ctx)
{
    NKikimrPQ::TEvProposeTransaction& event = ev->Record;
    Y_VERIFY(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_VERIFY(event.HasData());
    const NKikimrPQ::TDataTransaction& txBody = event.GetData();

    for (auto& operation : txBody.GetOperations()) {
        Y_VERIFY(!operation.HasPath() || (operation.GetPath() == TopicPath));

        bool isWriteOperation = !operation.HasBegin();

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                    "Tablet " << TabletID() <<
                    " tx=" << event.GetTxId() <<
                    ", lock_tx_id=" << txBody.GetLockTxId() <<
                    ", path=" << operation.GetPath() <<
                    ", partition=" << operation.GetPartitionId() <<
                    ", consumer=" << operation.GetConsumer() <<
                    ", begin=" << operation.GetBegin() <<
                    ", end=" << operation.GetEnd() <<
                    ", is_write=" << isWriteOperation);
    }

    if (TabletState != NKikimrPQ::ENormal) {
        SendProposeTransactionAbort(ActorIdFromProto(event.GetSourceActor()),
                                    event.GetTxId(),
                                    ctx);
        return;
    }

    //
    // TODO(abcdef): сохранить пока инициализируемся. TEvPersQueue::TEvHasDataInfo::TPtr как образец. не только конфиг. Inited==true
    //

    if (txBody.GetImmediate()) {
        //
        // FIXME(abcdef): вместо Y_VERIFY отправлять TEvProposeTransactionResult с кодом ошибки
        //
        Y_VERIFY(txBody.OperationsSize() > 0);

        auto i = Partitions.find(txBody.GetOperations(0).GetPartitionId());
        Y_VERIFY(i != Partitions.end());

        ctx.Send(i->second.Actor, ev.Release());
    } else {
        EvProposeTransactionQueue.emplace_back(ev.Release());

        TryWriteTxs(ctx);
    }
}

void TPersQueue::HandleConfigTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> ev,
                                         const TActorContext& ctx)
{
    NKikimrPQ::TEvProposeTransaction& event = ev->Record;
    Y_VERIFY(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kConfig);
    Y_VERIFY(event.HasConfig());

    EvProposeTransactionQueue.emplace_back(ev.Release());

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " Handle TEvTxProcessing::TEvPlanStep");

    NKikimrTx::TEvMediatorPlanStep& event = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                "Tablet: " << TabletID() <<
                ", PlanStep: " << event.GetStep() <<
                ", Mediator: " << event.GetMediatorID());

    EvPlanStepQueue.emplace_back(ev->Sender, ev->Release().Release());

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " Handle TEvTxProcessing::TEvReadSet");

    NKikimrTx::TEvReadSet& event = ev->Get()->Record;
    Y_VERIFY(event.HasTxId());

    std::unique_ptr<TEvTxProcessing::TEvReadSetAck> ack;
    if (!(event.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_ACK)) {
        ack = std::make_unique<TEvTxProcessing::TEvReadSetAck>(*ev->Get(), TabletID()); 
    }

    if (auto tx = GetTransaction(ctx, event.GetTxId()); tx && tx->Senders.contains(event.GetTabletProducer())) {
        tx->OnReadSet(event, ev->Sender, std::move(ack));

        if (tx->State == NKikimrPQ::TTransaction::WAIT_RS) {
            CheckTxState(ctx, *tx);

            TryWriteTxs(ctx);
        }
    } else if (ack) {
        //
        // для неизвестных транзакций подтверждение отправляется сразу
        //
        ctx.Send(ev->Sender, ack.release());
    }
}

void TPersQueue::Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " Handle TEvTxProcessing::TEvReadSetAck");

    NKikimrTx::TEvReadSetAck& event = ev->Get()->Record;
    Y_VERIFY(event.HasTxId());

    auto tx = GetTransaction(ctx, event.GetTxId());
    if (!tx) {
        return;
    }

    tx->OnReadSetAck(event);
    tx->UnbindMsgsFromPipe(event.GetTabletConsumer());

    if (tx->State == NKikimrPQ::TTransaction::EXECUTED) {
        CheckTxState(ctx, *tx);

        TryWriteTxs(ctx);
    }
}

void TPersQueue::Handle(TEvPQ::TEvTxCalcPredicateResult::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvTxCalcPredicateResult& event = *ev->Get();

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                "Tablet " << TabletID() <<
                " Handle TEvPQ::TEvTxCalcPredicateResult" <<
                " Step " << event.Step <<
                " TxId " << event.TxId <<
                " Partition " << event.Partition <<
                " Predicate " << (event.Predicate ? "true" : "false"));

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    tx->OnTxCalcPredicateResult(event);

    CheckTxState(ctx, *tx);

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPQ::TEvProposePartitionConfigResult::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvProposePartitionConfigResult& event = *ev->Get();
    
    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    Y_VERIFY(tx->State == NKikimrPQ::TTransaction::CALCULATING);

    tx->OnProposePartitionConfigResult(event);

    CheckTxState(ctx, *tx);

    TryWriteTxs(ctx);
}

void TPersQueue::Handle(TEvPQ::TEvTxCommitDone::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Tablet " << TabletID() << " Handle TEvPQ::TEvTxCommitDone");

    const TEvPQ::TEvTxCommitDone& event = *ev->Get();

    auto tx = GetTransaction(ctx, event.TxId);
    if (!tx) {
        return;
    }

    Y_VERIFY(tx->State == NKikimrPQ::TTransaction::EXECUTING);

    tx->OnTxCommitDone(event);

    CheckTxState(ctx, *tx);

    TryWriteTxs(ctx);
}

void TPersQueue::BeginWriteTxs(const TActorContext& ctx)
{
    Y_VERIFY(!WriteTxsInProgress);

    if (EvProposeTransactionQueue.empty() && EvPlanStepQueue.empty() && WriteTxs.empty() && DeleteTxs.empty()) {
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

    ctx.Send(ctx.SelfID, request.Release());

    TryReturnTabletStateAll(ctx);
}

void TPersQueue::EndWriteTxs(const NKikimrClient::TResponse& resp,
                             const TActorContext& ctx)
{
    Y_VERIFY(WriteTxsInProgress);

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
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
            "Tablet " << TabletID() << " SelfId " << ctx.SelfID << " TxInfo write error: " << resp.DebugString());

        ctx.Send(ctx.SelfID, new TEvents::TEvPoisonPill());
        return;
    }

    SendReplies(ctx);
    CheckChangedTxStates(ctx);

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
    while (!EvProposeTransactionQueue.empty()) {
        const auto front = std::move(EvProposeTransactionQueue.front());
        EvProposeTransactionQueue.pop_front();

        const NKikimrPQ::TEvProposeTransaction& event = front->Record;
        TDistributedTransaction& tx = Txs[event.GetTxId()];

        switch (tx.State) {
        case NKikimrPQ::TTransaction::UNKNOWN:
            tx.OnProposeTransaction(event, GetAllowedStep(),
                                    TabletID());
            CheckTxState(ctx, tx);
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
            Y_FAIL();
        }
    }
}

void TPersQueue::ProcessPlanStepQueue(const TActorContext& ctx)
{
    Y_VERIFY(!WriteTxsInProgress);

    while (!EvPlanStepQueue.empty()) {
        const auto front = std::move(EvPlanStepQueue.front());
        EvPlanStepQueue.pop_front();

        const TActorId& sender = front.first;
        const NKikimrTx::TEvMediatorPlanStep& event = front.second->Record;

        ui64 step = event.GetStep();

        TVector<ui64> txIds;
        THashMap<TActorId, TVector<ui64>> txAcks;

        for (auto& tx : event.GetTransactions()) {
            Y_VERIFY(tx.HasTxId());
            Y_VERIFY(tx.HasAckTo());

            txIds.push_back(tx.GetTxId());
            txAcks[ActorIdFromProto(tx.GetAckTo())].push_back(tx.GetTxId());
        }

        if (step >= LastStep) {
            ui64 lastPlannedTxId = 0;

            for (ui64 txId : txIds) {
                Y_VERIFY(lastPlannedTxId < txId);

                if (auto p = Txs.find(txId); p != Txs.end()) {
                    TDistributedTransaction& tx = p->second;

                    if (tx.Step == Max<ui64>()) {
                        Y_VERIFY(TxQueue.empty() || (TxQueue.back() < std::make_pair(step, txId)));

                        tx.OnPlanStep(step);
                        CheckTxState(ctx, tx);

                        TxQueue.emplace(step, txId);
                    } else {
                        LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE,
                                   "Tablet " << TabletID() <<
                                   " Transaction already planned for step " << tx.Step <<
                                   ", Step: " << step <<
                                   ", TxId: " << txId);
                    }
                } else {
                    LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE,
                               "Tablet " << TabletID() <<
                               " Unknown transaction " << txId <<
                               ", Step: " << step);
                }

                lastPlannedTxId = txId;
            }

            LastStep = step;
            LastTxId = lastPlannedTxId;
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                        "Tablet " << TabletID() <<
                        " Old plan step " << step <<
                        ", LastStep: " << LastStep);
        }

        SchedulePlanStepAck(step, txAcks);
        SchedulePlanStepAccepted(sender, step);
    }
}

void TPersQueue::ProcessWriteTxs(const TActorContext& ctx,
                                 NKikimrClient::TKeyValueRequest& request)
{
    Y_VERIFY(!WriteTxsInProgress);

    for (auto& [txId, state] : WriteTxs) {
        auto tx = GetTransaction(ctx, txId);
        Y_VERIFY(tx);

        tx->AddCmdWrite(request, state);

        ChangedTxs.insert(txId);
    }

    WriteTxs.clear();
}

void TPersQueue::ProcessDeleteTxs(const TActorContext& ctx,
                                  NKikimrClient::TKeyValueRequest& request)
{
    Y_VERIFY(!WriteTxsInProgress);

    for (ui64 txId : DeleteTxs) {
        auto tx = GetTransaction(ctx, txId);
        Y_VERIFY(tx);

        tx->AddCmdDelete(request);

        Txs.erase(tx->TxId);
    }

    DeleteTxs.clear();
}

void TPersQueue::ProcessConfigTx(const TActorContext& ctx,
                                 TEvKeyValue::TEvRequest* request)
{
    Y_VERIFY(!WriteTxsInProgress);

    if (!TabletConfigTx.Defined()) {
        return;
    }

    AddCmdWriteConfig(request,
                      *TabletConfigTx,
                      *BootstrapConfigTx,
                      ctx);

    TabletConfigTx = Nothing();
    BootstrapConfigTx = Nothing();
}

void TPersQueue::AddCmdWriteTabletTxInfo(NKikimrClient::TKeyValueRequest& request)
{
    NKikimrPQ::TTabletTxInfo info;
    info.SetLastStep(LastStep);
    info.SetLastTxId(LastTxId);

    TString value;
    Y_VERIFY(info.SerializeToString(&value));

    auto command = request.AddCmdWrite();
    command->SetKey(KeyTxInfo());
    command->SetValue(value);
}

void TPersQueue::ScheduleProposeTransactionResult(const TDistributedTransaction& tx)
{
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

    TString body;
    Y_VERIFY(data.SerializeToString(&body));

    for (ui64 receiverId : tx.Receivers) {
        if (receiverId != TabletID()) {
            auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(tx.Step,
                                                                       tx.TxId,
                                                                       TabletID(),
                                                                       receiverId,
                                                                       TabletID(),
                                                                       body,
                                                                       0);
            SendToPipe(receiverId, tx, std::move(event), ctx);
        }
    }
}

void TPersQueue::SendEvReadSetAckToSenders(const TActorContext& ctx,
                                           TDistributedTransaction& tx)
{
    for (auto& [target, event] : tx.ReadSetAcks) {
        ctx.Send(target, event.release());
    }
}

void TPersQueue::SendEvTxCalcPredicateToPartitions(const TActorContext& ctx,
                                                   TDistributedTransaction& tx)
{
    THashMap<ui32, std::unique_ptr<TEvPQ::TEvTxCalcPredicate>> events;

    for (auto& operation : tx.Operations) {
        auto& event = events[operation.GetPartitionId()];
        if (!event) {
            event = std::make_unique<TEvPQ::TEvTxCalcPredicate>(tx.Step, tx.TxId);
        }

        event->AddOperation(operation.GetConsumer(),
                            operation.GetBegin(),
                            operation.GetEnd());
    }

    for (auto& [partition, event] : events) {
        auto p = Partitions.find(partition);
        Y_VERIFY(p != Partitions.end());

        ctx.Send(p->second.Actor, event.release());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = events.size();
}

void TPersQueue::SendEvTxCommitToPartitions(const TActorContext& ctx,
                                            TDistributedTransaction& tx)
{
    for (ui32 partitionId : tx.Partitions) {
        auto event = std::make_unique<TEvPQ::TEvTxCommit>(tx.Step, tx.TxId);

        auto p = Partitions.find(partitionId);
        Y_VERIFY(p != Partitions.end());

        ctx.Send(p->second.Actor, event.release());
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = tx.Partitions.size();
}

void TPersQueue::SendEvTxRollbackToPartitions(const TActorContext& ctx,
                                              TDistributedTransaction& tx)
{
    for (ui32 partitionId : tx.Partitions) {
        auto event = std::make_unique<TEvPQ::TEvTxRollback>(tx.Step, tx.TxId);

        auto p = Partitions.find(partitionId);
        Y_VERIFY(p != Partitions.end());

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


    ctx.Send(tx.SourceActor, std::move(result));
}

void TPersQueue::SendToPipe(ui64 tabletId,
                            TDistributedTransaction& tx,
                            std::unique_ptr<IEventBase> event,
                            const TActorContext& ctx)
{
    Y_VERIFY(event);

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
    auto p = Txs.find(txId);
    if (p == Txs.end()) {
        LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE,
                   "Tablet " << TabletID() <<
                   " Unknown transaction " << txId);
        return nullptr;
    }
    return &p->second;
}

void TPersQueue::CheckTxState(const TActorContext& ctx,
                              TDistributedTransaction& tx)
{
    switch (tx.State) {
    case NKikimrPQ::TTransaction::UNKNOWN:
        Y_VERIFY(tx.TxId != Max<ui64>());

        WriteTx(tx, NKikimrPQ::TTransaction::PREPARED);
        ScheduleProposeTransactionResult(tx);

        tx.State = NKikimrPQ::TTransaction::PREPARING;

        break;

    case NKikimrPQ::TTransaction::PREPARING:
        Y_VERIFY(tx.WriteInProgress);

        tx.WriteInProgress = false;

        //
        // запланированные события будут отправлены в EndWriteTxs
        //

        tx.State = NKikimrPQ::TTransaction::PREPARED;

        break;

    case NKikimrPQ::TTransaction::PREPARED:
        Y_VERIFY(tx.Step != Max<ui64>());

        WriteTx(tx, NKikimrPQ::TTransaction::PLANNED);

        tx.State = NKikimrPQ::TTransaction::PLANNING;

        break;

    case NKikimrPQ::TTransaction::PLANNING:
        Y_VERIFY(tx.WriteInProgress);

        tx.WriteInProgress = false;

        //
        // запланированные события будут отправлены в EndWriteTxs
        //

        tx.State = NKikimrPQ::TTransaction::PLANNED;

        [[fallthrough]];

    case NKikimrPQ::TTransaction::PLANNED:
        if (!TxQueue.empty() && (TxQueue.front().second == tx.TxId)) {
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
                Y_VERIFY(false);
            }

            tx.State = NKikimrPQ::TTransaction::CALCULATING;
        }

        break;

    case NKikimrPQ::TTransaction::CALCULATING:
        Y_VERIFY(tx.PartitionRepliesCount <= tx.PartitionRepliesExpected);

        if (tx.PartitionRepliesCount == tx.PartitionRepliesExpected) {
            switch (tx.Kind) {
            case NKikimrPQ::TTransaction::KIND_DATA:
                SendEvReadSetToReceivers(ctx, tx);

                WriteTx(tx, NKikimrPQ::TTransaction::WAIT_RS);

                tx.State = NKikimrPQ::TTransaction::CALCULATED;
                break;
            case NKikimrPQ::TTransaction::KIND_CONFIG:
                SendEvReadSetToReceivers(ctx, tx);

                tx.State = NKikimrPQ::TTransaction::WAIT_RS;

                CheckTxState(ctx, tx);
                break;
            case NKikimrPQ::TTransaction::KIND_UNKNOWN:
                Y_VERIFY(false);
            }
        }

        break;

    case NKikimrPQ::TTransaction::CALCULATED:
        Y_VERIFY(tx.WriteInProgress);

        tx.WriteInProgress = false;

        tx.State = NKikimrPQ::TTransaction::WAIT_RS;

        [[fallthrough]];

    case NKikimrPQ::TTransaction::WAIT_RS:
        //
        // the number of TEvReadSetAck sent should not be greater than the number of senders
        // from TEvProposeTransaction
        //
        Y_VERIFY(tx.ReadSetAcks.size() <= tx.Senders.size());

        if (tx.HaveParticipantsDecision()) {
            SendEvProposeTransactionResult(ctx, tx);

            if (tx.GetDecision() == NKikimrTx::TReadSetData::DECISION_COMMIT) {
                SendEvTxCommitToPartitions(ctx, tx);
            } else {
                SendEvTxRollbackToPartitions(ctx, tx);
            }

            tx.State = NKikimrPQ::TTransaction::EXECUTING;
        } else {
            break;
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::EXECUTING:
        Y_VERIFY(tx.PartitionRepliesCount <= tx.PartitionRepliesExpected);

        if (tx.PartitionRepliesCount == tx.PartitionRepliesExpected) {
            Y_VERIFY(!TxQueue.empty());
            Y_VERIFY(TxQueue.front().second == tx.TxId);

            switch (tx.Kind) {
            case NKikimrPQ::TTransaction::KIND_DATA:
                SendEvReadSetAckToSenders(ctx, tx);
                break;
            case NKikimrPQ::TTransaction::KIND_CONFIG:
                ApplyNewConfig(tx.TabletConfig, ctx);
                TabletConfigTx = tx.TabletConfig;
                BootstrapConfigTx = tx.BootstrapConfig;
                break;
            case NKikimrPQ::TTransaction::KIND_UNKNOWN:
                Y_VERIFY(false);
            }

            tx.State = NKikimrPQ::TTransaction::EXECUTED;

            TxQueue.pop();
            TryStartTransaction(ctx);
        } else {
            break;
        }

        [[fallthrough]];

    case NKikimrPQ::TTransaction::EXECUTED:
        if (tx.HaveAllRecipientsReceive()) {
            DeleteTx(tx);
        }

        break;
    }
}

void TPersQueue::WriteTx(TDistributedTransaction& tx, NKikimrPQ::TTransaction::EState state)
{
    WriteTxs[tx.TxId] = state;

    tx.WriteInProgress = true;
}

void TPersQueue::DeleteTx(TDistributedTransaction& tx)
{
    DeleteTxs.insert(tx.TxId);

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
    for (ui64 txId : ChangedTxs) {
        auto tx = GetTransaction(ctx, txId);
        Y_VERIFY(tx);

        CheckTxState(ctx, *tx);
    }
    ChangedTxs.clear();
}
 
void TPersQueue::InitProcessingParams(const TActorContext& ctx)
{
    auto appdata = AppData(ctx);
    const ui32 domainId = appdata->DomainsInfo->GetDomainUidByTabletId(TabletID());
    Y_VERIFY(domainId != appdata->DomainsInfo->BadDomainId);
    const auto& domain = appdata->DomainsInfo->GetDomain(domainId);
    ProcessingParams = ExtractProcessingParams(domain);
}

bool TPersQueue::AllTransactionsHaveBeenProcessed() const
{
    return EvProposeTransactionQueue.empty() && Txs.empty();
}

void TPersQueue::SendProposeTransactionAbort(const TActorId& target,
                                             ui64 txId,
                                             const TActorContext& ctx)
{
    auto event = std::make_unique<TEvPersQueue::TEvProposeTransactionResult>();

    event->Record.SetOrigin(TabletID());
    event->Record.SetStatus(NKikimrPQ::TEvProposeTransactionResult::ABORTED);
    event->Record.SetTxId(txId);

    ctx.Send(target, std::move(event));
}

void TPersQueue::SendEvProposePartitionConfig(const TActorContext& ctx,
                                              TDistributedTransaction& tx)
{
    for (auto& [_, partition] : Partitions) {
        auto event = std::make_unique<TEvPQ::TEvProposePartitionConfig>(tx.Step, tx.TxId);

        event->TopicConverter = tx.TopicConverter;
        event->Config = tx.TabletConfig;

        ctx.Send(partition.Actor, std::move(event));
    }

    tx.PartitionRepliesCount = 0;
    tx.PartitionRepliesExpected = Partitions.size();
}

TPartition* TPersQueue::CreatePartitionActor(ui32 partitionId,
                                             const NPersQueue::TTopicConverterPtr topicConverter,
                                             const NKikimrPQ::TPQTabletConfig& config,
                                             bool newPartition,
                                             const TActorContext& ctx)
{
    int channels = Info()->Channels.size() - NKeyValue::BLOB_CHANNEL; // channels 0,1 are reserved in tablet
    Y_VERIFY(channels > 0);   

    return new TPartition(TabletID(),
                          partitionId,
                          ctx.SelfID,
                          CacheActor,
                          topicConverter,
                          DCId,
                          IsServerless,
                          config,
                          *Counters,
                          SubDomainOutOfSpace,
                          (ui32)channels,
                          newPartition);
}

void TPersQueue::CreateNewPartitions(NKikimrPQ::TPQTabletConfig& config,
                                     NPersQueue::TTopicConverterPtr topicConverter,
                                     const TActorContext& ctx)
{
    EnsurePartitionsAreNotDeleted(config);

    Y_VERIFY(ConfigInited && PartitionsInited == Partitions.size());

    if (!config.PartitionsSize()) {
        for (const auto partitionId : config.GetPartitionIds()) {
            config.AddPartitions()->SetPartitionId(partitionId);
        }
    }

    for (const auto& partition : config.GetPartitions()) {
        const auto partitionId = partition.GetPartitionId();
        if (Partitions.contains(partitionId)) {
            continue;
        }

        TActorId actorId = ctx.Register(CreatePartitionActor(partitionId, topicConverter, config, true, ctx));

        Partitions.emplace(std::piecewise_construct,
                           std::forward_as_tuple(partitionId),
                           std::forward_as_tuple(actorId,
                                                 GetPartitionKeyRange(config, partition),
                                                 true,
                                                 *Counters));

        ++PartitionsInited;
    }
}

void TPersQueue::EnsurePartitionsAreNotDeleted(const NKikimrPQ::TPQTabletConfig& config) const
{
    THashSet<ui32> was;

    if (config.PartitionsSize()) {
        for (const auto& partition : config.GetPartitions()) {
            was.insert(partition.GetPartitionId());
        }
    } else {
        for (const auto partitionId : config.GetPartitionIds()) {
            was.insert(partitionId);
        }
    }

    for (const auto& partition : Config.GetPartitions()) {
        Y_VERIFY_S(was.contains(partition.GetPartitionId()), "New config is bad, missing partition " << partition.GetPartitionId());
    }
}
 
void TPersQueue::InitTransactions(const NKikimrClient::TKeyValueResponse::TReadRangeResult& readRange,
                                  THashMap<ui32, TVector<TTransaction>>& partitionTxs)
{
    Txs.clear();
    TxQueue.clear();

    std::deque<std::pair<ui64, ui64>> plannedTxs;

    for (size_t i = 0; i < readRange.PairSize(); ++i) {
        auto& pair = readRange.GetPair(i);

        NKikimrPQ::TTransaction tx;
        Y_VERIFY(tx.ParseFromString(pair.GetValue()));

        Txs.emplace(tx.GetTxId(), tx);

        if (tx.HasStep()) {
            if (std::make_pair(tx.GetStep(), tx.GetTxId()) >= std::make_pair(LastStep, LastTxId)) {
                plannedTxs.emplace_back(tx.GetStep(), tx.GetTxId());
            }
        }
    }

    std::sort(plannedTxs.begin(), plannedTxs.end());
    for (auto& item : plannedTxs) {
        TxQueue.push(item);
    }

    Y_UNUSED(partitionTxs);
}

void TPersQueue::TryStartTransaction(const TActorContext& ctx)
{
    if (TxQueue.empty()) {
        return;
    }

    auto next = GetTransaction(ctx, TxQueue.front().second);
    Y_VERIFY(next);

    CheckTxState(ctx, *next);
}

void TPersQueue::OnInitComplete(const TActorContext& ctx)
{
    SignalTabletActive(ctx);
    TryStartTransaction(ctx);
}

ui64 TPersQueue::GetAllowedStep() const
{
    //
    // TODO(abcdef): использовать MediatorTimeCastEntry
    //
    return Max(LastStep + 1, TAppData::TimeProvider->Now().MilliSeconds());
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
    const TEvPQ::TEvSubDomainStatus& event = *ev->Get();
    SubDomainOutOfSpace = event.SubDomainOutOfSpace();

    for (auto& p : Partitions) {
        ctx.Send(p.second.Actor, new TEvPQ::TEvSubDomainStatus(event.SubDomainOutOfSpace()));
    }
}

void TPersQueue::Handle(TEvPersQueue::TEvProposeTransactionAttach::TPtr &ev, const TActorContext &ctx)
{
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

    ctx.Send(ev->Sender, new TEvPersQueue::TEvProposeTransactionAttachResult(TabletID(), txId, status), 0, ev->Cookie);
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
        default:
            return false;
    }
    return true;
}

} // namespace NKikimr::NPQ
