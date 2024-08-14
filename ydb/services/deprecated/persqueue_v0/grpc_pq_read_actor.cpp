#include "grpc_pq_actor.h"

#include <ydb/core/base/path.h>
#include <ydb/core/client/server/msgbus_server_persqueue.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/persqueue/percentile_counter.h>
#include <ydb/core/persqueue/pq_database.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/library/persqueue/topic_parser/type_definitions.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/library/persqueue/deprecated/read_batch_converter/read_batch_converter.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/protobuf/util/repeated_field_utils.h>

#include <util/string/strip.h>
#include <util/charset/utf8.h>

#include <algorithm>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr {

using namespace NMsgBusProxy;

namespace NGRpcProxy {

using namespace NPersQueue;
using namespace NSchemeCache;

#ifdef PQ_LOG_PREFIX
#undef PQ_LOG_PREFIX
#endif
#define PQ_LOG_PREFIX "session cookie " << Cookie << " client " << InternalClientId << " session " << Session


//11 tries = 10,23 seconds, then each try for 5 seconds , so 21 retries will take near 1 min
static const NTabletPipe::TClientRetryPolicy RetryPolicyForPipes = {
    .RetryLimitCount = 21,
    .MinRetryTime = TDuration::MilliSeconds(10),
    .MaxRetryTime = TDuration::Seconds(5),
    .BackoffMultiplier = 2,
    .DoFirstRetryInstantly = true
};

static const ui64 MAX_INFLY_BYTES = 25_MB;
static const ui32 MAX_INFLY_READS = 10;

static const TDuration READ_TIMEOUT_DURATION = TDuration::Seconds(1);

static const TDuration WAIT_DATA = TDuration::Seconds(10);
static const TDuration PREWAIT_DATA = TDuration::Seconds(9);
static const TDuration WAIT_DELTA = TDuration::MilliSeconds(500);

static const ui64 INIT_COOKIE = Max<ui64>(); //some identifier

static const ui32 MAX_PIPE_RESTARTS = 100; //after 100 restarts without progress kill session
static const ui32 RESTART_PIPE_DELAY_MS = 100;

static const ui64 MAX_READ_SIZE = 100 << 20; //100mb;

static const TDuration DEFAULT_COMMIT_RATE = TDuration::Seconds(1); //1 second;
static const ui32 MAX_COMMITS_INFLY = 3;

static const double LAG_GROW_MULTIPLIER = 1.2; //assume that 20% more data arrived to partitions


//TODO: add here tracking of bytes in/out

#define LOG_PROTO(FieldName)                                                                                \
    if (proto.Has##FieldName()) {                                                                           \
        res << " " << Y_STRINGIZE(FieldName) << " { " << proto.Get##FieldName().ShortDebugString() << " }"; \
    }

#define LOG_FIELD(proto, FieldName)                                                                         \
    if (proto.Has##FieldName()) {                                                                           \
        res << " " << Y_STRINGIZE(FieldName) << ": " << proto.Get##FieldName();                             \
    }

TString PartitionResponseToLog(const NKikimrClient::TPersQueuePartitionResponse& proto) {
    if (!proto.HasCmdReadResult()) {
        return proto.ShortDebugString();
    }
    TStringBuilder res;
    res << "{";


    if (proto.CmdWriteResultSize() > 0) {
        res << " CmdWriteResult {";
        for (const auto& writeRes : proto.GetCmdWriteResult()) {
            res << " { " << writeRes.ShortDebugString() << " }";
        }
        res << " }";
    }

    LOG_PROTO(CmdGetMaxSeqNoResult);
    LOG_PROTO(CmdGetClientOffsetResult);
    LOG_PROTO(CmdGetOwnershipResult);


    if (proto.HasCmdReadResult()) {
        const auto& readRes = proto.GetCmdReadResult();
        res << " CmdReadResult {";
        LOG_FIELD(readRes, MaxOffset);
        LOG_FIELD(readRes, BlobsFromDisk);
        LOG_FIELD(readRes, BlobsFromCache);
        //LOG_FIELD(readRes, ErrorCode);
        LOG_FIELD(readRes, ErrorReason);
        LOG_FIELD(readRes, BlobsCachedSize);
        LOG_FIELD(readRes, SizeLag);
        LOG_FIELD(readRes, RealReadOffset);
        if (readRes.ResultSize() > 0) {
            res << " Result {";
            for (const auto &tRes: readRes.GetResult()) {
                res << " {";
                LOG_FIELD(tRes, Offset);
                LOG_FIELD(tRes, SeqNo);
                LOG_FIELD(tRes, PartNo);
                LOG_FIELD(tRes, TotalParts);
                LOG_FIELD(tRes, TotalSize);
                LOG_FIELD(tRes, WriteTimestampMS);
                LOG_FIELD(tRes, CreateTimestampMS);
                LOG_FIELD(tRes, UncompressedSize);
                LOG_FIELD(tRes, PartitionKey);
                res << " }";
            }
            res << " }";
        }
        res << " }";
    }
    res << " }";
    return res;
}
#undef LOG_PROTO
#undef LOG_FIELD

class TPartitionActor : public NActors::TActorBootstrapped<TPartitionActor> {
public:
     TPartitionActor(const TActorId& parentId, const TString& clientId, const ui64 cookie, const TString& session, const ui32 generation,
                        const ui32 step, const NPersQueue::TTopicConverterPtr& topic, const ui32 partition, const ui64 tabletID,
                        const TReadSessionActor::TTopicCounters& counters, const TString& clientDC);
    ~TPartitionActor();

    void Bootstrap(const NActors::TActorContext& ctx);
    void Die(const NActors::TActorContext& ctx) override;


    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_PARTITION; }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup)
            HFunc(TEvPQProxy::TEvDeadlineExceeded, Handle)

            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvPQProxy::TEvRead, Handle)
            HFunc(TEvPQProxy::TEvCommit, Handle)
            HFunc(TEvPQProxy::TEvReleasePartition, Handle)
            HFunc(TEvPQProxy::TEvLockPartition, Handle)
            HFunc(TEvPQProxy::TEvGetStatus, Handle)
            HFunc(TEvPQProxy::TEvRestartPipe, Handle)

            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(TEvPersQueue::TEvHasDataInfoResponse, Handle);
        default:
            break;
        };
    }


    void Handle(TEvPQProxy::TEvReleasePartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvLockPartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvDeadlineExceeded::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvRead::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommit::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(const TEvPQProxy::TEvRestartPipe::TPtr&, const NActors::TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void HandlePoison(NActors::TEvents::TEvPoisonPill::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleWakeup(const NActors::TActorContext& ctx);

    void CheckRelease(const NActors::TActorContext& ctx);
    void InitLockPartition(const NActors::TActorContext& ctx);
    void InitStartReading(const NActors::TActorContext& ctx);

    void RestartPipe(const NActors::TActorContext& ctx, const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode);
    void WaitDataInPartition(const NActors::TActorContext& ctx);
    void SendCommit(const ui64 readId, const ui64 offset, const TActorContext& ctx);
    void SendPartitionReady(const TActorContext& ctx);

private:
    const TActorId ParentId;
    const TString InternalClientId;
    const TString ClientDC;
    const ui64 Cookie;
    const TString Session;
    const ui32 Generation;
    const ui32 Step;

    NPersQueue::TTopicConverterPtr Topic;
    const ui32 Partition;

    const ui64 TabletID;

    ui64 ReadOffset;
    ui64 ClientReadOffset;
    ui64 ClientCommitOffset;
    bool ClientVerifyReadOffset;
    ui64 CommittedOffset;
    ui64 WriteTimestampEstimateMs;

    ui64 WTime;
    bool InitDone;
    bool StartReading;
    bool AllPrepareInited;
    bool FirstInit;
    TActorId PipeClient;
    ui32 PipeGeneration;
    bool RequestInfly;
    NKikimrClient::TPersQueueRequest CurrentRequest;

    ui64 EndOffset;
    ui64 SizeLag;

    TString ReadGuid; // empty if not reading

    bool NeedRelease;
    bool Released;

    std::set<ui64> WaitDataInfly;
    ui64 WaitDataCookie;
    bool WaitForData;

    bool LockCounted;

    std::deque<std::pair<ui64, ui64>> CommitsInfly; //ReadId, Offset

    TReadSessionActor::TTopicCounters Counters;

    bool FirstRead;
    bool ReadingFinishedSent;
};


TReadSessionActor::TReadSessionActor(
        IReadSessionHandlerRef handler, const NPersQueue::TTopicsListController& topicsHandler, const ui64 cookie,
        const TActorId& pqMetaCache, const TActorId& newSchemeCache, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        const TMaybe<TString> clientDC
)
    : Handler(handler)
    , StartTimestamp(TInstant::Now())
    , PqMetaCache(pqMetaCache)
    , NewSchemeCache(newSchemeCache)
    , AuthInitActor()
    , AuthInflight(false)
    , ClientDC(clientDC ? *clientDC : "other")
    , ClientPath()
    , Session()
    , ClientsideLocksAllowed(false)
    , BalanceRightNow(false)
    , CommitsDisabled(false)
    , BalancersInitStarted(false)
    , InitDone(false)
    , ProtocolVersion(NPersQueue::TReadRequest::Base)
    , MaxReadMessagesCount(0)
    , MaxReadSize(0)
    , MaxReadPartitionsCount(0)
    , MaxTimeLagMs(0)
    , ReadTimestampMs(0)
    , ReadSettingsInited(false)
    , ForceACLCheck(false)
    , RequestNotChecked(true)
    , LastACLCheckTimestamp(TInstant::Zero())
    , ReadOnlyLocal(false)
    , ReadIdToResponse(1)
    , ReadIdCommitted(0)
    , LastCommitTimestamp(TInstant::Zero())
    , CommitInterval(DEFAULT_COMMIT_RATE)
    , CommitsInfly(0)
    , Cookie(cookie)
    , Counters(counters)
    , BytesInflight_(0)
    , RequestedBytes(0)
    , ReadsInfly(0)
    , TopicsHandler(topicsHandler) {
    Y_ASSERT(Handler);
}



TReadSessionActor::~TReadSessionActor() = default;


void TReadSessionActor::Bootstrap(const TActorContext& ctx) {
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ++(*GetServiceCounters(Counters, "pqproxy|readSession")->GetCounter("SessionsCreatedTotal", true));
    }
    StartTime = ctx.Now();
    Become(&TThis::StateFunc);
}


void TReadSessionActor::Die(const TActorContext& ctx) {

    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());

    for (auto& p : Partitions) {
        ctx.Send(p.second.Actor, new TEvents::TEvPoisonPill());

        if (!p.second.Released) {
            auto it = TopicCounters.find(p.second.Converter->GetInternalName());
            Y_ABORT_UNLESS(it != TopicCounters.end());
            it->second.PartitionsInfly.Dec();
            it->second.PartitionsReleased.Inc();
            if (p.second.Releasing)
                it->second.PartitionsToBeReleased.Dec();
        }
    }

    for (auto& t : Topics) {
        if (t.second.PipeClient)
            NTabletPipe::CloseClient(ctx, t.second.PipeClient);
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " is DEAD");

    if (SessionsActive) {
        --(*SessionsActive);
    }
    if (BytesInflight) {
        (*BytesInflight) -= BytesInflight_;
    }
    if (SessionsActive) { //PartsPerSession is inited too
        PartsPerSession.DecFor(Partitions.size(), 1);
    }
    if (!Handler->IsShuttingDown())
        Handler->Finish();
    TActorBootstrapped<TReadSessionActor>::Die(ctx);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext& ctx) {
    CloseSession(TStringBuilder() << "Reads done signal - closing everything", NPersQueue::NErrorCode::OK, ctx);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvWriteDone::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(BytesInflight_ >= ev->Get()->Size);
    BytesInflight_ -= ev->Get()->Size;
    if (BytesInflight) (*BytesInflight) -= ev->Get()->Size;

    const bool isAlive = ProcessReads(ctx);
    Y_UNUSED(isAlive);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvCommit::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (CommitsDisabled) {
        CloseSession(TStringBuilder() << "commits in session are disabled by client option", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    const ui64 readId = ev->Get()->ReadId;
    if (readId <= ReadIdCommitted) {
        CloseSession(TStringBuilder() << "commit of " << ev->Get()->ReadId << " that is already committed", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (readId >= ReadIdToResponse) {
        CloseSession(TStringBuilder() << "commit of unknown cookie " << readId, NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    if (NextCommits.size() >= AppData(ctx)->PQConfig.GetMaxReadCookies()) {
        CloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies() << " unordered cookies to commit " << readId, NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    bool res = NextCommits.insert(readId).second;
    if (!res) {
        CloseSession(TStringBuilder() << "double commit of cookie " << readId, NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " commit request from client for " << readId);
    MakeCommit(ctx);
}

void TReadSessionActor::MakeCommit(const TActorContext& ctx) {
    if (CommitsDisabled)
        return;
    if (ctx.Now() - LastCommitTimestamp < CommitInterval)
        return;
    if (CommitsInfly > MAX_COMMITS_INFLY)
        return;
    ui64 readId = ReadIdCommitted;
    auto it = NextCommits.begin();
    for (;it != NextCommits.end() && (*it) == readId + 1; ++it) {
        ++readId;
    }
    if (readId == ReadIdCommitted)
        return;
    NextCommits.erase(NextCommits.begin(), it);
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " commit request from " << ReadIdCommitted + 1 << " to " << readId);

    auto& commit = Commits[readId];
    commit.StartReadId = ReadIdCommitted + 1;
    commit.Partitions = 0;
    commit.StartTime = ctx.Now();
    ReadIdCommitted = readId;
    LastCommitTimestamp = ctx.Now();
    ++CommitsInfly;
    SLITotal.Inc();
    Y_ABORT_UNLESS(Commits.size() == CommitsInfly);

    // Find last offset info belonging to our read id and its ancestors.
    const auto firstGreater = std::upper_bound(Offsets.begin(), Offsets.end(), readId);
    THashSet<std::pair<TString, ui64>> processedPartitions;

    // Iterate from last to first offsets to find partitions' offsets.
    // Offsets in queue have nondecreasing values (for each partition),
    // so it it sufficient to take only the last offset for each partition.
    // Note: reverse_iterator(firstGreater) points to _before_ firstGreater

    for (auto i = std::make_reverse_iterator(firstGreater), end = std::make_reverse_iterator(Offsets.begin()); i != end; ++i) {
        const TOffsetsInfo& info = *i;
        for (const TOffsetsInfo::TPartitionOffsetInfo& pi : info.PartitionOffsets) {
            if (!ActualPartitionActor(pi.Sender)) {
                continue;
            }
            const auto partitionKey = std::make_pair(pi.Topic, pi.Partition);
            if (!processedPartitions.insert(partitionKey).second) {
                continue; // already processed
            }
            const auto partitionIt = Partitions.find(partitionKey);
            if (partitionIt != Partitions.end() && !partitionIt->second.Released) {
                ctx.Send(partitionIt->second.Actor, new TEvPQProxy::TEvCommit(readId, pi.Offset));
                partitionIt->second.Commits.push_back(readId);
                ++commit.Partitions;
            }
        }
    }
    Offsets.erase(Offsets.begin(), firstGreater);

    AnswerForCommitsIfCan(ctx); //Could be done if all partitions are lost because of balancer dead
}

void TReadSessionActor::Handle(TEvPQProxy::TEvAuth::TPtr& ev, const TActorContext&) {
    ProcessAuth(ev->Get()->Auth);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const TActorContext& ctx) {

    if (!ClientsideLocksAllowed) {
        CloseSession("Partition status available only when ClientsideLocksAllowed is true", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    auto it = Partitions.find(std::make_pair(ev->Get()->Topic, ev->Get()->Partition));

    if (it == Partitions.end() || it->second.Releasing || it->second.LockGeneration != ev->Get()->Generation) {
        //do nothing - already released partition
        LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got NOTACTUAL get status request from client for " << ev->Get()->Topic
                         << ":" << ev->Get()->Partition << " generation " << ev->Get()->Generation);
        return;
    }

    //proxy request to partition - allow initing
    //TODO: add here VerifyReadOffset too and check it against Committed position
    ctx.Send(it->second.Actor, new TEvPQProxy::TEvGetStatus(ev->Get()->Topic, ev->Get()->Partition, ev->Get()->Generation));
}


void TReadSessionActor::Handle(TEvPQProxy::TEvLocked::TPtr& ev, const TActorContext& ctx) {

    RequestNotChecked = true;
    if (!ClientsideLocksAllowed) {
        CloseSession("Locked requests are allowed only when ClientsideLocksAllowed is true", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }
    auto& topic = ev->Get()->Topic;
    if (topic.empty()) {
        CloseSession("empty topic in start_read request", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;

    }
    auto it = Partitions.find(std::make_pair(topic, ev->Get()->Partition));

    if (it == Partitions.end() || it->second.Releasing || it->second.LockGeneration != ev->Get()->Generation) {
        //do nothing - already released partition
        LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got NOTACTUAL lock from client for " << topic
                         << ":" << ev->Get()->Partition << " at offset " << ev->Get()->ReadOffset << " generation " << ev->Get()->Generation);

        return;
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got lock from client for " << ev->Get()->Topic
                         << ":" << ev->Get()->Partition << " at readOffset " << ev->Get()->ReadOffset << " commitOffset " << ev->Get()->CommitOffset <<  " generation " << ev->Get()->Generation);

    //proxy request to partition - allow initing
    //TODO: add here VerifyReadOffset too and check it against Committed position
    ctx.Send(it->second.Actor, new TEvPQProxy::TEvLockPartition(ev->Get()->ReadOffset, ev->Get()->CommitOffset, ev->Get()->VerifyReadOffset, true));
}

void TReadSessionActor::DropPartitionIfNeeded(THashMap<std::pair<TString, ui32>, TPartitionActorInfo>::iterator it, const TActorContext& ctx) {
    if (it->second.Commits.empty() && it->second.Released) {
        ctx.Send(it->second.Actor, new TEvents::TEvPoisonPill());
        bool res = ActualPartitionActors.erase(it->second.Actor);
        Y_ABORT_UNLESS(res);

        if (--NumPartitionsFromTopic[it->second.Converter->GetInternalName()] == 0) {
            bool res = TopicCounters.erase(it->second.Converter->GetInternalName());
            Y_ABORT_UNLESS(res);
        }

        if (SessionsActive) {
            PartsPerSession.DecFor(Partitions.size(), 1);
        }
        Partitions.erase(it);
        if (SessionsActive) {
            PartsPerSession.IncFor(Partitions.size(), 1);
        }
    }
}


void TReadSessionActor::Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const TActorContext& ctx) {

    Y_ABORT_UNLESS(!CommitsDisabled);

    if (!ActualPartitionActor(ev->Sender))
        return;

    ui64 readId = ev->Get()->ReadId;

    auto it = Commits.find(readId);
    Y_ABORT_UNLESS(it != Commits.end());
    --it->second.Partitions;

    auto jt = Partitions.find(std::make_pair(ev->Get()->Topic->GetClientsideName(), ev->Get()->Partition));
    Y_ABORT_UNLESS(jt != Partitions.end());
    Y_ABORT_UNLESS(!jt->second.Commits.empty() && jt->second.Commits.front() == readId);
    jt->second.Commits.pop_front();

    DropPartitionIfNeeded(jt, ctx);

    AnswerForCommitsIfCan(ctx);

    MakeCommit(ctx);
}

void TReadSessionActor::AnswerForCommitsIfCan(const TActorContext& ctx) {
    while (!Commits.empty() && Commits.begin()->second.Partitions == 0) {
        auto it = Commits.begin();
        ui64 readId = it->first;
        TReadResponse result;
        for (ui64 i = it->second.StartReadId; i <= readId; ++i){
            result.MutableCommit()->AddCookie(i);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " replying for commits from " << it->second.StartReadId
                                    << " to " << readId);
        ui64 diff = result.ByteSize();
        BytesInflight_ += diff;
        if (BytesInflight) (*BytesInflight) += diff;
        Handler->Reply(result);

        ui32 commitDurationMs = (ctx.Now() - it->second.StartTime).MilliSeconds();
        CommitLatency.IncFor(commitDurationMs, 1);
        if (commitDurationMs >= AppData(ctx)->PQConfig.GetCommitLatencyBigMs()) {
            SLIBigLatency.Inc();
        }
        Commits.erase(it);
        --CommitsInfly;
        Y_ABORT_UNLESS(Commits.size() == CommitsInfly);
    }
}


void TReadSessionActor::Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev, const TActorContext& ctx) {

    THolder<TEvPQProxy::TEvReadSessionStatusResponse> result(new TEvPQProxy::TEvReadSessionStatusResponse());
    result->Record.SetSession(Session);
    result->Record.SetTimestamp(StartTimestamp.MilliSeconds());

    result->Record.SetClientNode(PeerName);
    result->Record.SetProxyNodeId(ctx.SelfID.NodeId());

    for (auto& p : Partitions) {
        auto part = result->Record.AddPartition();
        part->SetTopic(p.first.first);
        part->SetPartition(p.first.second);
        part->SetAssignId(0);
        for (auto& c : NextCommits) {
            part->AddNextCommits(c);
        }
        part->SetReadIdCommitted(ReadIdCommitted);
        part->SetLastReadId(ReadIdToResponse - 1);
        part->SetTimestampMs(0);
    }

    ctx.Send(ev->Sender, result.Release());
}

void TReadSessionActor::Handle(TEvPQProxy::TEvReadInit::TPtr& ev, const TActorContext& ctx) {

    THolder<TEvPQProxy::TEvReadInit> event(ev->Release());

    if (!Topics.empty()) {
        //answer error
        CloseSession("got second init request", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    const auto& init = event->Request.GetInit();

    if (!init.TopicsSize()) {
        CloseSession("no topics in init request", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (init.GetClientId().empty()) {
        CloseSession("no clientId in init request", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (init.GetProxyCookie() != ctx.SelfID.NodeId() && init.GetProxyCookie() != MAGIC_COOKIE_VALUE) {
        CloseSession("you must perform ChooseProxy request at first and go to ProxyName server with ProxyCookie",  NPersQueue::NErrorCode::BAD_REQUEST, ctx);
        return;
    }

    // ToDo[migration] - consider separate consumer conversion logic - ?
    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ClientPath = init.GetClientId();
        ExternalClientId = ClientPath;
        InternalClientId = ConvertNewConsumerName(init.GetClientId());
    } else {
        ClientPath = StripLeadSlash(MakeConsumerPath(init.GetClientId()));
        ExternalClientId = ClientPath;
        InternalClientId = ConvertNewConsumerName(init.GetClientId());
    }

    Auth = event->Request.GetCredentials();
    event->Request.ClearCredentials();
    Y_PROTOBUF_SUPPRESS_NODISCARD Auth.SerializeToString(&AuthStr);
    TStringBuilder session;
    session << ExternalClientId << "_" << ctx.SelfID.NodeId() << "_" << Cookie << "_" << TAppData::RandomProvider->GenRand64();
    Session = session;
    ProtocolVersion = init.GetProtocolVersion();
    CommitsDisabled = init.GetCommitsDisabled();

    if (ProtocolVersion >= NPersQueue::TReadRequest::ReadParamsInInit) {
        ReadSettingsInited = true;
        MaxReadMessagesCount = NormalizeMaxReadMessagesCount(init.GetMaxReadMessagesCount());
        MaxReadSize = NormalizeMaxReadSize(init.GetMaxReadSize());
        MaxReadPartitionsCount = NormalizeMaxReadPartitionsCount(init.GetMaxReadPartitionsCount());
        MaxTimeLagMs = init.GetMaxTimeLagMs();
        ReadTimestampMs = init.GetReadTimestampMs();
    }

    PeerName = event->PeerName;
    Database = event->Database;

    ReadOnlyLocal = init.GetReadOnlyLocal();

    if (init.GetCommitIntervalMs()) {
        CommitInterval = Min(CommitInterval, TDuration::MilliSeconds(init.GetCommitIntervalMs()));
    }

    for (ui32 i = 0; i < init.PartitionGroupsSize(); ++i) {
        Groups.push_back(init.GetPartitionGroups(i));
    }
    THashSet<TString> topicsToResolve;
    for (ui32 i = 0; i < init.TopicsSize(); ++i) {
        const auto& t = init.GetTopics(i);

        if  (t.empty()) {
            CloseSession("empty topic in init request", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return;
        }

        topicsToResolve.insert(t);
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " init: " << event->Request << " from " << PeerName);

    ClientsideLocksAllowed = init.GetClientsideLocksAllowed();
    BalanceRightNow = init.GetBalancePartitionRightNow() || CommitsDisabled;

    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        SetupCounters();
    }

    if (Auth.GetCredentialsCase() == NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET) {
        LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, "session without AuthInfo : " << ExternalClientId << " from " << PeerName);
        if (SessionsWithoutAuth) {
            ++(*SessionsWithoutAuth);
        }
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            CloseSession("Unauthenticated access is forbidden, please provide credentials", NPersQueue::NErrorCode::ACCESS_DENIED, ctx);
            return;
        }
    }
    TopicsList = TopicsHandler.GetReadTopicsList(
            topicsToResolve, ReadOnlyLocal, Database
    );
    if (!TopicsList.IsValid) {
        return CloseSession(
                TopicsList.Reason,
                NPersQueue::NErrorCode::BAD_REQUEST, ctx
        );
    }
    SendAuthRequest(ctx);

    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", ClientPath.substr(0, ClientPath.find("/"))}}, {"total"}}};
    SLITotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
    SLIErrors = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);

    SLITotal.Inc();
}


void TReadSessionActor::SendAuthRequest(const TActorContext& ctx) {
    AuthInitActor = {};
    AuthInflight = true;

    if (Auth.GetCredentialsCase() == NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET) {
        Token = nullptr;
        CreateInitAndAuthActor(ctx);
        return;
    }
    auto database = Database.empty() ? NKikimr::NPQ::GetDatabaseFromConfig(AppData(ctx)->PQConfig) : Database;
    Y_ABORT_UNLESS(TopicsList.IsValid);
    TVector<TDiscoveryConverterPtr> topics;
    for(const auto& t : TopicsList.Topics) {
        if (topics.size() >= 10) {
            break;
        }
        topics.push_back(t.second);
    }
    ctx.Send(PqMetaCache, new TEvDescribeTopicsRequest(topics, false));
}



void TReadSessionActor::HandleDescribeTopicsResponse(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    TString dbId, folderId;
    for (const auto& entry : ev->Get()->Result->ResultSet) {
        if (!entry.PQGroupInfo)
            continue;
        auto& pqDescr = entry.PQGroupInfo->Description;
        dbId = pqDescr.GetPQTabletConfig().GetYdbDatabaseId();
        folderId = pqDescr.GetPQTabletConfig().GetYcFolderId();
        break;
    }

    auto entries = NKikimr::NGRpcProxy::V1::GetTicketParserEntries(dbId, folderId);

    TString ticket;
    switch (Auth.GetCredentialsCase()) {
        case NPersQueueCommon::TCredentials::kTvmServiceTicket:
            ticket = Auth.GetTvmServiceTicket();
            break;
        case NPersQueueCommon::TCredentials::kOauthToken:
            ticket = Auth.GetOauthToken();
            break;
        default:
            CloseSession("Unknown Credentials case", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return;
    }

    ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket({
            .Database = Database,
            .Ticket = ticket,
            .PeerName = PeerName,
            .Entries = entries
        }));
}

void TReadSessionActor::CreateInitAndAuthActor(const TActorContext& ctx) {
    auto database = Database.empty() ? NKikimr::NPQ::GetDatabaseFromConfig(AppData(ctx)->PQConfig) : Database;
    AuthInitActor = ctx.Register(new V1::TReadInitAndAuthActor(
            ctx, ctx.SelfID, InternalClientId, Cookie, Session, PqMetaCache, NewSchemeCache, Counters, Token,
            TopicsList, TopicsHandler.GetLocalCluster()
    ));
}

void TReadSessionActor::Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx) {
    TString ticket = ev->Get()->Ticket;
    TString maskedTicket = ticket.size() > 5 ? (ticket.substr(0, 5) + "***" + ticket.substr(ticket.size() - 5)) : "***";
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "CheckACL ticket " << maskedTicket << " got result from TICKET_PARSER response: error: " << ev->Get()->Error << " user: "
                            << (ev->Get()->Error.empty() ? ev->Get()->Token->GetUserSID() : ""));

    if (!ev->Get()->Error.empty()) {
        CloseSession(TStringBuilder() << "Ticket parsing error: " << ev->Get()->Error, NPersQueue::NErrorCode::ACCESS_DENIED, ctx);
        return;
    }
    Token = ev->Get()->Token;
    CreateInitAndAuthActor(ctx);
}


void TReadSessionActor::RegisterSession(const TActorId& pipe, const TString& topic, const TActorContext& ctx) {

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " register session to " << topic);
    THolder<TEvPersQueue::TEvRegisterReadSession> request;
    request.Reset(new TEvPersQueue::TEvRegisterReadSession);
    auto& req = request->Record;
    req.SetSession(Session);
    req.SetClientNode(PeerName);
    ActorIdToProto(pipe, req.MutablePipeClient());
    req.SetClientId(InternalClientId);

    for (ui32 i = 0; i < Groups.size(); ++i) {
        req.AddGroups(Groups[i]);
    }

    NTabletPipe::SendData(ctx, pipe, request.Release());
}

void TReadSessionActor::RegisterSessions(const TActorContext& ctx) {
    InitDone = true;

    for (auto& t : Topics) {
        auto& topic = t.first;
        RegisterSession(t.second.PipeClient, topic, ctx);
        NumPartitionsFromTopic[topic] = 0;
    }
}


void TReadSessionActor::SetupCounters()
{
    if (SessionsCreated) {
        return;
    }

    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession")->GetSubgroup("Client", InternalClientId)->GetSubgroup("ConsumerPath", ClientPath);
    SessionsCreated = subGroup->GetExpiringCounter("SessionsCreated", true);
    SessionsActive = subGroup->GetExpiringCounter("SessionsActive", false);
    SessionsWithoutAuth = subGroup->GetExpiringCounter("WithoutAuth", true);
    SessionsWithOldBatchingVersion = subGroup->GetExpiringCounter("SessionsWithOldBatchingVersion", true); // monitoring to ensure that old version is not used anymore
    Errors = subGroup->GetExpiringCounter("Errors", true);
    PipeReconnects = subGroup->GetExpiringCounter("PipeReconnects", true);

    BytesInflight = subGroup->GetExpiringCounter("BytesInflight", false);

    PartsPerSession = NKikimr::NPQ::TPercentileCounter(subGroup->GetSubgroup("sensor", "PartsPerSession"), {}, {}, "Count",
                                            TVector<std::pair<ui64, TString>>{{1, "1"}, {2, "2"}, {5, "5"},
                                                                              {10, "10"}, {20, "20"}, {50, "50"}, {70, "70"},
                                                                              {100, "100"}, {150, "150"}, {300,"300"}, {99999999, "99999999"}}, false);

    ++(*SessionsCreated);
    ++(*SessionsActive);
    PartsPerSession.IncFor(Partitions.size(), 1); //for 0

    if (ProtocolVersion < NPersQueue::TReadRequest::Batching) {
        ++(*SessionsWithOldBatchingVersion);
    }
}


void TReadSessionActor::SetupTopicCounters(const TTopicConverterPtr& topic)
{
    auto& topicCounters = TopicCounters[topic->GetInternalName()];
    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
//client/consumerPath Account/Producer OriginDC Topic/TopicPath

    auto aggr = GetLabels(topic);
    TVector<std::pair<TString, TString>> cons = {{"Client", InternalClientId}, {"ConsumerPath", ClientPath}};

    topicCounters.PartitionsLocked       = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsLocked"}, true);
    topicCounters.PartitionsReleased     = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsReleased"}, true);
    topicCounters.PartitionsToBeReleased = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeReleased"}, false);
    topicCounters.PartitionsToBeLocked   = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeLocked"}, false);
    topicCounters.PartitionsInfly        = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsInfly"}, false);
    topicCounters.Errors                 = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsErrors"}, true);
    topicCounters.Commits                = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"Commits"}, true);
    topicCounters.WaitsForData           = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"WaitsForData"}, true);
}

void TReadSessionActor::SetupTopicCounters(const TTopicConverterPtr& topic, const TString& cloudId,
                                           const TString& dbId, const TString& dbPath, const bool isServerless, const TString& folderId)
{
    auto& topicCounters = TopicCounters[topic->GetInternalName()];
    auto subGroup = NPersQueue::GetCountersForTopic(Counters, isServerless);
//client/consumerPath Account/Producer OriginDC Topic/TopicPath
    auto subgroups = GetSubgroupsForTopic(topic, cloudId, dbId, dbPath, folderId);
    subgroups.push_back({"consumer", ClientPath});

    topicCounters.PartitionsLocked       = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.started"}, true, "name");
    topicCounters.PartitionsReleased     = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.stopped"}, true, "name");
    topicCounters.PartitionsToBeReleased = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.stopping_count"}, false, "name");
    topicCounters.PartitionsToBeLocked   = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.starting_count"}, false, "name");
    topicCounters.PartitionsInfly        = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.count"}, false, "name");
    topicCounters.Errors                 = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.partition_session.errors"}, true, "name");
    topicCounters.Commits                = NKikimr::NPQ::TMultiCounter(subGroup, {}, subgroups, {"api.grpc.topic.stream_read.commits"}, true, "name");
}

void TReadSessionActor::Handle(V1::TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {

    LastACLCheckTimestamp = ctx.Now();

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth ok, got " << ev->Get()->TopicAndTablets.size() << " topics, init done " << InitDone);

    AuthInitActor = TActorId();
    AuthInflight = false;

    if (!InitDone) {

        ui32 initBorder = AppData(ctx)->PQConfig.GetReadInitLatencyBigMs();
        ui32 readBorder = AppData(ctx)->PQConfig.GetReadLatencyBigMs();
        ui32 readBorderFromDisk = AppData(ctx)->PQConfig.GetReadLatencyFromDiskBigMs();

        auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
        InitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "ReadInit", initBorder, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        CommitLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "Commit", AppData(ctx)->PQConfig.GetCommitLatencyBigMs(), {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        SLIBigLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsBigLatency"}, true, "sensor", false);
        ReadLatency = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "Read", readBorder, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        ReadLatencyFromDisk = NKikimr::NPQ::CreateSLIDurationCounter(subGroup, Aggr, "ReadFromDisk", readBorderFromDisk, {100, 200, 500, 1000, 1500, 2000, 5000, 10000, 30000, 99999999});
        SLIBigReadLatency = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"ReadBigLatency"}, true, "sensor", false);
        ReadsTotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"ReadsTotal"}, true, "sensor", false);

        ui32 initDurationMs = (ctx.Now() - StartTime).MilliSeconds();
        InitLatency.IncFor(initDurationMs, 1);
        if (initDurationMs >= initBorder) {
            SLIBigLatency.Inc();
        }


        TReadResponse result;
        result.MutableInit()->SetSessionId(Session);
        ui64 diff = result.ByteSize();
        BytesInflight_ += diff;
        if (BytesInflight) (*BytesInflight) += diff;

        Handler->Reply(result);

        Handler->ReadyForNextRead();

        Y_ABORT_UNLESS(!BalancersInitStarted);
        BalancersInitStarted = true;

        for (auto& [name, t] : ev->Get()->TopicAndTablets) {
            auto& topicHolder = Topics[t.TopicNameConverter->GetInternalName()];
            topicHolder.TabletID = t.TabletID;
            topicHolder.CloudId = t.CloudId;
            topicHolder.DbId = t.DbId;
            topicHolder.DbPath = t.DbPath;
            topicHolder.IsServerless = t.IsServerless;
            topicHolder.FolderId = t.FolderId;
            topicHolder.FullConverter = t.TopicNameConverter;
            FullPathToConverter[t.TopicNameConverter->GetPrimaryPath()] = t.TopicNameConverter;
            const auto& second = t.TopicNameConverter->GetSecondaryPath();
            if (!second.empty()) {
                FullPathToConverter[second] = t.TopicNameConverter;
            }
        }

        for (auto& t : Topics) {
            NTabletPipe::TClientConfig clientConfig;

            clientConfig.CheckAliveness = false;

            clientConfig.RetryPolicy = RetryPolicyForPipes;
            t.second.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, t.second.TabletID, clientConfig));
        }

        RegisterSessions(ctx);

        ctx.Schedule(Min(CommitInterval, CHECK_ACL_DELAY), new TEvents::TEvWakeup());
    } else {
        for (auto& [name, t] : ev->Get()->TopicAndTablets) {
            if (Topics.find(t.TopicNameConverter->GetInternalName()) == Topics.end()) {
                CloseSession(TStringBuilder() << "list of topics changed - new topic '" <<
                             t.TopicNameConverter->GetInternalName() << "' found",
                             NPersQueue::NErrorCode::BAD_REQUEST, ctx);
                return;
            }
        }
    }
}


void TReadSessionActor::Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {

    auto& record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.GetSession() == Session);
    Y_ABORT_UNLESS(record.GetClientId() == InternalClientId);

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());
    auto path = record.GetPath();
    if (path.empty()) {
        path = record.GetTopic();
    }

    auto converterIter = FullPathToConverter.find(NPersQueue::NormalizeFullPath(path));
    if (converterIter.IsEnd()) {
        LOG_ALERT_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " ignored ev lock for event = " << record.ShortDebugString() << " path not recognized"
        );
        CloseSession(
                TStringBuilder() << "Internal server error, cannot parse lock event: " << record.ShortDebugString() << ", reason: topic not found",
                NPersQueue::NErrorCode::ERROR, ctx
        );
        return;
    }
    //auto topic = converterIter->second->GetClientsideName();
    auto intName = converterIter->second->GetInternalName();
    Y_ABORT_UNLESS(!intName.empty());
    auto jt = Topics.find(intName);

    if (jt == Topics.end() || pipe != jt->second.PipeClient) { //this is message from old version of pipe
        LOG_DEBUG_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " ignored ev lock for topic = " << converterIter->second->GetPrintableString()
                              << " path recognized, but topic is unknown, this is unexpected"
        );
        return;
    }
    // ToDo[counters]
    if (NumPartitionsFromTopic[intName]++ == 0) {
        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            SetupTopicCounters(converterIter->second, jt->second.CloudId, jt->second.DbId, jt->second.DbPath, jt->second.IsServerless, jt->second.FolderId);
        } else {
            SetupTopicCounters(converterIter->second);
        }
    }

    auto it = TopicCounters.find(intName);
    Y_ABORT_UNLESS(it != TopicCounters.end());

    IActor* partitionActor = new TPartitionActor(
            ctx.SelfID, InternalClientId, Cookie, Session, record.GetGeneration(),
            record.GetStep(), jt->second.FullConverter, record.GetPartition(), record.GetTabletId(), it->second,
            ClientDC
    );

    TActorId actorId = ctx.Register(partitionActor);
    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }
    Y_ABORT_UNLESS(record.GetGeneration() > 0);
    //Partitions use clientside name !
    auto pp = Partitions.insert({
        std::make_pair(jt->second.FullConverter->GetClientsideName(), record.GetPartition()),
        TPartitionActorInfo{actorId, (((ui64)record.GetGeneration()) << 32) + record.GetStep(), jt->second.FullConverter}
    });
    Y_ABORT_UNLESS(pp.second);
    if (SessionsActive) {
        PartsPerSession.IncFor(Partitions.size(), 1);
    }

    bool res = ActualPartitionActors.insert(actorId).second;
    Y_ABORT_UNLESS(res);

    it->second.PartitionsLocked.Inc();
    it->second.PartitionsInfly.Inc();

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " lock: " << record);

    ctx.Send(actorId, new TEvPQProxy::TEvLockPartition(0, 0, false, !ClientsideLocksAllowed));
}

void TReadSessionActor::Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const TActorContext&) {
    if (!ActualPartitionActor(ev->Sender))
        return;

    auto& evTopic = ev->Get()->Topic;
    auto it = Partitions.find(std::make_pair(evTopic->GetClientsideName(), ev->Get()->Partition));
    Y_ABORT_UNLESS(it != Partitions.end());
    Y_ABORT_UNLESS(it->second.LockGeneration);

    if (it->second.Releasing) //lock request for already released partition - ignore
        return;

    if (ev->Get()->Init) {
        Y_ABORT_UNLESS(!it->second.LockSent);

        it->second.LockSent = true;
        auto topicIter = Topics.find(evTopic->GetInternalName());
        Y_ABORT_UNLESS(topicIter != Topics.end());
        Y_ABORT_UNLESS(ClientsideLocksAllowed);
        TReadResponse result;
        auto lock = result.MutableLock();
        lock->SetTopic(topicIter->second.FullConverter->GetClientsideName());
        lock->SetPartition(ev->Get()->Partition);
        lock->SetReadOffset(ev->Get()->Offset);
        lock->SetEndOffset(ev->Get()->EndOffset);
        lock->SetGeneration(it->second.LockGeneration);
        auto jt = PartitionToReadResponse.find(it->second.Actor);
        if (jt == PartitionToReadResponse.end()) {
            ui64 diff = result.ByteSize();
            BytesInflight_ += diff;
            if (BytesInflight) (*BytesInflight) += diff;
            Handler->Reply(result);
        } else {
            jt->second->ControlMessages.push_back(result);
        }
    } else {
        Y_ABORT_UNLESS(it->second.LockSent);
        TReadResponse result;
        auto status = result.MutablePartitionStatus();
        status->SetTopic(ev->Get()->Topic->GetClientsideName());
        status->SetPartition(ev->Get()->Partition);
        status->SetEndOffset(ev->Get()->EndOffset);
        status->SetGeneration(it->second.LockGeneration);
        status->SetCommittedOffset(ev->Get()->Offset);
        status->SetWriteWatermarkMs(ev->Get()->WriteTimestampEstimateMs);
        auto jt = PartitionToReadResponse.find(it->second.Actor);
        if (jt == PartitionToReadResponse.end()) {
            ui64 diff = result.ByteSize();
            BytesInflight_ += diff;
            if (BytesInflight) (*BytesInflight) += diff;
            Handler->Reply(result);
        } else {
            jt->second->ControlMessages.push_back(result);
        }

    }
}

void TReadSessionActor::Handle(TEvPersQueue::TEvError::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Record.GetDescription(), ev->Get()->Record.GetCode(), ctx);
}


void TReadSessionActor::Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.GetSession() == Session);
    Y_ABORT_UNLESS(record.GetClientId() == InternalClientId);
    auto topic = record.GetPath();
    if (topic.empty()) {
        topic = record.GetTopic();
    }
    ui32 group = record.HasGroup() ? record.GetGroup() : 0;

    auto converterIter = FullPathToConverter.find(NPersQueue::NormalizeFullPath(topic));
    if (converterIter.IsEnd()) {
        LOG_ALERT_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Failed to parse balancer response: " << record.ShortDebugString());
        CloseSession(
                TStringBuilder() << "Internal server error, cannot parse release event: " << record.ShortDebugString() << ", path not recognized",
                NPersQueue::NErrorCode::ERROR, ctx
        );
        return;
    }
    auto name = converterIter->second->GetInternalName();
    auto clientName = converterIter->second->GetClientsideName();

    auto it = Topics.find(name);
    Y_ABORT_UNLESS(it != Topics.end());

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());

    if (pipe != it->second.PipeClient) { //this is message from old version of pipe
        return;
    }

    Y_ABORT_UNLESS(!Partitions.empty());

    TActorId actorId = TActorId{};
    auto jt = Partitions.begin();
    ui32 i = 0;
    for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
        if (it->first.first == clientName && !it->second.Releasing && (group == 0 || it->first.second + 1 == group)) {
            ++i;
            if (rand() % i == 0) { //will lead to 1/n probability for each of n partitions
                actorId = it->second.Actor;
                jt = it;
            }
        }
    }
    Y_ABORT_UNLESS(actorId);

    {
        auto it = TopicCounters.find(name);
        Y_ABORT_UNLESS(it != TopicCounters.end());
        it->second.PartitionsToBeReleased.Inc();
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " releasing " << jt->first.first << ":" << jt->first.second);
    jt->second.Releasing = true;

    ctx.Send(actorId, new TEvPQProxy::TEvReleasePartition());
    if (ClientsideLocksAllowed && jt->second.LockSent && !jt->second.Reading) { //locked and no active reads
        if (!ProcessReleasePartition(jt, BalanceRightNow, false, ctx)) { // returns false if actor died
            return;
        }
    }
    AnswerForCommitsIfCan(ctx); // in case of killing partition
}


void TReadSessionActor::Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActor(ev->Sender))
        return;

    const auto& topic = ev->Get()->Topic;
    const ui32 partition = ev->Get()->Partition;

    auto jt = Partitions.find(std::make_pair(topic->GetClientsideName(), partition));
    Y_ABORT_UNLESS(jt != Partitions.end(), "session %s topic %s part %u", Session.c_str(), topic->GetInternalName().c_str(), partition);
    Y_ABORT_UNLESS(jt->second.Releasing);
    jt->second.Released = true;

    {
        auto it = TopicCounters.find(topic->GetInternalName());
        Y_ABORT_UNLESS(it != TopicCounters.end());
        it->second.PartitionsReleased.Inc();
        it->second.PartitionsInfly.Dec();
        it->second.PartitionsToBeReleased.Dec();

    }

    InformBalancerAboutRelease(jt, ctx);

    DropPartitionIfNeeded(jt, ctx);
}

void TReadSessionActor::InformBalancerAboutRelease(const THashMap<std::pair<TString, ui32>, TPartitionActorInfo>::iterator& it, const TActorContext& ctx) {

    THolder<TEvPersQueue::TEvPartitionReleased> request;
    request.Reset(new TEvPersQueue::TEvPartitionReleased);
    auto& req = request->Record;

    auto jt = Topics.find(it->second.Converter->GetInternalName());
    Y_ABORT_UNLESS(jt != Topics.end());

    req.SetSession(Session);
    ActorIdToProto(jt->second.PipeClient, req.MutablePipeClient());
    req.SetClientId(InternalClientId);
    req.SetTopic(it->first.first);
    req.SetPartition(it->first.second);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " released: " << it->first.first << ":" << it->first.second);

    NTabletPipe::SendData(ctx, jt->second.PipeClient, request.Release());
}


void TReadSessionActor::CloseSession(const TString& errorReason, const NPersQueue::NErrorCode::EErrorCode errorCode, const NActors::TActorContext& ctx) {

    if (errorCode != NPersQueue::NErrorCode::OK) {

        if (InternalErrorCode(errorCode)) {
            SLIErrors.Inc();
        }

        if (Errors) {
            ++(*Errors);
        } else {
            if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
                ++(*GetServiceCounters(Counters, "pqproxy|readSession")->GetCounter("Errors", true));
            }
        }

        TReadResponse result;

        auto error = result.MutableError();
        error->SetDescription(errorReason);
        error->SetCode(errorCode);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed with error reason: " << errorReason);
        if (!Handler->IsShuttingDown()) {
            ui64 diff = result.ByteSize();
            BytesInflight_ += diff;
            if (BytesInflight) (*BytesInflight) += diff;
            Handler->Reply(result);
        } else {
            LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " GRps is shutting dows, skip reply");
        }
    } else {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed");
    }

    Die(ctx);
}


void TReadSessionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        if (msg->Dead) {
            CloseSession(TStringBuilder() << "one of topics is deleted, tablet " << msg->TabletId, NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return;
        }
        //TODO: remove it
        CloseSession(TStringBuilder() << "unable to connect to one of topics, tablet " << msg->TabletId, NPersQueue::NErrorCode::ERROR, ctx);
        return;

        const bool isAlive = ProcessBalancerDead(msg->TabletId, ctx); // returns false if actor died
        Y_UNUSED(isAlive);
        return;
    }
}

bool TReadSessionActor::ActualPartitionActor(const TActorId& part) {
    return ActualPartitionActors.contains(part);
}


bool TReadSessionActor::ProcessReleasePartition(const THashMap<std::pair<TString, ui32>, TPartitionActorInfo>::iterator& it,
                                    bool kill, bool couldBeReads, const TActorContext& ctx)
{
    //inform client
    if (ClientsideLocksAllowed && it->second.LockSent) {
        TReadResponse result;
        result.MutableRelease()->SetTopic(it->first.first);
        result.MutableRelease()->SetPartition(it->first.second);
        result.MutableRelease()->SetCanCommit(!kill);
        result.MutableRelease()->SetGeneration(it->second.LockGeneration);
        auto jt = PartitionToReadResponse.find(it->second.Actor);
        if (jt == PartitionToReadResponse.end()) {
            ui64 diff = result.ByteSize();
            BytesInflight_ += diff;
            if (BytesInflight) (*BytesInflight) += diff;
            Handler->Reply(result);
        } else {
            jt->second->ControlMessages.push_back(result);
        }
        it->second.LockGeneration = 0;
        it->second.LockSent = false;
    }

    if (!kill) {
        return true;
    }

    {
        auto jt = TopicCounters.find(it->second.Converter->GetInternalName());
        Y_ABORT_UNLESS(jt != TopicCounters.end());
        jt->second.PartitionsReleased.Inc();
        jt->second.PartitionsInfly.Dec();
        if (!it->second.Released && it->second.Releasing) {
            jt->second.PartitionsToBeReleased.Dec();
        }
    }

    //process commits
    for (auto& c : it->second.Commits) {
        auto kt = Commits.find(c);
        Y_ABORT_UNLESS(kt != Commits.end());
        --kt->second.Partitions;
    }
    it->second.Commits.clear();

    Y_ABORT_UNLESS(couldBeReads || !it->second.Reading);
    //process reads
    TFormedReadResponse::TPtr formedResponseToAnswer;
    if (it->second.Reading) {
        const auto readIt = PartitionToReadResponse.find(it->second.Actor);
        Y_ABORT_UNLESS(readIt != PartitionToReadResponse.end());
        if (--readIt->second->RequestsInfly == 0) {
            formedResponseToAnswer = readIt->second;
        }
    }

    InformBalancerAboutRelease(it, ctx);

    it->second.Released = true; //to force drop
    DropPartitionIfNeeded(it, ctx); //partition will be dropped

    if (formedResponseToAnswer) {
        return ProcessAnswer(ctx, formedResponseToAnswer); // returns false if actor died
    }
    return true;
}


bool TReadSessionActor::ProcessBalancerDead(const ui64 tablet, const TActorContext& ctx) {
    for (auto& t : Topics) {
        if (t.second.TabletID == tablet) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " balancer for topic " << t.first << " is dead, restarting all from this topic");

            //Drop all partitions from this topic
            for (auto it = Partitions.begin(); it != Partitions.end();) {
                if (it->second.Converter->GetInternalName() == t.first) { //partition from this topic
                    // kill actor
                    auto jt = it;
                    ++it;
                    if (!ProcessReleasePartition(jt, true, true, ctx)) { // returns false if actor died
                        return false;
                    }
                } else {
                    ++it;
                }
            }

            AnswerForCommitsIfCan(ctx);

            //reconnect pipe
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.CheckAliveness = false;
            clientConfig.RetryPolicy = RetryPolicyForPipes;
            t.second.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, t.second.TabletID, clientConfig));
            if (InitDone) {
                if (PipeReconnects) {
                    ++(*PipeReconnects);
                    ++(*Errors);
                }

                RegisterSession(t.second.PipeClient, t.first, ctx);
            }
        }
    }
    return true;
}


void TReadSessionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    const bool isAlive = ProcessBalancerDead(ev->Get()->TabletId, ctx); // returns false if actor died
    Y_UNUSED(isAlive);
}

void TReadSessionActor::ProcessAuth(const NPersQueueCommon::TCredentials& auth) {
    TString tmp;
    Y_PROTOBUF_SUPPRESS_NODISCARD auth.SerializeToString(&tmp);
    if (auth.GetCredentialsCase() != NPersQueueCommon::TCredentials::CREDENTIALS_NOT_SET && tmp != AuthStr) {
        Auth = auth;
        AuthStr = tmp;
        ForceACLCheck = true;
    }
}

void TReadSessionActor::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    THolder<TEvPQProxy::TEvRead> event(ev->Release());

    Handler->ReadyForNextRead();


    ProcessAuth(event->Request.GetCredentials());
    event->Request.ClearCredentials();

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got read request: " << event->Request.GetRead() << " with guid: " << event->Guid);

    Reads.emplace_back(event.Release());

    const bool isAlive = ProcessReads(ctx); // returns false if actor died
    Y_UNUSED(isAlive);
}


i64 TReadSessionActor::TFormedReadResponse::ApplyResponse(NPersQueue::TReadResponse&& resp) {
    Y_ABORT_UNLESS(resp.GetBatchedData().PartitionDataSize() == 1);
    Response.MutableBatchedData()->AddPartitionData()->Swap(resp.MutableBatchedData()->MutablePartitionData(0));
    i64 prev = Response.ByteSize();
    std::swap<i64>(prev, ByteSize);
    return ByteSize - prev;
}


void TReadSessionActor::Handle(TEvPQProxy::TEvReadResponse::TPtr& ev, const TActorContext& ctx) {
    TActorId sender = ev->Sender;
    if (!ActualPartitionActor(sender))
        return;

    THolder<TEvPQProxy::TEvReadResponse> event(ev->Release());

    Y_ABORT_UNLESS(event->Response.GetBatchedData().GetCookie() == 0); // cookie is not assigned
    Y_ABORT_UNLESS(event->Response.GetBatchedData().PartitionDataSize() == 1);

    const TString topic = event->Response.GetBatchedData().GetPartitionData(0).GetTopic();
    const ui32 partition = event->Response.GetBatchedData().GetPartitionData(0).GetPartition();
    std::pair<TString, ui32> key(topic, partition);
    // Topic is expected to have clientSide name
    const auto partitionIt = Partitions.find(key);
    Y_ABORT_UNLESS(partitionIt != Partitions.end());
    Y_ABORT_UNLESS(partitionIt->second.Reading);
    partitionIt->second.Reading = false;

    auto it = PartitionToReadResponse.find(sender);
    Y_ABORT_UNLESS(it != PartitionToReadResponse.end());

    TFormedReadResponse::TPtr formedResponse = it->second;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " read done guid " << formedResponse->Guid
                                                                  << " " << key.first << ":" << key.second
                                                                  << " size " << event->Response.ByteSize());

    const i64 diff = formedResponse->ApplyResponse(std::move(event->Response));
    if (event->FromDisk) {
        formedResponse->FromDisk = true;
    }
    formedResponse->WaitQuotaTime = Max(formedResponse->WaitQuotaTime, event->WaitQuotaTime);
    --formedResponse->RequestsInfly;
    formedResponse->Offsets.PartitionOffsets.emplace_back(sender, topic, partition, event->NextReadOffset);

    BytesInflight_ += diff;
    if (BytesInflight) (*BytesInflight) += diff;

    if (ClientsideLocksAllowed && partitionIt->second.LockSent && partitionIt->second.Releasing) { //locked and need to be released
        if (!ProcessReleasePartition(partitionIt, BalanceRightNow, false, ctx)) { // returns false if actor died
            return;
        }
    }
    AnswerForCommitsIfCan(ctx); // in case of killing partition

    if (formedResponse->RequestsInfly == 0) {
        const bool isAlive = ProcessAnswer(ctx, formedResponse); // returns false if actor died
        Y_UNUSED(isAlive);
    }
}


bool TReadSessionActor::ProcessAnswer(const TActorContext& ctx, TFormedReadResponse::TPtr formedResponse) {
    ui32 readDurationMs = (ctx.Now() - formedResponse->Start - formedResponse->WaitQuotaTime).MilliSeconds();
    if (formedResponse->FromDisk) {
        ReadLatencyFromDisk.IncFor(readDurationMs, 1);
    } else {
        ReadLatency.IncFor(readDurationMs, 1);
    }
    if (readDurationMs >= (formedResponse->FromDisk ? AppData(ctx)->PQConfig.GetReadLatencyFromDiskBigMs() : AppData(ctx)->PQConfig.GetReadLatencyBigMs())) {
        SLIBigReadLatency.Inc();
    }

    Y_ABORT_UNLESS(formedResponse->RequestsInfly == 0);
    i64 diff = formedResponse->Response.ByteSize();
    const bool hasMessages = RemoveEmptyMessages(*formedResponse->Response.MutableBatchedData());
    if (hasMessages) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " assign read id " << ReadIdToResponse << " to read request " << formedResponse->Guid);
        formedResponse->Response.MutableBatchedData()->SetCookie(ReadIdToResponse);
        // reply to client
        if (ProtocolVersion < NPersQueue::TReadRequest::Batching) {
            ConvertToOldBatch(formedResponse->Response);
        }
        diff -= formedResponse->Response.ByteSize(); // Bytes will be tracked inside handler
        Handler->Reply(formedResponse->Response);
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " empty read result " << formedResponse->Guid << ", start new reading");
    }

    BytesInflight_ -= diff;
    if (BytesInflight) (*BytesInflight) -= diff;

    for (auto& r : formedResponse->ControlMessages) {
        ui64 diff = r.ByteSize();
        BytesInflight_ += diff;
        if (BytesInflight) (*BytesInflight) += diff;
        Handler->Reply(r);
    }

    for (const TActorId& p : formedResponse->PartitionsTookPartInRead) {
        PartitionToReadResponse.erase(p);
    }

    // Bring back available partitions.
    // If some partition was removed from partitions container, it is not bad because it will be checked during read processing.
    AvailablePartitions.insert(formedResponse->PartitionsBecameAvailable.begin(), formedResponse->PartitionsBecameAvailable.end());

    formedResponse->Offsets.ReadId = ReadIdToResponse;

    RequestedBytes -= formedResponse->RequestedBytes;

    ReadsInfly--;

    if (hasMessages) {
        if (!CommitsDisabled)
            Offsets.emplace_back(std::move(formedResponse->Offsets)); // even empty responses are needed for correct offsets commit.
        ReadIdToResponse++;
    } else {
        // process new read
        NPersQueue::TReadRequest req;
        req.MutableRead();
        Reads.emplace_back(new TEvPQProxy::TEvRead(req, formedResponse->Guid)); // Start new reading request with the same guid
    }

    return ProcessReads(ctx); // returns false if actor died
}


void TReadSessionActor::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}

void TReadSessionActor::Handle(V1::TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, NErrorCode::EErrorCode(ev->Get()->ErrorCode - 500000), ctx);
}

ui32 TReadSessionActor::NormalizeMaxReadMessagesCount(ui32 sourceValue) {
    ui32 count = Min<ui32>(sourceValue, Max<i32>());
    if (count == 0) {
        count = Max<i32>();
    }
    return count;
}

ui32 TReadSessionActor::NormalizeMaxReadSize(ui32 sourceValue) {
    ui32 size = Min<ui32>(sourceValue, MAX_READ_SIZE);
    if (size == 0) {
        size = MAX_READ_SIZE;
    }
    return size;
}

ui32 TReadSessionActor::NormalizeMaxReadPartitionsCount(ui32 sourceValue) {
    ui32 maxPartitions = sourceValue;
    if (maxPartitions == 0) {
        maxPartitions = Max<ui32>();
    }
    return maxPartitions;
}

bool TReadSessionActor::CheckAndUpdateReadSettings(const NPersQueue::TReadRequest::TRead& readRequest) {
    if (ReadSettingsInited) { // already updated. Check that settings are not changed.
        const bool hasSettings = readRequest.GetMaxCount()
            || readRequest.GetMaxSize()
            || readRequest.GetPartitionsAtOnce()
            || readRequest.GetMaxTimeLagMs()
            || readRequest.GetReadTimestampMs();
        if (!hasSettings) {
            return true;
        }

        const bool settingsChanged = NormalizeMaxReadMessagesCount(readRequest.GetMaxCount()) != MaxReadMessagesCount
            || NormalizeMaxReadSize(readRequest.GetMaxSize()) != MaxReadSize
            || NormalizeMaxReadPartitionsCount(readRequest.GetPartitionsAtOnce()) != MaxReadPartitionsCount
            || readRequest.GetMaxTimeLagMs() != MaxTimeLagMs
            || readRequest.GetReadTimestampMs() != ReadTimestampMs;
        return !settingsChanged;
    } else {
        // Update settings for the first time
        ReadSettingsInited = true;
        MaxReadMessagesCount = NormalizeMaxReadMessagesCount(readRequest.GetMaxCount());
        MaxReadSize = NormalizeMaxReadSize(readRequest.GetMaxSize());
        MaxReadPartitionsCount = NormalizeMaxReadPartitionsCount(readRequest.GetPartitionsAtOnce());
        MaxTimeLagMs = readRequest.GetMaxTimeLagMs();
        ReadTimestampMs = readRequest.GetReadTimestampMs();
        return true;
    }
}

bool TReadSessionActor::ProcessReads(const TActorContext& ctx) {
    while (!Reads.empty() && BytesInflight_ + RequestedBytes < MAX_INFLY_BYTES && ReadsInfly < MAX_INFLY_READS) {
        const auto& readRequest = Reads.front()->Request.GetRead();
        if (!CheckAndUpdateReadSettings(readRequest)) {
            CloseSession("read settings were changed in read request", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return false;
        }

        if (Offsets.size() >= AppData(ctx)->PQConfig.GetMaxReadCookies() + 10) {
            CloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies() << " uncommitted reads", NPersQueue::NErrorCode::BAD_REQUEST, ctx);
            return false;
        }

        ui32 count = MaxReadMessagesCount;
        ui64 size = MaxReadSize;
        ui32 maxPartitions = MaxReadPartitionsCount;
        ui32 partitionsAsked = 0;

        TFormedReadResponse::TPtr formedResponse = new TFormedReadResponse(Reads.front()->Guid, ctx.Now());
        while (!AvailablePartitions.empty()) {
            auto part = *AvailablePartitions.begin();
            AvailablePartitions.erase(AvailablePartitions.begin());

            auto it = Partitions.find(std::make_pair(part.Topic->GetClientsideName(), part.Partition));
            if (it == Partitions.end() || it->second.Releasing || it->second.Actor != part.Actor) { //this is already released partition
                continue;
            }
            //add this partition to reading
            ++partitionsAsked;

            TAutoPtr<TEvPQProxy::TEvRead> read = new TEvPQProxy::TEvRead(Reads.front()->Request, Reads.front()->Guid);
            const ui32 ccount = Min<ui32>(part.MsgLag * LAG_GROW_MULTIPLIER, count);
            count -= ccount;
            const ui64 csize = (ui64)Min<double>(part.SizeLag * LAG_GROW_MULTIPLIER, size);
            size -= csize;

            Y_ABORT_UNLESS(csize < Max<i32>());
            auto* readR = read->Request.MutableRead();
            readR->SetMaxCount(ccount);
            readR->SetMaxSize(csize);
            readR->SetMaxTimeLagMs(MaxTimeLagMs);
            readR->SetReadTimestampMs(ReadTimestampMs);

            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX
                                        << " performing read request: " << (*readR) << " with guid " << read->Guid
                                        << " from " << part.Topic->GetPrintableString() << ", partition:" << part.Partition
                                        << " count " << ccount << " size " << csize
                                        << " partitionsAsked " << partitionsAsked << " maxTimeLag " << MaxTimeLagMs << "ms");


            Y_ABORT_UNLESS(!it->second.Reading);
            it->second.Reading = true;
            formedResponse->PartitionsTookPartInRead.insert(it->second.Actor);

            RequestedBytes += csize;
            formedResponse->RequestedBytes += csize;

            ctx.Send(it->second.Actor, read.Release());
            const auto insertResult = PartitionToReadResponse.insert(std::make_pair(it->second.Actor, formedResponse));
            Y_ABORT_UNLESS(insertResult.second);

            if (--maxPartitions == 0 || count == 0 || size == 0)
                break;
        }
        if (partitionsAsked == 0)
            break;
        ReadsTotal.Inc();
        formedResponse->RequestsInfly = partitionsAsked;

        ReadsInfly++;

        i64 diff = formedResponse->Response.ByteSize();
        BytesInflight_ += diff;
        formedResponse->ByteSize = diff;
        if (BytesInflight) (*BytesInflight) += diff;
        Reads.pop_front();
    }
    return true;
}


void TReadSessionActor::Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const TActorContext& ctx) {

    if (!ActualPartitionActor(ev->Sender))
        return;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << ev->Get()->Topic->GetPrintableString()
                    << " partition:" << ev->Get()->Partition << " ready for read with readOffset "
                    << ev->Get()->ReadOffset << " endOffset " << ev->Get()->EndOffset << " WTime "
                    << ev->Get()->WTime << " sizeLag " << ev->Get()->SizeLag);

    const auto it = PartitionToReadResponse.find(ev->Sender); // check whether this partition is taking part in read response
    auto& container = it != PartitionToReadResponse.end() ? it->second->PartitionsBecameAvailable : AvailablePartitions;
    auto res = container.insert({ev->Get()->Topic, ev->Get()->Partition, ev->Get()->WTime, ev->Get()->SizeLag,
                                 ev->Get()->EndOffset - ev->Get()->ReadOffset, ev->Sender});
    Y_ABORT_UNLESS(res.second);
    const bool isAlive = ProcessReads(ctx); // returns false if actor died
    Y_UNUSED(isAlive);
}


void TReadSessionActor::HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}


void TReadSessionActor::HandleWakeup(const TActorContext& ctx) {
    ctx.Schedule(Min(CommitInterval, CHECK_ACL_DELAY), new TEvents::TEvWakeup());
    MakeCommit(ctx);
    if (!AuthInflight && (ForceACLCheck || (ctx.Now() - LastACLCheckTimestamp > TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()) && RequestNotChecked))) {
        ForceACLCheck = false;
        RequestNotChecked = false;
        Y_ABORT_UNLESS(!AuthInitActor);
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " checking auth because of timeout");

        SendAuthRequest(ctx);
    }
}

bool TReadSessionActor::RemoveEmptyMessages(TReadResponse::TBatchedData& data) {
    bool hasNonEmptyMessages = false;
    auto isMessageEmpty = [&](TReadResponse::TBatchedData::TMessageData& message) -> bool {
        if (message.GetData().empty()) {
            return true;
        } else {
            hasNonEmptyMessages = true;
            return false;
        }
    };
    auto batchRemover = [&](TReadResponse::TBatchedData::TBatch& batch) -> bool {
        NProtoBuf::RemoveRepeatedFieldItemIf(batch.MutableMessageData(), isMessageEmpty);
        return batch.MessageDataSize() == 0;
    };
    auto partitionDataRemover = [&](TReadResponse::TBatchedData::TPartitionData& partition) -> bool {
        NProtoBuf::RemoveRepeatedFieldItemIf(partition.MutableBatch(), batchRemover);
        return partition.BatchSize() == 0;
    };
    NProtoBuf::RemoveRepeatedFieldItemIf(data.MutablePartitionData(), partitionDataRemover);
    return hasNonEmptyMessages;
}


void TReadSessionActor::Handle(TEvPQProxy::TEvReadingStarted::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();

    auto it = Topics.find(msg->Topic);
    if (it == Topics.end()) {
        return;
    }

    auto& topic = it->second;
    NTabletPipe::SendData(ctx, topic.PipeClient, new TEvPersQueue::TEvReadingPartitionStartedRequest(InternalClientId, msg->PartitionId));
}

void TReadSessionActor::Handle(TEvPQProxy::TEvReadingFinished::TPtr& ev, const TActorContext& ctx) {
    auto* msg = ev->Get();

    auto it = Topics.find(msg->Topic);
    if (it == Topics.end()) {
        return;
    }

    auto& topic = it->second;
    NTabletPipe::SendData(ctx, topic.PipeClient, new TEvPersQueue::TEvReadingPartitionFinishedRequest(InternalClientId, msg->PartitionId, false, msg->FirstMessage));
}


////////////////// PARTITION ACTOR

TPartitionActor::TPartitionActor(
        const TActorId& parentId, const TString& internalClientId, const ui64 cookie, const TString& session,
        const ui32 generation, const ui32 step, const NPersQueue::TTopicConverterPtr& topic, const ui32 partition,
        const ui64 tabletID, const TReadSessionActor::TTopicCounters& counters, const TString& clientDC
)
    : ParentId(parentId)
    , InternalClientId(internalClientId)
    , ClientDC(clientDC)
    , Cookie(cookie)
    , Session(session)
    , Generation(generation)
    , Step(step)
    , Topic(topic)
    , Partition(partition)
    , TabletID(tabletID)
    , ReadOffset(0)
    , ClientReadOffset(0)
    , ClientCommitOffset(0)
    , ClientVerifyReadOffset(false)
    , CommittedOffset(0)
    , WriteTimestampEstimateMs(0)
    , WTime(0)
    , InitDone(false)
    , StartReading(false)
    , AllPrepareInited(false)
    , FirstInit(true)
    , PipeClient()
    , PipeGeneration(0)
    , RequestInfly(false)
    , EndOffset(0)
    , SizeLag(0)
    , NeedRelease(false)
    , Released(false)
    , WaitDataCookie(0)
    , WaitForData(false)
    , LockCounted(false)
    , Counters(counters)
    , FirstRead(true)
    , ReadingFinishedSent(false)
{
}


TPartitionActor::~TPartitionActor() = default;


void TPartitionActor::Bootstrap(const TActorContext&) {
    Become(&TThis::StateFunc);
}


void TPartitionActor::CheckRelease(const TActorContext& ctx) {
    const bool hasUncommittedData = ReadOffset > ClientCommitOffset && ReadOffset > ClientReadOffset;
    if (NeedRelease) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                        << " checking release readOffset " << ReadOffset << " committedOffset " << CommittedOffset << " ReadGuid " << ReadGuid
                        << " CommitsInfly.size " << CommitsInfly.size() << " Released " << Released);
    }

    if (NeedRelease && ReadGuid.empty() && CommitsInfly.empty() && !hasUncommittedData && !Released) {
        Released = true;
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReleased(Topic, Partition));
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                        << " check release done - releasing; readOffset " << ReadOffset << " committedOffset " << CommittedOffset << " ReadGuid " << ReadGuid
                        << " CommitsInfly.size " << CommitsInfly.size() << " Released " << Released);

    }
}


void TPartitionActor::SendCommit(const ui64 readId, const ui64 offset, const TActorContext& ctx) {
    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(Topic->GetClientsideName());
    request.MutablePartitionRequest()->SetPartition(Partition);
    request.MutablePartitionRequest()->SetCookie(readId);

    Y_ABORT_UNLESS(PipeClient);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto commit = request.MutablePartitionRequest()->MutableCmdSetClientOffset();
    commit->SetClientId(InternalClientId);
    commit->SetOffset(offset);
    Y_ABORT_UNLESS(!Session.empty());
    commit->SetSessionId(Session);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:"
                        << Partition << " committing to position " << offset << " prev " << CommittedOffset
                        << " end " << EndOffset << " by cookie " << readId);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, req.Release());
}

void TPartitionActor::SendPartitionReady(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << Topic->GetPrintableString() << " partition:" << Partition
                        << " ready for read with readOffset " << ReadOffset << " endOffset " << EndOffset);
    if (FirstRead) {
        ctx.Send(ParentId, new TEvPQProxy::TEvReadingStarted(Topic->GetInternalName(), Partition));
        FirstRead = false;
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Topic, Partition, WTime, SizeLag, ReadOffset, EndOffset));
}


void TPartitionActor::RestartPipe(const TActorContext& ctx, const TString& reason, const NPersQueue::NErrorCode::EErrorCode errorCode) {

    if (!PipeClient)
        return;

    Counters.Errors.Inc();

    NTabletPipe::CloseClient(ctx, PipeClient);
    PipeClient = TActorId{};
    if (errorCode != NPersQueue::NErrorCode::OVERLOAD)
        ++PipeGeneration;

    if (PipeGeneration == MAX_PIPE_RESTARTS) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("too much attempts to restart pipe", NPersQueue::NErrorCode::ERROR));
        return;
    }

    ctx.Schedule(TDuration::MilliSeconds(RESTART_PIPE_DELAY_MS), new TEvPQProxy::TEvRestartPipe());

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                            << " schedule pipe restart attempt " << PipeGeneration << " reason: " << reason);
}


void TPartitionActor::Handle(const TEvPQProxy::TEvRestartPipe::TPtr&, const TActorContext& ctx) {

    Y_ABORT_UNLESS(!PipeClient);

    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
    PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, TabletID, clientConfig));
    Y_ABORT_UNLESS(TabletID);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                            << " pipe restart attempt " << PipeGeneration << " RequestInfly " << RequestInfly << " ReadOffset " << ReadOffset << " EndOffset " << EndOffset
                            << " InitDone " << InitDone << " WaitForData " << WaitForData);

    if (RequestInfly) { //got read infly
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                            << " resend " << CurrentRequest);

        TAutoPtr<TEvPersQueue::TEvRequest> event(new TEvPersQueue::TEvRequest);
        event->Record = CurrentRequest;

        ActorIdToProto(PipeClient, event->Record.MutablePartitionRequest()->MutablePipeClient());

        NTabletPipe::SendData(ctx, PipeClient, event.Release());
    }
    if (InitDone) {
        for (auto& c : CommitsInfly) { //resend all commits
            if (c.second != Max<ui64>())
                SendCommit(c.first, c.second, ctx);
        }
        if (WaitForData) { //resend wait-for-data requests
            WaitDataInfly.clear();
            WaitDataInPartition(ctx);
        }
    }
}

void TPartitionActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {

    if (ev->Get()->Record.HasErrorCode() && ev->Get()->Record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        const auto errorCode = ev->Get()->Record.GetErrorCode();
        if (errorCode == NPersQueue::NErrorCode::WRONG_COOKIE || errorCode == NPersQueue::NErrorCode::BAD_REQUEST) {
            Counters.Errors.Inc();
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("status is not ok: " + ev->Get()->Record.GetErrorReason(), ev->Get()->Record.GetErrorCode()));
        } else {
            RestartPipe(ctx, TStringBuilder() << "status is not ok. Code: " << EErrorCode_Name(errorCode) << ". Reason: " << ev->Get()->Record.GetErrorReason(), errorCode);
        }
        return;
    }

    if (ev->Get()->Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) { //this is incorrect answer, die
        Y_ABORT_UNLESS(!ev->Get()->Record.HasErrorCode());
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("status is not ok: " + ev->Get()->Record.GetErrorReason(), NPersQueue::NErrorCode::ERROR));
        return;
    }
    if (!ev->Get()->Record.HasPartitionResponse()) { //this is incorrect answer, die
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("empty partition in response", NPersQueue::NErrorCode::ERROR));
        return;
    }

    const auto& result = ev->Get()->Record.GetPartitionResponse();

    if (!result.HasCookie()) { //this is incorrect answer, die
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("no cookie in response", NPersQueue::NErrorCode::ERROR));
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString()
                        << " partition:" << Partition
                        << " initDone " << InitDone << " event " << PartitionResponseToLog(result));


    if (!InitDone) {
        if (result.GetCookie() != INIT_COOKIE) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString()
                            << " partition:" << Partition
                            << " unwaited response in init with cookie " << result.GetCookie());
            return;
        }
        Y_ABORT_UNLESS(RequestInfly);
        CurrentRequest.Clear();
        RequestInfly = false;

        Y_ABORT_UNLESS(result.HasCmdGetClientOffsetResult());
        const auto& resp = result.GetCmdGetClientOffsetResult();
        Y_ABORT_UNLESS(resp.HasEndOffset());
        EndOffset = resp.GetEndOffset();
        SizeLag = resp.GetSizeLag();

        ClientCommitOffset = ReadOffset = CommittedOffset = resp.HasOffset() ? resp.GetOffset() : 0;
        Y_ABORT_UNLESS(EndOffset >= CommittedOffset);

        if (resp.HasWriteTimestampMS())
            WTime = resp.GetWriteTimestampMS();
        WriteTimestampEstimateMs = resp.GetWriteTimestampEstimateMS();
        InitDone = true;
        PipeGeneration = 0; //reset tries counter - all ok
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " INIT DONE " << Topic->GetPrintableString()
                            << " partition:" << Partition
                            << " EndOffset " << EndOffset << " readOffset " << ReadOffset << " committedOffset " << CommittedOffset);


        if (!StartReading) {
            ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Topic, Partition, CommittedOffset, EndOffset, WriteTimestampEstimateMs, true));
        } else {
            InitStartReading(ctx);
        }
        return;
    }

    if (!result.HasCmdReadResult()) { //this is commit response
        if (CommitsInfly.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString()
                            << " partition:" << Partition
                            << " unwaited commit-response with cookie " << result.GetCookie() << "; waiting for nothing");
            return;
        }
        ui64 readId = CommitsInfly.front().first;

        if (result.GetCookie() != readId) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString()
                            << " partition:" << Partition
                            << " unwaited commit-response with cookie " << result.GetCookie() << "; waiting for " << readId);
            return;
        }

        Counters.Commits.Inc();

        CommittedOffset = CommitsInfly.front().second;
        CommitsInfly.pop_front();
        if (readId != Max<ui64>()) //this readId is reserved for upcommits on client skipping with ClientCommitOffset
            ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(readId, Topic, Partition));
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString()
                        << " partition:" << Partition
                        << " commit done to position " << CommittedOffset << " endOffset " << EndOffset << " with cookie " << readId);

        while (!CommitsInfly.empty() && CommitsInfly.front().second == Max<ui64>()) { //this is cookies that have no effect on this partition
            readId = CommitsInfly.front().first;
            CommitsInfly.pop_front();
            ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(readId, Topic, Partition));
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString()
                        << "partition :" << Partition
                        << " commit done with no effect with cookie " << readId);
        }

        CheckRelease(ctx);
        PipeGeneration = 0; //reset tries counter - all ok
        return;
    }

    //This is read

    Y_ABORT_UNLESS(result.HasCmdReadResult());
    const auto& res = result.GetCmdReadResult();

    if (result.GetCookie() != (ui64)ReadOffset) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Topic->GetPrintableString()
                    << "partition :" << Partition
                    << " unwaited read-response with cookie " << result.GetCookie() << "; waiting for " << ReadOffset << "; current read guid is " << ReadGuid);
        return;
    }

    Y_ABORT_UNLESS(res.HasMaxOffset());
    EndOffset = res.GetMaxOffset();
    SizeLag = res.GetSizeLag();

    const ui64 realReadOffset = res.HasRealReadOffset() ? res.GetRealReadOffset() : 0;

    TReadResponse response;

    auto* data = response.MutableBatchedData();
    auto* partitionData = data->AddPartitionData();
    partitionData->SetTopic(Topic->GetClientsideName());
    partitionData->SetPartition(Partition);

    bool hasOffset = false;

    TReadResponse::TBatchedData::TBatch* currentBatch = nullptr;
    for (ui32 i = 0; i < res.ResultSize(); ++i) {
        const auto& r = res.GetResult(i);

        WTime = r.GetWriteTimestampMS();
        WriteTimestampEstimateMs = Max(WriteTimestampEstimateMs, WTime);
        Y_ABORT_UNLESS(r.GetOffset() >= ReadOffset);
        ReadOffset = r.GetOffset() + 1;
        hasOffset = true;

        auto proto(GetDeserializedData(r.GetData()));
        if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
            continue; //TODO - no such chunks must be on prod
        }
        TString sourceId = "";
        if (!r.GetSourceId().empty()) {
            if (!NPQ::NSourceIdEncoding::IsValidEncoded(r.GetSourceId())) {
                LOG_ERROR_S(
                        ctx, NKikimrServices::PQ_READ_PROXY,
                        PQ_LOG_PREFIX << "read bad sourceId from topic " << Topic->GetPrintableString()
                            << " partition:" << Partition
                            << " offset " << r.GetOffset() << " seqNo " << r.GetSeqNo() << " sourceId '" << r.GetSourceId() << "' ReadGuid " << ReadGuid);
            }
            sourceId = NPQ::NSourceIdEncoding::Decode(r.GetSourceId());
        }

        if (!currentBatch || currentBatch->GetWriteTimeMs() != r.GetWriteTimestampMS() || currentBatch->GetSourceId() != sourceId) {
            // If write time and source id are the same, the rest fields will be the same too.
            currentBatch = partitionData->AddBatch();
            currentBatch->SetWriteTimeMs(r.GetWriteTimestampMS());
            currentBatch->SetSourceId(sourceId);

            if (proto.HasMeta()) {
                const auto& header = proto.GetMeta();
                if (header.HasServer()) {
                    auto* item = currentBatch->MutableExtraFields()->AddItems();
                    item->SetKey("server");
                    item->SetValue(header.GetServer());
                }
                if (header.HasFile()) {
                    auto* item = currentBatch->MutableExtraFields()->AddItems();
                    item->SetKey("file");
                    item->SetValue(header.GetFile());
                }
                if (header.HasIdent()) {
                    auto* item = currentBatch->MutableExtraFields()->AddItems();
                    item->SetKey("ident");
                    item->SetValue(header.GetIdent());
                }
                if (header.HasLogType()) {
                    auto* item = currentBatch->MutableExtraFields()->AddItems();
                    item->SetKey("logtype");
                    item->SetValue(header.GetLogType());
                }
            }

            if (proto.HasExtraFields()) {
                const auto& map = proto.GetExtraFields();
                for (const auto& kv : map.GetItems()) {
                    auto* item = currentBatch->MutableExtraFields()->AddItems();
                    item->SetKey(kv.GetKey());
                    item->SetValue(kv.GetValue());
                }
            }

            if (proto.HasIp() && IsUtf(proto.GetIp())) {
                currentBatch->SetIp(proto.GetIp());
            }
        }

        auto* message = currentBatch->AddMessageData();
        message->SetSeqNo(r.GetSeqNo());
        message->SetCreateTimeMs(r.GetCreateTimestampMS());
        message->SetOffset(r.GetOffset());
        message->SetUncompressedSize(r.GetUncompressedSize());
        if (proto.HasCodec()) {
            const auto codec = proto.GetCodec();
            if (codec < Min<int>() || codec > Max<int>() || !NPersQueueCommon::ECodec_IsValid(codec)) {
                LOG_ERROR_S(
                        ctx, NKikimrServices::PQ_READ_PROXY,
                        PQ_LOG_PREFIX << "data chunk (topic " << Topic->GetInternalName() << ", partition " << Partition
                            << ", offset " << r.GetOffset() << ", seqNo " << r.GetSeqNo() << ", sourceId "
                            << r.GetSourceId() << ")  codec (id " << codec
                            << ") is not valid NPersQueueCommon::ECodec, loss of data compression codec information"
                );
            }
            message->SetCodec((NPersQueueCommon::ECodec)proto.GetCodec());
        }
        message->SetData(proto.GetData());
    }

    if (!hasOffset) { //no data could be read from paritition at offset ReadOffset - no data in partition at all???
        ReadOffset = Min(Max(ReadOffset + 1, realReadOffset + 1), EndOffset);
    }

    CurrentRequest.Clear();
    RequestInfly = false;

    Y_ABORT_UNLESS(!WaitForData);

    if (EndOffset > ReadOffset) {
        SendPartitionReady(ctx);
    } else {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }

    LOG_DEBUG_S(
            ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " after read state " << Topic->GetPrintableString()
                << " partition:" << Partition
                << " EndOffset " << EndOffset << " ReadOffset " << ReadOffset << " ReadGuid " << ReadGuid);

    ReadGuid = TString();

    auto readResponse = MakeHolder<TEvPQProxy::TEvReadResponse>(
        std::move(response),
        ReadOffset,
        res.GetBlobsFromDisk() > 0,
        TDuration::MilliSeconds(res.GetWaitQuotaTimeMs())
    );
    ctx.Send(ParentId, readResponse.Release());
    CheckRelease(ctx);

    PipeGeneration = 0; //reset tries counter - all ok
}

void TPartitionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();

    LOG_INFO_S(
            ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                          << " pipe restart attempt " << PipeGeneration << " pipe creation result: " << msg->Status);

    if (msg->Status != NKikimrProto::OK) {
        RestartPipe(ctx, TStringBuilder() << "pipe to tablet is dead " << msg->TabletId, NPersQueue::NErrorCode::ERROR);
        return;
    }
}

void TPartitionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    RestartPipe(ctx, TStringBuilder() << "pipe to tablet is dead " << ev->Get()->TabletId, NPersQueue::NErrorCode::ERROR);
}


void TPartitionActor::Handle(TEvPQProxy::TEvReleasePartition::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(
            ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " (partition)releasing " << Topic->GetPrintableString() << " partition:" << Partition
                          << " ReadOffset " << ReadOffset << " ClientCommitOffset " << ClientCommitOffset
                          << " CommittedOffst " << CommittedOffset
    );
    NeedRelease = true;
    CheckRelease(ctx);
}


void TPartitionActor::Handle(TEvPQProxy::TEvGetStatus::TPtr&, const TActorContext& ctx) {
    ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Topic, Partition, CommittedOffset, EndOffset, WriteTimestampEstimateMs, false));
}


void TPartitionActor::Handle(TEvPQProxy::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {
    ClientReadOffset = ev->Get()->ReadOffset;
    ClientCommitOffset = ev->Get()->CommitOffset;
    ClientVerifyReadOffset = ev->Get()->VerifyReadOffset;

    if (StartReading) {
        Y_ABORT_UNLESS(ev->Get()->StartReading); //otherwise it is signal from actor, this could not be done
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("double partition locking", NPersQueue::NErrorCode::BAD_REQUEST));
        return;
    }

    StartReading = ev->Get()->StartReading;
    InitLockPartition(ctx);
}

void TPartitionActor::InitStartReading(const TActorContext& ctx) {

    Y_ABORT_UNLESS(AllPrepareInited);
    Y_ABORT_UNLESS(!WaitForData);
    LOG_INFO_S(
            ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " Start reading " << Topic->GetPrintableString() << " partition:" << Partition
                          << " EndOffset " << EndOffset << " readOffset " << ReadOffset << " committedOffset "
                          << CommittedOffset << " clientCommittedOffset " << ClientCommitOffset
                          << " clientReadOffset " << ClientReadOffset
    );

    Counters.PartitionsToBeLocked.Dec();
    LockCounted = false;

    ReadOffset = Max<ui64>(CommittedOffset, ClientReadOffset);

    if (ClientVerifyReadOffset) {
        if (ClientReadOffset < CommittedOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                    << "trying to read from position that is less than committed: read " << ClientReadOffset << " committed " << CommittedOffset,
                    NPersQueue::NErrorCode::BAD_REQUEST));
            return;
        }
    }

    if (ClientCommitOffset > CommittedOffset) {
        if (ClientCommitOffset > ReadOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                        << "trying to read from position that is less than provided to commit: read " << ReadOffset << " commit " << ClientCommitOffset,
                        NPersQueue::NErrorCode::BAD_REQUEST));
            return;
        }
        if (ClientCommitOffset > EndOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                        << "trying to commit to future: commit " << ClientCommitOffset << " endOffset " << EndOffset,
                        NPersQueue::NErrorCode::BAD_REQUEST));
            return;
        }
        Y_ABORT_UNLESS(CommitsInfly.empty());
        CommitsInfly.push_back(std::pair<ui64, ui64>(Max<ui64>(), ClientCommitOffset));
        if (PipeClient) //pipe will be recreated soon
            SendCommit(CommitsInfly.back().first, CommitsInfly.back().second, ctx);
    } else {
        ClientCommitOffset = CommittedOffset;
    }

    if (EndOffset > ReadOffset) {
        SendPartitionReady(ctx);
    } else {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }
}

void TPartitionActor::InitLockPartition(const TActorContext& ctx) {
    if (PipeClient && AllPrepareInited) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("double partition locking", NPersQueue::NErrorCode::BAD_REQUEST));
        return;
    }
    if (!LockCounted) {
        Counters.PartitionsToBeLocked.Inc();
        LockCounted = true;
    }
    if (StartReading)
        AllPrepareInited = true;

    if (FirstInit) {
        Y_ABORT_UNLESS(!PipeClient);
        FirstInit = false;
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, TabletID, clientConfig));

        NKikimrClient::TPersQueueRequest request;

        request.MutablePartitionRequest()->SetTopic(Topic->GetClientsideName());
        request.MutablePartitionRequest()->SetPartition(Partition);
        request.MutablePartitionRequest()->SetCookie(INIT_COOKIE);

        ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());

        auto cmd = request.MutablePartitionRequest()->MutableCmdCreateSession();
        cmd->SetClientId(InternalClientId);
        cmd->SetSessionId(Session);
        cmd->SetGeneration(Generation);
        cmd->SetStep(Step);

        LOG_INFO_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " INITING " << Topic->GetPrintableString() << " partition:" << Partition);

        TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        Y_ABORT_UNLESS(!RequestInfly);
        CurrentRequest = request;
        RequestInfly = true;
        req->Record.Swap(&request);

        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    } else {
        Y_ABORT_UNLESS(StartReading); //otherwise it is double locking from actor, not client - client makes lock always with StartReading == true
        Y_ABORT_UNLESS(InitDone);
        InitStartReading(ctx);
    }
}


void TPartitionActor::WaitDataInPartition(const TActorContext& ctx) {
    if (ReadingFinishedSent) {
        return;
    }

    if (WaitDataInfly.size() > 1) { //already got 2 requests inflight
        return;
    }

    Y_ABORT_UNLESS(InitDone);
    Y_ABORT_UNLESS(PipeClient);

    if (!WaitForData)
        return;

    Y_ABORT_UNLESS(ReadOffset >= EndOffset);

    TAutoPtr<TEvPersQueue::TEvHasDataInfo> event(new TEvPersQueue::TEvHasDataInfo());
    event->Record.SetPartition(Partition);
    event->Record.SetOffset(ReadOffset);
    event->Record.SetCookie(++WaitDataCookie);
    ui64 deadline = (ctx.Now() + WAIT_DATA - WAIT_DELTA).MilliSeconds();
    event->Record.SetDeadline(deadline);
    event->Record.SetClientId(InternalClientId);

    LOG_DEBUG_S(
            ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                          << " wait data in partition inited, cookie " << WaitDataCookie
    );

    NTabletPipe::SendData(ctx, PipeClient, event.Release());

    ctx.Schedule(PREWAIT_DATA, new TEvents::TEvWakeup());

    ctx.Schedule(WAIT_DATA, new TEvPQProxy::TEvDeadlineExceeded(WaitDataCookie));

    WaitDataInfly.insert(WaitDataCookie);
}

void TPartitionActor::Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    WriteTimestampEstimateMs = Max(WriteTimestampEstimateMs, record.GetWriteTimestampEstimateMS());

    auto it = WaitDataInfly.find(ev->Get()->Record.GetCookie());
    if (it == WaitDataInfly.end()) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                              << " unwaited response for WaitData " << ev->Get()->Record);
        return;
    }
    WaitDataInfly.erase(it);
    if (!WaitForData) {
        return;
    }

    Counters.WaitsForData.Inc();

    Y_ABORT_UNLESS(record.HasEndOffset());
    Y_ABORT_UNLESS(EndOffset <= record.GetEndOffset()); //end offset could not be changed if no data arrived, but signal will be sended anyway after timeout
    Y_ABORT_UNLESS(ReadOffset >= EndOffset); //otherwise no WaitData were needed

    LOG_DEBUG_S(
            ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                          << " wait for data done: " << " readOffset " << ReadOffset << " EndOffset " << EndOffset
                          << " newEndOffset " << record.GetEndOffset() << " commitOffset " << CommittedOffset
                          << " clientCommitOffset " << ClientCommitOffset  << " cookie " << ev->Get()->Record.GetCookie()
    );

    EndOffset = record.GetEndOffset();
    SizeLag = record.GetSizeLag();

    if (ReadOffset < EndOffset) {
        WaitForData = false;
        WaitDataInfly.clear();
        SendPartitionReady(ctx);
    } else if (PipeClient) {
        WaitDataInPartition(ctx);
    }

    if (!ReadingFinishedSent) {
        if (record.GetReadingFinished()) {
            ReadingFinishedSent = true;

            // TODO Tx
            ctx.Send(ParentId, new TEvPQProxy::TEvReadingFinished(Topic->GetInternalName(), Partition, FirstRead));
        } else if (FirstRead) {
            ctx.Send(ParentId, new TEvPQProxy::TEvReadingStarted(Topic->GetInternalName(), Partition));
        }
        FirstRead = false;
    }

    CheckRelease(ctx); //just for logging purpose
}


void TPartitionActor::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(
            ctx, NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " READ FROM " << Topic->GetPrintableString() << " partition:" << Partition
                          << " event " << ev->Get()->Request << " readOffset " << ReadOffset
                          << " EndOffset " << EndOffset << " ClientCommitOffset " << ClientCommitOffset
                          << " committedOffset " << CommittedOffset << " Guid " << ev->Get()->Guid
    );

    Y_ABORT_UNLESS(!NeedRelease);
    Y_ABORT_UNLESS(!Released);

    Y_ABORT_UNLESS(ReadGuid.empty());
    Y_ABORT_UNLESS(!RequestInfly);

    ReadGuid = ev->Get()->Guid;

    const auto& req = ev->Get()->Request.GetRead();

    NKikimrClient::TPersQueueRequest request;

    request.MutablePartitionRequest()->SetTopic(Topic->GetClientsideName());

    request.MutablePartitionRequest()->SetPartition(Partition);
    request.MutablePartitionRequest()->SetCookie((ui64)ReadOffset);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId(InternalClientId);
    read->SetClientDC(ClientDC);
    if (req.GetMaxCount()) {
        read->SetCount(req.GetMaxCount());
    }
    if (req.GetMaxSize()) {
        read->SetBytes(req.GetMaxSize());
    }
    if (req.GetMaxTimeLagMs()) {
        read->SetMaxTimeLagMs(req.GetMaxTimeLagMs());
    }
    if (req.GetReadTimestampMs()) {
        read->SetReadTimestampMs(req.GetReadTimestampMs());
    }

    read->SetOffset(ReadOffset);
    read->SetTimeoutMs(READ_TIMEOUT_DURATION.MilliSeconds());
    RequestInfly = true;
    CurrentRequest = request;

    if (!PipeClient) //Pipe will be recreated soon
        return;

    TAutoPtr<TEvPersQueue::TEvRequest> event(new TEvPersQueue::TEvRequest);
    event->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, event.Release());
}


void TPartitionActor::Handle(TEvPQProxy::TEvCommit::TPtr& ev, const TActorContext& ctx) {
    const ui64 readId = ev->Get()->ReadId;
    const ui64 offset = ev->Get()->Offset;
    Y_ABORT_UNLESS(offset != Max<ui64>()); // has concreete offset
    if (offset < ClientCommitOffset) {
        LOG_ERROR_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                              << " commit done to too small position " << offset
                              << " committedOffset " << ClientCommitOffset << " cookie " << readId
        );
    }
    Y_ABORT_UNLESS(offset >= ClientCommitOffset);

    const bool hasProgress = offset > ClientCommitOffset;

    if (!hasProgress) {//nothing to commit for this partition
        if (CommitsInfly.empty()) {
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PQ_READ_PROXY,
                    PQ_LOG_PREFIX << " " << Topic->GetPrintableString() << " partition:" << Partition
                                  << " commit done with no effect with cookie " << readId
            );
            ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(readId, Topic, Partition));
            CheckRelease(ctx);
        } else {
            CommitsInfly.push_back(std::pair<ui64, ui64>(readId, Max<ui64>()));
        }
        return;
    }

    ClientCommitOffset = offset;
    CommitsInfly.push_back(std::pair<ui64, ui64>(readId, offset));

    if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
        SendCommit(readId, offset, ctx);
}


void TPartitionActor::Die(const TActorContext& ctx) {
    if (PipeClient)
        NTabletPipe::CloseClient(ctx, PipeClient);
    TActorBootstrapped<TPartitionActor>::Die(ctx);
}

void TPartitionActor::HandlePoison(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    if (LockCounted)
        Counters.PartitionsToBeLocked.Dec();
    Die(ctx);
}

void TPartitionActor::Handle(TEvPQProxy::TEvDeadlineExceeded::TPtr& ev, const TActorContext& ctx) {

    WaitDataInfly.erase(ev->Get()->Cookie);
    if (ReadOffset >= EndOffset && WaitDataInfly.size() <= 1 && PipeClient) {
        Y_ABORT_UNLESS(WaitForData);
        WaitDataInPartition(ctx);
    }

}

void TPartitionActor::HandleWakeup(const TActorContext& ctx) {
    if (ReadOffset >= EndOffset && WaitDataInfly.size() <= 1 && PipeClient) {
        Y_ABORT_UNLESS(WaitForData);
        WaitDataInPartition(ctx);
    }
}
}
}
