#include "grpc_pq_actor.h"
#include "grpc_pq_read.h"

#include <ydb/core/base/path.h>
#include <ydb/core/client/server/msgbus_server_persqueue.h>
#include <ydb/core/client/server/msgbus_server_pq_read_session_info.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/persqueue/percentile_counter.h>
#include <ydb/core/persqueue/codecs/pqv1.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>


#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/protobuf/util/repeated_field_utils.h>
#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/string/join.h>
#include <util/string/strip.h>
#include <util/charset/utf8.h>

#include <algorithm>

using namespace NActors;
using namespace NKikimrClient;

namespace NKikimr {

using namespace NMsgBusProxy;

namespace NGRpcProxy::V1 {

using namespace PersQueue::V1;

#define PQ_LOG_PREFIX "session cookie " << Cookie << " consumer " << ClientPath << " session " << Session


//11 tries = 10,23 seconds, then each try for 5 seconds , so 21 retries will take near 1 min
static const NTabletPipe::TClientRetryPolicy RetryPolicyForPipes = {
    .RetryLimitCount = 21,
    .MinRetryTime = TDuration::MilliSeconds(10),
    .MaxRetryTime = TDuration::Seconds(5),
    .BackoffMultiplier = 2,
    .DoFirstRetryInstantly = true
};

static const ui64 MAX_INFLY_BYTES = 25 * 1024 * 1024;
static const ui32 MAX_INFLY_READS = 10;

static const TDuration READ_TIMEOUT_DURATION = TDuration::Seconds(1);

static const TDuration WAIT_DATA = TDuration::Seconds(10);
static const TDuration PREWAIT_DATA = TDuration::Seconds(9);
static const TDuration WAIT_DELTA = TDuration::MilliSeconds(500);

static const ui64 INIT_COOKIE = Max<ui64>(); //some identifier

static const ui32 MAX_PIPE_RESTARTS = 100; //after 100 restarts without progress kill session
static const ui32 RESTART_PIPE_DELAY_MS = 100;

static const ui64 MAX_READ_SIZE = 100 << 20; //100mb;

static const ui32 MAX_COMMITS_INFLY = 3;

static const double LAG_GROW_MULTIPLIER = 1.2; //assume that 20% more data arrived to partitions


//TODO: add here tracking of bytes in/out


IOutputStream& operator <<(IOutputStream& out, const TPartitionId& partId) {
    out << "TopicId: " << partId.TopicConverter->GetClientsideName() << ":" << partId.Partition << "(assignId:" << partId.AssignId << ")";
    return out;
}

struct TOffsetInfo {
    // find by read id
    bool operator<(ui64 readId) const {
        return ReadId < readId;
    }

    friend bool operator<(ui64 readId, const TOffsetInfo& info) {
        return readId < info.ReadId;
    }


    ui64 ReadId = 0;
    ui64 Offset = 0;
};


bool RemoveEmptyMessages(MigrationStreamingReadServerMessage::DataBatch& data) {
    auto batchRemover = [&](MigrationStreamingReadServerMessage::DataBatch::Batch& batch) -> bool {
        return batch.message_data_size() == 0;
    };
    auto partitionDataRemover = [&](MigrationStreamingReadServerMessage::DataBatch::PartitionData& partition) -> bool {
        NProtoBuf::RemoveRepeatedFieldItemIf(partition.mutable_batches(), batchRemover);
        return partition.batches_size() == 0;
    };
    NProtoBuf::RemoveRepeatedFieldItemIf(data.mutable_partition_data(), partitionDataRemover);
    return !data.partition_data().empty();
}


class TPartitionActor : public NActors::TActorBootstrapped<TPartitionActor> {
public:
     TPartitionActor(const TActorId& parentId, const TString& clientId, const TString& clientPath, const ui64 cookie, const TString& session, const TPartitionId& partition, ui32 generation, ui32 step,
                     const ui64 tabletID, const TReadSessionActor::TTopicCounters& counters, const bool commitsDisabled, const TString& clientDC, bool rangesMode);
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
            HFunc(TEvPQProxy::TEvCommitCookie, Handle)
            HFunc(TEvPQProxy::TEvCommitRange, Handle)
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
    void Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const NActors::TActorContext& ctx);
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
    void MakeCommit(const TActorContext& ctx);


private:
    const TActorId ParentId;
    const TString ClientId;
    const TString ClientPath;
    const ui64 Cookie;
    const TString Session;
    const TString ClientDC;

    const TPartitionId Partition;
    const ui32 Generation;
    const ui32 Step;

    const ui64 TabletID;

    ui64 ReadOffset;
    ui64 ClientReadOffset;
    ui64 ClientCommitOffset;
    bool ClientVerifyReadOffset;
    ui64 CommittedOffset;
    ui64 WriteTimestampEstimateMs;

    ui64 ReadIdToResponse;
    ui64 ReadIdCommitted;
    TSet<ui64> NextCommits;
    TDisjointIntervalTree<ui64> NextRanges;

    bool RangesMode;
    std::deque<TOffsetInfo> Offsets;

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
    struct TCommitInfo {
        ui64 StartReadId;
        ui64 Offset;
        TInstant StartTime;
    };

    std::deque<std::pair<ui64, TCommitInfo>> CommitsInfly; //ReadId, Offset

    TReadSessionActor::TTopicCounters Counters;

    bool CommitsDisabled;
    ui64 CommitCookie;
};


TReadSessionActor::TReadSessionActor(
        NKikimr::NGRpcService::TEvStreamPQReadRequest* request, const ui64 cookie, const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsHandler
)
    : Request(request)
    , ClientDC(clientDC ? *clientDC : "other")
    , StartTimestamp(TInstant::Now())
    , SchemeCache(schemeCache)
    , NewSchemeCache(newSchemeCache)
    , AuthInitActor()
    , ClientId()
    , ClientPath()
    , Session()
    , CommitsDisabled(false)
    , BalancersInitStarted(false)
    , InitDone(false)
    , MaxReadMessagesCount(0)
    , MaxReadSize(0)
    , MaxTimeLagMs(0)
    , ReadTimestampMs(0)
    , ForceACLCheck(false)
    , RequestNotChecked(true)
    , LastACLCheckTimestamp(TInstant::Zero())
    , NextAssignId(1)
    , ReadOnlyLocal(false)
    , Cookie(cookie)
    , Counters(counters)
    , BytesInflight_(0)
    , RequestedBytes(0)
    , ReadsInfly(0)
    , TopicsHandler(topicsHandler)
{
    Y_ASSERT(Request);
}


TReadSessionActor::~TReadSessionActor() = default;


void TReadSessionActor::Bootstrap(const TActorContext& ctx) {
    Y_VERIFY(Request);
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ++(*GetServiceCounters(Counters, "pqproxy|readSession")
           ->GetNamedCounter("sensor", "SessionsCreatedTotal", true));
    }

    Request->GetStreamCtx()->Attach(ctx.SelfID);
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
        Die(ctx);
        return;
    }
    StartTime = ctx.Now();

    Become(&TThis::StateFunc);
}

void TReadSessionActor::HandleDone(const TActorContext& ctx) {

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc closed");
    Die(ctx);
}


void TReadSessionActor::Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {

    auto& request = ev->Get()->Record;
    auto token = request.token();
    request.set_token("");

    if (!token.empty()) { //TODO refreshtoken here
        ctx.Send(ctx.SelfID, new TEvPQProxy::TEvAuth(token));
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read done: success: " << ev->Get()->Success << " data: " << request);

    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed");
        ctx.Send(ctx.SelfID, new TEvPQProxy::TEvDone());
        return;
    }
    auto converterFactory = TopicsHandler.GetConverterFactory();
    auto MakePartitionId = [&](auto& request) {
        auto converter = converterFactory->MakeTopicNameConverter(
                request.topic().path(), request.cluster(), Request->GetDatabaseName().GetOrElse(TString())
        );
        if (!converter->IsValid()) {
            CloseSession(
                    TStringBuilder() << "Topic " << request.topic().path() << " not recognized: " << converter->GetReason(),
                    PersQueue::ErrorCode::BAD_REQUEST, ctx
            );
        }
        const ui32 partition = request.partition();
        const ui64 assignId = request.assign_id();
        return TPartitionId{converter, partition, assignId};
    };
    switch (request.request_case()) {
        case MigrationStreamingReadClientMessage::kInitRequest: {
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvReadInit(request, Request->GetStreamCtx()->GetPeerName()));
            break;
        }
        case MigrationStreamingReadClientMessage::kStatus: {
            //const auto& req = request.status();
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvGetStatus(MakePartitionId(request.status())));
            if (!Request->GetStreamCtx()->Read()) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                Die(ctx);
                return;
            }
            break;

        }
        case MigrationStreamingReadClientMessage::kRead: {
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvRead()); // Proto read message have no parameters
            break;
        }
        case MigrationStreamingReadClientMessage::kReleased: {
            //const auto& req = request.released();
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvReleased(MakePartitionId(request.released())));
            if (!Request->GetStreamCtx()->Read()) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                Die(ctx);
                return;
            }
            break;

        }
        case MigrationStreamingReadClientMessage::kStartRead: {
            const auto& req = request.start_read();

            const ui64 readOffset = req.read_offset();
            const ui64 commitOffset = req.commit_offset();
            const bool verifyReadOffset = req.verify_read_offset();

            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvStartRead(MakePartitionId(request.start_read()), readOffset, commitOffset, verifyReadOffset));
            if (!Request->GetStreamCtx()->Read()) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                Die(ctx);
                return;
            }
            break;
        }
        case MigrationStreamingReadClientMessage::kCommit: {
            const auto& req = request.commit();

            if (!req.cookies_size() && !RangesMode) {
                CloseSession(TStringBuilder() << "can't commit without cookies", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                return;
            }
            if (RangesMode && !req.offset_ranges_size()) {
                CloseSession(TStringBuilder() << "can't commit without offsets", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                return;

            }

            THashMap<ui64, TEvPQProxy::TCommitCookie> commitCookie;
            THashMap<ui64, TEvPQProxy::TCommitRange> commitRange;

            for (auto& c: req.cookies()) {
                commitCookie[c.assign_id()].Cookies.push_back(c.partition_cookie());
            }
            for (auto& c: req.offset_ranges()) {
                commitRange[c.assign_id()].Ranges.push_back(std::make_pair(c.start_offset(), c.end_offset()));
            }

            for (auto& c : commitCookie) {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitCookie(c.first, std::move(c.second)));
            }

            for (auto& c : commitRange) {
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvCommitRange(c.first, std::move(c.second)));
            }

            if (!Request->GetStreamCtx()->Read()) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
                Die(ctx);
                return;
            }
            break;
        }

        default: {
            CloseSession(TStringBuilder() << "unsupported request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            break;
        }
    }
}


void TReadSessionActor::Handle(IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
        Die(ctx);
    }
    Y_VERIFY(!ActiveWrites.empty());
    ui64 sz = ActiveWrites.front();
    ActiveWrites.pop();
    Y_VERIFY(BytesInflight_ >= sz);
    BytesInflight_ -= sz;
    if (BytesInflight) (*BytesInflight) -= sz;

    ProcessReads(ctx);
}


void TReadSessionActor::Die(const TActorContext& ctx) {

    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());

    for (auto& p : Partitions) {
        ctx.Send(p.second.Actor, new TEvents::TEvPoisonPill());

        if (!p.second.Released) {
            auto it = TopicCounters.find(p.second.Partition.TopicConverter->GetClientsideName());
            Y_VERIFY(it != TopicCounters.end());
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

    if (BytesInflight) {
        (*BytesInflight) -= BytesInflight_;
    }
    if (SessionsActive) {
        --(*SessionsActive);
    }
    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }

    ctx.Send(GetPQReadServiceActorID(), new TEvPQProxy::TEvSessionDead(Cookie));

    TActorBootstrapped<TReadSessionActor>::Die(ctx);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext& ctx) {
    CloseSession(TStringBuilder() << "Reads done signal - closing everything", PersQueue::ErrorCode::OK, ctx);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (CommitsDisabled) {
        CloseSession("commits in session are disabled by client option", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    const ui64& assignId = ev->Get()->AssignId;
    auto it = Partitions.find(assignId);
    if (it == Partitions.end()) //stale commit - ignore it
        return;

    for (auto& c : ev->Get()->CommitInfo.Cookies) {
        if(RangesMode) {
            CloseSession("Commits cookies in ranges commit mode is illegal", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        it->second.NextCommits.insert(c);
    }

    ctx.Send(it->second.Actor, new TEvPQProxy::TEvCommitCookie(ev->Get()->AssignId, std::move(ev->Get()->CommitInfo)));
}

void TReadSessionActor::Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    if (CommitsDisabled) {
        CloseSession("commits in session are disabled by client option", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    const ui64& assignId = ev->Get()->AssignId;
    auto it = Partitions.find(assignId);
    if (it == Partitions.end()) //stale commit - ignore it
        return;

    for (auto& c : ev->Get()->CommitInfo.Ranges) {
        if(!RangesMode) {
            CloseSession("Commits ranges in cookies commit mode is illegal", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        if (c.first >= c.second || it->second.NextRanges.Intersects(c.first, c.second) || c.first < it->second.Offset) {
            CloseSession(TStringBuilder() << "Offsets range [" << c.first << ", " << c.second << ") has already committed offsets, double committing is forbiden; or incorrect", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;

        }
        it->second.NextRanges.InsertInterval(c.first, c.second);
    }

    ctx.Send(it->second.Actor, new TEvPQProxy::TEvCommitRange(ev->Get()->AssignId, std::move(ev->Get()->CommitInfo)));
}

void TReadSessionActor::Handle(TEvPQProxy::TEvAuth::TPtr& ev, const TActorContext& ctx) {
    ProcessAuth(ev->Get()->Auth, ctx);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvStartRead::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    if (it == Partitions.end()) {
        return;
    }

    if (it == Partitions.end() || it->second.Releasing) {
        //do nothing - already released partition
        LOG_WARN_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got NOTACTUAL StartRead from client for " << ev->Get()->Partition
                   << " at offset " << ev->Get()->ReadOffset);
        return;
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got StartRead from client for "
               << ev->Get()->Partition <<
               " at readOffset " << ev->Get()->ReadOffset <<
               " commitOffset " << ev->Get()->CommitOffset);

    //proxy request to partition - allow initing
    //TODO: add here VerifyReadOffset too and check it againts Committed position
    ctx.Send(it->second.Actor, new TEvPQProxy::TEvLockPartition(ev->Get()->ReadOffset, ev->Get()->CommitOffset, ev->Get()->VerifyReadOffset, true));
}

void TReadSessionActor::Handle(TEvPQProxy::TEvReleased::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    if (it == Partitions.end()) {
        return;
    }
    if (!it->second.Releasing) {
        CloseSession(TStringBuilder() << "Release of partition that is not requested for release is forbiden for " << it->second.Partition, PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;

    }
    Y_VERIFY(it->second.LockSent);
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got Released from client for " << ev->Get()->Partition);

    ReleasePartition(it, true, ctx);
}

void TReadSessionActor::Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const TActorContext& ctx) {
    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    if (it == Partitions.end() || it->second.Releasing) {
        // Ignore request - client asking status after releasing of partition.
        return;
    }
    ctx.Send(it->second.Actor, new TEvPQProxy::TEvGetStatus(ev->Get()->Partition));
}

void TReadSessionActor::DropPartition(THashMap<ui64, TPartitionActorInfo>::iterator it, const TActorContext& ctx) {
    ctx.Send(it->second.Actor, new TEvents::TEvPoisonPill());
    bool res = ActualPartitionActors.erase(it->second.Actor);
    Y_VERIFY(res);

    if (--NumPartitionsFromTopic[it->second.Partition.TopicConverter->GetClientsideName()] == 0) {
        bool res = TopicCounters.erase(it->second.Partition.TopicConverter->GetClientsideName());
        Y_VERIFY(res);
    }

    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }
    BalancerGeneration.erase(it->first);
    Partitions.erase(it);
    if (SessionsActive) {
        PartsPerSession.IncFor(Partitions.size(), 1);
    }
}


void TReadSessionActor::Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const TActorContext& ctx) {

    Y_VERIFY(!CommitsDisabled);

    if (!ActualPartitionActor(ev->Sender))
        return;

    ui64 assignId = ev->Get()->AssignId;

    auto it = Partitions.find(assignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(it->second.Offset < ev->Get()->Offset);
    it->second.NextRanges.EraseInterval(it->second.Offset, ev->Get()->Offset);


    if (ev->Get()->StartCookie == Max<ui64>()) //means commit at start
        return;

    MigrationStreamingReadServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);
    if (!RangesMode) {
        for (ui64 i = ev->Get()->StartCookie; i <= ev->Get()->LastCookie; ++i) {
            auto c = result.mutable_committed()->add_cookies();
            c->set_partition_cookie(i);
            c->set_assign_id(assignId);
            it->second.NextCommits.erase(i);
            it->second.ReadIdCommitted = i;
        }
    } else {
        auto c = result.mutable_committed()->add_offset_ranges();
        c->set_assign_id(assignId);
        c->set_start_offset(it->second.Offset);
        c->set_end_offset(ev->Get()->Offset);
    }

    it->second.Offset = ev->Get()->Offset;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " replying for commits from assignId " << assignId << " from " << ev->Get()->StartCookie << " to " << ev->Get()->LastCookie << " to offset " << it->second.Offset);
    if (!WriteResponse(std::move(result))) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
        Die(ctx);
        return;
    }
}


void TReadSessionActor::Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvPQProxy::TEvReadSessionStatusResponse> result(new TEvPQProxy::TEvReadSessionStatusResponse());
    for (auto& p : Partitions) {
        auto part = result->Record.AddPartition();
        part->SetTopic(p.second.Partition.TopicConverter->GetPrimaryPath());
        part->SetPartition(p.second.Partition.Partition);
        part->SetAssignId(p.second.Partition.AssignId);
        for (auto& c : p.second.NextCommits) {
            part->AddNextCommits(c);
        }
        part->SetReadIdCommitted(p.second.ReadIdCommitted);
        part->SetLastReadId(p.second.ReadIdToResponse - 1);
        part->SetTimestampMs(p.second.AssignTimestamp.MilliSeconds());
    }
    result->Record.SetSession(Session);
    result->Record.SetTimestamp(StartTimestamp.MilliSeconds());

    result->Record.SetClientNode(PeerName);
    result->Record.SetProxyNodeId(ctx.SelfID.NodeId());

    ctx.Send(ev->Sender, result.Release());
}

void TReadSessionActor::Handle(TEvPQProxy::TEvReadInit::TPtr& ev, const TActorContext& ctx) {

    THolder<TEvPQProxy::TEvReadInit> event(ev->Release());

    if (!Topics.empty()) {
        //answer error
        CloseSession("got second init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    const auto& init = event->Request.init_request();

    if (!init.topics_read_settings_size()) {
        CloseSession("no topics in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (init.consumer().empty()) {
        CloseSession("no consumer in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    ClientId = NPersQueue::ConvertNewConsumerName(init.consumer(), ctx);
    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ClientPath = init.consumer();
    } else {
        ClientPath = NPersQueue::NormalizeFullPath(NPersQueue::MakeConsumerPath(init.consumer()));
    }

    TStringBuilder session;
    session << ClientPath << "_" << ctx.SelfID.NodeId() << "_" << Cookie << "_" << TAppData::RandomProvider->GenRand64() << "_v1";
    Session = session;
    CommitsDisabled = false;
    RangesMode = init.ranges_mode();

    MaxReadMessagesCount = NormalizeMaxReadMessagesCount(init.read_params().max_read_messages_count());
    MaxReadSize = NormalizeMaxReadSize(init.read_params().max_read_size());
    if (init.max_lag_duration_ms() < 0) {
        CloseSession("max_lag_duration_ms must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    MaxTimeLagMs = init.max_lag_duration_ms();
    if (init.start_from_written_at_ms() < 0) {
        CloseSession("start_from_written_at_ms must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    ReadTimestampMs = static_cast<ui64>(init.start_from_written_at_ms());

    PeerName = event->PeerName;

    ReadOnlyLocal = init.read_only_original();

    for (const auto& topic : init.topics_read_settings()) {
        auto converter = TopicsHandler.GetConverterFactory()->MakeTopicNameConverter(
                topic.topic(), TString(), Request->GetDatabaseName().GetOrElse(TString())
        );
        if (!converter->IsValid()) {
            CloseSession(TStringBuilder() << "invalid topic '" << topic.topic() << "' in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        const auto topicName = converter->GetModernName();
        if (topicName.empty()) {
            CloseSession("empty topic in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        if (topic.start_from_written_at_ms() < 0) {
            CloseSession("start_from_written_at_ms must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        TopicsToResolve.insert(topicName);
        for (i64 pg : topic.partition_group_ids()) {
            if (pg < 0) {
                CloseSession("partition group id must be nonnegative number", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                return;
            }
            if (pg > Max<ui32>()) {
                CloseSession(TStringBuilder() << "partition group id " << pg << " is too big for partition group id", PersQueue::ErrorCode::BAD_REQUEST, ctx);
                return;
            }
            TopicGroups[topicName].push_back(static_cast<ui32>(pg));
        }
        ReadFromTimestamp[topicName] = topic.start_from_written_at_ms();
    }
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " init: " << event->Request << " from " << PeerName);


    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        SetupCounters();
    }

    if (Request->GetInternalToken().empty()) {
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            CloseSession("Unauthenticated access is forbidden, please provide credentials", PersQueue::ErrorCode::ACCESS_DENIED, ctx);
            return;
        }
    } else {
        Y_VERIFY(Request->GetYdbToken());
        Auth = *(Request->GetYdbToken());
        Token = new NACLib::TUserToken(Request->GetInternalToken());
    }
    AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
            ctx, ctx.SelfID, ClientId, Cookie, Session, SchemeCache, NewSchemeCache, Counters, Token,
            TopicsHandler.GetReadTopicsList(TopicsToResolve, ReadOnlyLocal, Request->GetDatabaseName().GetOrElse(TString())), TopicsHandler.GetLocalCluster()
    ));


    auto subGroup = GetServiceCounters(Counters, "pqproxy|SLI");
    Aggr = {{{{"Account", ClientPath.substr(0, ClientPath.find("/"))}}, {"total"}}};

    SLIErrors = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsError"}, true, "sensor", false);
    SLITotal = NKikimr::NPQ::TMultiCounter(subGroup, Aggr, {}, {"RequestsTotal"}, true, "sensor", false);
    SLITotal.Inc();

}


void TReadSessionActor::RegisterSession(const TActorId& pipe, const TString& topic, const TVector<ui32>& groups, const TActorContext& ctx)
{

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " register session to " << topic);
    THolder<TEvPersQueue::TEvRegisterReadSession> request;
    request.Reset(new TEvPersQueue::TEvRegisterReadSession);
    auto& req = request->Record;
    req.SetSession(Session);
    req.SetClientNode(PeerName);
    ActorIdToProto(pipe, req.MutablePipeClient());
    req.SetClientId(ClientId);

    for (ui32 i = 0; i < groups.size(); ++i) {
        req.AddGroups(groups[i]);
    }

    NTabletPipe::SendData(ctx, pipe, request.Release());
}

void TReadSessionActor::RegisterSessions(const TActorContext& ctx) {
    InitDone = true;

    for (auto& t : Topics) {
        auto& topic = t.first;
        RegisterSession(t.second.PipeClient, topic, t.second.Groups, ctx);
        NumPartitionsFromTopic[t.second.TopicNameConverter->GetClientsideName()] = 0;
    }
}


void TReadSessionActor::SetupCounters()
{
    if (SessionsCreated) {
        return;
    }

    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
    subGroup = subGroup->GetSubgroup("Client", ClientId)->GetSubgroup("ConsumerPath", ClientPath);
    const TString name = "sensor";

    BytesInflight = subGroup->GetExpiringNamedCounter(name, "BytesInflight", false);
    Errors = subGroup->GetExpiringNamedCounter(name, "Errors", true);
    PipeReconnects = subGroup->GetExpiringNamedCounter(name, "PipeReconnects", true);
    SessionsActive = subGroup->GetExpiringNamedCounter(name, "SessionsActive", false);
    SessionsCreated = subGroup->GetExpiringNamedCounter(name, "SessionsCreated", true);
    PartsPerSession = NKikimr::NPQ::TPercentileCounter(
        subGroup->GetSubgroup(name, "PartsPerSession"),
        {}, {}, "Count",
        TVector<std::pair<ui64, TString>>{{1, "1"}, {2, "2"}, {5, "5"},
                                          {10, "10"}, {20, "20"}, {50, "50"},
                                          {70, "70"}, {100, "100"}, {150, "150"},
                                          {300,"300"}, {99999999, "99999999"}},
        false, true);

    ++(*SessionsCreated);
    ++(*SessionsActive);
    PartsPerSession.IncFor(Partitions.size(), 1); //for 0
}


void TReadSessionActor::SetupTopicCounters(const TString& topic)
{
    auto& topicCounters = TopicCounters[topic];
    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
//client/consumerPath Account/Producer OriginDC Topic/TopicPath
    TVector<NPQ::TLabelsInfo> aggr = NKikimr::NPQ::GetLabels(topic);
    TVector<std::pair<TString, TString>> cons = {{"Client", ClientId}, {"ConsumerPath", ClientPath}};

    topicCounters.PartitionsLocked       = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsLocked"}, true);
    topicCounters.PartitionsReleased     = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsReleased"}, true);
    topicCounters.PartitionsToBeReleased = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeReleased"}, false);
    topicCounters.PartitionsToBeLocked   = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsToBeLocked"}, false);
    topicCounters.PartitionsInfly        = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsInfly"}, false);
    topicCounters.Errors                 = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"PartitionsErrors"}, true);
    topicCounters.Commits                = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"Commits"}, true);
    topicCounters.WaitsForData           = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"WaitsForData"}, true);

    topicCounters.CommitLatency          = CommitLatency;
    topicCounters.SLIBigLatency          = SLIBigLatency;
    topicCounters.SLITotal               = SLITotal;
}

void TReadSessionActor::SetupTopicCounters(const TString& topic, const TString& cloudId,
                                           const TString& dbId, const TString& folderId)
{
    auto& topicCounters = TopicCounters[topic];
    auto subGroup = NKikimr::NPQ::GetCountersForStream(Counters);
//client/consumerPath Account/Producer OriginDC Topic/TopicPath
    TVector<NPQ::TLabelsInfo> aggr = NKikimr::NPQ::GetLabelsForStream(topic, cloudId, dbId, folderId);
    TVector<std::pair<TString, TString>> cons = {{"consumer", ClientPath}};

    topicCounters.PartitionsLocked       = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_locked_per_second"}, true, "name");
    topicCounters.PartitionsReleased     = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_released_per_second"}, true, "name");
    topicCounters.PartitionsToBeReleased = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_to_be_released"}, false, "name");
    topicCounters.PartitionsToBeLocked   = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_to_be_locked"}, false, "name");
    topicCounters.PartitionsInfly        = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_locked"}, false, "name");
    topicCounters.Errors                 = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.partitions_errors_per_second"}, true, "name");
    topicCounters.Commits                = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.commits_per_second"}, true, "name");
    topicCounters.WaitsForData           = NKikimr::NPQ::TMultiCounter(subGroup, aggr, cons, {"stream.internal_read.waits_for_data"}, true, "name");

    topicCounters.CommitLatency          = CommitLatency;
    topicCounters.SLIBigLatency          = SLIBigLatency;
    topicCounters.SLITotal               = SLITotal;
}

void TReadSessionActor::Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {

    LastACLCheckTimestamp = ctx.Now();

    LOG_INFO_S(
            ctx,
            NKikimrServices::PQ_READ_PROXY,
            PQ_LOG_PREFIX << " auth ok, got " << ev->Get()->TopicAndTablets.size() << " topics, init done " << InitDone
    );

    AuthInitActor = TActorId();

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

        MigrationStreamingReadServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);

        result.mutable_init_response()->set_session_id(Session);
        if (!WriteResponse(std::move(result))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }

        if (!Request->GetStreamCtx()->Read()) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
            Die(ctx);
            return;
        }


        Y_VERIFY(!BalancersInitStarted);
        BalancersInitStarted = true;

        for (auto& t : ev->Get()->TopicAndTablets) { // ToDo - return something from Init and Auth Actor (Full Path - ?)
            auto& topicHolder = Topics[t.TopicNameConverter->GetClientsideName()];
            topicHolder.TabletID = t.TabletID;
            topicHolder.TopicNameConverter = t.TopicNameConverter;
            topicHolder.CloudId = t.CloudId;
            topicHolder.DbId = t.DbId;
            topicHolder.FolderId = t.FolderId;
            FullPathToConverter[t.TopicNameConverter->GetPrimaryPath()] = t.TopicNameConverter;
        }

        for (auto& t : Topics) {
            NTabletPipe::TClientConfig clientConfig;

            clientConfig.CheckAliveness = false;

            clientConfig.RetryPolicy = RetryPolicyForPipes;
            t.second.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, t.second.TabletID, clientConfig));

            auto it = TopicGroups.find(t.second.TopicNameConverter->GetModernName());
            if (it != TopicGroups.end()) {
                t.second.Groups = it->second;
            }
        }

        RegisterSessions(ctx);

        ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());
    } else {
        for (auto& t : ev->Get()->TopicAndTablets) {
            if (Topics.find(t.TopicNameConverter->GetClientsideName()) == Topics.end()) {
                CloseSession(
                        TStringBuilder() << "list of topics changed - new topic '"
                                         << t.TopicNameConverter->GetClientsideName() << "' found",
                        PersQueue::ErrorCode::BAD_REQUEST, ctx
                );
                return;
            }
        }
    }
}


void TReadSessionActor::Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {

    auto& record = ev->Get()->Record;
    Y_VERIFY(record.GetSession() == Session);
    Y_VERIFY(record.GetClientId() == ClientId);

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());
    auto converterIter = FullPathToConverter.find(NPersQueue::NormalizeFullPath(record.GetPath()));

    if (converterIter.IsEnd()) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " ignored ev lock for path = " << record.GetPath() << ", path not recognized"
        );
        return;
    }
    const auto& topic = converterIter->second->GetClientsideName();
    auto jt = Topics.find(topic); // ToDo - Check
    if (jt == Topics.end() || pipe != jt->second.PipeClient) { //this is message from old version of pipe
        LOG_ALERT_S(
                ctx, NKikimrServices::PQ_READ_PROXY,
                PQ_LOG_PREFIX << " ignored ev lock for topic = " << topic
                              << " path recognized, but topic is unknown, this is unexpected"
        );
        return;
    }

    if (NumPartitionsFromTopic[topic]++ == 0) {
        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            SetupTopicCounters(topic, jt->second.CloudId, jt->second.DbId, jt->second.FolderId);
        } else {
            SetupTopicCounters(topic);
        }
    }

    auto it = TopicCounters.find(topic);
    Y_VERIFY(it != TopicCounters.end());

    ui64 assignId = NextAssignId++;
    BalancerGeneration[assignId] = {record.GetGeneration(), record.GetStep()};
    TPartitionId partitionId{converterIter->second, record.GetPartition(), assignId};

    IActor* partitionActor = new TPartitionActor(ctx.SelfID, ClientId, ClientPath, Cookie, Session, partitionId, record.GetGeneration(),
                                                 record.GetStep(), record.GetTabletId(), it->second, CommitsDisabled, ClientDC, RangesMode);

    TActorId actorId = ctx.Register(partitionActor);
    if (SessionsActive) {
        PartsPerSession.DecFor(Partitions.size(), 1);
    }
    Y_VERIFY(record.GetGeneration() > 0);
    auto pp = Partitions.insert(std::make_pair(assignId, TPartitionActorInfo{actorId, partitionId, ctx}));
    Y_VERIFY(pp.second);
    if (SessionsActive) {
        PartsPerSession.IncFor(Partitions.size(), 1);
    }

    bool res = ActualPartitionActors.insert(actorId).second;
    Y_VERIFY(res);

    it->second.PartitionsLocked.Inc();
    it->second.PartitionsInfly.Inc();

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Assign: " << record);

    ctx.Send(actorId, new TEvPQProxy::TEvLockPartition(0, 0, false, false));
}

void TReadSessionActor::Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActor(ev->Sender))
        return;

    auto it = Partitions.find(ev->Get()->Partition.AssignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(!it->second.Releasing); // if releasing and no lock sent yet - then server must already release partition

    if (ev->Get()->Init) {
        Y_VERIFY(!it->second.LockSent);

        it->second.LockSent = true;
        it->second.Offset = ev->Get()->Offset;

        MigrationStreamingReadServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);

        result.mutable_assigned()->mutable_topic()->set_path(ev->Get()->Partition.TopicConverter->GetModernName());
        result.mutable_assigned()->set_cluster(ev->Get()->Partition.TopicConverter->GetCluster());
        result.mutable_assigned()->set_partition(ev->Get()->Partition.Partition);
        result.mutable_assigned()->set_assign_id(it->first);

        result.mutable_assigned()->set_read_offset(ev->Get()->Offset);
        result.mutable_assigned()->set_end_offset(ev->Get()->EndOffset);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " sending to client create partition stream event");

        auto pp = it->second.Partition;
        pp.AssignId = 0;
        auto jt = PartitionToControlMessages.find(pp);
        if (jt == PartitionToControlMessages.end()) {
            if (!WriteResponse(std::move(result))) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
                Die(ctx);
                return;
            }
        } else {
            Y_VERIFY(jt->second.Infly);
            jt->second.ControlMessages.push_back(result);
        }
    } else {
        Y_VERIFY(it->second.LockSent);

        MigrationStreamingReadServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);

        result.mutable_partition_status()->mutable_topic()->set_path(ev->Get()->Partition.TopicConverter->GetModernName());
        result.mutable_partition_status()->set_cluster(ev->Get()->Partition.TopicConverter->GetCluster());
        result.mutable_partition_status()->set_partition(ev->Get()->Partition.Partition);
        result.mutable_partition_status()->set_assign_id(it->first);

        result.mutable_partition_status()->set_committed_offset(ev->Get()->Offset);
        result.mutable_partition_status()->set_end_offset(ev->Get()->EndOffset);
        result.mutable_partition_status()->set_write_watermark_ms(ev->Get()->WriteTimestampEstimateMs);

        auto pp = it->second.Partition;
        pp.AssignId = 0;
        auto jt = PartitionToControlMessages.find(pp);
        if (jt == PartitionToControlMessages.end()) {
            if (!WriteResponse(std::move(result))) {
                LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
                Die(ctx);
                return;
            }
        } else {
            Y_VERIFY(jt->second.Infly);
            jt->second.ControlMessages.push_back(result);
        }
    }
}

void TReadSessionActor::Handle(TEvPersQueue::TEvError::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Record.GetDescription(), ConvertOldCode(ev->Get()->Record.GetCode()), ctx);
}


void TReadSessionActor::SendReleaseSignalToClient(const THashMap<ui64, TPartitionActorInfo>::iterator& it, bool kill, const TActorContext& ctx)
{
    MigrationStreamingReadServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);

    result.mutable_release()->mutable_topic()->set_path(it->second.Partition.TopicConverter->GetModernName());
    result.mutable_release()->set_cluster(it->second.Partition.TopicConverter->GetCluster());
    result.mutable_release()->set_partition(it->second.Partition.Partition);
    result.mutable_release()->set_assign_id(it->second.Partition.AssignId);
    result.mutable_release()->set_forceful_release(kill);
    result.mutable_release()->set_commit_offset(it->second.Offset);

    auto pp = it->second.Partition;
    pp.AssignId = 0;
    auto jt = PartitionToControlMessages.find(pp);
    if (jt == PartitionToControlMessages.end()) {
        if (!WriteResponse(std::move(result))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }
    } else {
        Y_VERIFY(jt->second.Infly);
        jt->second.ControlMessages.push_back(result);
    }
    Y_VERIFY(it->second.LockSent);
    it->second.ReleaseSent = true;
}


void TReadSessionActor::Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    Y_VERIFY(record.GetSession() == Session);
    Y_VERIFY(record.GetClientId() == ClientId);
    TString topicPath = NPersQueue::NormalizeFullPath(record.GetPath());

    ui32 group = record.HasGroup() ? record.GetGroup() : 0;
    auto pathIter = FullPathToConverter.find(topicPath);
    Y_VERIFY(!pathIter.IsEnd());
    auto it = Topics.find(pathIter->second->GetClientsideName());
    Y_VERIFY(!it.IsEnd());
    auto& converter = it->second.TopicNameConverter;

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());

    if (pipe != it->second.PipeClient) { //this is message from old version of pipe
        return;
    }

    for (ui32 c = 0; c < record.GetCount(); ++c) {
        Y_VERIFY(!Partitions.empty());

        TActorId actorId = TActorId{};
        auto jt = Partitions.begin();
        ui32 i = 0;
        for (auto it = Partitions.begin(); it != Partitions.end(); ++it) {
            if (it->second.Partition.TopicConverter->GetPrimaryPath() == converter->GetPrimaryPath()
                && !it->second.Releasing
                && (group == 0 || it->second.Partition.Partition + 1 == group)
            ) {
                ++i;
                if (rand() % i == 0) { //will lead to 1/n probability for each of n partitions
                    actorId = it->second.Actor;
                    jt = it;
                }
            }
        }
        Y_VERIFY(actorId);

        {
            auto it = TopicCounters.find(converter->GetClientsideName());
            Y_VERIFY(it != TopicCounters.end());
            it->second.PartitionsToBeReleased.Inc();
        }

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " releasing " << jt->second.Partition);
        jt->second.Releasing = true;
        if (!jt->second.LockSent) { //no lock yet - can release silently
            ReleasePartition(jt, true, ctx);
        } else {
            SendReleaseSignalToClient(jt, false, ctx);
        }
    }
}


void TReadSessionActor::Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx) {
    if (!ActualPartitionActor(ev->Sender))
        return;

    const auto assignId = ev->Get()->Partition.AssignId;

    auto it = Partitions.find(assignId);
    Y_VERIFY(it != Partitions.end());
    Y_VERIFY(it->second.Releasing);

    ReleasePartition(it, false, ctx); //no reads could be here - this is release from partition
}

void TReadSessionActor::InformBalancerAboutRelease(const THashMap<ui64, TPartitionActorInfo>::iterator& it, const TActorContext& ctx) {

    THolder<TEvPersQueue::TEvPartitionReleased> request;
    request.Reset(new TEvPersQueue::TEvPartitionReleased);
    auto& req = request->Record;

    const auto& converter = it->second.Partition.TopicConverter;
    auto jt = Topics.find(converter->GetClientsideName());
    Y_VERIFY(jt != Topics.end());

    req.SetSession(Session);
    ActorIdToProto(jt->second.PipeClient, req.MutablePipeClient());
    req.SetClientId(ClientId);
    req.SetTopic(converter->GetPrimaryPath());
    req.SetPartition(it->second.Partition.Partition);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " released: " << it->second.Partition);

    NTabletPipe::SendData(ctx, jt->second.PipeClient, request.Release());
}


void TReadSessionActor::CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {

    if (errorCode != PersQueue::ErrorCode::OK) {
        if (InternalErrorCode(errorCode)) {
            SLIErrors.Inc();
        }
        if (Errors) {
            ++(*Errors);
        } else if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            ++(*GetServiceCounters(Counters, "pqproxy|readSession")->GetCounter("Errors", true));
        }

        MigrationStreamingReadServerMessage result;
        result.set_status(ConvertPersQueueInternalCodeToStatus(errorCode));

        FillIssue(result.add_issues(), errorCode, errorReason);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed with error reason: " << errorReason);

        if (!WriteResponse(std::move(result), true)) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }
    } else {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed");
        if (!Request->GetStreamCtx()->Finish(std::move(grpc::Status::OK))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc double finish failed");
            Die(ctx);
            return;
        }

    }

    Die(ctx);
}


void TReadSessionActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        if (msg->Dead) {
            CloseSession(TStringBuilder() << "one of topics is deleted, tablet " << msg->TabletId, PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        //TODO: remove it
        CloseSession(TStringBuilder() << "unable to connect to one of topics, tablet " << msg->TabletId, PersQueue::ErrorCode::ERROR, ctx);
        return;

#if 0
        const bool isAlive = ProcessBalancerDead(msg->TabletId, ctx); // returns false if actor died
        Y_UNUSED(isAlive);
        return;
#endif
    }
}

bool TReadSessionActor::ActualPartitionActor(const TActorId& part) {
    return ActualPartitionActors.contains(part);
}


void TReadSessionActor::ReleasePartition(const THashMap<ui64, TPartitionActorInfo>::iterator& it,
                                        bool couldBeReads, const TActorContext& ctx)
{
    {
        auto jt = TopicCounters.find(it->second.Partition.TopicConverter->GetClientsideName());
        Y_VERIFY(jt != TopicCounters.end());
        jt->second.PartitionsReleased.Inc();
        jt->second.PartitionsInfly.Dec();
        if (!it->second.Released && it->second.Releasing) {
            jt->second.PartitionsToBeReleased.Dec();
        }
    }

    Y_VERIFY(couldBeReads || !it->second.Reading);
    //process reads
    TFormedReadResponse::TPtr formedResponseToAnswer;
    if (it->second.Reading) {
        const auto readIt = PartitionToReadResponse.find(it->second.Actor);
        Y_VERIFY(readIt != PartitionToReadResponse.end());
        if (--readIt->second->RequestsInfly == 0) {
            formedResponseToAnswer = readIt->second;
        }
    }

    InformBalancerAboutRelease(it, ctx);

    it->second.Released = true; //to force drop
    DropPartition(it, ctx); //partition will be dropped

    if (formedResponseToAnswer) {
        ProcessAnswer(ctx, formedResponseToAnswer); // returns false if actor died
    }
}


bool TReadSessionActor::ProcessBalancerDead(const ui64 tablet, const TActorContext& ctx) {
    for (auto& t : Topics) {
        if (t.second.TabletID == tablet) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " balancer for topic " << t.first << " is dead, restarting all from this topic");

            //Drop all partitions from this topic
            for (auto it = Partitions.begin(); it != Partitions.end();) {
                if (it->second.Partition.TopicConverter->GetClientsideName() == t.first) { //partition from this topic
                    // kill actor
                    auto jt = it;
                    ++it;
                    if (jt->second.LockSent) {
                        SendReleaseSignalToClient(jt, true, ctx);
                    }
                    ReleasePartition(jt, true, ctx);
                } else {
                    ++it;
                }
            }

            //reconnect pipe
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.CheckAliveness = false;
            clientConfig.RetryPolicy = RetryPolicyForPipes;
            t.second.PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, t.second.TabletID, clientConfig));
            if (InitDone) {
                if (PipeReconnects) {
                    ++(*PipeReconnects);
                }
                if (Errors) {
                    ++(*Errors);
                }

                RegisterSession(t.second.PipeClient, t.first, t.second.Groups, ctx);
            }
        }
    }
    return true;
}


void TReadSessionActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    const bool isAlive = ProcessBalancerDead(ev->Get()->TabletId, ctx); // returns false if actor died
    Y_UNUSED(isAlive);
}

void TReadSessionActor::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr &ev , const TActorContext& ctx) {
    if (ev->Get()->Authenticated && !ev->Get()->InternalToken.empty()) {
        Token = new NACLib::TUserToken(ev->Get()->InternalToken);
        ForceACLCheck = true;
    } else {
        Request->ReplyUnauthenticated("refreshed token is invalid");
        Die(ctx);
    }
}

void TReadSessionActor::ProcessAuth(const TString& auth, const TActorContext& ctx) {
    if (!auth.empty() && auth != Auth) {
        Auth = auth;
        Request->RefreshToken(auth, ctx, ctx.SelfID);
    }
}

void TReadSessionActor::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    RequestNotChecked = true;

    THolder<TEvPQProxy::TEvRead> event(ev->Release());

    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc read failed at start");
        Die(ctx);
        return;
    }


    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " got read request with guid: " << event->Guid);

    Reads.emplace_back(event.Release());

    ProcessReads(ctx);
}


i64 TReadSessionActor::TFormedReadResponse::ApplyResponse(MigrationStreamingReadServerMessage&& resp) {
    Y_VERIFY(resp.data_batch().partition_data_size() == 1);
    Response.set_status(Ydb::StatusIds::SUCCESS);

    Response.mutable_data_batch()->add_partition_data()->Swap(resp.mutable_data_batch()->mutable_partition_data(0));
    i64 prev = Response.ByteSize();
    std::swap<i64>(prev, ByteSize);
    return ByteSize - prev;
}

void TReadSessionActor::Handle(TEvPQProxy::TEvReadResponse::TPtr& ev, const TActorContext& ctx) {
    TActorId sender = ev->Sender;
    if (!ActualPartitionActor(sender))
        return;

    THolder<TEvPQProxy::TEvReadResponse> event(ev->Release());

    Y_VERIFY(event->Response.data_batch().partition_data_size() == 1);
    const ui64 partitionCookie = event->Response.data_batch().partition_data(0).cookie().partition_cookie();
    Y_VERIFY(partitionCookie != 0); // cookie is assigned
    const ui64 assignId = event->Response.data_batch().partition_data(0).cookie().assign_id();
    const auto partitionIt = Partitions.find(assignId);
    Y_VERIFY(partitionIt != Partitions.end());
    Y_VERIFY(partitionIt->second.Reading);
    partitionIt->second.Reading = false;

    partitionIt->second.ReadIdToResponse = partitionCookie + 1;

    auto it = PartitionToReadResponse.find(sender);
    Y_VERIFY(it != PartitionToReadResponse.end());

    TFormedReadResponse::TPtr formedResponse = it->second;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " read done guid " << formedResponse->Guid
                                                                  << partitionIt->second.Partition
                                                                  << " size " << event->Response.ByteSize());

    const i64 diff = formedResponse->ApplyResponse(std::move(event->Response));
    if (event->FromDisk) {
        formedResponse->FromDisk = true;
    }
    formedResponse->WaitQuotaTime = Max(formedResponse->WaitQuotaTime, event->WaitQuotaTime);
    --formedResponse->RequestsInfly;

    BytesInflight_ += diff;
    if (BytesInflight) {
        (*BytesInflight) += diff;
    }

    if (formedResponse->RequestsInfly == 0) {
        ProcessAnswer(ctx, formedResponse);
    }
}

bool TReadSessionActor::WriteResponse(PersQueue::V1::MigrationStreamingReadServerMessage&& response, bool finish) {
    ui64 sz = response.ByteSize();
    ActiveWrites.push(sz);
    BytesInflight_ += sz;
    if (BytesInflight) {
        (*BytesInflight) += sz;
    }

    return finish ? Request->GetStreamCtx()->WriteAndFinish(std::move(response), grpc::Status::OK) : Request->GetStreamCtx()->Write(std::move(response));
}

void TReadSessionActor::ProcessAnswer(const TActorContext& ctx, TFormedReadResponse::TPtr formedResponse) {
    ui32 readDurationMs = (ctx.Now() - formedResponse->Start - formedResponse->WaitQuotaTime).MilliSeconds();
    if (formedResponse->FromDisk) {
        ReadLatencyFromDisk.IncFor(readDurationMs, 1);
    } else {
        ReadLatency.IncFor(readDurationMs, 1);
    }
    if (readDurationMs >= (formedResponse->FromDisk ? AppData(ctx)->PQConfig.GetReadLatencyFromDiskBigMs() : AppData(ctx)->PQConfig.GetReadLatencyBigMs())) {
        SLIBigReadLatency.Inc();
    }

    Y_VERIFY(formedResponse->RequestsInfly == 0);
    const ui64 diff = formedResponse->Response.ByteSize();
    const bool hasMessages = RemoveEmptyMessages(*formedResponse->Response.mutable_data_batch());
    if (hasMessages) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " response to read " << formedResponse->Guid);

        if (!WriteResponse(std::move(formedResponse->Response))) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
            Die(ctx);
            return;
        }
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " empty read result " << formedResponse->Guid << ", start new reading");
    }

    BytesInflight_ -= diff;
    if (BytesInflight) {
        (*BytesInflight) -= diff;
    }

    for (auto& pp : formedResponse->PartitionsTookPartInControlMessages) {
        auto it = PartitionToControlMessages.find(pp);
        Y_VERIFY(it != PartitionToControlMessages.end());
        if (--it->second.Infly == 0) {
            for (auto& r : it->second.ControlMessages) {
                if (!WriteResponse(std::move(r))) {
                    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc write failed");
                    Die(ctx);
                    return;
                }
            }
            PartitionToControlMessages.erase(it);
        }
    }

    for (const TActorId& p : formedResponse->PartitionsTookPartInRead) {
        PartitionToReadResponse.erase(p);
    }

    RequestedBytes -= formedResponse->RequestedBytes;
    ReadsInfly--;

    // Bring back available partitions.
    // If some partition was removed from partitions container, it is not bad because it will be checked during read processing.
    AvailablePartitions.insert(formedResponse->PartitionsBecameAvailable.begin(), formedResponse->PartitionsBecameAvailable.end());

    if (!hasMessages) {
        // process new read
        MigrationStreamingReadClientMessage req;
        req.mutable_read();
        Reads.emplace_back(new TEvPQProxy::TEvRead(formedResponse->Guid)); // Start new reading request with the same guid
    }

    ProcessReads(ctx); // returns false if actor died
}

void TReadSessionActor::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
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

void TReadSessionActor::ProcessReads(const TActorContext& ctx) {
    while (!Reads.empty() && BytesInflight_ + RequestedBytes < MAX_INFLY_BYTES && ReadsInfly < MAX_INFLY_READS) {
        ui32 count = MaxReadMessagesCount;
        ui64 size = MaxReadSize;
        ui32 partitionsAsked = 0;

        TFormedReadResponse::TPtr formedResponse = new TFormedReadResponse(Reads.front()->Guid, ctx.Now());
        while (!AvailablePartitions.empty()) {
            auto part = *AvailablePartitions.begin();
            AvailablePartitions.erase(AvailablePartitions.begin());

            auto it = Partitions.find(part.AssignId);
            if (it == Partitions.end() || it->second.Releasing) { //this is already released partition
                continue;
            }
            //add this partition to reading
            ++partitionsAsked;

            const ui32 ccount = Min<ui32>(part.MsgLag * LAG_GROW_MULTIPLIER, count);
            count -= ccount;
            const ui64 csize = (ui64)Min<double>(part.SizeLag * LAG_GROW_MULTIPLIER, size);
            size -= csize;
            Y_VERIFY(csize < Max<i32>());

            auto jt = ReadFromTimestamp.find(it->second.Partition.TopicConverter->GetModernName());
            Y_VERIFY(jt != ReadFromTimestamp.end());
            ui64 readTimestampMs = Max(ReadTimestampMs, jt->second);

            TAutoPtr<TEvPQProxy::TEvRead> read = new TEvPQProxy::TEvRead(Reads.front()->Guid, ccount, csize, MaxTimeLagMs, readTimestampMs);

            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX
                                        << " performing read request with guid " << read->Guid
                                        << " from " << it->second.Partition << " count " << ccount << " size " << csize
                                        << " partitionsAsked " << partitionsAsked << " maxTimeLag " << MaxTimeLagMs << "ms");


            Y_VERIFY(!it->second.Reading);
            it->second.Reading = true;
            formedResponse->PartitionsTookPartInRead.insert(it->second.Actor);
            auto pp = it->second.Partition;
            pp.AssignId = 0;
            PartitionToControlMessages[pp].Infly++;
            bool res = formedResponse->PartitionsTookPartInControlMessages.insert(pp).second;
            Y_VERIFY(res);

            RequestedBytes += csize;
            formedResponse->RequestedBytes += csize;

            ctx.Send(it->second.Actor, read.Release());
            const auto insertResult = PartitionToReadResponse.insert(std::make_pair(it->second.Actor, formedResponse));
            Y_VERIFY(insertResult.second);

            if (count == 0 || size == 0)
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
        if (BytesInflight) {
            (*BytesInflight) += diff;
        }
        Reads.pop_front();
    }
}


void TReadSessionActor::Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const TActorContext& ctx) {

    if (!ActualPartitionActor(ev->Sender))
        return;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << ev->Get()->Partition
                    << " ready for read with readOffset "
                    << ev->Get()->ReadOffset << " endOffset " << ev->Get()->EndOffset << " WTime "
                    << ev->Get()->WTime << " sizeLag " << ev->Get()->SizeLag);

    const auto it = PartitionToReadResponse.find(ev->Sender); // check whether this partition is taking part in read response
    auto& container = it != PartitionToReadResponse.end() ? it->second->PartitionsBecameAvailable : AvailablePartitions;
    auto res = container.insert(TPartitionInfo{ev->Get()->Partition.AssignId, ev->Get()->WTime, ev->Get()->SizeLag,
                                 ev->Get()->EndOffset - ev->Get()->ReadOffset});
    Y_VERIFY(res.second);
    ProcessReads(ctx);
}


void TReadSessionActor::HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx) {
    CloseSession(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}


void TReadSessionActor::HandleWakeup(const TActorContext& ctx) {
    ctx.Schedule(CHECK_ACL_DELAY, new TEvents::TEvWakeup());
    if (Token && !AuthInitActor && (ForceACLCheck || (ctx.Now() - LastACLCheckTimestamp > TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()) && RequestNotChecked))) {
        ForceACLCheck = false;
        RequestNotChecked = false;
        Y_VERIFY(!AuthInitActor);
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " checking auth because of timeout");

        AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
                ctx, ctx.SelfID, ClientId, Cookie, Session, SchemeCache, NewSchemeCache, Counters, Token,
                TopicsHandler.GetReadTopicsList(TopicsToResolve, ReadOnlyLocal, Request->GetDatabaseName().GetOrElse(TString())), TopicsHandler.GetLocalCluster()
        ));
    }
}


////////////////// PARTITION ACTOR

TPartitionActor::TPartitionActor(const TActorId& parentId, const TString& clientId, const TString& clientPath, const ui64 cookie, const TString& session,
                                    const TPartitionId& partition, const ui32 generation, const ui32 step, const ui64 tabletID,
                                 const TReadSessionActor::TTopicCounters& counters, bool commitsDisabled, const TString& clientDC, bool rangesMode)
    : ParentId(parentId)
    , ClientId(clientId)
    , ClientPath(clientPath)
    , Cookie(cookie)
    , Session(session)
    , ClientDC(clientDC)
    , Partition(partition)
    , Generation(generation)
    , Step(step)
    , TabletID(tabletID)
    , ReadOffset(0)
    , ClientReadOffset(0)
    , ClientCommitOffset(0)
    , ClientVerifyReadOffset(false)
    , CommittedOffset(0)
    , WriteTimestampEstimateMs(0)
    , ReadIdToResponse(1)
    , ReadIdCommitted(0)
    , RangesMode(rangesMode)
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
    , CommitsDisabled(commitsDisabled)
    , CommitCookie(1)
{
}


void TPartitionActor::MakeCommit(const TActorContext& ctx) {
    ui64 offset = ClientReadOffset;
    if (CommitsDisabled)
        return;
    if (CommitsInfly.size() > MAX_COMMITS_INFLY)
        return;

    //Ranges mode
    if (!NextRanges.Empty() && NextRanges.Min() == ClientCommitOffset) {
        auto first = NextRanges.begin();
        offset = first->second;
        NextRanges.EraseInterval(first->first, first->second);

        ClientCommitOffset = offset;
        ++CommitCookie;
        CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(CommitCookie, {CommitCookie, offset, ctx.Now()}));
        Counters.SLITotal.Inc();

        if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
            SendCommit(CommitCookie, offset, ctx);
        return;
    }

    //Now commits by cookies.
    ui64 readId = ReadIdCommitted;
    auto it = NextCommits.begin();
    if (it != NextCommits.end() && *it == 0) { //commit of readed in prev session data
        NextCommits.erase(NextCommits.begin());
        if (ClientReadOffset <= ClientCommitOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(Partition.AssignId, 0, 0, CommittedOffset));
        } else {
            ClientCommitOffset = ClientReadOffset;
            CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(0, {0, ClientReadOffset, ctx.Now()}));
            Counters.SLITotal.Inc();
            if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
                SendCommit(0, ClientReadOffset, ctx);
        }
        MakeCommit(ctx);
        return;
    }
    for (;it != NextCommits.end() && (*it) == readId + 1; ++it) {
        ++readId;
    }
    if (readId == ReadIdCommitted)
        return;
    NextCommits.erase(NextCommits.begin(), it);
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " commit request from " << ReadIdCommitted + 1 << " to " << readId << " in " << Partition);

    ui64 startReadId = ReadIdCommitted + 1;

    ReadIdCommitted = readId;

    auto jt = Offsets.begin();
    while(jt != Offsets.end() && jt->ReadId != readId) ++jt;
    Y_VERIFY(jt != Offsets.end());

    offset = Max(offset, jt->Offset);

    Offsets.erase(Offsets.begin(), ++jt);

    Y_VERIFY(offset > ClientCommitOffset);

    ClientCommitOffset = offset;
    CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(readId, {startReadId, offset, ctx.Now()}));
    Counters.SLITotal.Inc();

    if (PipeClient) //if not then pipe will be recreated soon and SendCommit will be done
        SendCommit(readId, offset, ctx);
}

TPartitionActor::~TPartitionActor() = default;


void TPartitionActor::Bootstrap(const TActorContext&) {
    Become(&TThis::StateFunc);
}


void TPartitionActor::CheckRelease(const TActorContext& ctx) {
    const bool hasUncommittedData = ReadOffset > ClientCommitOffset && ReadOffset > ClientReadOffset; //TODO: remove ReadOffset > ClientReadOffset - otherwise wait for commit with cookie(0)
    if (NeedRelease) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " checking release readOffset " << ReadOffset << " committedOffset " << CommittedOffset << " ReadGuid " << ReadGuid
                        << " CommitsInfly.size " << CommitsInfly.size() << " Released " << Released);
    }

    if (NeedRelease && (ReadGuid.empty() && CommitsInfly.empty() && !hasUncommittedData && !Released)) {
        Released = true;
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReleased(Partition));
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " check release done - releasing; readOffset " << ReadOffset << " committedOffset " << CommittedOffset << " ReadGuid " << ReadGuid
                        << " CommitsInfly.size " << CommitsInfly.size() << " Released " << Released);

    }
}


void TPartitionActor::SendCommit(const ui64 readId, const ui64 offset, const TActorContext& ctx) {
    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(Partition.TopicConverter->GetPrimaryPath());
    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie(readId);

    Y_VERIFY(PipeClient);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto commit = request.MutablePartitionRequest()->MutableCmdSetClientOffset();
    commit->SetClientId(ClientId);
    commit->SetOffset(offset);
    Y_VERIFY(!Session.empty());
    commit->SetSessionId(Session);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " committing to position " << offset << " prev " << CommittedOffset
                        << " end " << EndOffset << " by cookie " << readId);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, req.Release());
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
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("too much attempts to restart pipe", PersQueue::ErrorCode::ERROR));
        return;
    }

    ctx.Schedule(TDuration::MilliSeconds(RESTART_PIPE_DELAY_MS), new TEvPQProxy::TEvRestartPipe());

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " schedule pipe restart attempt " << PipeGeneration << " reason: " << reason);
}


void TPartitionActor::Handle(const TEvPQProxy::TEvRestartPipe::TPtr&, const TActorContext& ctx) {

    Y_VERIFY(!PipeClient);

    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
    PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, TabletID, clientConfig));
    Y_VERIFY(TabletID);

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " pipe restart attempt " << PipeGeneration << " RequestInfly " << RequestInfly << " ReadOffset " << ReadOffset << " EndOffset " << EndOffset
                            << " InitDone " << InitDone << " WaitForData " << WaitForData);

    if (RequestInfly) { //got read infly
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " resend " << CurrentRequest);

        TAutoPtr<TEvPersQueue::TEvRequest> event(new TEvPersQueue::TEvRequest);
        event->Record = CurrentRequest;

        ActorIdToProto(PipeClient, event->Record.MutablePartitionRequest()->MutablePipeClient());

        NTabletPipe::SendData(ctx, PipeClient, event.Release());
    }
    if (InitDone) {
        for (auto& c : CommitsInfly) { //resend all commits
            if (c.second.Offset != Max<ui64>())
                SendCommit(c.first, c.second.Offset, ctx);
        }
        if (WaitForData) { //resend wait-for-data requests
            WaitDataInfly.clear();
            WaitDataInPartition(ctx);
        }
    }
}

bool FillBatchedData(MigrationStreamingReadServerMessage::DataBatch * data, const NKikimrClient::TCmdReadResult& res, const TPartitionId& Partition, ui64 ReadIdToResponse, ui64& ReadOffset, ui64& WTime, ui64 EndOffset, const TActorContext& ctx) {

    auto* partitionData = data->add_partition_data();
    partitionData->mutable_topic()->set_path(Partition.TopicConverter->GetModernName());
    partitionData->set_cluster(Partition.TopicConverter->GetCluster());
    partitionData->set_partition(Partition.Partition);
    partitionData->set_deprecated_topic(Partition.TopicConverter->GetFullLegacyName());
    partitionData->mutable_cookie()->set_assign_id(Partition.AssignId);
    partitionData->mutable_cookie()->set_partition_cookie(ReadIdToResponse);

    bool hasOffset = false;
    bool hasData = false;

    MigrationStreamingReadServerMessage::DataBatch::Batch* currentBatch = nullptr;
    for (ui32 i = 0; i < res.ResultSize(); ++i) {
        const auto& r = res.GetResult(i);
        WTime = r.GetWriteTimestampMS();
        Y_VERIFY(r.GetOffset() >= ReadOffset);
        ReadOffset = r.GetOffset() + 1;
        hasOffset = true;

        auto proto(GetDeserializedData(r.GetData()));
        if (proto.GetChunkType() != NKikimrPQClient::TDataChunk::REGULAR) {
            continue; //TODO - no such chunks must be on prod
        }

        TString sourceId;
        if (!r.GetSourceId().empty()) {
            if (!NPQ::NSourceIdEncoding::IsValidEncoded(r.GetSourceId())) {
                LOG_ERROR_S(ctx, NKikimrServices::PQ_READ_PROXY, "read bad sourceId from " << Partition
                                                                                           << " offset " << r.GetOffset() << " seqNo " << r.GetSeqNo() << " sourceId '" << r.GetSourceId() << "'");
            }
            sourceId = NPQ::NSourceIdEncoding::Decode(r.GetSourceId());
        }

        if (!currentBatch || currentBatch->write_timestamp_ms() != r.GetWriteTimestampMS() || currentBatch->source_id() != sourceId) {
            // If write time and source id are the same, the rest fields will be the same too.
            currentBatch = partitionData->add_batches();
            currentBatch->set_write_timestamp_ms(r.GetWriteTimestampMS());
            currentBatch->set_source_id(sourceId);

            if (proto.HasMeta()) {
                const auto& header = proto.GetMeta();
                if (header.HasServer()) {
                    auto* item = currentBatch->add_extra_fields();
                    item->set_key("server");
                    item->set_value(header.GetServer());
                }
                if (header.HasFile()) {
                    auto* item = currentBatch->add_extra_fields();
                    item->set_key("file");
                    item->set_value(header.GetFile());
                }
                if (header.HasIdent()) {
                    auto* item = currentBatch->add_extra_fields();
                    item->set_key("ident");
                    item->set_value(header.GetIdent());
                }
                if (header.HasLogType()) {
                    auto* item = currentBatch->add_extra_fields();
                    item->set_key("logtype");
                    item->set_value(header.GetLogType());
                }
            }

            if (proto.HasExtraFields()) {
                const auto& map = proto.GetExtraFields();
                for (const auto& kv : map.GetItems()) {
                    auto* item = currentBatch->add_extra_fields();
                    item->set_key(kv.GetKey());
                    item->set_value(kv.GetValue());
                }
            }

            if (proto.HasIp() && IsUtf(proto.GetIp())) {
                currentBatch->set_ip(proto.GetIp());
            }
        }

        auto* message = currentBatch->add_message_data();
        message->set_seq_no(r.GetSeqNo());
        message->set_create_timestamp_ms(r.GetCreateTimestampMS());
        message->set_offset(r.GetOffset());

        message->set_explicit_hash(r.GetExplicitHash());
        message->set_partition_key(r.GetPartitionKey());

        if (proto.HasCodec()) {
            message->set_codec(NPQ::ToV1Codec((NPersQueueCommon::ECodec)proto.GetCodec()));
        }
        message->set_uncompressed_size(r.GetUncompressedSize());
        message->set_data(proto.GetData());
        hasData = true;
    }

    const ui64 realReadOffset = res.HasRealReadOffset() ? res.GetRealReadOffset() : 0;

    if (!hasOffset) { //no data could be read from partition at offset ReadOffset - no data in partition at all???
        ReadOffset = Min(Max(ReadOffset + 1, realReadOffset + 1), EndOffset);
    }
    return hasData;
}


void TPartitionActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {

    if (ev->Get()->Record.HasErrorCode() && ev->Get()->Record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
        const auto errorCode = ev->Get()->Record.GetErrorCode();
        if (errorCode == NPersQueue::NErrorCode::WRONG_COOKIE || errorCode == NPersQueue::NErrorCode::BAD_REQUEST) {
            Counters.Errors.Inc();
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("status is not ok: " + ev->Get()->Record.GetErrorReason(), ConvertOldCode(ev->Get()->Record.GetErrorCode())));
        } else {
            RestartPipe(ctx, TStringBuilder() << "status is not ok. Code: " << EErrorCode_Name(errorCode) << ". Reason: " << ev->Get()->Record.GetErrorReason(), errorCode);
        }
        return;
    }

    if (ev->Get()->Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) { //this is incorrect answer, die
        Y_VERIFY(!ev->Get()->Record.HasErrorCode());
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("status is not ok: " + ev->Get()->Record.GetErrorReason(), PersQueue::ErrorCode::ERROR));
        return;
    }
    if (!ev->Get()->Record.HasPartitionResponse()) { //this is incorrect answer, die
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("empty partition in response", PersQueue::ErrorCode::ERROR));
        return;
    }

    const auto& result = ev->Get()->Record.GetPartitionResponse();

    if (!result.HasCookie()) { //this is incorrect answer, die
        Counters.Errors.Inc();
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("no cookie in response", PersQueue::ErrorCode::ERROR));
        return;
    }

    auto MaskResult = [](const NKikimrClient::TPersQueuePartitionResponse& resp) {
            if (resp.HasCmdReadResult()) {
                auto res = resp;
                for (auto& rr : *res.MutableCmdReadResult()->MutableResult()) {
                    rr.SetData(TStringBuilder() << "... " << rr.GetData().size() << " bytes ...");
                }
                return res;
            }
            return resp;
        };

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " initDone " << InitDone << " event " << MaskResult(result));


    if (!InitDone) {
        if (result.GetCookie() != INIT_COOKIE) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " unwaited response in init with cookie " << result.GetCookie());
            return;
        }
        Y_VERIFY(RequestInfly);
        CurrentRequest.Clear();
        RequestInfly = false;

        Y_VERIFY(result.HasCmdGetClientOffsetResult());
        const auto& resp = result.GetCmdGetClientOffsetResult();
        Y_VERIFY(resp.HasEndOffset());
        EndOffset = resp.GetEndOffset();
        SizeLag = resp.GetSizeLag();
        WriteTimestampEstimateMs = resp.GetWriteTimestampEstimateMS();

        ClientCommitOffset = ReadOffset = CommittedOffset = resp.HasOffset() ? resp.GetOffset() : 0;
        Y_VERIFY(EndOffset >= CommittedOffset);

        if (resp.HasWriteTimestampMS())
            WTime = resp.GetWriteTimestampMS();

        InitDone = true;
        PipeGeneration = 0; //reset tries counter - all ok
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " INIT DONE " << Partition
                            << " EndOffset " << EndOffset << " readOffset " << ReadOffset << " committedOffset " << CommittedOffset);


        if (!StartReading) {
            ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Partition, CommittedOffset, EndOffset, WriteTimestampEstimateMs));
        } else {
            InitStartReading(ctx);
        }
        return;
    }

    if (!result.HasCmdReadResult()) { //this is commit response
        if (CommitsInfly.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " unwaited commit-response with cookie " << result.GetCookie() << "; waiting for nothing");
            return;
        }
        ui64 readId = CommitsInfly.front().first;

        if (result.GetCookie() != readId) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                            << " unwaited commit-response with cookie " << result.GetCookie() << "; waiting for " << readId);
            return;
        }

        Counters.Commits.Inc();

        ui32 commitDurationMs = (ctx.Now() - CommitsInfly.front().second.StartTime).MilliSeconds();
        Counters.CommitLatency.IncFor(commitDurationMs, 1);
        if (commitDurationMs >= AppData(ctx)->PQConfig.GetCommitLatencyBigMs()) {
            Counters.SLIBigLatency.Inc();
        }

        CommittedOffset = CommitsInfly.front().second.Offset;
        ui64 startReadId = CommitsInfly.front().second.StartReadId;
        ctx.Send(ParentId, new TEvPQProxy::TEvCommitDone(Partition.AssignId, startReadId, readId, CommittedOffset));

        CommitsInfly.pop_front();

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " commit done to position " << CommittedOffset << " endOffset " << EndOffset << " with cookie " << readId);

        CheckRelease(ctx);
        PipeGeneration = 0; //reset tries counter - all ok
        MakeCommit(ctx);
        return;
    }

    //This is read
    Y_VERIFY(result.HasCmdReadResult());
    const auto& res = result.GetCmdReadResult();

    if (result.GetCookie() != (ui64)ReadOffset) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                    << " unwaited read-response with cookie " << result.GetCookie() << "; waiting for " << ReadOffset << "; current read guid is " << ReadGuid);
        return;
    }

    Y_VERIFY(res.HasMaxOffset());
    EndOffset = res.GetMaxOffset();
    SizeLag = res.GetSizeLag();

    MigrationStreamingReadServerMessage response;
    response.set_status(Ydb::StatusIds::SUCCESS);

    auto* data = response.mutable_data_batch();
    bool hasData = FillBatchedData(data, res, Partition, ReadIdToResponse, ReadOffset, WTime, EndOffset, ctx);
    WriteTimestampEstimateMs = Max(WriteTimestampEstimateMs, WTime);

    if (!CommitsDisabled && !RangesMode) {
        Offsets.push_back({ReadIdToResponse, ReadOffset});
    }

    if (Offsets.size() >= AppData(ctx)->PQConfig.GetMaxReadCookies() + 10) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies() << " uncommitted reads", PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }

    CurrentRequest.Clear();
    RequestInfly = false;

    Y_VERIFY(!WaitForData);

    if (EndOffset > ReadOffset) {
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Partition, WTime, SizeLag, ReadOffset, EndOffset));
    } else {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }

    if (hasData) {
        ++ReadIdToResponse;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " after read state " << Partition
                << " EndOffset " << EndOffset << " ReadOffset " << ReadOffset << " ReadGuid " << ReadGuid << " has messages " << hasData);

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

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
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
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " (partition)releasing " << Partition << " ReadOffset " << ReadOffset << " ClientCommitOffset " << ClientCommitOffset
                        << " CommittedOffst " << CommittedOffset);
    NeedRelease = true;
    CheckRelease(ctx);
}


void TPartitionActor::Handle(TEvPQProxy::TEvGetStatus::TPtr&, const TActorContext& ctx) {
    ctx.Send(ParentId, new TEvPQProxy::TEvPartitionStatus(Partition, CommittedOffset, EndOffset, WriteTimestampEstimateMs, false));
}


void TPartitionActor::Handle(TEvPQProxy::TEvLockPartition::TPtr& ev, const TActorContext& ctx) {
    ClientReadOffset = ev->Get()->ReadOffset;
    ClientCommitOffset = ev->Get()->CommitOffset;
    ClientVerifyReadOffset = ev->Get()->VerifyReadOffset;

    if (StartReading) {
        Y_VERIFY(ev->Get()->StartReading); //otherwise it is signal from actor, this could not be done
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("double partition locking", PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }

    StartReading = ev->Get()->StartReading;
    InitLockPartition(ctx);
}

void TPartitionActor::InitStartReading(const TActorContext& ctx) {

    Y_VERIFY(AllPrepareInited);
    Y_VERIFY(!WaitForData);
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Start reading " << Partition
                        << " EndOffset " << EndOffset << " readOffset " << ReadOffset << " committedOffset " << CommittedOffset << " clientCommitOffset " << ClientCommitOffset << " clientReadOffset " << ClientReadOffset);

    Counters.PartitionsToBeLocked.Dec();
    LockCounted = false;

    ReadOffset = Max<ui64>(CommittedOffset, ClientReadOffset);

    if (ClientVerifyReadOffset) {
        if (ClientReadOffset < ClientCommitOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                << "trying to read from position that is less than position provided to commit: read " << ClientReadOffset << " committed " << ClientCommitOffset,
                PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }

        if (ClientCommitOffset < CommittedOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                    << "trying to commit to position that is less than committed: read " << ClientCommitOffset << " committed " << CommittedOffset,
                    PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        if (ClientReadOffset < CommittedOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                    << "trying to read from position that is less than committed: read " << ClientReadOffset << " committed " << CommittedOffset,
                    PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
    }

    if (ClientCommitOffset > CommittedOffset) {
        if (ClientCommitOffset > ReadOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                        << "trying to read from position that is less than provided to commit: read " << ReadOffset << " commit " << ClientCommitOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        if (ClientCommitOffset > EndOffset) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder()
                        << "trying to commit to future: commit " << ClientCommitOffset << " endOffset " << EndOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        Y_VERIFY(CommitsInfly.empty());
        CommitsInfly.push_back(std::pair<ui64, TCommitInfo>(Max<ui64>(), {Max<ui64>(), ClientCommitOffset, ctx.Now()}));
        Counters.SLITotal.Inc();
        if (PipeClient) //pipe will be recreated soon
            SendCommit(CommitsInfly.back().first, CommitsInfly.back().second.Offset, ctx);
    } else {
        ClientCommitOffset = CommittedOffset;
    }

    if (EndOffset > ReadOffset) {
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Partition, WTime, SizeLag, ReadOffset, EndOffset));
    } else {
        WaitForData = true;
        if (PipeClient) //pipe will be recreated soon
            WaitDataInPartition(ctx);
    }
}

//TODO: add here reaction on client release request

void TPartitionActor::InitLockPartition(const TActorContext& ctx) {
    if (PipeClient && AllPrepareInited) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession("double partition locking", PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }
    if (!LockCounted) {
        Counters.PartitionsToBeLocked.Inc();
        LockCounted = true;
    }
    if (StartReading)
        AllPrepareInited = true;

    if (FirstInit) {
        Y_VERIFY(!PipeClient);
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

        request.MutablePartitionRequest()->SetTopic(Partition.TopicConverter->GetPrimaryPath());
        request.MutablePartitionRequest()->SetPartition(Partition.Partition);
        request.MutablePartitionRequest()->SetCookie(INIT_COOKIE);

        ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());

        auto cmd = request.MutablePartitionRequest()->MutableCmdCreateSession();
        cmd->SetClientId(ClientId);
        cmd->SetSessionId(Session);
        cmd->SetGeneration(Generation);
        cmd->SetStep(Step);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " INITING " << Partition);

        TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        Y_VERIFY(!RequestInfly);
        CurrentRequest = request;
        RequestInfly = true;
        req->Record.Swap(&request);

        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    } else {
        Y_VERIFY(StartReading); //otherwise it is double locking from actor, not client - client makes lock always with StartReading == true
        Y_VERIFY(InitDone);
        InitStartReading(ctx);
    }
}


void TPartitionActor::WaitDataInPartition(const TActorContext& ctx) {

    if (WaitDataInfly.size() > 1) //already got 2 requests inflight
        return;
    Y_VERIFY(InitDone);

    Y_VERIFY(PipeClient);

    if (!WaitForData)
        return;

    Y_VERIFY(ReadOffset >= EndOffset);

    TAutoPtr<TEvPersQueue::TEvHasDataInfo> event(new TEvPersQueue::TEvHasDataInfo());
    event->Record.SetPartition(Partition.Partition);
    event->Record.SetOffset(ReadOffset);
    event->Record.SetCookie(++WaitDataCookie);
    ui64 deadline = (ctx.Now() + WAIT_DATA - WAIT_DELTA).MilliSeconds();
    event->Record.SetDeadline(deadline);
    event->Record.SetClientId(ClientId);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition << " wait data in partition inited, cookie " << WaitDataCookie);

    NTabletPipe::SendData(ctx, PipeClient, event.Release());

    ctx.Schedule(PREWAIT_DATA, new TEvents::TEvWakeup());

    ctx.Schedule(WAIT_DATA, new TEvPQProxy::TEvDeadlineExceeded(WaitDataCookie));

    WaitDataInfly.insert(WaitDataCookie);
}

void TPartitionActor::Handle(TEvPersQueue::TEvHasDataInfoResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    WriteTimestampEstimateMs = record.GetWriteTimestampEstimateMS();

    auto it = WaitDataInfly.find(ev->Get()->Record.GetCookie());
    if (it == WaitDataInfly.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " unwaited response for WaitData " << ev->Get()->Record);
        return;
    }
    WaitDataInfly.erase(it);
    if (!WaitForData)
        return;

    Counters.WaitsForData.Inc();

    Y_VERIFY(record.HasEndOffset());
    Y_VERIFY(EndOffset <= record.GetEndOffset()); //end offset could not be changed if no data arrived, but signal will be sended anyway after timeout
    Y_VERIFY(ReadOffset >= EndOffset); //otherwise no WaitData were needed

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                    << " wait for data done: " << " readOffset " << ReadOffset << " EndOffset " << EndOffset << " newEndOffset "
                    << record.GetEndOffset() << " commitOffset " << CommittedOffset << " clientCommitOffset " << ClientCommitOffset  << " cookie " << ev->Get()->Record.GetCookie());

    EndOffset = record.GetEndOffset();
    SizeLag = record.GetSizeLag();

    if (ReadOffset < EndOffset) {
        WaitForData = false;
        WaitDataInfly.clear();
        ctx.Send(ParentId, new TEvPQProxy::TEvPartitionReady(Partition, WTime, SizeLag, ReadOffset, EndOffset));
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " " << Partition
                        << " ready for read with readOffset " << ReadOffset << " endOffset " << EndOffset);
    } else {
        if (PipeClient)
            WaitDataInPartition(ctx);
    }
    CheckRelease(ctx); //just for logging purpose
}


void TPartitionActor::Handle(TEvPQProxy::TEvRead::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " READ FROM " << Partition
                    << "maxCount " << ev->Get()->MaxCount << " maxSize " << ev->Get()->MaxSize << " maxTimeLagMs " << ev->Get()->MaxTimeLagMs << " readTimestampMs " << ev->Get()->ReadTimestampMs
                    << " readOffset " << ReadOffset << " EndOffset " << EndOffset << " ClientCommitOffset " << ClientCommitOffset << " committedOffset " << CommittedOffset << " Guid " << ev->Get()->Guid);

    Y_VERIFY(!NeedRelease);
    Y_VERIFY(!Released);

    Y_VERIFY(ReadGuid.empty());
    Y_VERIFY(!RequestInfly);

    ReadGuid = ev->Get()->Guid;

    const auto req = ev->Get();

    NKikimrClient::TPersQueueRequest request;

    request.MutablePartitionRequest()->SetTopic(Partition.TopicConverter->GetPrimaryPath());

    request.MutablePartitionRequest()->SetPartition(Partition.Partition);
    request.MutablePartitionRequest()->SetCookie((ui64)ReadOffset);

    ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());
    auto read = request.MutablePartitionRequest()->MutableCmdRead();
    read->SetClientId(ClientId);
    read->SetClientDC(ClientDC);
    if (req->MaxCount) {
        read->SetCount(req->MaxCount);
    }
    if (req->MaxSize) {
        read->SetBytes(req->MaxSize);
    }
    if (req->MaxTimeLagMs) {
        read->SetMaxTimeLagMs(req->MaxTimeLagMs);
    }
    if (req->ReadTimestampMs) {
        read->SetReadTimestampMs(req->ReadTimestampMs);
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


void TPartitionActor::Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const TActorContext& ctx) {
    //TODO: add here processing of cookie == 0 if ReadOffset > ClientCommittedOffset if any
    Y_VERIFY(ev->Get()->AssignId == Partition.AssignId);
    for (auto& readId : ev->Get()->CommitInfo.Cookies) {
        if (readId == 0) {
            if (ReadIdCommitted > 0) {
                ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "commit of 0 allowed only as first commit in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
                return;
            }
            NextCommits.insert(0);
            continue;
        }
        if (readId <= ReadIdCommitted) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "commit of " << readId << " that is already committed in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        if (readId >= ReadIdToResponse) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "commit of unknown cookie " << readId << " in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        bool res = NextCommits.insert(readId).second;
        if (!res) {
            ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "double commit of cookie " << readId << " in " << Partition, PersQueue::ErrorCode::BAD_REQUEST));
            return;
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " commit request from client for " << readId << " in " << Partition);
    }

    MakeCommit(ctx);

    if (NextCommits.size() >= AppData(ctx)->PQConfig.GetMaxReadCookies()) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies()
                                                            << " unordered cookies to commit in " << Partition << ", last cookie is " << ReadIdCommitted,
                        PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }
}

void TPartitionActor::Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(ev->Get()->AssignId == Partition.AssignId);

    for (auto& c : ev->Get()->CommitInfo.Ranges) {
        NextRanges.InsertInterval(c.first, c.second);
    }

    MakeCommit(ctx);

    if (NextRanges.GetNumIntervals() >= AppData(ctx)->PQConfig.GetMaxReadCookies()) {
        ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(TStringBuilder() << "got more than " << AppData(ctx)->PQConfig.GetMaxReadCookies()
                                                            << " unordered offset ranges to commit in " << Partition
                                                            << ", last to be committed offset is " << ClientCommitOffset
                                                            << ", committed offset is " << CommittedOffset,
                        PersQueue::ErrorCode::BAD_REQUEST));
        return;
    }
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
        Y_VERIFY(WaitForData);
        WaitDataInPartition(ctx);
    }

}


void TPartitionActor::HandleWakeup(const TActorContext& ctx) {
    if (ReadOffset >= EndOffset && WaitDataInfly.size() <= 1 && PipeClient) { //send one more
        Y_VERIFY(WaitForData);
        WaitDataInPartition(ctx);
    }
}

///////////////// AuthAndInitActor
TReadInitAndAuthActor::TReadInitAndAuthActor(
        const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
        const TString& session, const NActors::TActorId& metaCache, const NActors::TActorId& newSchemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TIntrusivePtr<NACLib::TUserToken> token,
        const NPersQueue::TTopicsToConverter& topics, const TString& localCluster
)
    : ParentId(parentId)
    , Cookie(cookie)
    , Session(session)
    , MetaCacheId(metaCache)
    , NewSchemeCache(newSchemeCache)
    , ClientId(clientId)
    , ClientPath(NPersQueue::ConvertOldConsumerName(ClientId, ctx))
    , Token(token)
    , Counters(counters)
    , LocalCluster(localCluster)
{
    for (const auto& [path, converter] : topics) {
        Topics[path].TopicNameConverter = converter;
    }
}


TReadInitAndAuthActor::~TReadInitAndAuthActor() = default;


void TReadInitAndAuthActor::Bootstrap(const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth for : " << ClientId);
    Become(&TThis::StateFunc);
    DoCheckACL = AppData(ctx)->PQConfig.GetCheckACL() && Token;
    DescribeTopics(ctx);
}

void TReadInitAndAuthActor::DescribeTopics(const NActors::TActorContext& ctx, bool showPrivate) {
    TVector<TString> topicNames;
    for (const auto& [_, holder] : Topics) {
        topicNames.emplace_back(holder.TopicNameConverter->GetPrimaryPath());
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " describe topics: " << JoinSeq(", ", topicNames));
    ctx.Send(MetaCacheId, new TEvDescribeTopicsRequest(topicNames, true, showPrivate));
}

void TReadInitAndAuthActor::Die(const TActorContext& ctx) {
    for (auto& [_, holder] : Topics) {
        if (holder.PipeClient)
            NTabletPipe::CloseClient(ctx, holder.PipeClient);
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth is DEAD");

    TActorBootstrapped<TReadInitAndAuthActor>::Die(ctx);
}

void TReadInitAndAuthActor::CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode code, const TActorContext& ctx)
{
    ctx.Send(ParentId, new TEvPQProxy::TEvCloseSession(errorReason, code));
    Die(ctx);
}

void TReadInitAndAuthActor::SendCacheNavigateRequest(const TActorContext& ctx, const TString& path) {
    auto schemeCacheRequest = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath(path);
    entry.SyncVersion = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    schemeCacheRequest->ResultSet.emplace_back(entry);
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Send client acl request");
    ctx.Send(NewSchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
}


bool TReadInitAndAuthActor::ProcessTopicSchemeCacheResponse(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, THashMap<TString, TTopicHolder>::iterator topicsIter,
        const TActorContext& ctx
) {
    Y_VERIFY(entry.PQGroupInfo); // checked at ProcessMetaCacheTopicResponse()
    auto& pqDescr = entry.PQGroupInfo->Description;
    topicsIter->second.TabletID = pqDescr.GetBalancerTabletID();
    topicsIter->second.CloudId = pqDescr.GetPQTabletConfig().GetYcCloudId();
    topicsIter->second.DbId = pqDescr.GetPQTabletConfig().GetYdbDatabaseId();
    topicsIter->second.FolderId = pqDescr.GetPQTabletConfig().GetYcFolderId();
    return CheckTopicACL(entry, topicsIter->first, ctx);
}


void TReadInitAndAuthActor::HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " Handle describe topics response");

    bool reDescribe = false;
    for (const auto& entry : ev->Get()->Result->ResultSet) {
        auto path = JoinPath(entry.Path);
        auto it = Topics.find(path);
        Y_VERIFY(it != Topics.end());

        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
            Y_VERIFY(entry.ListNodeEntry->Children.size() == 1);
            const auto& topic = entry.ListNodeEntry->Children.at(0);

            it->second.TopicNameConverter->SetPrimaryPath(JoinPath(ChildPath(entry.Path, topic.Name)));
            Topics[it->second.TopicNameConverter->GetPrimaryPath()] = it->second;
            Topics.erase(it);

            reDescribe = true;
            continue;
        }

        auto processResult = ProcessMetaCacheTopicResponse(entry);
        if (processResult.IsFatal) {
            Topics.erase(it);
            if (Topics.empty()) {
                TStringBuilder reason;
                reason << "Discovery for all topics failed. The last error was: " << processResult.Reason;
                return CloseSession(reason, processResult.ErrorCode, ctx);
            } else {
                continue;
            }
        }

        if (!ProcessTopicSchemeCacheResponse(entry, it, ctx)) {
            return;
        }
    }

    if (Topics.empty()) {
        CloseSession("no topics found", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }

    if (reDescribe) {
        return DescribeTopics(ctx, true);
    }

    // ToDo[migration] - separate option - ?
    bool doCheckClientAcl = DoCheckACL && !AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen();
    if (doCheckClientAcl) {
        CheckClientACL(ctx);
    } else {
        FinishInitialization(ctx);
    }
}


bool TReadInitAndAuthActor::CheckTopicACL(
        const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& topic, const TActorContext& ctx
) {
    auto& pqDescr = entry.PQGroupInfo->Description;
    //ToDo[migration] - proper auth setup
    if (Token && !CheckACLPermissionsForNavigate(
            entry.SecurityObject, topic, NACLib::EAccessRights::SelectRow,
            "No ReadTopic permissions", ctx
    )) {
        return false;
    }
    if (Token || AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        bool found = false;
        for (auto& cons : pqDescr.GetPQTabletConfig().GetReadRules() ) {
            if (cons == ClientId) {
                found = true;
                break;
            }
        }
        if (!found) {
            CloseSession(
                    TStringBuilder() << "no read rule provided for consumer '" << ClientPath << "' in topic '" << topic << "' in current cluster '" << LocalCluster,
                    PersQueue::ErrorCode::BAD_REQUEST, ctx
            );
            return false;
        }
    }
    return true;
}


void TReadInitAndAuthActor::CheckClientACL(const TActorContext& ctx) {
    // ToDo[migration] - Through converter/metacache - ?
    SendCacheNavigateRequest(ctx, AppData(ctx)->PQConfig.GetRoot() + "/" + ClientPath);
}


void TReadInitAndAuthActor::HandleClientSchemeCacheResponse(
        TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx
) {
    TEvTxProxySchemeCache::TEvNavigateKeySetResult* msg = ev->Get();
    const NSchemeCache::TSchemeCacheNavigate* navigate = msg->Request.Get();

    Y_VERIFY(navigate->ResultSet.size() == 1);
    auto& entry = navigate->ResultSet.front();
    auto path = "/" + JoinPath(entry.Path); // ToDo [migration] - through converter ?
    if (navigate->ErrorCount > 0) {
        const NSchemeCache::TSchemeCacheNavigate::EStatus status = navigate->ResultSet.front().Status;
        CloseSession(TStringBuilder() << "Failed to read ACL for '" << path << "' Scheme cache error : " << status, PersQueue::ErrorCode::ERROR, ctx);
        return;
    }

    NACLib::EAccessRights rights = (NACLib::EAccessRights)(NACLib::EAccessRights::ReadAttributes + NACLib::EAccessRights::WriteAttributes);
    if (
            !CheckACLPermissionsForNavigate(entry.SecurityObject, path, rights, "No ReadAsConsumer permissions", ctx)
    ) {
        return;
    }
    FinishInitialization(ctx);
}


bool TReadInitAndAuthActor::CheckACLPermissionsForNavigate(
        const TIntrusivePtr<TSecurityObject>& secObject, const TString& path,
        NACLib::EAccessRights rights, const TString& errorTextWhenAccessDenied, const TActorContext& ctx
) {
    // TODO: SCHEME_ERROR если нет топика/консумера
    // TODO: если AccessDenied на корень, то надо ACCESS_DENIED, а не SCHEME_ERROR

    if (DoCheckACL && !secObject->CheckAccess(rights, *Token)) {
        CloseSession(
                TStringBuilder() << errorTextWhenAccessDenied << " for '" << path
                                 << "' for subject '" << Token->GetUserSID() << "'",
                PersQueue::ErrorCode::ACCESS_DENIED, ctx
        );
        return false;
    }
    return true;
}


void TReadInitAndAuthActor::FinishInitialization(const TActorContext& ctx) {
    TTopicTabletsPairs res;
    for (auto& [_, holder] : Topics) {
        res.emplace_back(decltype(res)::value_type({
            holder.TopicNameConverter, holder.TabletID, holder.CloudId, holder.DbId, holder.FolderId
        }));
    }
    ctx.Send(ParentId, new TEvPQProxy::TEvAuthResultOk(std::move(res)));
    Die(ctx);
}

// READINFOACTOR
TReadInfoActor::TReadInfoActor(
        TEvPQReadInfoRequest* request, const NPersQueue::TTopicsListController& topicsHandler,
        const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters
)
    : TBase(request)
    , SchemeCache(schemeCache)
    , NewSchemeCache(newSchemeCache)
    , AuthInitActor()
    , Counters(counters)
    , TopicsHandler(topicsHandler)
{
    Y_ASSERT(request);
}



TReadInfoActor::~TReadInfoActor() = default;


void TReadInfoActor::Bootstrap(const TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    Become(&TThis::StateFunc);

    auto request = dynamic_cast<const ReadInfoRequest*>(GetProtoRequest());
    Y_VERIFY(request);
    ClientId = NPersQueue::ConvertNewConsumerName(request->consumer().path(), ctx);

    bool readOnlyLocal = request->get_only_original();

    TIntrusivePtr<NACLib::TUserToken> token;
    if (Request_->GetInternalToken().empty()) {
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            AnswerError("Unauthenticated access is forbidden, please provide credentials", PersQueue::ErrorCode::ACCESS_DENIED, ctx);
            return;
        }
    } else {
        token = new NACLib::TUserToken(Request_->GetInternalToken());
    }

    THashSet<TString> topicsToResolve;

    for (auto& t : request->topics()) {
        if (t.path().empty()) {
            AnswerError("empty topic in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        topicsToResolve.insert(t.path());
    }

    AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
            ctx, ctx.SelfID, ClientId, 0, TString("read_info:") + Request().GetPeerName(),
            SchemeCache, NewSchemeCache, Counters, token,
            TopicsHandler.GetReadTopicsList(topicsToResolve, readOnlyLocal, Request().GetDatabaseName().GetOrElse(TString())),
            TopicsHandler.GetLocalCluster()
    ));
}


void TReadInfoActor::Die(const TActorContext& ctx) {

    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());

    TActorBootstrapped<TReadInfoActor>::Die(ctx);
}


void TReadInfoActor::Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "GetReadInfo auth ok fo read info, got " << ev->Get()->TopicAndTablets.size() << " topics");
    TopicAndTablets = std::move(ev->Get()->TopicAndTablets);
    if (TopicAndTablets.empty()) {
        AnswerError("empty list of topics", PersQueue::ErrorCode::UNKNOWN_TOPIC, ctx);
        return;
    }

    NKikimrClient::TPersQueueRequest proto;
    proto.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->SetClientId(ClientId);
    for (auto& t : TopicAndTablets) {
        proto.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->AddTopic(t.TopicNameConverter->GetClientsideName());
    }

    ctx.Register(NMsgBusProxy::CreateActorServerPersQueue(
        ctx.SelfID,
        proto,
        SchemeCache
    ));

}


void TReadInfoActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Record.GetStatus() != MSTATUS_OK) {
        return AnswerError(ev->Get()->Record.GetErrorReason(), PersQueue::ErrorCode::ERROR, ctx);
    }

    // Convert to correct response.

    ReadInfoResult result;

    const auto& resp = ev->Get()->Record;
    Y_VERIFY(resp.HasMetaResponse());

    Y_VERIFY(resp.GetMetaResponse().GetCmdGetReadSessionsInfoResult().TopicResultSize() == TopicAndTablets.size());
    TMap<std::pair<TString, ui64>, ReadInfoResult::TopicInfo::PartitionInfo*> partResultMap;
    for (auto& tt : resp.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetTopicResult()) {
        auto topicRes = result.add_topics();
        topicRes->mutable_topic()->set_path(NPersQueue::GetTopicPath(tt.GetTopic()));
        topicRes->set_cluster(NPersQueue::GetDC(tt.GetTopic()));
        topicRes->set_status(ConvertPersQueueInternalCodeToStatus(ConvertOldCode(tt.GetErrorCode())));
        if (tt.GetErrorCode() != NPersQueue::NErrorCode::OK)
            FillIssue(topicRes->add_issues(), ConvertOldCode(tt.GetErrorCode()), tt.GetErrorReason());

        for (auto& pp : tt.GetPartitionResult()) {
            auto partRes = topicRes->add_partitions();

            partRes->set_partition(pp.GetPartition());
            partRes->set_status(ConvertPersQueueInternalCodeToStatus(ConvertOldCode(pp.GetErrorCode())));
            if (pp.GetErrorCode() != NPersQueue::NErrorCode::OK)
                FillIssue(partRes->add_issues(), ConvertOldCode(pp.GetErrorCode()), pp.GetErrorReason());

            partRes->set_start_offset(pp.GetStartOffset());
            partRes->set_end_offset(pp.GetEndOffset());

            partRes->set_commit_offset(pp.GetClientOffset());
            partRes->set_commit_time_lag_ms(pp.GetTimeLag());

            partRes->set_read_offset(pp.GetClientReadOffset());
            partRes->set_read_time_lag_ms(pp.GetReadTimeLag());

            partRes->set_session_id(pp.GetSession()); //TODO: fill error when no session returned result

            partRes->set_client_node(pp.GetClientNode());
            partRes->set_proxy_node(pp.GetProxyNode());
            partRes->set_tablet_node(pp.GetTabletNode());
            partResultMap[std::make_pair<TString, ui64>(TString(tt.GetTopic()), pp.GetPartition())] = partRes;
        }
    }
    for (auto& ss : resp.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetSessionResult()) {
        for (auto& pr : ss.GetPartitionResult()) {
            auto it = partResultMap.find(std::make_pair<TString, ui64>(TString(pr.GetTopic()), pr.GetPartition()));
            if (it == partResultMap.end())
                continue;
            auto sesRes = it->second;
            sesRes->set_session_id(ss.GetSession());
            sesRes->set_status(ConvertPersQueueInternalCodeToStatus(ConvertOldCode(ss.GetErrorCode())));
            if (ss.GetErrorCode() != NPersQueue::NErrorCode::OK) //TODO: what if this is result for already dead session?
                FillIssue(sesRes->add_issues(), ConvertOldCode(ss.GetErrorCode()), ss.GetErrorReason());

            for (auto& nc : pr.GetNextCommits()) {
                sesRes->add_out_of_order_read_cookies_to_commit(nc);
            }
            sesRes->set_last_read_cookie(pr.GetLastReadId());
            sesRes->set_committed_read_cookie(pr.GetReadIdCommitted());
            sesRes->set_assign_timestamp_ms(pr.GetTimestamp());

            sesRes->set_client_node(ss.GetClientNode());
            sesRes->set_proxy_node(ss.GetProxyNode());
        }
    }
    Request().SendResult(result, Ydb::StatusIds::SUCCESS);
    Die(ctx);
}

void FillIssue(Ydb::Issue::IssueMessage* issue, const PersQueue::ErrorCode::ErrorCode errorCode, const TString& errorReason) {
    issue->set_message(errorReason);
    issue->set_severity(NYql::TSeverityIds::S_ERROR);
    issue->set_issue_code(errorCode);
}

PersQueue::ErrorCode::ErrorCode ConvertOldCode(const NPersQueue::NErrorCode::EErrorCode code)
{
    if (code == NPersQueue::NErrorCode::OK)
        return PersQueue::ErrorCode::OK;
    return PersQueue::ErrorCode::ErrorCode(code + 500000);
}


void TReadInfoActor::AnswerError(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {

    ReadInfoResponse response;
    response.mutable_operation()->set_ready(true);
    auto issue = response.mutable_operation()->add_issues();
    FillIssue(issue, errorCode, errorReason);
    response.mutable_operation()->set_status(ConvertPersQueueInternalCodeToStatus(errorCode));
    Reply(ConvertPersQueueInternalCodeToStatus(errorCode), response.operation().issues(), ctx);
}


void TReadInfoActor::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    AnswerError(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}

} // namespace NGRpcProxy::V1
} // namespace NKikimr
