#include "read_balancer.h"
#include "read_balancer__balancing.h"
#include "read_balancer__metrics.h"
#include "read_balancer__mlp_balancing.h"
#include "read_balancer__txpreinit.h"
#include "read_balancer__txwrite.h"
#include "read_balancer_log.h"
#include "mirror_describer_factory.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/tablet/tablet_exception.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/random_provider/random_provider.h>

#define PQ_ENSURE(condition) AFL_ENSURE(condition)("tablet_id", TabletID())("path", Path)("topic", Topic)

namespace NKikimr {
namespace NPQ {

using namespace NBalancing;


TString EncodeAnchor(const TString& v) {
    auto r = Base64Encode(v);
    while (r.EndsWith('=')) {
        r.resize(r.size() - 1);
    }
    return r;
}

TPersQueueReadBalancer::TPersQueueReadBalancer(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , Inited(false)
        , PathId(0)
        , Generation(0)
        , Version(-1)
        , MaxPartsPerTablet(0)
        , SchemeShardId(0)
        , TxId(0)
        , NumActiveParts(0)
        , MaxIdx(0)
        , NextPartitionId(0)
        , NextPartitionIdForWrite(0)
        , StartPartitionIdForWrite(0)
        , TotalGroups(0)
        , ResourceMetrics(nullptr)
        , TopicMetricsHandler(std::make_unique<TTopicMetricsHandler>())
        , StatsReportRound(0)
    {
        Balancer = std::make_unique<TBalancer>(*this);
        MLPBalancer = std::make_unique<TMLPBalancer>(*this);
    }

struct TPersQueueReadBalancer::TTxWritePartitionStats : public ITransaction {
    TPersQueueReadBalancer * const Self;

    TTxWritePartitionStats(TPersQueueReadBalancer *self)
        : Self(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Self->TTxWritePartitionStatsScheduled = false;

        auto& metrics = Self->TopicMetricsHandler->GetPartitionMetrics();

        NIceDb::TNiceDb db(txc.DB);
        for (auto& [partition, stats] : metrics) {
            auto it = Self->PartitionsInfo.find(partition);
            if (it == Self->PartitionsInfo.end()) {
                continue;
            }

            db.Table<Schema::Partitions>().Key(partition).Update(
                NIceDb::TUpdate<Schema::Partitions::DataSize>(stats.DataSize),
                NIceDb::TUpdate<Schema::Partitions::UsedReserveSize>(stats.UsedReserveSize)
            );
        }

        return true;
    }

    void Complete(const TActorContext&) override {};
};

void TPersQueueReadBalancer::Die(const TActorContext& ctx) {
    StopFindSubDomainPathId();
    StopWatchingSubDomainPathId();

    for (auto& pipe : TabletPipes) {
        NTabletPipe::CloseClient(ctx, pipe.second.PipeActor);
    }
    TabletPipes.clear();
    if (PartitionsScaleManager) {
        PartitionsScaleManager->Die(ctx);
    }
    if (MirrorTopicDescriberActorId) {
        ctx.Send(MirrorTopicDescriberActorId, new TEvents::TEvPoisonPill());
    }
    TActor<TPersQueueReadBalancer>::Die(ctx);
}

void TPersQueueReadBalancer::OnActivateExecutor(const TActorContext &ctx) {
    ResourceMetrics = Executor()->GetResourceMetrics();
    Become(&TThis::StateWork);
    if (Executor()->GetStats().IsFollower())
        Y_ABORT("is follower works well with Balancer?");
    else
        Execute(new TTxPreInit(this), ctx);
}

void TPersQueueReadBalancer::OnDetach(const TActorContext &ctx) {
    Die(ctx);
}

void TPersQueueReadBalancer::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext &ctx) {
    Die(ctx);
}

void TPersQueueReadBalancer::DefaultSignalTabletActive(const TActorContext &) {
    // must be empty
}

void TPersQueueReadBalancer::InitDone(const TActorContext &ctx) {
    if (SubDomainPathId) {
        StartWatchingSubDomainPathId();
    } else {
        StartFindSubDomainPathId(true);
    }

    StartPartitionIdForWrite = NextPartitionIdForWrite = rand() % TotalGroups;

    auto getInitLog = [&]() {
        TStringBuilder s;
        s << "BALANCER INIT DONE for " << Topic << ": ";
        for (auto& p : PartitionsInfo) {
            s << "(" << p.first << ", " << p.second.TabletId << ") ";
        }
        return s;
    };
    PQ_LOG_D(getInitLog());

    for (auto &ev : UpdateEvents) {
        ctx.Send(ctx.SelfID, ev.Release());
    }
    UpdateEvents.clear();

    for (auto &ev : RegisterEvents) {
        ctx.Send(ctx.SelfID, ev.Release());
    }
    RegisterEvents.clear();

    auto wakeupInterval = std::max<ui64>(AppData(ctx)->PQConfig.GetBalancerWakeupIntervalSec(), 1);
    ctx.Schedule(TDuration::Seconds(wakeupInterval), new TEvents::TEvWakeup());
}

void TPersQueueReadBalancer::HandleWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext &ctx) {
    PQ_LOG_D("TPersQueueReadBalancer::HandleWakeup");

    switch (ev->Get()->Tag) {
        case TPartitionScaleManager::TRY_SCALE_REQUEST_WAKE_UP_TAG: {
            if (PartitionsScaleManager && SplitMergeEnabled(TabletConfig)) {
                PartitionsScaleManager->TrySendScaleRequest(ctx);
            }
            break;
        }
        default: {
            GetStat(ctx); //TODO: do it only on signals from outerspace right now
            auto wakeupInterval = std::max<ui64>(AppData(ctx)->PQConfig.GetBalancerWakeupIntervalSec(), 1);
            ctx.Schedule(TDuration::Seconds(wakeupInterval), new TEvents::TEvWakeup());
        }
    }
}

void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext&) {

    UpdateEvents.push_back(ev->Release().Release());
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetPartitionIdForWrite::TPtr &ev, const TActorContext &ctx) {
    NextPartitionIdForWrite = (NextPartitionIdForWrite + 1) % TotalGroups; //TODO: change here when there will be more than 1 partition in partition_group.
    THolder<TEvPersQueue::TEvGetPartitionIdForWriteResponse> response = MakeHolder<TEvPersQueue::TEvGetPartitionIdForWriteResponse>();
    response->Record.SetPartitionId(NextPartitionIdForWrite);
    ctx.Send(ev->Sender, response.Release());
    if (NextPartitionIdForWrite == StartPartitionIdForWrite) { // randomize next cycle
        StartPartitionIdForWrite = NextPartitionIdForWrite = rand() % TotalGroups;
    }
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    if ((int)record.GetVersion() < Version && Inited) {
        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR_BAD_VERSION);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(ev->Sender, res.Release());
        return;
    }

    if ((int)record.GetVersion() == Version) {
        if (!WaitingResponse.empty()) { //got transaction infly
            WaitingResponse.push_back(ev->Sender);
        } else { //version already applied
            PQ_LOG_D("BALANCER Topic " << Topic << "Tablet " << TabletID()
                    << " Config already applied version " << record.GetVersion() << " actor " << ev->Sender
                    << " txId " << record.GetTxId());
            THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
            res->Record.SetStatus(NKikimrPQ::OK);
            res->Record.SetTxId(record.GetTxId());
            res->Record.SetOrigin(TabletID());
            ctx.Send(ev->Sender, res.Release());
        }
        return;
    }

    if ((int)record.GetVersion() > Version && !WaitingResponse.empty()) { //old transaction is not done yet
        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR_UPDATE_IN_PROGRESS);
        res->Record.SetTxId(ev->Get()->Record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(ev->Sender, res.Release());
        return;
    }
    WaitingResponse.push_back(ev->Sender);

    Version = record.GetVersion();
    MaxPartsPerTablet = record.GetPartitionPerTablet();
    PathId = record.GetPathId();
    Topic = std::move(record.GetTopicName());
    Path = std::move(record.GetPath());
    TxId = record.GetTxId();
    TabletConfig = std::move(record.GetTabletConfig());

    if (!TabletConfig.GetAllPartitions().size()) {
        for (auto& p : record.GetPartitions()) {
            auto* ap = TabletConfig.AddAllPartitions();
            ap->SetPartitionId(p.GetPartition());
            ap->SetTabletId(p.GetTabletId());
            ap->SetCreateVersion(p.GetCreateVersion());
            if (p.HasKeyRange()) {
                ap->MutableKeyRange()->CopyFrom(p.GetKeyRange());
            }
            ap->SetStatus(p.GetStatus());
            ap->MutableParentPartitionIds()->Reserve(p.GetParentPartitionIds().size());
            for (const auto parent : p.GetParentPartitionIds()) {
                ap->MutableParentPartitionIds()->AddAlreadyReserved(parent);
            }
            ap->MutableChildPartitionIds()->Reserve(p.GetChildPartitionIds().size());
            for (const auto children : p.GetChildPartitionIds()) {
                ap->MutableChildPartitionIds()->AddAlreadyReserved(children);
            }
        }
    }

    Migrate(TabletConfig);

    SchemeShardId = record.GetSchemeShardId();
    TotalGroups = record.HasTotalGroupCount() ? record.GetTotalGroupCount() : 0;

    ui32 prevNextPartitionId = NextPartitionId;
    NextPartitionId = record.HasNextPartitionId() ? record.GetNextPartitionId() : 0;

    if (record.HasSubDomainPathId()) {
        SubDomainPathId.emplace(record.GetSchemeShardId(), record.GetSubDomainPathId());
    }

    PartitionGraph = MakePartitionGraph(record);
    UpdateActivePartitions();

    std::vector<std::pair<ui64, TTabletInfo>> newTablets;
    std::vector<std::pair<ui32, ui32>> newGroups;
    std::vector<std::pair<ui64, TTabletInfo>> reallocatedTablets;

    if (SplitMergeEnabled(TabletConfig)) {
        if (!PartitionsScaleManager) {
            PartitionsScaleManager = std::make_unique<TPartitionScaleManager>(Topic, Path, DatabaseInfo.DatabasePath, PathId, Version, TabletConfig, PartitionGraph);
        } else {
            PartitionsScaleManager->UpdateBalancerConfig(PathId, Version, TabletConfig);
        }
    }
    if (SplitMergeEnabled(TabletConfig) && MirroringEnabled(TabletConfig) && AppData(ctx)->FeatureFlags.GetEnableMirroredTopicSplitMerge()) {
        if (MirrorTopicDescriberActorId) {
            ctx.Send(MirrorTopicDescriberActorId, new TEvPQ::TEvChangePartitionConfig(nullptr, TabletConfig));
        } else {
            MirrorTopicDescriberActorId = ctx.Register(CreateMirrorDescriber(TabletID(), SelfId(), Topic, TabletConfig.GetPartitionConfig().GetMirrorFrom()));
        }
    } else {
        if (MirrorTopicDescriberActorId) {
            ctx.Send(MirrorTopicDescriberActorId, new TEvents::TEvPoisonPill());
            MirrorTopicDescriberActorId = TActorId();
        }
    }

    for (auto& p : record.GetTablets()) {
        auto it = TabletsInfo.find(p.GetTabletId());
        if (it == TabletsInfo.end()) {
            TTabletInfo info{p.GetOwner(), p.GetIdx()};
            TabletsInfo[p.GetTabletId()] = info;
            newTablets.push_back(std::make_pair(p.GetTabletId(), info));
        } else {
            if (it->second.Owner != p.GetOwner() || it->second.Idx != p.GetIdx()) {
                TTabletInfo info{p.GetOwner(), p.GetIdx()};
                TabletsInfo[p.GetTabletId()] = info;
                reallocatedTablets.push_back(std::make_pair(p.GetTabletId(), info));
            }
        }

    }

    std::map<ui32, TPartitionInfo> partitionsInfo;
    std::vector<TPartInfo> newPartitions;
    std::vector<ui32> newPartitionsIds;
    for (auto& p : record.GetPartitions()) {
        auto it = PartitionsInfo.find(p.GetPartition());
        if (it == PartitionsInfo.end()) {
            PQ_ENSURE((p.GetPartition() >= prevNextPartitionId && p.GetPartition() < NextPartitionId) || NextPartitionId == 0);

            partitionsInfo[p.GetPartition()] = {p.GetTabletId()};

            newPartitionsIds.push_back(p.GetPartition());
            newPartitions.push_back(TPartInfo{p.GetPartition(), p.GetTabletId(), 0, p.GetKeyRange()});

            ++NumActiveParts;

            // for back compatibility. Remove it after 24-3
            newGroups.push_back({p.GetGroup(), p.GetPartition()});
        } else { //group is already defined
            partitionsInfo[p.GetPartition()] = it->second;
        }
    }

    if (TotalGroups == 0) {
        NextPartitionId = TotalGroups = partitionsInfo.size(); // this will not work when we support the deletion of the partition
    }

    std::vector<ui32> deletedPartitions;
    for (auto& p : PartitionsInfo) {
        if (partitionsInfo.find(p.first) == partitionsInfo.end()) {
            Y_ABORT("deleting of partitions is not fully supported yet");
            deletedPartitions.push_back(p.first);
        }
    }
    PartitionsInfo = std::unordered_map<ui32, TPartitionInfo>(partitionsInfo.rbegin(), partitionsInfo.rend());

    Balancer->UpdateConfig(newPartitionsIds, deletedPartitions, ctx);
    MLPBalancer->UpdateConfig(newPartitionsIds);

    Execute(new TTxWrite(this, std::move(deletedPartitions), std::move(newPartitions), std::move(newTablets), std::move(newGroups), std::move(reallocatedTablets)), ctx);

    if (SubDomainPathId && (!WatchingSubDomainPathId || *WatchingSubDomainPathId != *SubDomainPathId)) {
        StartWatchingSubDomainPathId();
    }

    UpdateConfigCounters();
}


TStringBuilder TPersQueueReadBalancer::LogPrefix() const {
    return TStringBuilder() << "[" << TabletID() << "][" << Topic << "] ";
}


void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx)
{
    auto tabletId = ev->Get()->TabletId;
    PQ_LOG_D("TEvClientDestroyed " << tabletId);

    ClosePipe(tabletId, ctx);
    RequestTabletIfNeeded(tabletId, ctx, true);
}


void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx)
{
    auto tabletId = ev->Get()->TabletId;

    PipesRequested.erase(tabletId);

    if (ev->Get()->Status != NKikimrProto::OK) {
        ClosePipe(ev->Get()->TabletId, ctx);
        RequestTabletIfNeeded(ev->Get()->TabletId, ctx, true);

        PQ_LOG_ERROR("TEvClientConnected Status " << ev->Get()->Status << ", TabletId " << tabletId);
        return;
    }

    Y_VERIFY_DEBUG_S(ev->Get()->Generation, "Tablet generation should be greater than 0");

    auto it = TabletPipes.find(tabletId);
    if (it != TabletPipes.end()) {
        it->second.Generation = ev->Get()->Generation;
        it->second.NodeId = ev->Get()->ServerId.NodeId();

        PQ_LOG_D("TEvClientConnected TabletId " << tabletId << ", NodeId " << ev->Get()->ServerId.NodeId() << ", Generation " << ev->Get()->Generation);
    }
    else
        PQ_LOG_I("TEvClientConnected Pipe is not found, TabletId " << tabletId);
}

void TPersQueueReadBalancer::ClosePipe(const ui64 tabletId, const TActorContext& ctx)
{
    auto it = TabletPipes.find(tabletId);
    if (it != TabletPipes.end()) {
        NTabletPipe::CloseClient(ctx, it->second.PipeActor);
        TabletPipes.erase(it);
        PipesRequested.erase(tabletId);
    }
}

TActorId TPersQueueReadBalancer::GetPipeClient(const ui64 tabletId, const TActorContext& ctx) {
    TActorId pipeClient;

    auto it = TabletPipes.find(tabletId);
    if (it == TabletPipes.end()) {
        NTabletPipe::TClientConfig clientConfig(NTabletPipe::TClientRetryPolicy::WithRetries());
        pipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
        TabletPipes[tabletId].PipeActor = pipeClient;
        auto res = PipesRequested.insert(tabletId);
        PQ_ENSURE(res.second);
    } else {
        pipeClient = it->second.PipeActor;
    }

    return pipeClient;
}

void TPersQueueReadBalancer::RequestTabletIfNeeded(const ui64 tabletId, const TActorContext& ctx, bool pipeReconnected)
{
    TActorId pipeClient = GetPipeClient(tabletId, ctx);

    if (SchemeShardId != tabletId) {
        NTabletPipe::SendData(ctx, pipeClient, new TEvPQ::TEvSubDomainStatus(SubDomainOutOfSpace));
    }

    auto it = StatsRequestTracker.Cookies.find(tabletId);
    if (!pipeReconnected || it != StatsRequestTracker.Cookies.end()) {
        ui64 cookie;
        if (pipeReconnected) {
            cookie = it->second;
        } else {
            cookie = ++StatsRequestTracker.NextCookie;
            StatsRequestTracker.Cookies[tabletId] = cookie;
        }

        PQ_LOG_D("Send TEvPersQueue::TEvStatus TabletId: " << tabletId << " Cookie: " << cookie);
        NTabletPipe::SendData(ctx, pipeClient, new TEvPersQueue::TEvStatus("", true), cookie);
    }
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    ui64 tabletId = record.GetTabletId();
    ui64 cookie = ev->Cookie;

    if ((0 != cookie && cookie != StatsRequestTracker.Cookies[tabletId]) || (0 == cookie && !StatsRequestTracker.Cookies.contains(tabletId))) {
        return;
    }

    StatsRequestTracker.Cookies.erase(tabletId);

    Balancer->Handle(ev, ctx);
    MLPBalancer->Handle(ev, ctx);

    for (auto& partRes : *record.MutablePartResult()) {
        ui32 partitionId = partRes.GetPartition();
        if (!PartitionsInfo.contains(partitionId)) {
            continue;
        }

        if (SplitMergeEnabled(TabletConfig) && PartitionsScaleManager) {
            PartitionsScaleManager->HandleScaleStatusChange(
                partitionId,
                partRes.GetScaleStatus(),
                partRes.HasScaleParticipatingPartitions() ? MakeMaybe(partRes.GetScaleParticipatingPartitions()) : Nothing(),
                partRes.HasSplitBoundary() ? MakeMaybe(partRes.GetSplitBoundary()) : Nothing(),
                ctx
            );
        }

        TopicMetricsHandler->Handle(std::move(partRes));
    }

    if (StatsRequestTracker.Cookies.empty()) {
        StatsRequestTracker.StatsReceived = true;

        CheckStat(ctx);
        Balancer->ProcessPendingStats(ctx);
        ProcessPendingMLPGetPartitionRequests(ctx);
    }
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvStatsWakeup::TPtr& ev, const TActorContext& ctx) {
    if (StatsRequestTracker.Round != ev->Get()->Round) {
        // old message
        return;
    }

    if (StatsRequestTracker.Cookies.empty()) {
        return;
    }

    CheckStat(ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext&) {
    Send(ev.Get()->Sender, GetStatsEvent());
}

void TPersQueueReadBalancer::CheckStat(const TActorContext& ctx) {
    Y_UNUSED(ctx);
    //TODO: Decide about changing number of partitions and send request to SchemeShard
    //TODO: make AlterTopic request via TX_PROXY

    if (!TTxWritePartitionStatsScheduled) {
        TTxWritePartitionStatsScheduled = true;
        Execute(new TTxWritePartitionStats(this));
    }

    UpdateCounters(ctx);

    TEvPersQueue::TEvPeriodicTopicStats* ev = GetStatsEvent();
    PQ_LOG_D("Send TEvPeriodicTopicStats PathId: " << PathId
            << " Generation: " << Generation
            << " StatsReportRound: " << StatsReportRound
            << " DataSize: " << TopicMetricsHandler->GetTopicMetrics().TotalDataSize
            << " UsedReserveSize: " << TopicMetricsHandler->GetTopicMetrics().TotalUsedReserveSize);

    NTabletPipe::SendData(ctx, GetPipeClient(SchemeShardId, ctx), ev);

}

void TPersQueueReadBalancer::InitCounters(const TActorContext& ctx) {
    if (DatabaseInfo.DatabasePath.empty()) {
        return;
    }

    TopicMetricsHandler->Initialize(TabletConfig, DatabaseInfo, Path, ctx);
}

void TPersQueueReadBalancer::UpdateConfigCounters() {
    TopicMetricsHandler->UpdateConfig(TabletConfig, DatabaseInfo, Path, ActorContext());
}

void TPersQueueReadBalancer::UpdateCounters(const TActorContext&) {
    TopicMetricsHandler->UpdateMetrics();
}

TEvPersQueue::TEvPeriodicTopicStats* TPersQueueReadBalancer::GetStatsEvent() {
    auto& metrics = TopicMetricsHandler->GetTopicMetrics();

    TEvPersQueue::TEvPeriodicTopicStats* ev = new TEvPersQueue::TEvPeriodicTopicStats();
    auto& rec = ev->Record;
    rec.SetPathId(PathId);
    rec.SetGeneration(Generation);

    rec.SetRound(++StatsReportRound);
    rec.SetDataSize(metrics.TotalDataSize);
    rec.SetUsedReserveSize(metrics.TotalUsedReserveSize);
    rec.SetSubDomainOutOfSpace(SubDomainOutOfSpace);

    return ev;
}

void TPersQueueReadBalancer::GetStat(const TActorContext& ctx) {
    if (!StatsRequestTracker.Cookies.empty()) {
        StatsRequestTracker.Cookies.clear();
        CheckStat(ctx);
    }

    for (auto& p : PartitionsInfo) {
        const ui64& tabletId = p.second.TabletId;
        if (StatsRequestTracker.Cookies.contains(tabletId)) { //already asked stat
            continue;
        }
        RequestTabletIfNeeded(tabletId, ctx);
    }

    // TEvStatsWakeup must processed before next TEvWakeup, which send next status request to TPersQueue
    const auto& config = AppData(ctx)->PQConfig;
    auto wakeupInterval = std::max<ui64>(config.GetBalancerWakeupIntervalSec(), 1);
    auto stateWakeupInterval = std::max<ui64>(config.GetBalancerStatsWakeupIntervalSec(), 1);
    ui64 delayMs = std::min(stateWakeupInterval * 1000, wakeupInterval * 500);
    if (0 < delayMs) {
        Schedule(TDuration::MilliSeconds(delayMs), new TEvPQ::TEvStatsWakeup(++StatsRequestTracker.Round));
    }
}

void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx) {
    auto* evResponse = new TEvPersQueue::TEvGetPartitionsLocationResponse();
    evResponse->Record.SetStatus(false);
    ctx.Send(ev->Sender, evResponse);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx) {
    const auto& request = ev->Get()->Record;
    auto evResponse = std::make_unique<TEvPersQueue::TEvGetPartitionsLocationResponse>();

    auto addPartitionToResponse = [&](ui64 partitionId, ui64 tabletId) {
        if (PipesRequested.contains(tabletId)) {
            return false;
        }
        auto iter = TabletPipes.find(tabletId);
        if (iter == TabletPipes.end()) {
            GetPipeClient(tabletId, ctx);
            return false;
        }

        auto* pResponse = evResponse->Record.AddLocations();
        pResponse->SetPartitionId(partitionId);
        pResponse->SetNodeId(iter->second.NodeId.GetRef());
        pResponse->SetGeneration(iter->second.Generation.GetRef());

        PQ_LOG_D("The partition location was added to response: TabletId " << tabletId << ", PartitionId " << partitionId
                << ", NodeId " << pResponse->GetNodeId() << ", Generation " << pResponse->GetGeneration());

        return true;
    };

    auto sendError = [&]() {
        auto response = std::make_unique<TEvPersQueue::TEvGetPartitionsLocationResponse>();
        response->Record.SetStatus(false);
        ctx.Send(ev->Sender, response.release());
    };

    if (request.PartitionsSize() == 0) {
        if (!PipesRequested.empty() || TabletPipes.size() < TabletsInfo.size()) {
            // Do not have all pipes connected.
            return sendError();
        }
        for (const auto& [partitionId, partitionInfo] : PartitionsInfo) {
            if (!addPartitionToResponse(partitionId, partitionInfo.TabletId)) {
                return sendError();
            }
        }
    } else {
        for (const auto& partitionInRequest : request.GetPartitions()) {
            auto partitionInfoIter = PartitionsInfo.find(partitionInRequest);
            if (partitionInfoIter == PartitionsInfo.end()) {
                return sendError();
            }
            if (!addPartitionToResponse(partitionInRequest, partitionInfoIter->second.TabletId)) {
                return sendError();
            }
        }
    }

    evResponse->Record.SetStatus(true);
    ctx.Send(ev->Sender, evResponse.release());
}




//
// Watching PQConfig
//

struct TTxWriteSubDomainPathId : public ITransaction {
    TPersQueueReadBalancer* const Self;

    TTxWriteSubDomainPathId(TPersQueueReadBalancer* self)
        : Self(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) {
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Data>().Key(1).Update(
            NIceDb::TUpdate<Schema::Data::SubDomainPathId>(Self->SubDomainPathId->LocalPathId));
        return true;
    }

    void Complete(const TActorContext&) {
    }
};

static constexpr TDuration MaxFindSubDomainPathIdDelay = TDuration::Minutes(1);

void TPersQueueReadBalancer::StopFindSubDomainPathId() {
    if (FindSubDomainPathIdActor) {
        Send(FindSubDomainPathIdActor, new TEvents::TEvPoison);
        FindSubDomainPathIdActor = { };
    }
}

void TPersQueueReadBalancer::StartFindSubDomainPathId(bool delayFirstRequest) {
    if (!FindSubDomainPathIdActor &&
        SchemeShardId != 0 &&
        (!SubDomainPathId || SubDomainPathId->OwnerId != SchemeShardId))
    {
        FindSubDomainPathIdActor = Register(CreateFindSubDomainPathIdActor(SelfId(), TabletID(), SchemeShardId, delayFirstRequest, MaxFindSubDomainPathIdDelay));
    }
}

void TPersQueueReadBalancer::Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    if (FindSubDomainPathIdActor == ev->Sender) {
        FindSubDomainPathIdActor = { };
    }

    if (SchemeShardId == msg->SchemeShardId &&
       (!SubDomainPathId || SubDomainPathId->OwnerId != msg->SchemeShardId))
    {
        PQ_LOG_D("Discovered subdomain " << msg->LocalPathId << " at RB " << TabletID());

        SubDomainPathId.emplace(msg->SchemeShardId, msg->LocalPathId);
        Execute(new TTxWriteSubDomainPathId(this), ctx);
        StartWatchingSubDomainPathId();
    }
}

void TPersQueueReadBalancer::StopWatchingSubDomainPathId() {
    if (WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        WatchingSubDomainPathId.reset();
    }
}

void TPersQueueReadBalancer::StartWatchingSubDomainPathId() {
    if (!SubDomainPathId || SubDomainPathId->OwnerId != SchemeShardId) {
        return;
    }

    if (WatchingSubDomainPathId && *WatchingSubDomainPathId != *SubDomainPathId) {
        StopWatchingSubDomainPathId();
    }

    if (!WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(*SubDomainPathId));
        WatchingSubDomainPathId = *SubDomainPathId;
    }
}

void TPersQueueReadBalancer::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    if (DatabaseInfo.DatabasePath.empty()) {
        DatabaseInfo.DatabasePath = msg->Result->GetPath();
        for (const auto& attr : msg->Result->GetPathDescription().GetUserAttributes()) {
            if (attr.GetKey() == "folder_id") DatabaseInfo.FolderId = attr.GetValue();
            if (attr.GetKey() == "cloud_id") DatabaseInfo.CloudId = attr.GetValue();
            if (attr.GetKey() == "database_id") DatabaseInfo.DatabaseId = attr.GetValue();
        }

        InitCounters(ctx);
        UpdateConfigCounters();
    }

    if (PartitionsScaleManager) {
        PartitionsScaleManager->UpdateDatabasePath(DatabaseInfo.DatabasePath);
    }

    if (SubDomainPathId && msg->PathId == *SubDomainPathId) {
        const bool outOfSpace = msg->Result->GetPathDescription()
            .GetDomainDescription()
            .GetDomainState()
            .GetDiskQuotaExceeded();

        PQ_LOG_D("Discovered subdomain " << msg->PathId << " state, outOfSpace = " << outOfSpace
                << " at RB " << TabletID());

        SubDomainOutOfSpace = outOfSpace;

        for (auto& p : PartitionsInfo) {
            const ui64& tabletId = p.second.TabletId;
            TActorId pipeClient = GetPipeClient(tabletId, ctx);
            NTabletPipe::SendData(ctx, pipeClient, new TEvPQ::TEvSubDomainStatus(outOfSpace));
        }
    }
}

void TPersQueueReadBalancer::UpdateActivePartitions() {
    ActivePartitions.clear();
    for (const auto& partition : TabletConfig.GetAllPartitions()) {
        if (partition.GetStatus() == NKikimrPQ::ETopicPartitionStatus::Active) {
            ActivePartitions.push_back(partition.GetPartitionId());
        }
    }
}


//
// Balancing
//

void TPersQueueReadBalancer::Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
    MLPBalancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvReadingPartitionStartedRequest::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvBalanceConsumer::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext&)
{
    RegisterEvents.push_back(ev->Release().Release());
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvMLPConsumerStatus::TPtr& ev)
{
    PQ_LOG_D("Handle TEvPQ::TEvMLPConsumerStatus " << ev->Get()->Record.ShortDebugString());
    MLPBalancer->Handle(ev);
}



//
// Kafka integration
//

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvBalancingSubscribe::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvBalancingUnsubscribe::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}



//
// Autoscaling
//

void TPersQueueReadBalancer::Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx) {
    if (!SplitMergeEnabled(TabletConfig)) {
        PQ_LOG_D("Skip TEvPartitionScaleStatusChanged: autopartitioning disabled.");
        return;
    }
    auto& record = ev->Get()->Record;
    auto* node = PartitionGraph.GetPartition(record.GetPartitionId());
    if (!node) {
        PQ_LOG_D("Skip TEvPartitionScaleStatusChanged: partition " << record.GetPartitionId() << " not found.");
        return;
    }

    if (PartitionsScaleManager) {
        PartitionsScaleManager->HandleScaleStatusChange(
            record.GetPartitionId(),
            record.GetScaleStatus(),
            record.HasParticipatingPartitions() ? MakeMaybe(record.GetParticipatingPartitions()) : Nothing(),
            record.HasSplitBoundary() ? MakeMaybe(record.GetSplitBoundary()) : Nothing(),
            ctx
        );
    } else {
        PQ_LOG_NOTICE("Skip TEvPartitionScaleStatusChanged: scale manager isn`t initialized.");
    }
}

void TPersQueueReadBalancer::Handle(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx) {
    if (!SplitMergeEnabled(TabletConfig)) {
        return;
    }
    if (PartitionsScaleManager) {
        PartitionsScaleManager->HandleScaleRequestResult(ev, ctx);
    }
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_D("Received TEvMirrorTopicDescription");
    if (!MirroringEnabled(TabletConfig)) {
        return;
    }
    if (PartitionsScaleManager) {
        auto result = PartitionsScaleManager->HandleMirrorTopicDescriptionResult(ev, ctx);
        if (!result.has_value()) {
            BroadcastPartitionError(std::move(result).error(), NKikimrServices::EServiceKikimr::PQ_MIRROR_DESCRIBER, ctx);
        }
    }
}

void TPersQueueReadBalancer::BroadcastPartitionError(const TString& message, const NKikimrServices::EServiceKikimr service, const TActorContext& ctx) {
    const TInstant now = TInstant::Now();
    for (const auto& [_, pipeLocation] : TabletPipes) {
        THolder<TEvPQ::TBroadcastPartitionError> ev{new TEvPQ::TBroadcastPartitionError(message, service, now)};
        NTabletPipe::SendData(ctx, pipeLocation.PipeActor, ev.Release());
    }
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvMLPGetPartitionRequest::TPtr& ev) {
    if (StatsRequestTracker.StatsReceived) {
        return MLPBalancer->Handle(ev);
    }

    PendingMLPGetPartitionRequests.push_back(std::move(ev));
}

void TPersQueueReadBalancer::ProcessPendingMLPGetPartitionRequests(const TActorContext&) {
    while (!PendingMLPGetPartitionRequests.empty()) {
        auto ev = std::move(PendingMLPGetPartitionRequests.front());
        PendingMLPGetPartitionRequests.pop_front();
        MLPBalancer->Handle(ev);
    }

    if (!PendingMLPGetPartitionRequests.empty()) {
        std::exchange(PendingMLPGetPartitionRequests, {});
    }
}

STFUNC(TPersQueueReadBalancer::StateInit) {
    auto ctx(ActorContext());
    TMetricsTimeKeeper keeper(ResourceMetrics, ctx);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPersQueue::TEvUpdateBalancerConfig, HandleOnInit);
        HFunc(TEvPersQueue::TEvRegisterReadSession, HandleOnInit);
        HFunc(TEvPersQueue::TEvGetReadSessionsInfo, Handle);
        HFunc(TEvTabletPipe::TEvServerConnected, Handle);
        HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
        HFunc(TEvPersQueue::TEvGetPartitionIdForWrite, Handle);
        HFunc(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound, Handle);
        HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        HFunc(TEvPersQueue::TEvGetPartitionsLocation, HandleOnInit);
        // MLP
        hFunc(TEvPQ::TEvMLPConsumerStatus, Handle);
        // From kafka
        HFunc(TEvPersQueue::TEvBalancingSubscribe, Handle);
        HFunc(TEvPersQueue::TEvBalancingUnsubscribe, Handle);
        default:
            StateInitImpl(ev, SelfId());
            break;
    }
}

STFUNC(TPersQueueReadBalancer::StateWork) {
    auto ctx(ActorContext());
    TMetricsTimeKeeper keeper(ResourceMetrics, ctx);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvPersQueue::TEvGetPartitionIdForWrite, Handle);
        HFunc(TEvPersQueue::TEvUpdateBalancerConfig, Handle);
        HFunc(TEvPersQueue::TEvRegisterReadSession, Handle);
        HFunc(TEvPersQueue::TEvGetReadSessionsInfo, Handle);
        HFunc(TEvPersQueue::TEvPartitionReleased, Handle);
        HFunc(TEvTabletPipe::TEvServerConnected, Handle);
        HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFunc(TEvPersQueue::TEvStatusResponse, Handle);
        HFunc(TEvPQ::TEvStatsWakeup, Handle);
        HFunc(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound, Handle);
        HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
        HFunc(TEvPersQueue::TEvStatus, Handle);
        HFunc(TEvPersQueue::TEvGetPartitionsLocation, Handle);
        HFunc(TEvPQ::TEvReadingPartitionStatusRequest, Handle);
        HFunc(TEvPersQueue::TEvReadingPartitionStartedRequest, Handle);
        HFunc(TEvPersQueue::TEvReadingPartitionFinishedRequest, Handle);
        HFunc(TEvPQ::TEvWakeupReleasePartition, Handle);
        HFunc(TEvPQ::TEvBalanceConsumer, Handle);
        // From kafka
        HFunc(TEvPersQueue::TEvBalancingSubscribe, Handle);
        HFunc(TEvPersQueue::TEvBalancingUnsubscribe, Handle);
        // from PQ
        HFunc(TEvPQ::TEvPartitionScaleStatusChanged, Handle);
        // from TPartitionScaleRequest
        HFunc(TPartitionScaleRequest::TEvPartitionScaleRequestDone, Handle);
        // from MirrorDescriber
        HFunc(TEvPQ::TEvMirrorTopicDescription, Handle);
        // MLP
        hFunc(TEvPQ::TEvMLPGetPartitionRequest, Handle);
        hFunc(TEvPQ::TEvMLPConsumerStatus, Handle);
        default:
            HandleDefaultEvents(ev, SelfId());
            break;
    }
}

} // NPQ
} // NKikimr

namespace NKikimr {

IActor* CreatePersQueueReadBalancer(const TActorId& tablet, TTabletStorageInfo *info) {
    return new NPQ::TPersQueueReadBalancer(tablet, info);
}

} // NKikimr
