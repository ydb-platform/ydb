#include "datashard_impl.h"
#include <ydb/core/tablet_flat/flat_scan_spent.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet_flat/flat_stat_table.h>
#include <ydb/core/tablet_flat/flat_bio_stats.h>
#include <ydb/core/tablet_flat/flat_dbase_sz_env.h>
#include "ydb/core/tablet_flat/shared_sausagecache.h"
#include <ydb/core/protos/datashard_config.pb.h>

namespace NKikimr {
namespace NDataShard {

using namespace NResourceBroker;
using namespace NTable;

class TTableStatsCoroBuilder : public TActorCoroImpl, private IPages {
private:
    using ECode = TDataShard::TEvPrivate::TEvTableStatsError::ECode;

    static constexpr TDuration MaxCoroutineExecutionTime = TDuration::MilliSeconds(5);

    enum {
        EvResume = EventSpaceBegin(TEvents::ES_PRIVATE)
    };

    struct TExTableStatsError {
        TExTableStatsError(ECode code, const TString& msg)
            : Code(code)
            , Message(msg)
        {}

        TExTableStatsError(ECode code)
            : TExTableStatsError(code, "")
        {}

        ECode Code;
        TString Message;
    };

public:
    TTableStatsCoroBuilder(TActorId replyTo, ui64 tabletId, ui64 tableId, TActorId executorId, ui64 indexSize,
                            const TAutoPtr<TSubset> subset, ui64 memRowCount, ui64 memDataSize,
                            ui64 rowCountResolution, ui64 dataSizeResolution, ui32 histogramBucketsCount, ui64 searchHeight, TInstant statsUpdateTime)
        : TActorCoroImpl(/* stackSize */ 64_KB, /* allowUnhandledDtor */ true)
        , ReplyTo(replyTo)
        , TabletId(tabletId)
        , TableId(tableId)
        , ExecutorId(executorId)
        , IndexSize(indexSize)
        , StatsUpdateTime(statsUpdateTime)
        , Subset(subset)
        , MemRowCount(memRowCount)
        , MemDataSize(memDataSize)
        , RowCountResolution(rowCountResolution)
        , DataSizeResolution(dataSizeResolution)
        , HistogramBucketsCount(histogramBucketsCount)
        , SearchHeight(searchHeight)
    {}

    void Run() override {
        try {
            RunImpl();
        } catch (const TDtorException&) {
            return; // coroutine terminated
        } catch (const TExTableStatsError& ex) {
            Send(ReplyTo, new TDataShard::TEvPrivate::TEvTableStatsError(TableId, ex.Code, ex.Message));
        } catch (...) {
            Send(ReplyTo, new TDataShard::TEvPrivate::TEvTableStatsError(TableId, ECode::UNKNOWN));

            Y_DEBUG_ABORT("unhandled exception");
        }

        Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvNotifyActorDied);
        Send(MakeSharedPageCacheId(), new NSharedCache::TEvUnregister);
    }

    TResult Locate(const TMemTable*, ui64, ui32) noexcept override {
        Y_ABORT("IPages::Locate(TMemTable*, ...) shouldn't be used here");
    }

    TResult Locate(const TPart*, ui64, ELargeObj) noexcept override {
        Y_ABORT("IPages::Locate(TPart*, ...) shouldn't be used here");
    }

    const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
        Y_ABORT_UNLESS(groupId.IsMain(), "Unsupported column group");

        auto partStore = CheckedCast<const TPartStore*>(part);
        auto info = partStore->PageCollections.at(groupId.Index).Get();
        auto type = info->GetPageType(pageId);
        Y_ABORT_UNLESS(type == EPage::FlatIndex || type == EPage::BTreeIndex);

        auto& partPages = Pages[part];
        auto page = partPages.FindPtr(pageId);
        if (page != nullptr) {
            return page;
        }

        auto fetchEv = new NPageCollection::TFetch{ {}, info->PageCollection, TVector<TPageId>{ pageId } };
        PagesSize += info->GetPageSize(pageId);
        Send(MakeSharedPageCacheId(), new NSharedCache::TEvRequest(NSharedCache::EPriority::Bkgr, fetchEv, SelfActorId));

        Spent->Alter(false); // pause measurement
        ReleaseResources();

        auto ev = WaitForSpecificEvent<NSharedCache::TEvResult>(&TTableStatsCoroBuilder::ProcessUnexpectedEvent);
        auto msg = ev->Get();

        if (msg->Status != NKikimrProto::OK) {
            LOG_ERROR_S(GetActorContext(), NKikimrServices::TX_DATASHARD, "Stats build failed at datashard "
                << TabletId << ", for tableId " << TableId << " requested pages but got " << msg->Status);
            throw TExTableStatsError(ECode::FETCH_PAGE_FAILED, NKikimrProto::EReplyStatus_Name(msg->Status));
        }

        ObtainResources();
        Spent->Alter(true); // resume measurement
        
        for (auto& loaded : msg->Loaded) {
            partPages.emplace(pageId, TPinnedPageRef(loaded.Page).GetData());
        }

        page = partPages.FindPtr(pageId);
        Y_ABORT_UNLESS(page != nullptr);

        return page;
    }

private:
    void RunImpl() {
        ObtainResources();

        auto ev = MakeHolder<TDataShard::TEvPrivate::TEvAsyncTableStats>();
        ev->TableId = TableId;
        ev->StatsUpdateTime = StatsUpdateTime;
        ev->PartCount = Subset->Flatten.size() + Subset->ColdParts.size();
        ev->MemRowCount = MemRowCount;
        ev->MemDataSize = MemDataSize;
        ev->SearchHeight = SearchHeight;

        GetPartOwners(*Subset, ev->PartOwners);

        Subset->ColdParts.clear(); // stats won't include cold parts, if any
        Spent = new TSpent(TAppData::TimeProvider.Get());

        BuildStats(*Subset, ev->Stats, RowCountResolution, DataSizeResolution, HistogramBucketsCount, this, [this](){
            const auto now = GetCycleCountFast();
    
            if (now > CoroutineDeadline) {
                Spent->Alter(false); // pause measurement
                ReleaseResources();

                Send(new IEventHandle(EvResume, 0, SelfActorId, {}, nullptr, 0));
                WaitForSpecificEvent([](IEventHandle& ev) { 
                    return ev.Type == EvResume; 
                }, &TTableStatsCoroBuilder::ProcessUnexpectedEvent);

                ObtainResources();
                Spent->Alter(true); // resume measurement
            }
        });
        
        Y_DEBUG_ABORT_UNLESS(IndexSize == ev->Stats.IndexSize.Size);

        LOG_DEBUG_S(GetActorContext(), NKikimrServices::TX_DATASHARD, "BuildStats result at datashard " << TabletId << ", for tableId " << TableId
            << ": RowCount " << ev->Stats.RowCount << ", DataSize " << ev->Stats.DataSize.Size << ", IndexSize " << ev->Stats.IndexSize.Size << ", PartCount " << ev->PartCount
            << (ev->PartOwners.size() > 1 || ev->PartOwners.size() == 1 && *ev->PartOwners.begin() != TabletId ? ", with borrowed parts" : "")
            << ", LoadedSize " << PagesSize << ", " << NFmt::Do(*Spent) << ", HistogramKeys " << ev->Stats.DataSizeHistogram.size());

        Send(ReplyTo, ev.Release());

        ReleaseResources();
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            case TEvResourceBroker::EvTaskOperationError: {
                const auto* msg = ev->CastAsLocal<TEvResourceBroker::TEvTaskOperationError>();
                LOG_ERROR_S(GetActorContext(), NKikimrServices::TX_DATASHARD, "TEvResourceAllocated: " << msg->Status.Message);
                throw TExTableStatsError(ECode::RESOURCE_ALLOCATION_FAILED, msg->Status.Message);
            }

            case ui32(NTabletFlatExecutor::NBlockIO::EEv::Stat): {
                ev->Rewrite(ev->GetTypeRewrite(), ExecutorId);
                Send(ev.Release());
                break;
            }

            case ui32(NKikimr::NSharedCache::EEv::EvUpdated):
                // ignore shared cache Dropped events
                break;

            case TEvents::TSystem::Poison:
                throw TExTableStatsError(ECode::ACTOR_DIED);

            default: {
                const auto typeName = ev->GetTypeName();
                Y_DEBUG_ABORT("unexpected event Type: %s", typeName.c_str());
            }
        }
    }

    void ObtainResources() {
        Send(MakeResourceBrokerID(),
            new TEvResourceBroker::TEvSubmitTask(
                /* task id */ 1,
                /* task name */ TStringBuilder() << "build-stats-table-" << TableId << "-tablet-" << TabletId,
                /* cpu & memory */ {{ 1, 0 }},
                /* task type */ "datashard_build_stats",
                /* priority */ 5,
                /* cookie */ nullptr));

        auto ev = WaitForSpecificEvent<TEvResourceBroker::TEvResourceAllocated>(&TTableStatsCoroBuilder::ProcessUnexpectedEvent);
        auto msg = ev->Get();
        Y_ABORT_UNLESS(!msg->Cookie.Get(), "Unexpected cookie in TEvResourceAllocated");
        Y_ABORT_UNLESS(msg->TaskId == 1, "Unexpected task id in TEvResourceAllocated");

        CoroutineDeadline = GetCycleCountFast() + DurationToCycles(MaxCoroutineExecutionTime);
    }

    void ReleaseResources() {
        Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvFinishTask(/* task id */ 1, /* cancelled */ false));
    }

    TActorId ReplyTo;
    ui64 TabletId;
    ui64 TableId;
    TActorId ExecutorId;
    ui64 IndexSize;
    TInstant StatsUpdateTime;
    TAutoPtr<TSubset> Subset;
    ui64 MemRowCount;
    ui64 MemDataSize;
    ui64 RowCountResolution;
    ui64 DataSizeResolution;
    ui32 HistogramBucketsCount;
    ui64 SearchHeight;
    THashMap<const TPart*, THashMap<TPageId, TSharedData>> Pages;
    ui64 PagesSize = 0;
    ui64 CoroutineDeadline;
    TAutoPtr<TSpent> Spent;
};

class TDataShard::TTxGetTableStats : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvGetTableStats::TPtr Ev;
    TAutoPtr<TEvDataShard::TEvGetTableStatsResult> Result;

public:
    TTxGetTableStats(TDataShard* ds, TEvDataShard::TEvGetTableStats::TPtr ev)
        : TBase(ds)
        , Ev(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_GET_TABLE_STATS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        ui64 tableId = Ev->Get()->Record.GetTableId();

        Result = new TEvDataShard::TEvGetTableStatsResult(Self->TabletID(), Self->PathOwnerId, tableId);

        if (!Self->TableInfos.contains(tableId))
            return true;

        if (Ev->Get()->Record.GetCollectKeySample()) {
            Self->EnableKeyAccessSampling(ctx, AppData(ctx)->TimeProvider->Now() + TDuration::Seconds(60));
        }

        const TUserTable& tableInfo = *Self->TableInfos[tableId];

        // Fill stats with current mem table size:
        auto memSize = txc.DB.GetTableMemSize(tableInfo.LocalTid);
        auto memRowCount = txc.DB.GetTableMemRowCount(tableInfo.LocalTid);
        if (tableInfo.ShadowTid) {
            memSize += txc.DB.GetTableMemSize(tableInfo.ShadowTid);
            memRowCount += txc.DB.GetTableMemRowCount(tableInfo.ShadowTid);
        }

        Result->Record.MutableTableStats()->SetInMemSize(memSize);
        Result->Record.MutableTableStats()->SetLastAccessTime(tableInfo.Stats.AccessTime.MilliSeconds());
        Result->Record.MutableTableStats()->SetLastUpdateTime(tableInfo.Stats.UpdateTime.MilliSeconds());

        tableInfo.Stats.DataSizeResolution = Ev->Get()->Record.GetDataSizeResolution();
        tableInfo.Stats.RowCountResolution = Ev->Get()->Record.GetRowCountResolution();
        tableInfo.Stats.HistogramBucketsCount = Ev->Get()->Record.GetHistogramBucketsCount();

        // Check if first stats update has been completed:
        bool ready = (tableInfo.Stats.StatsUpdateTime != TInstant());
        Result->Record.SetFullStatsReady(ready);
        if (!ready) {
            return true;
        }

        const TStats& stats = tableInfo.Stats.DataStats;
        Result->Record.MutableTableStats()->SetIndexSize(stats.IndexSize.Size);
        Result->Record.MutableTableStats()->SetByKeyFilterSize(stats.ByKeyFilterSize.Size);
        Result->Record.MutableTableStats()->SetDataSize(stats.DataSize.Size + memSize);
        Result->Record.MutableTableStats()->SetRowCount(stats.RowCount + memRowCount);
        FillHistogram(stats.DataSizeHistogram, *Result->Record.MutableTableStats()->MutableDataSizeHistogram());
        FillHistogram(stats.RowCountHistogram, *Result->Record.MutableTableStats()->MutableRowCountHistogram());
        // Fill key access sample if it was collected not too long ago:
        if (Self->StopKeyAccessSamplingAt + TDuration::Seconds(30) >= AppData(ctx)->TimeProvider->Now()) {
            FillKeyAccessSample(tableInfo.Stats.AccessStats, *Result->Record.MutableTableStats()->MutableKeyAccessSample());
        }

        Result->Record.MutableTableStats()->SetPartCount(tableInfo.Stats.PartCount);
        Result->Record.MutableTableStats()->SetSearchHeight(tableInfo.Stats.SearchHeight);
        Result->Record.MutableTableStats()->SetLastFullCompactionTs(tableInfo.Stats.LastFullCompaction.Seconds());
        Result->Record.MutableTableStats()->SetHasLoanedParts(Self->Executor()->HasLoanedParts());

        Result->Record.SetShardState(Self->State);
        for (const auto& pi : tableInfo.Stats.PartOwners) {
            Result->Record.AddUserTablePartOwners(pi);
        }

        for (const auto& pi : Self->SysTablesPartOwners) {
            Result->Record.AddSysTablesPartOwners(pi);
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        ctx.Send(Ev->Sender, Result.Release());
    }

private:
    static void FillHistogram(const THistogram& h, NKikimrTableStats::THistogram& pb) {
        for (auto& b : h) {
            auto bucket = pb.AddBuckets();
            bucket->SetKey(b.EndKey);
            bucket->SetValue(b.Value);
        }
    }

    static void FillKeyAccessSample(const TKeyAccessSample& s, NKikimrTableStats::THistogram& pb) {
        for (const auto& k : s.GetSample()) {
            auto bucket = pb.AddBuckets();
            bucket->SetKey(k.first);
            bucket->SetValue(1);
        }
    }
};

void TDataShard::Handle(TEvDataShard::TEvGetTableStats::TPtr& ev, const TActorContext& ctx) {
    Executor()->Execute(new TTxGetTableStats(this, ev), ctx);
}

template <class TTables>
void ListTableNames(const TTables& tables, TStringBuilder& names) {
    for (auto& t : tables) {
        if (!names.Empty()) {
            names << ", ";
        }
        names << "[" << t.second->Path << "]";
    }
}

void TDataShard::Handle(TEvPrivate::TEvAsyncTableStats::TPtr& ev, const TActorContext& ctx) {
    Actors.erase(ev->Sender);

    ui64 tableId = ev->Get()->TableId;
    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "BuildStats result received at datashard " << TabletID() << ", for tableId " << tableId);

    i64 dataSize = 0;
    if (TableInfos.contains(tableId)) {
        const TUserTable& tableInfo = *TableInfos[tableId];

        if (!tableInfo.StatsUpdateInProgress) {
            // How can this happen?
            LOG_ERROR(ctx, NKikimrServices::TX_DATASHARD,
                      "Unexpected async stats update at datashard %" PRIu64, TabletID());
        }
        tableInfo.Stats.DataStats = std::move(ev->Get()->Stats);
        tableInfo.Stats.PartOwners = std::move(ev->Get()->PartOwners);
        tableInfo.Stats.PartCount = ev->Get()->PartCount;
        tableInfo.Stats.StatsUpdateTime = ev->Get()->StatsUpdateTime;
        tableInfo.Stats.MemRowCount = ev->Get()->MemRowCount;
        tableInfo.Stats.MemDataSize = ev->Get()->MemDataSize;

        dataSize += tableInfo.Stats.DataStats.DataSize.Size;

        tableInfo.Stats.SearchHeight = ev->Get()->SearchHeight;

        tableInfo.StatsUpdateInProgress = false;

        SendPeriodicTableStats(ctx);

    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Drop stats at datashard " << TabletID()
                    << ", built for tableId " << tableId << ", but table is gone (moved ot dropped)");
    }

    if (dataSize > HighDataSizeReportThreshlodBytes) {
        TInstant now = AppData(ctx)->TimeProvider->Now();

        if (LastDataSizeWarnTime + TDuration::Seconds(HighDataSizeReportIntervalSeconds) > now)
            return;

        LastDataSizeWarnTime = now;

        TStringBuilder names;
        ListTableNames(GetUserTables(), names);

        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Data size " << dataSize
                    << " is higher than threshold of " << (i64)HighDataSizeReportThreshlodBytes
                    << " at datashard: " << TabletID()
                    << " table: " << names
                    << " consider reconfiguring table partitioning settings");
    }
}

void TDataShard::Handle(TEvPrivate::TEvTableStatsError::TPtr& ev, const TActorContext& ctx) {
    Actors.erase(ev->Sender);

    auto msg = ev->Get();

    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Stats rebuilt error '" << msg->Message 
        << "', code: " << ui32(msg->Code) << ", datashard " << TabletID() << ", tableId " << msg->TableId);

    auto it = TableInfos.find(msg->TableId);
    if (it != TableInfos.end()) {
        it->second->StatsUpdateInProgress = false;
        it->second->StatsNeedUpdate = true;
    }
}

class TDataShard::TTxInitiateStatsUpdate : public NTabletFlatExecutor::TTransactionBase<TDataShard> {
private:
    TEvDataShard::TEvGetTableStats::TPtr Ev;
    TAutoPtr<TEvDataShard::TEvGetTableStatsResult> Result;

public:
    TTxInitiateStatsUpdate(TDataShard* ds)
        : TBase(ds)
    {}

    TTxType GetTxType() const override { return TXTYPE_INITIATE_STATS_UPDATE; }

    void CheckIdleMemCompaction(const TUserTable& table, TTransactionContext& txc, const TActorContext& ctx) {
        // Note: we only care about changes in the main table
        auto lastTableChange = txc.DB.Head(table.LocalTid);
        if (table.LastTableChange != lastTableChange) {
            table.LastTableChange = lastTableChange;
            table.LastTableChangeTimestamp = ctx.Monotonic();
            return;
        }

        // We only want to start idle compaction when there are some operations in the mem table
        if (txc.DB.GetTableMemOpsCount(table.LocalTid) == 0) {
            return;
        }

        // Compact non-empty mem table when there have been no changes for a while
        TDuration elapsed = ctx.Monotonic() - table.LastTableChangeTimestamp;
        TDuration idleInterval = TDuration::Seconds(AppData(ctx)->DataShardConfig.GetIdleMemCompactionIntervalSeconds());
        if (elapsed >= idleInterval) {
            Self->Executor()->CompactMemTable(table.LocalTid);
            table.LastTableChangeTimestamp = ctx.Monotonic();
        }
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (Self->State != TShardState::Ready)
            return true;

        for (auto& ti : Self->TableInfos) {
            const ui32 localTableId = ti.second->LocalTid;
            const ui32 shadowTableId = ti.second->ShadowTid;

            CheckIdleMemCompaction(*ti.second, txc, ctx);

            if (ti.second->StatsUpdateInProgress) {
                // We don't want to update mem counters during updates, since
                // it would result in value inconsistencies
                continue;
            }

            ui64 memRowCount = txc.DB.GetTableMemRowCount(localTableId);
            ui64 memDataSize = txc.DB.GetTableMemSize(localTableId);
            ui64 searchHeight = txc.DB.GetTableSearchHeight(localTableId);
            if (shadowTableId) {
                memRowCount += txc.DB.GetTableMemRowCount(shadowTableId);
                memDataSize += txc.DB.GetTableMemSize(shadowTableId);
                searchHeight = 0;
            }

            if (!ti.second->StatsNeedUpdate) {
                ti.second->Stats.MemRowCount = memRowCount;
                ti.second->Stats.MemDataSize = memDataSize;
                ti.second->Stats.SearchHeight = searchHeight;
                continue;
            }

            const ui32 MaxBuckets = 500;

            ui64 tableId = ti.first;
            ui64 rowCountResolution = gDbStatsRowCountResolution;
            ui64 dataSizeResolution = gDbStatsDataSizeResolution;
            ui32 histogramBucketsCount = gDbStatsHistogramBucketsCount;


            if (ti.second->Stats.DataSizeResolution &&
                ti.second->Stats.DataStats.DataSize.Size / ti.second->Stats.DataSizeResolution <= MaxBuckets)
            {
                dataSizeResolution = ti.second->Stats.DataSizeResolution;
            }

            if (ti.second->Stats.RowCountResolution &&
                ti.second->Stats.DataStats.RowCount / ti.second->Stats.RowCountResolution <= MaxBuckets)
            {
                rowCountResolution = ti.second->Stats.RowCountResolution;
            }

            if (ti.second->Stats.HistogramBucketsCount) {
                histogramBucketsCount = Min(MaxBuckets, ti.second->Stats.HistogramBucketsCount);
            }

            ti.second->StatsUpdateInProgress = true;
            ti.second->StatsNeedUpdate = false;

            ui64 indexSize = txc.DB.GetTableIndexSize(localTableId);
            if (shadowTableId) {
                indexSize += txc.DB.GetTableIndexSize(shadowTableId);
            }

            TAutoPtr<TSubset> subsetForStats = txc.DB.Subset(localTableId, TEpoch::Max(), { }, { });
            // Remove memtables from the subset as we only want to look at indexes for parts
            subsetForStats->Frozen.clear();

            if (shadowTableId) {
                // HACK: we combine subsets of different tables
                // It's only safe to do as long as stats collector performs
                // index lookups only, and doesn't care about the actual lsm
                // part order.
                auto shadowSubset = txc.DB.Subset(shadowTableId, TEpoch::Max(), { }, { });
                subsetForStats->Flatten.insert(
                    subsetForStats->Flatten.end(),
                    shadowSubset->Flatten.begin(),
                    shadowSubset->Flatten.end());
                subsetForStats->ColdParts.insert(
                    subsetForStats->ColdParts.end(),
                    shadowSubset->ColdParts.begin(),
                    shadowSubset->ColdParts.end());
            }

            auto builder = new TActorCoro(MakeHolder<TTableStatsCoroBuilder>(ctx.SelfID,
                Self->TabletID(),
                tableId,
                Self->ExecutorID(),
                indexSize,
                subsetForStats,
                memRowCount,
                memDataSize,
                rowCountResolution,
                dataSizeResolution,
                histogramBucketsCount,
                searchHeight,
                AppData(ctx)->TimeProvider->Now()), NKikimrServices::TActivity::DATASHARD_STATS_BUILDER);

            TActorId actorId = ctx.Register(builder, TMailboxType::HTSwap, AppData(ctx)->BatchPoolId);
            Self->Actors.insert(actorId);
        }

        Self->SysTablesPartOwners.clear();
        for (ui32 sysTableId : Self->SysTablesToTransferAtSplit) {
            THashSet<ui64> sysPartOwners;
            auto subset = txc.DB.Subset(sysTableId, TEpoch::Max(), { }, { });
            GetPartOwners(*subset, sysPartOwners);
            Self->SysTablesPartOwners.insert(sysPartOwners.begin(), sysPartOwners.end());
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_UNUSED(ctx);
    }
};

void TDataShard::UpdateTableStats(const TActorContext &ctx) {
    if (StatisticsDisabled)
        return;

    TInstant now = AppData(ctx)->TimeProvider->Now();

    if (LastDbStatsUpdateTime + gDbStatsReportInterval > now)
        return;

    if (State != TShardState::Ready)
        return;

    LastDbStatsUpdateTime = now;

    LOG_DEBUG(ctx, NKikimrServices::TX_DATASHARD, "UpdateTableStats at datashard %" PRIu64, TabletID());

    Executor()->Execute(new TTxInitiateStatsUpdate(this), ctx);
}

void TDataShard::CollectCpuUsage(const TActorContext &ctx) {
    auto* metrics = Executor()->GetResourceMetrics();
    TInstant now = AppData(ctx)->TimeProvider->Now();

    // advance CPU usage collector to the current time and report very-very small usage
    metrics->CPU.Increment(10, now);
    metrics->TryUpdate(ctx);

    if (!metrics->CPU.IsValueReady()) {
        return;
    }

    ui64 cpuUsec = metrics->CPU.GetValue();
    float cpuPercent = cpuUsec / 10000.0;

    if (cpuPercent > CpuUsageReportThreshlodPercent) {
        if (LastCpuWarnTime + TDuration::Seconds(CpuUsageReportIntervalSeconds) > now)
            return;

        LastCpuWarnTime = now;

        TStringBuilder names;
        ListTableNames(GetUserTables(), names);

        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "CPU usage " << cpuPercent
                    << "% is higher than threshold of " << (i64)CpuUsageReportThreshlodPercent
                    << "% in-flight Tx: " << TxInFly()
                    << " immediate Tx: " << ImmediateInFly()
                    << " readIterators: " << ReadIteratorsInFly()
                    << " at datashard: " << TabletID()
                    << " table: " << names);
    }
}

}}
