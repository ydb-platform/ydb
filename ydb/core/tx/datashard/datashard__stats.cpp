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

struct TTableStatsCoroBuilderArgs {
    TActorId ReplyTo;
    ui64 TabletId;
    ui64 TableId;
    TActorId ExecutorId;
    ui64 IndexSize;
    TAutoPtr<TSubset> Subset;
    ui64 MemRowCount;
    ui64 MemDataSize;
    ui64 RowCountResolution;
    ui64 DataSizeResolution;
    ui32 HistogramBucketsCount;
    ui64 SearchHeight;
    bool HasSchemaChanges;
    TInstant StatsUpdateTime;
};

class TTableStatsCoroBuilder : public TActorCoroImpl, private IPages, TTableStatsCoroBuilderArgs {
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
    TTableStatsCoroBuilder(TTableStatsCoroBuilderArgs args)
        : TActorCoroImpl(/* stackSize */ 64_KB, /* allowUnhandledDtor */ true)
        , TTableStatsCoroBuilderArgs(args)
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
            LOG_ERROR_S(GetActorContext(), NKikimrServices::TABLET_STATS_BUILDER, "Failed to build at datashard "
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
        ev->IndexSize = IndexSize;
        ev->StatsUpdateTime = StatsUpdateTime;
        ev->PartCount = Subset->Flatten.size() + Subset->ColdParts.size();
        ev->MemRowCount = MemRowCount;
        ev->MemDataSize = MemDataSize;
        ev->SearchHeight = SearchHeight;
        ev->HasSchemaChanges = HasSchemaChanges;

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
        }, TStringBuilder() << "Building stats at datashard " << TabletId << ", for tableId " << TableId << ": ");

        Y_DEBUG_ABORT_UNLESS(IndexSize == ev->Stats.IndexSize.Size);

        LOG_INFO_S(GetActorContext(), NKikimrServices::TABLET_STATS_BUILDER, "Stats at datashard " << TabletId << ", for tableId " << TableId << ": "
            << ev->Stats.ToString()
            << " PartCount: " << ev->PartCount
            << (ev->PartOwners.size() > 1 || ev->PartOwners.size() == 1 && *ev->PartOwners.begin() != TabletId ? ", with borrowed parts" : "")
            << (ev->HasSchemaChanges ? ", with schema changes" : "")
            << ", LoadedSize " << PagesSize << ", " << NFmt::Do(*Spent));

        Send(ReplyTo, ev.Release());

        ReleaseResources();
    }

    void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvResourceBroker::EvTaskOperationError: {
                const auto* msg = ev->CastAsLocal<TEvResourceBroker::TEvTaskOperationError>();
                LOG_ERROR_S(GetActorContext(), NKikimrServices::TABLET_STATS_BUILDER, "Failed to allocate resource"
                    << " error '" << msg->Status.Message << "'"
                    << " at datashard " << TabletId << ", for tableId " << TableId);
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

        auto indexSize = txc.DB.GetTableIndexSize(tableInfo.LocalTid);
        auto memSize = txc.DB.GetTableMemSize(tableInfo.LocalTid);
        auto memRowCount = txc.DB.GetTableMemRowCount(tableInfo.LocalTid);

        if (tableInfo.ShadowTid) {
            indexSize += txc.DB.GetTableIndexSize(tableInfo.ShadowTid);
            memSize += txc.DB.GetTableMemSize(tableInfo.ShadowTid);
            memRowCount += txc.DB.GetTableMemRowCount(tableInfo.ShadowTid);
        }

        Result->Record.MutableTableStats()->SetIndexSize(indexSize);
        Result->Record.MutableTableStats()->SetInMemSize(memSize);
        Result->Record.MutableTableStats()->SetLastAccessTime(tableInfo.Stats.AccessTime.MilliSeconds());
        Result->Record.MutableTableStats()->SetLastUpdateTime(tableInfo.Stats.UpdateTime.MilliSeconds());

        tableInfo.Stats.DataSizeResolution = Ev->Get()->Record.GetDataSizeResolution();
        tableInfo.Stats.RowCountResolution = Ev->Get()->Record.GetRowCountResolution();
        tableInfo.Stats.HistogramBucketsCount = Ev->Get()->Record.GetHistogramBucketsCount();

        // Check if first stats update has been completed
        bool ready = (tableInfo.Stats.StatsUpdateTime != TInstant());
        Result->Record.SetFullStatsReady(ready);
        if (!ready)
            return true;

        const TStats& stats = tableInfo.Stats.DataStats;
        Result->Record.MutableTableStats()->SetDataSize(stats.DataSize.Size + memSize);
        Result->Record.MutableTableStats()->SetRowCount(stats.RowCount + memRowCount);
        FillHistogram(stats.DataSizeHistogram, *Result->Record.MutableTableStats()->MutableDataSizeHistogram());
        FillHistogram(stats.RowCountHistogram, *Result->Record.MutableTableStats()->MutableRowCountHistogram());
        // Fill key access sample if it was collected not too long ago
        if (Self->StopKeyAccessSamplingAt + TDuration::Seconds(30) >= AppData(ctx)->TimeProvider->Now()) {
            FillKeyAccessSample(tableInfo.Stats.AccessStats, *Result->Record.MutableTableStats()->MutableKeyAccessSample());
        }

        Result->Record.MutableTableStats()->SetPartCount(tableInfo.Stats.PartCount);
        Result->Record.MutableTableStats()->SetSearchHeight(tableInfo.Stats.SearchHeight);
        Result->Record.MutableTableStats()->SetHasSchemaChanges(tableInfo.Stats.HasSchemaChanges);
        Result->Record.MutableTableStats()->SetLastFullCompactionTs(tableInfo.Stats.LastFullCompaction.Seconds());
        Result->Record.MutableTableStats()->SetHasLoanedParts(Self->Executor()->HasLoanedParts());

        Result->Record.SetShardState(Self->State);
        for (const auto& pi : tableInfo.Stats.PartOwners) {
            Result->Record.AddUserTablePartOwners(pi);
        }

        for (const auto& pi : Self->SysTablesPartOnwers) {
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
    LOG_INFO_S(ctx, NKikimrServices::TABLET_STATS_BUILDER, "Result received at datashard " << TabletID() << ", for tableId " << tableId
        << ": " << ev->Get()->Stats.ToString());

    i64 dataSize = 0;
    if (TableInfos.contains(tableId)) {
        const TUserTable& tableInfo = *TableInfos[tableId];

        if (!tableInfo.StatsUpdateInProgress) { // How can this happen?
            LOG_ERROR_S(ctx, NKikimrServices::TABLET_STATS_BUILDER, "Unexpected async stats update at datashard " << TabletID() << ", for tableId " << tableId);
        }
        tableInfo.Stats.Update(std::move(ev->Get()->Stats), ev->Get()->IndexSize,
            std::move(ev->Get()->PartOwners), ev->Get()->PartCount,
            ev->Get()->StatsUpdateTime);
        tableInfo.Stats.MemRowCount = ev->Get()->MemRowCount;
        tableInfo.Stats.MemDataSize = ev->Get()->MemDataSize;

        dataSize += tableInfo.Stats.DataStats.DataSize.Size;

        tableInfo.Stats.SearchHeight = ev->Get()->SearchHeight;
        tableInfo.Stats.HasSchemaChanges = ev->Get()->HasSchemaChanges;

        tableInfo.StatsUpdateInProgress = false;

        SendPeriodicTableStats(ctx);

    } else {
        LOG_INFO_S(ctx, NKikimrServices::TABLET_STATS_BUILDER, "Result dropped at datashard " << TabletID() << ", for tableId " << tableId
            << ", but table is gone (moved ot dropped)");
    }

    if (dataSize > HighDataSizeReportThresholdBytes) {
        TInstant now = AppData(ctx)->TimeProvider->Now();

        if (LastDataSizeWarnTime + TDuration::Seconds(HighDataSizeReportIntervalSeconds) > now)
            return;

        LastDataSizeWarnTime = now;

        TStringBuilder names;
        ListTableNames(GetUserTables(), names);

        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Data size " << dataSize
                    << " is higher than threshold of " << (i64)HighDataSizeReportThresholdBytes
                    << " at datashard: " << TabletID()
                    << " table: " << names
                    << " consider reconfiguring table partitioning settings");
    }
}

void TDataShard::Handle(TEvPrivate::TEvTableStatsError::TPtr& ev, const TActorContext& ctx) {
    Actors.erase(ev->Sender);

    auto msg = ev->Get();

    LOG_ERROR_S(ctx, NKikimrServices::TABLET_STATS_BUILDER, "Stats rebuilt error '" << msg->Message
        << "', code: " << ui32(msg->Code)
        << " at datashard " << TabletID() << ", for tableId " << msg->TableId);

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
            const ui64 tableId = ti.first;

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

            // Note: ignore shadow table for simplicity
            bool hasSchemaChanges = Self->Executor()->HasSchemaChanges(localTableId);

            if (!ti.second->StatsNeedUpdate) {
                auto& stats = ti.second->Stats;
                stats.MemRowCount = memRowCount;
                stats.MemDataSize = memDataSize;
                stats.SearchHeight = searchHeight;
                stats.HasSchemaChanges = hasSchemaChanges;

                LOG_INFO_S(ctx, NKikimrServices::TABLET_STATS_BUILDER, "Skipped at datashard " << Self->TabletID() << ", for tableId " << tableId << ": "
                    << stats.DataStats.ToString() << " PartCount " << stats.PartCount
                    << (stats.HasSchemaChanges ? ", with schema changes" : ""));

                continue;
            }

            const ui32 MaxBuckets = 500;
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

            auto builder = new TActorCoro(MakeHolder<TTableStatsCoroBuilder>(
                TTableStatsCoroBuilderArgs{
                    .ReplyTo = ctx.SelfID,
                    .TabletId = Self->TabletID(),
                    .TableId = tableId,
                    .ExecutorId = Self->ExecutorID(),
                    .IndexSize = indexSize,
                    .Subset = subsetForStats,
                    .MemRowCount = memRowCount,
                    .MemDataSize = memDataSize,
                    .RowCountResolution = rowCountResolution,
                    .DataSizeResolution = dataSizeResolution,
                    .HistogramBucketsCount = histogramBucketsCount,
                    .SearchHeight = searchHeight,
                    .HasSchemaChanges = hasSchemaChanges,
                    .StatsUpdateTime = AppData(ctx)->TimeProvider->Now()
                }), NKikimrServices::TActivity::DATASHARD_STATS_BUILDER);

            TActorId actorId = ctx.Register(builder, TMailboxType::HTSwap, AppData(ctx)->BatchPoolId);
            Self->Actors.insert(actorId);
        }

        Self->SysTablesPartOnwers.clear();
        for (ui32 sysTableId : Self->SysTablesToTransferAtSplit) {
            THashSet<ui64> sysPartOwners;
            auto subset = txc.DB.Subset(sysTableId, TEpoch::Max(), { }, { });
            GetPartOwners(*subset, sysPartOwners);
            Self->SysTablesPartOnwers.insert(sysPartOwners.begin(), sysPartOwners.end());
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

    LOG_INFO_S(ctx, NKikimrServices::TABLET_STATS_BUILDER, "UpdateTableStats at datashard " << TabletID());

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

    if (cpuPercent > CpuUsageReportThresholdPercent) {
        if (LastCpuWarnTime + TDuration::Seconds(CpuUsageReportIntervalSeconds) > now)
            return;

        LastCpuWarnTime = now;

        TStringBuilder names;
        ListTableNames(GetUserTables(), names);

        LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "CPU usage " << cpuPercent
                    << " is higher than threshold of " << (i64)CpuUsageReportThresholdPercent
                    << " in-flight Tx: " << TxInFly()
                    << " immediate Tx: " << ImmediateInFly()
                    << " readIterators: " << ReadIteratorsInFly()
                    << " at datashard: " << TabletID()
                    << " table: " << names);
    }
}

}}
