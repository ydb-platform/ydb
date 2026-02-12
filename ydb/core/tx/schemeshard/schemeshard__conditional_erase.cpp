#include "schemeshard_impl.h"
#include "schemeshard__conditional_erase.h"

#include <util/string/join.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr {
namespace NSchemeShard {

std::function<void (const TInstant, const THashMap<TPathId, TCondEraseAffectedTable>&)> CondEraseTestObserver;

using namespace NTabletFlatExecutor;

namespace {

    std::tuple<TTableInfo::TPtr, TPathId, TShardIdx> ResolveInfo(const TSchemeShard* self, TTabletId tabletId) {
        const auto shardIdx = self->GetShardIdx(tabletId);
        if (!self->ShardInfos.contains(shardIdx)) {
            return std::make_tuple(nullptr, InvalidPathId, InvalidShardIdx);
        }

        const auto& pathId = self->ShardInfos.at(shardIdx).PathId;
        if (!self->TTLEnabledTables.contains(pathId)) {
            return std::make_tuple(nullptr, InvalidPathId, InvalidShardIdx);
        }

        auto tableInfo = self->TTLEnabledTables.at(pathId);
        if (!tableInfo->IsTTLEnabled()) {
            return std::make_tuple(nullptr, InvalidPathId, InvalidShardIdx);
        }

        return std::make_tuple(tableInfo, pathId, shardIdx);
    }

} // anonymous

struct TSchemeShard::TTxRunConditionalErase: public TSchemeShard::TRwTxBase {
    TTableInfo::TPtr TableInfo;
    TPathId TablePathId;
    THashMap<TTabletId, NKikimrTxDataShard::TEvConditionalEraseRowsRequest> RunOnTablets;

    TTxRunConditionalErase(TSelf *self, TEvPrivate::TEvRunConditionalErase::TPtr& ev)
        : TRwTxBase(self)
        , TableInfo(nullptr)
    {
        Y_UNUSED(ev);
    }

    TTxRunConditionalErase(TSelf *self, TTableInfo::TPtr tableInfo, TPathId tablePathId)
        : TRwTxBase(self)
        , TableInfo(tableInfo)
        , TablePathId(tablePathId)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_RUN_CONDITIONAL_ERASE;
    }

    void DoExecute(TTransactionContext&, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxRunConditionalErase DoExecute"
            << ": at schemeshard: " << Self->TabletID());

        if (!Self->AllowConditionalEraseOperations) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Conditional erase operations are not allowed"
                << ", skip TTxRunConditionalErase"
                << ": at schemeshard: " << Self->TabletID());
            return;
        }

        if (!TableInfo) {
            for (const auto& [tablePathId, tableInfo] : Self->TTLEnabledTables) {
                DoExecuteOnTable(tableInfo, tablePathId, ctx);
            }
        } else {
            DoExecuteOnTable(TableInfo, TablePathId, ctx);
        }
    }

    void DoExecuteOnTable(TTableInfo::TPtr tableInfo, const TPathId tablePathId, const TActorContext& ctx) {
        if (!tableInfo->IsTTLEnabled()) {
            LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTL is not enabled for table #P1"
                << ", at schemeshard: " << Self->TabletID());
            return;
        }

        {
            auto path = Self->PathsById.at(tablePathId);
            if (path->Dropped()) {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Table is dropped"
                    << ", path: " << TPath::Init(tablePathId, Self).PathString()
                    << ", pathId: " << tablePathId
                    << ", at schemeshard: " << Self->TabletID()
                );
                return;
            }
        }
        {
            const auto checkedTable = Self->Tables.FindPtr(tablePathId);
            if (!checkedTable) {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve table"
                    << ", path: " << TPath::Init(tablePathId, Self).PathString()
                    << ", pathId: " << tablePathId
                    << ", at schemeshard: " << Self->TabletID()
                );
                return;
            }
            Y_ASSERT(*checkedTable == tableInfo);
        }

        const auto& settings = tableInfo->TTLSettings().GetEnabled();
        const auto expireAfter = GetExpireAfter(settings, true);
        if (expireAfter.IsFail()) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Invalid TTL settings: " << expireAfter.GetErrorMessage()
                << ", path: " << TPath::Init(tablePathId, Self).PathString()
                << ", pathId: " << tablePathId
                << ", at schemeshard: " << Self->TabletID()
            );
            return;
        }

        // table-level MaxShardsInFlight overrides database-level MaxTTLShardsInFlight
        const auto& sysSettings = tableInfo->TTLSettings().GetEnabled().GetSysSettings();
        const auto maxInFlight = (sysSettings.HasMaxShardsInFlight()
            ? sysSettings.GetMaxShardsInFlight()
            : Self->MaxTTLShardsInFlight
        );

        while (true) {
            if (maxInFlight && tableInfo->GetInFlightCondErase().size() >= maxInFlight) {
                break;
            }

            const auto* tableShardInfo = tableInfo->GetScheduledCondEraseShard();
            if (!tableShardInfo) {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxRunConditionalErase: no more scheduled shards"
                    << ", path: " << TPath::Init(tablePathId, Self).PathString()
                    << ", pathId: " << tablePathId
                    << ", at schemeshard: " << Self->TabletID()
                );
                break;
            }

            if (!DoExecuteOnShard(tableInfo, *tableShardInfo, settings, *expireAfter, ctx)) {
                break;
            }

            tableInfo->AddInFlightCondErase(tableShardInfo->ShardIdx);
        }
    }

    bool DoExecuteOnShard(
        TTableInfo::TPtr tableInfo,
        const TTableShardInfo& tableShardInfo,
        const NKikimrSchemeOp::TTTLSettings::TEnabled& settings,
        const TDuration expireAfter,
        const TActorContext& ctx
    ) {
        auto logContext = [this, &tableShardInfo](const TInstant& now) {
            const auto shardInfo = Self->ShardInfos.FindPtr(tableShardInfo.ShardIdx);

            return TStringBuilder()
                << ", table: " << (shardInfo ? TPath::Init(shardInfo->PathId, Self).PathString() : "unknown")
                << ", pathId: " << (shardInfo ? shardInfo->PathId : InvalidPathId)
                << ", shardIdx: " << tableShardInfo.ShardIdx
                << ", tabletId: " << (shardInfo ? shardInfo->TabletID : InvalidTabletId)
                << ", last: now - " << (now - tableShardInfo.LastCondErase)
                << ", next: now + " << (tableShardInfo.NextCondErase - now)
                << ", now: " << now
                << ", at schemeshard: " << Self->TabletID()
            ;
        };

        const auto now = ctx.Now();

        if (tableShardInfo.NextCondErase > now) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Skip conditional erase" << logContext(now));
            return false;
        }

        if (!Self->ShardInfos.contains(tableShardInfo.ShardIdx)) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info" << logContext(now));
            return false;
        }

        const TShardInfo& shardInfo = Self->ShardInfos.at(tableShardInfo.ShardIdx);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxRunConditionalErase DoExecuteOnShard" << logContext(now));

        const TInstant wallClock = now - expireAfter;

        NKikimrTxDataShard::TEvConditionalEraseRowsRequest request;
        request.SetDatabaseName(CanonizePath(Self->RootPathElements));
        request.SetTableId(shardInfo.PathId.LocalPathId);
        request.SetSchemaVersion(tableInfo->AlterVersion);

        for (const auto& [indexId, columnIds]: MakeIndexes(shardInfo.PathId)) {
            auto& index = *request.MutableIndexes()->Add();

            index.SetOwnerId(indexId.PathId.OwnerId);
            index.SetPathId(indexId.PathId.LocalPathId);
            index.SetSchemaVersion(indexId.SchemaVersion);

            for (const auto& [indexColumnId, mainColumnId] : columnIds) {
                auto& keyMap = *index.MutableKeyMap()->Add();
                keyMap.SetIndexColumnId(indexColumnId);
                keyMap.SetMainColumnId(mainColumnId);
            }
        }

        request.MutableExpiration()->SetColumnId(tableInfo->GetTTLColumnId());
        request.MutableExpiration()->SetWallClockTimestamp(wallClock.GetValue());
        request.MutableExpiration()->SetColumnUnit(settings.GetColumnUnit());

        const auto& sysSettings = settings.GetSysSettings();
        request.MutableLimits()->SetBatchMaxBytes(sysSettings.GetBatchMaxBytes());
        request.MutableLimits()->SetBatchMinKeys(sysSettings.GetBatchMinKeys());
        request.MutableLimits()->SetBatchMaxKeys(sysSettings.GetBatchMaxKeys());

        RunOnTablets.emplace(shardInfo.TabletID, std::move(request));

        return true;
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxRunConditionalErase DoComplete"
            << ": at schemeshard: " << Self->TabletID());

        for (auto& kv : RunOnTablets) {
            const auto& tabletId = kv.first;
            auto& request = kv.second;

            const auto [tableInfo, tablePathId, shardIdx] = ResolveInfo(Self, tabletId);
            if (!tableInfo || tablePathId == InvalidPathId || shardIdx == InvalidShardIdx) {
                Y_DEBUG_ABORT("Unreachable");
                continue;
            }

            auto& inFlight = tableInfo->GetInFlightCondErase();
            auto it = inFlight.find(shardIdx);
            if (it == inFlight.end()) {
                continue;
            }

            auto ev = MakeHolder<TEvDataShard::TEvConditionalEraseRowsRequest>();
            ev->Record = std::move(request);
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Run conditional erase"
                << ", tabletId: " << tabletId
                << ", request: " << ev->Record.ShortDebugString()
                << ", at schemeshard: " << Self->TabletID());
            it->second = Self->PipeClientCache->Send(ctx, ui64(tabletId), ev.Release());
        }

        if (!TableInfo) {
            Self->ScheduleConditionalEraseRun(ctx);
        }
    }

private:
    THashMap<TTableId, TVector<std::pair<ui32, ui32>>> MakeIndexes(const TPathId& mainPathId) const {
        THashMap<TTableId, TVector<std::pair<ui32, ui32>>> result;

        Y_ABORT_UNLESS(Self->PathsById.contains(mainPathId));
        auto mainPath = Self->PathsById.at(mainPathId);

        Y_ABORT_UNLESS(Self->Tables.contains(mainPathId));
        auto mainTable = Self->Tables.at(mainPathId);

        for (const auto& [_, childPathId] : mainPath->GetChildren()) {
            Y_ABORT_UNLESS(Self->PathsById.contains(childPathId));
            auto childPath = Self->PathsById.at(childPathId);

            if (!childPath->IsTableIndex() || childPath->Dropped()) {
                continue;
            }

            auto index = GetIndex(childPath);
            if (index->Type == NKikimrSchemeOp::EIndexTypeGlobalAsync
                || !DoesIndexSupportTTL(index->Type)) {
                continue;
            }

            auto [indexImplTableId, indexImplTable] = GetIndexImplTable(childPath);

            auto ret = result.emplace(
                TTableId(indexImplTableId, indexImplTable->AlterVersion),
                MakeColumnIds(mainTable, index, indexImplTable)
            );
            Y_ABORT_UNLESS(ret.second);
        }

        return result;
    }

    TTableIndexInfo::TPtr GetIndex(TPathElement::TPtr indexPath) const {
        Y_ABORT_UNLESS(Self->Indexes.contains(indexPath->PathId));
        return Self->Indexes.at(indexPath->PathId);
    }

    std::pair<TPathId, TTableInfo::TPtr> GetIndexImplTable(TPathElement::TPtr indexPath) const {
        Y_ABORT_UNLESS(indexPath->GetChildren().size() == 1);

        for (const auto& [_, indexImplPathId] : indexPath->GetChildren()) {
            auto childPath = Self->PathsById.at(indexImplPathId);

            Y_ABORT_UNLESS(!childPath->Dropped());

            Y_ABORT_UNLESS(Self->Tables.contains(indexImplPathId));
            return std::make_pair(indexImplPathId, Self->Tables.at(indexImplPathId));
        }

        Y_ABORT("Unreachable");
    }

    static TVector<std::pair<ui32, ui32>> MakeColumnIds(TTableInfo::TPtr mainTable, TTableIndexInfo::TPtr index, TTableInfo::TPtr indexImplTable) {
        Y_ABORT_UNLESS(DoesIndexSupportTTL(index->Type));
        TVector<std::pair<ui32, ui32>> result;
        THashSet<TString> keys;

        const auto mainColumns = MakeColumnNameToId(mainTable->Columns);
        const auto indexImplColumns = MakeColumnNameToId(indexImplTable->Columns);

        for (const TString& indexKey : index->IndexKeys) {
            Y_ABORT_UNLESS(mainColumns.contains(indexKey));
            Y_ABORT_UNLESS(indexImplColumns.contains(indexKey));

            result.emplace_back(std::make_pair(indexImplColumns.at(indexKey), mainColumns.at(indexKey)));
            keys.insert(indexKey);
        }

        for (const ui32 mainColumnId : mainTable->KeyColumnIds) {
            auto it = mainTable->Columns.find(mainColumnId);
            Y_ABORT_UNLESS(it != mainTable->Columns.end());
            const TString& mainKey = it->second.Name;

            if (keys.contains(mainKey)) {
                continue;
            }

            Y_ABORT_UNLESS(indexImplColumns.contains(mainKey));
            result.emplace_back(std::make_pair(indexImplColumns.at(mainKey), mainColumnId));
        }

        return result;
    }

    static THashMap<TString, ui32> MakeColumnNameToId(const TMap<ui32, TTableInfo::TColumn>& columns) {
        THashMap<TString, ui32> result;

        for (const auto& [id, column] : columns) {
            if (column.IsDropped()) {
                continue;
            }

            auto ret = result.emplace(column.Name, id);
            Y_ABORT_UNLESS(ret.second);
        }

        return result;
    }

}; // TTxRunConditionalErase

struct TSchemeShard::TTxScheduleConditionalErase : public TTransactionBase<TSchemeShard> {
    TVector<TEvDataShard::TEvConditionalEraseRowsResponse::TPtr> Responses;
    TInstant BatchStartTime;
    TVector<THolder<NSysView::TEvSysView::TEvUpdateTtlStats>> StatsCollectorEvents;

    THashMap<TPathId, TCondEraseAffectedTable> AffectedTables;

    TTxScheduleConditionalErase(TSelf* self, TVector<TEvDataShard::TEvConditionalEraseRowsResponse::TPtr>&& responses, const TInstant batchStartTime)
        : TBase(self)
        , Responses(std::move(responses))
        , BatchStartTime(batchStartTime)
    {
    }

    virtual ~TTxScheduleConditionalErase() = default;

    TTxType GetTxType() const override {
        return TXTYPE_SCHEDULE_CONDITIONAL_ERASE;
    }

    TDuration ProcessCondEraseResponse(
        const TActorContext& ctx,
        TPathId tablePathId,
        TTableInfo::TPtr tableInfo,
        TShardIdx shardIdx,
        const NKikimrTxDataShard::TEvConditionalEraseRowsResponse& record,
        TInstant now
    ) {
        const auto& sysSettings = tableInfo->TTLSettings().GetEnabled().GetSysSettings();
        TDuration next = TDuration::FromValue(sysSettings.GetRunInterval());

        switch (record.GetStatus()) {
            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ACCEPTED:
            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::PARTIAL:
                // Both cases are dead code here.
                // Both do not affect state in any way and both processed at the event handler
                // to avoid wasting a local transaction on a no-op.
                //TODO: remember progress for PARTIAL (and until that, PARTIAL will not affect state)
                return next;

            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::OK:
                LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Successful conditional erase"
                    << ": tabletId: " << record.GetTabletID()
                    << ", at schemeshard: " << Self->TabletID());
                break;

            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::BAD_REQUEST:
            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ABORTED:
            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ERASE_ERROR:
            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::OVERLOADED:
            case NKikimrTxDataShard::TEvConditionalEraseRowsResponse::SCHEME_ERROR:
                next = TDuration::FromValue(sysSettings.GetRetryInterval());
                LOG_ERROR_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unsuccessful conditional erase"
                    << ": tabletId: " << record.GetTabletID()
                    << ", status: " << NKikimrTxDataShard::TEvConditionalEraseRowsResponse_EStatus_Name(record.GetStatus())
                    << ", error: " << record.GetErrorDescription()
                    << ", retry after: " << next
                    << ", at schemeshard: " << Self->TabletID());
                break;
        }
        if (!NKikimrTxDataShard::TEvConditionalEraseRowsResponse_EStatus_IsValid(record.GetStatus())) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unknown conditional erase status"
                << ": tabletId: " << record.GetTabletID()
                << ", status: " << static_cast<ui32>(record.GetStatus())
                << ", error: " << record.GetErrorDescription()
                << ", at schemeshard: " << Self->TabletID()
            );
        }

        auto statsEv = MakeHolder<NSysView::TEvSysView::TEvUpdateTtlStats>(
            Self->GetDomainKey(tablePathId), tablePathId, std::make_pair(ui64(shardIdx.GetOwnerId()), ui64(shardIdx.GetLocalId()))
        );

        auto& stats = statsEv->Stats;
        stats.SetLastRunTime(now.MilliSeconds());
        stats.SetLastRowsProcessed(record.GetStats().GetRowsProcessed());
        stats.SetLastRowsErased(record.GetStats().GetRowsErased());

        StatsCollectorEvents.push_back(std::move(statsEv));

        return next;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxScheduleConditionalErase Execute"
            << ": responses: " << Responses.size()
            << ", at schemeshard: " << Self->TabletID());

        if (!Self->AllowConditionalEraseOperations) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Conditional erase operations are not allowed"
                << ", skip TTxScheduleConditionalErase"
                << ", at schemeshard: " << Self->TabletID());
            return true;
        }

        const auto now = ctx.Now();

        for (const auto& ev : Responses) {
            const auto& record = ev->Get()->Record;
            const TTabletId tabletId(record.GetTabletID());
            const auto [tableInfo, tablePathId, shardIdx] = ResolveInfo(Self, tabletId);

            if (!tableInfo || tablePathId == InvalidPathId || shardIdx == InvalidShardIdx) {
                LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve info"
                    << ": tabletId: " << tabletId
                    << ", at schemeshard: " << Self->TabletID());
                continue;
            }

            if (!tableInfo->GetInFlightCondErase().contains(shardIdx)) {
                continue;
            }

            const auto& shardToPartition = tableInfo->GetShard2PartitionIdx();
            Y_ABORT_UNLESS(shardToPartition.contains(shardIdx));
            const ui64 partitionIdx = shardToPartition.at(shardIdx);
            Y_ABORT_UNLESS(partitionIdx < tableInfo->GetPartitions().size());

            TDuration next = ProcessCondEraseResponse(ctx, tablePathId, tableInfo, shardIdx, record, now);

            // Track affected tables and shards
            {
                auto [it, _] = AffectedTables.try_emplace(tablePathId, TCondEraseAffectedTable{
                    .TableInfo = tableInfo
                });
                it->second.AffectedShards.emplace_back(TCondEraseAffectedShard{
                    .ShardIdx = shardIdx,
                    .PartitionIdx = partitionIdx,
                    .TabletId = tabletId,
                    .Next = next,
                });
            }
        }

        // update CondErase states
        for (const auto& item : AffectedTables | std::views::values) {
            const auto& tableInfo = item.TableInfo;
            for (const auto& i : item.AffectedShards) {
                {
                    const auto& lag = tableInfo->GetPartitions().at(i.PartitionIdx).LastCondEraseLag;
                    if (lag) {
                        Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].DecrementFor(lag->Seconds());
                    } else {
                        Y_DEBUG_ABORT_UNLESS(false);
                    }
                }

                // ScheduleNextCondErase (also) changes LastCondEraseLag
                tableInfo->ScheduleNextCondErase(i.ShardIdx, now, i.Next);

                {
                    const auto& lag = tableInfo->GetPartitions().at(i.PartitionIdx).LastCondEraseLag;
                    Y_ABORT_UNLESS(lag.Defined());
                    Self->TabletCounters->Percentile()[COUNTER_NUM_SHARDS_BY_TTL_LAG].IncrementFor(lag->Seconds());
                }
            }
        }

        // save CondErase states
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& [tablePathId, item] : AffectedTables) {
            const auto& tableInfo = item.TableInfo;
            const auto& affectedShards = item.AffectedShards;
            for (const auto& i : affectedShards) {
                Self->PersistTablePartitionCondErase(db, tablePathId, i.PartitionIdx, tableInfo);
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxScheduleConditionalErase Complete"
            << ": affected tables: " << AffectedTables.size()
            << ", at schemeshard: " << Self->TabletID());

        // Send stats events
        for (auto& ev : StatsCollectorEvents) {
            ctx.Send(Self->SysPartitionStatsCollector, ev.Release());
        }

        // Report batch to the observer, if registered
        if (CondEraseTestObserver) {
            CondEraseTestObserver(BatchStartTime, AffectedTables);
        }

        // Trigger one TTxRunConditionalErase per affected table
        for (const auto& [tablePathId, item] : AffectedTables) {
            Self->Execute(new TTxRunConditionalErase(Self, item.TableInfo, tablePathId), ctx);
        }
    }

}; // TTxScheduleConditionalErase

ITransaction* TSchemeShard::CreateTxRunConditionalErase(TEvPrivate::TEvRunConditionalErase::TPtr& ev) {
    return new TTxRunConditionalErase(this, ev);
}

ITransaction* TSchemeShard::CreateTxScheduleConditionalErase(TVector<TEvDataShard::TEvConditionalEraseRowsResponse::TPtr>&& responses, const TInstant batchStartTime) {
    return new TTxScheduleConditionalErase(this, std::move(responses), batchStartTime);
}

void TSchemeShard::ConditionalEraseHandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    auto [tableInfo, tablePathId, shardIdx] = ResolveInfo(this, tabletId);
    if (!tableInfo || tablePathId == InvalidPathId || shardIdx == InvalidShardIdx) {
        return;
    }

    const auto& inFlight = tableInfo->GetInFlightCondErase();
    auto it = inFlight.find(shardIdx);
    if (it == inFlight.end() || it->second != clientId) {
        return;
    }

    tableInfo->RescheduleCondErase(shardIdx);
    Execute(new TTxRunConditionalErase(this, tableInfo, tablePathId), ctx);
}

} // NSchemeShard
} // NKikimr
