#pragma once
#include "portions_index.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/index/index.h>

#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/reader/position.h>

namespace NKikimr::NOlap {

class TGranulesStorage;
class TGranulesStat;
class TColumnChunkLoadContext;

class TDataClassSummary: public NColumnShard::TBaseGranuleDataClassSummary {
private:
    friend class TGranuleMeta;
    THashMap<ui32, NArrow::NSplitter::TSimpleSerializationStat> ColumnStats;

public:
    const THashMap<ui32, NArrow::NSplitter::TSimpleSerializationStat>& GetColumnStats() const {
        return ColumnStats;
    }

    void AddPortion(const TPortionInfo& info) {
        ColumnPortionsSize += info.GetColumnBlobBytes();
        TotalPortionsSize += info.GetTotalBlobBytes();
        MetadataMemoryPortionsSize += info.GetMetadataMemorySize();
        RecordsCount += info.NumRows();
        ++PortionsCount;

        for (auto&& c : info.Records) {
            auto it = ColumnStats.find(c.ColumnId);
            if (it == ColumnStats.end()) {
                it = ColumnStats.emplace(c.ColumnId, c.GetSerializationStat()).first;
            } else {
                it->second.AddStat(c.GetSerializationStat());
            }
        }
    }

    void RemovePortion(const TPortionInfo& info) {
        MetadataMemoryPortionsSize -= info.GetMetadataMemorySize();
        Y_ABORT_UNLESS(MetadataMemoryPortionsSize >= 0);
        ColumnPortionsSize -= info.GetColumnBlobBytes();
        Y_ABORT_UNLESS(ColumnPortionsSize >= 0);
        TotalPortionsSize -= info.GetTotalBlobBytes();
        Y_ABORT_UNLESS(TotalPortionsSize >= 0);
        RecordsCount -= info.NumRows();
        Y_ABORT_UNLESS(RecordsCount >= 0);
        --PortionsCount;
        Y_ABORT_UNLESS(PortionsCount >= 0);

        for (auto&& c : info.Records) {
            auto it = ColumnStats.find(c.ColumnId);
            if (it == ColumnStats.end()) {
                it = ColumnStats.emplace(c.ColumnId, c.GetSerializationStat()).first;
            } else {
                it->second.RemoveStat(c.GetSerializationStat());
            }
        }
    }
};

class TGranuleAdditiveSummary {
private:
    TDataClassSummary Inserted;
    TDataClassSummary Compacted;
    friend class TGranuleMeta;
public:
    const TDataClassSummary& GetInserted() const {
        return Inserted;
    }
    const TDataClassSummary& GetCompacted() const {
        return Compacted;
    }
    ui64 GetMetadataMemoryPortionsSize() const {
        return Inserted.GetMetadataMemoryPortionsSize() + Compacted.GetMetadataMemoryPortionsSize();
    }
    ui64 GetGranuleSize() const {
        return Inserted.GetTotalPortionsSize() + Compacted.GetTotalPortionsSize();
    }
    ui64 GetActivePortionsCount() const {
        return Inserted.GetPortionsCount() + Compacted.GetPortionsCount();
    }

    class TEditGuard: TNonCopyable {
    private:
        const NColumnShard::TGranuleDataCounters& Counters;
        TGranuleAdditiveSummary& Owner;
    public:
        TEditGuard(const NColumnShard::TGranuleDataCounters& counters, TGranuleAdditiveSummary& owner)
            : Counters(counters)
            , Owner(owner)
        {

        }

        ~TEditGuard() {
            Counters.OnPortionsDataRefresh(Owner.GetInserted(), Owner.GetCompacted());
        }

        void AddPortion(const TPortionInfo& info) {
            if (info.GetMeta().GetProduced() == NPortion::EProduced::INSERTED) {
                Owner.Inserted.AddPortion(info);
            } else {
                Owner.Compacted.AddPortion(info);
            }
        }
        void RemovePortion(const TPortionInfo& info) {
            if (info.GetMeta().GetProduced() == NPortion::EProduced::INSERTED) {
                Owner.Inserted.RemovePortion(info);
            } else {
                Owner.Compacted.RemovePortion(info);
            }
        }
    };

    TEditGuard StartEdit(const NColumnShard::TGranuleDataCounters& counters) {
        return TEditGuard(counters, *this);
    }

    TString DebugString() const {
        return TStringBuilder() << "inserted:(" << Inserted.DebugString() << ");other:(" << Compacted.DebugString() << "); ";
    }
};

class TGranuleMeta: TNonCopyable {
private:
    TMonotonic ModificationLastTime = TMonotonic::Now();
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    THashMap<TInsertWriteId, std::shared_ptr<TPortionInfo>> InsertedPortions;
    mutable std::optional<TGranuleAdditiveSummary> AdditiveSummaryCache;

    void RebuildHardMetrics() const;
    void RebuildAdditiveMetrics() const;

    mutable bool AllowInsertionFlag = false;
    const ui64 PathId;
    const NColumnShard::TGranuleDataCounters Counters;
    NColumnShard::TEngineLogsCounters::TPortionsInfoGuard PortionInfoGuard;
    std::shared_ptr<TGranulesStat> Stats;
    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<NStorageOptimizer::IOptimizerPlanner> OptimizerPlanner;
    std::shared_ptr<NActualizer::TGranuleActualizationIndex> ActualizationIndex;
    mutable TInstant NextActualizations = TInstant::Zero();

    NGranule::NPortionsIndex::TPortionsIndex PortionsIndex;

    void OnBeforeChangePortion(const std::shared_ptr<TPortionInfo> portionBefore);
    void OnAfterChangePortion(const std::shared_ptr<TPortionInfo> portionAfter, NStorageOptimizer::IOptimizerPlanner::TModificationGuard* modificationGuard);
    void OnAdditiveSummaryChange() const;
    YDB_READONLY(TMonotonic, LastCompactionInstant, TMonotonic::Zero());
public:
    void RefreshTiering(const std::optional<TTiering>& tiering) {
        NActualizer::TAddExternalContext context(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now(), Portions);
        ActualizationIndex->RefreshTiering(tiering, context);
    }

    TConclusionStatus IsInnerPortion(const std::shared_ptr<TPortionInfo>& portion) const {
        if (!portion) {
            return TConclusionStatus::Fail("empty portion pointer");
        }
        auto it = Portions.find(portion->GetPortionId());
        if (it == Portions.end()) {
            return TConclusionStatus::Fail("portion id is incorrect: " + ::ToString(portion->GetPortionId()));
        }
        if (portion->GetPathId() != GetPathId()) {
            return TConclusionStatus::Fail("portion path_id is incorrect: " + ::ToString(portion->GetPathId()) + " != " + ::ToString(GetPathId()));
        }
        return TConclusionStatus::Success();
    }

    template <class TModifier>
    void ModifyPortionOnExecute(NTable::TDatabase& db, const std::shared_ptr<TPortionInfo>& portion, const TModifier& modifier) const {
        IsInnerPortion(portion).Validate("modify portion on execute");
        auto copy = *portion;
        modifier(copy);
        TDbWrapper wrapper(db, nullptr);
        copy.SaveToDatabase(wrapper, 0, true);
    }

    template <class TModifier>
    void ModifyPortionOnComplete(const std::shared_ptr<TPortionInfo>& portion, const TModifier& modifier) {
        IsInnerPortion(portion).Validate("modify portion on complete");
        OnBeforeChangePortion(portion);
        modifier(portion);
        OnAfterChangePortion(portion, nullptr);
    }

    void InsertPortionOnExecute(
        NTabletFlatExecutor::TTransactionContext& txc, const std::shared_ptr<TPortionInfo>& portion) const {
        AFL_VERIFY(!InsertedPortions.contains(portion->GetInsertWriteIdVerified()));
        TDbWrapper wrapper(txc.DB, nullptr);
        portion->SaveToDatabase(wrapper, 0, false);
    }

    void InsertPortionOnComplete(const std::shared_ptr<TPortionInfo>& portion) {
        AFL_VERIFY(InsertedPortions.emplace(portion->GetInsertWriteIdVerified(), portion).second);
    }

    void CommitPortionOnExecute(
        NTabletFlatExecutor::TTransactionContext& txc, const TInsertWriteId insertWriteId, const TSnapshot& snapshot) const {
        auto it = InsertedPortions.find(insertWriteId);
        AFL_VERIFY(it != InsertedPortions.end());
        it->second->SetCommitSnapshot(snapshot);
        TDbWrapper wrapper(txc.DB, nullptr);
        it->second->SaveToDatabase(wrapper, 0, true);
    }

    void CommitPortionOnComplete(const TInsertWriteId insertWriteId, IColumnEngine& engine);

    void AbortPortionOnExecute(
        NTabletFlatExecutor::TTransactionContext& txc, const TInsertWriteId insertWriteId) const {
        auto it = InsertedPortions.find(insertWriteId);
        AFL_VERIFY(it != InsertedPortions.end());
        TDbWrapper wrapper(txc.DB, nullptr);
        it->second->RemoveFromDatabase(wrapper);
    }

    void AbortPortionOnComplete(const TInsertWriteId insertWriteId) {
        AFL_VERIFY(InsertedPortions.erase(insertWriteId));
    }

    void CommitImmediateOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TSnapshot& snapshot,
        const std::shared_ptr<TPortionInfo>& portion) const;

    void CommitImmediateOnComplete(const std::shared_ptr<TPortionInfo> portion, IColumnEngine& engine);

    std::vector<NStorageOptimizer::TTaskDescription> GetOptimizerTasksDescription() const {
        return OptimizerPlanner->GetTasksDescription();
    }

    void ResetOptimizer(const std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor>& constructor, std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema);

    void RefreshScheme() {
        NActualizer::TAddExternalContext context(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now(), Portions);
        ActualizationIndex->RefreshScheme(context);
    }

    void ReturnToIndexes(const THashSet<ui64>& portionIds) {
        NActualizer::TAddExternalContext context(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now(), Portions);
        context.SetPortionExclusiveGuarantee(false);
        for (auto&& p : portionIds) {
            auto it = Portions.find(p);
            AFL_VERIFY(it != Portions.end());
            ActualizationIndex->AddPortion(it->second, context);
        }
    }

    void StartActualizationIndex() {
        ActualizationIndex->Start();
    }

    NJson::TJsonValue OptimizerSerializeToJson() const {
        return OptimizerPlanner->SerializeToJsonVisual();
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions() const {
        return OptimizerPlanner->GetBucketPositions();
    }

    void OnStartCompaction() {
        LastCompactionInstant = TMonotonic::Now();
    }

    void BuildActualizationTasks(NActualizer::TTieringProcessContext& context, const TDuration actualizationLag) const;

    std::shared_ptr<TColumnEngineChanges> GetOptimizationTask(std::shared_ptr<TGranuleMeta> self, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        return OptimizerPlanner->GetOptimizationTask(self, locksManager);
    }

    const NGranule::NPortionsIndex::TPortionsIndex& GetPortionsIndex() const {
        return PortionsIndex;
    }

    std::map<ui32, std::shared_ptr<TPortionInfo>> GetPortionsOlderThenSnapshot(const TSnapshot& border) const {
        std::map<ui32, std::shared_ptr<TPortionInfo>> result;
        for (auto&& i : Portions) {
            if (i.second->RecordSnapshotMin() <= border) {
                result.emplace(i.first, i.second);
            }
        }
        return result;
    }

    void OnAfterPortionsLoad() {
        auto g = OptimizerPlanner->StartModificationGuard();
        for (auto&& i : Portions) {
            OnAfterChangePortion(i.second, &g);
        }
    }

    std::shared_ptr<NArrow::NSplitter::TSerializationStats> BuildSerializationStats(ISnapshotSchema::TPtr schema) const {
        auto result = std::make_shared<NArrow::NSplitter::TSerializationStats>();
        for (auto&& i : GetAdditiveSummary().GetCompacted().GetColumnStats()) {
            auto field = schema->GetFieldByColumnIdVerified(i.first);
            NArrow::NSplitter::TColumnSerializationStat columnInfo(i.first, field->name());
            columnInfo.Merge(i.second);
            result->AddStat(columnInfo);
        }
        return result;
    }

    const TGranuleAdditiveSummary& GetAdditiveSummary() const;

    NStorageOptimizer::TOptimizationPriority GetCompactionPriority() const {
        return OptimizerPlanner->GetUsefulMetric();
    }

    void ActualizeOptimizer(const TInstant currentInstant, const TDuration recalcLag) const {
        if (OptimizerPlanner->GetActualizationInstant() + recalcLag < currentInstant) {
            OptimizerPlanner->Actualize(currentInstant);
        }
    }

    bool IsErasable() const {
        return Portions.empty();
    }

    void OnCompactionStarted();

    void OnCompactionFailed(const TString& reason);
    void OnCompactionFinished();

    void UpsertPortion(const TPortionInfo& info);

    TString DebugString() const {
        return TStringBuilder() << "(granule:" << GetPathId() << ";"
            << "path_id:" << GetPathId() << ";"
            << "size:" << GetAdditiveSummary().GetGranuleSize() << ";"
            << "portions_count:" << Portions.size() << ";"
            << ")"
            ;
    }

    std::shared_ptr<TPortionInfo> UpsertPortionOnLoad(TPortionInfo&& portion);

    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }

    const THashMap<TInsertWriteId, std::shared_ptr<TPortionInfo>>& GetInsertedPortions() const {
        return InsertedPortions;
    }

    std::vector<std::shared_ptr<TPortionInfo>> GetPortionsVector() const {
        std::vector<std::shared_ptr<TPortionInfo>> result;
        for (auto&& i : Portions) {
            result.emplace_back(i.second);
        }
        return result;
    }

    ui64 GetPathId() const {
        return PathId;
    }

    const TPortionInfo& GetPortionVerified(const ui64 portion) const {
        auto it = Portions.find(portion);
        AFL_VERIFY(it != Portions.end())("portion_id", portion)("count", Portions.size());
        return *it->second;
    }

    std::shared_ptr<TPortionInfo> GetPortionOptional(const ui64 portion) const {
        auto it = Portions.find(portion);
        if (it == Portions.end()) {
            return nullptr;
        }
        return it->second;
    }

    bool ErasePortion(const ui64 portion);

    explicit TGranuleMeta(const ui64 pathId, const TGranulesStorage& owner, const NColumnShard::TGranuleDataCounters& counters, const TVersionedIndex& versionedIndex);

    bool Empty() const noexcept { return Portions.empty(); }
};

} // namespace NKikimr::NOlap
