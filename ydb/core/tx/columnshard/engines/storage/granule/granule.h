#pragma once
#include "portions_index.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/data_accessor/abstract/manager.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/index/index.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap {

namespace NLoading {
class TPortionsLoadContext;
}

class TGranulesStorage;
class TGranulesStat;
class TColumnChunkLoadContext;
class TVersionedIndex;

class TDataClassSummary: public NColumnShard::TBaseGranuleDataClassSummary {
private:
    friend class TGranuleMeta;

public:
    void AddPortion(const TPortionInfo& info) {
        ColumnPortionsSize += info.GetColumnBlobBytes();
        TotalPortionsSize += info.GetTotalBlobBytes();
        MetadataMemoryPortionsSize += info.GetMetadataMemorySize();
        RecordsCount += info.GetRecordsCount();
        ++PortionsCount;
    }

    void RemovePortion(const TPortionInfo& info) {
        MetadataMemoryPortionsSize -= info.GetMetadataMemorySize();
        Y_ABORT_UNLESS(MetadataMemoryPortionsSize >= 0);
        ColumnPortionsSize -= info.GetColumnBlobBytes();
        Y_ABORT_UNLESS(ColumnPortionsSize >= 0);
        TotalPortionsSize -= info.GetTotalBlobBytes();
        Y_ABORT_UNLESS(TotalPortionsSize >= 0);
        RecordsCount -= info.GetRecordsCount();
        Y_ABORT_UNLESS(RecordsCount >= 0);
        --PortionsCount;
        Y_ABORT_UNLESS(PortionsCount >= 0);
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
            , Owner(owner) {
        }

        ~TEditGuard() {
            Counters.OnPortionsDataRefresh(Owner.GetInserted(), Owner.GetCompacted());
        }

        void AddPortion(const TPortionInfo& info) {
            if (info.GetPortionType() == EPortionType::Written) {
                Owner.Inserted.AddPortion(info);
            } else {
                Owner.Compacted.AddPortion(info);
            }
        }
        void RemovePortion(const TPortionInfo& info) {
            if (info.GetPortionType() == EPortionType::Written) {
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
    TAtomic LastInsertWriteId = 1;
    THashMap<TInsertWriteId, std::shared_ptr<TWrittenPortionInfo>> InsertedPortions;
    THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>> InsertedPortionsById;
    THashMap<TInsertWriteId, TPortionDataAccessor> InsertedAccessors;
    mutable std::optional<TGranuleAdditiveSummary> AdditiveSummaryCache;

    void RebuildHardMetrics() const;
    void RebuildAdditiveMetrics() const;

    mutable bool AllowInsertionFlag = false;
    const TInternalPathId PathId;
    std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    const NColumnShard::TGranuleDataCounters Counters;
    NColumnShard::TEngineLogsCounters::TPortionsInfoGuard PortionInfoGuard;
    std::shared_ptr<TGranulesStat> Stats;
    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<NStorageOptimizer::IOptimizerPlanner> OptimizerPlanner;
    std::shared_ptr<NDataAccessorControl::IMetadataMemoryManager> MetadataMemoryManager;
    std::unique_ptr<NActualizer::TGranuleActualizationIndex> ActualizationIndex;
    mutable TInstant NextActualizations = TInstant::Zero();

    NGranule::NPortionsIndex::TPortionsIndex PortionsIndex;

    void OnBeforeChangePortion(const std::shared_ptr<TPortionInfo> portionBefore);
    void OnAfterChangePortion(const std::shared_ptr<TPortionInfo> portionAfter,
        NStorageOptimizer::IOptimizerPlanner::TModificationGuard* modificationGuard, const bool onLoad = false);
    void OnAdditiveSummaryChange() const;
    YDB_READONLY(TMonotonic, LastCompactionInstant, TMonotonic::Zero());

    TConclusion<std::shared_ptr<TPortionInfo>> GetInnerPortion(const TPortionInfo::TConstPtr& portion) const {
        if (!portion) {
            return TConclusionStatus::Fail("empty input portion pointer");
        }
        auto it = Portions.find(portion->GetPortionId());
        if (it == Portions.end()) {
            return TConclusionStatus::Fail("portion id is incorrect: " + ::ToString(portion->GetPortionId()));
        }
        if (portion->GetPathId() != GetPathId()) {
            return TConclusionStatus::Fail(
                "portion path_id is incorrect: " + ::ToString(portion->GetPathId()) + " != " + ::ToString(GetPathId()));
        }
        return it->second;
    }
    bool DataAccessorConstructed = false;

public:
    std::vector<TCSMetadataRequest> CollectMetadataRequests() {
        return ActualizationIndex->CollectMetadataRequests(Portions);
    }

    TInsertWriteId BuildNextInsertWriteId() {
        return (TInsertWriteId)AtomicIncrement(LastInsertWriteId);
    }

    const NStorageOptimizer::IOptimizerPlanner& GetOptimizerPlanner() const {
        return *OptimizerPlanner;
    }

    std::shared_ptr<ITxReader> BuildLoader(const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector, const TVersionedIndex& vIndex);
    bool TestingLoad(IDbWrapper& db, const TVersionedIndex& versionedIndex);
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& GetDataAccessorsManager() const {
        return DataAccessorsManager;
    }

    void RefreshTiering(const std::optional<TTiering>& tiering) {
        NActualizer::TAddExternalContext context(HasAppData() ? AppDataVerified().TimeProvider->Now() : TInstant::Now(), Portions);
        ActualizationIndex->RefreshTiering(tiering, context);
    }

    template <class TModifier>
    void ModifyPortionOnExecute(
        IDbWrapper& wrapper, const TPortionDataAccessor& portion, const TModifier& modifier, const ui32 firstPKColumnId) const {
        const auto innerPortion = GetInnerPortion(portion.GetPortionInfoPtr()).DetachResult();
        AFL_VERIFY((ui64)innerPortion.get() == (ui64)&portion.GetPortionInfo());
        auto copy = innerPortion->MakeCopy();
        modifier(*copy);
        if (!HasAppData() || AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
            auto accessorCopy = portion.SwitchPortionInfo(std::move(copy));
            accessorCopy.SaveToDatabase(wrapper, firstPKColumnId, false);
        } else {
            wrapper.WritePortion(portion.GetBlobIds(), *copy);
        }
    }

    template <class TModifier>
    void ModifyPortionOnComplete(const TPortionInfo::TConstPtr& portion, const TModifier& modifier) {
        const auto innerPortion = GetInnerPortion(portion).DetachResult();
        AFL_VERIFY((ui64)innerPortion.get() == (ui64)portion.get());
        OnBeforeChangePortion(innerPortion);
        modifier(innerPortion);
        OnAfterChangePortion(innerPortion, nullptr);
    }

    void InsertPortionOnExecute(
        NTabletFlatExecutor::TTransactionContext& txc, const TPortionDataAccessor& portion, const ui64 firstPKColumnId) const;
    void InsertPortionOnComplete(const TPortionDataAccessor& portion, IColumnEngine& engine);

    void CommitPortionOnExecute(
        NTabletFlatExecutor::TTransactionContext& txc, const TInsertWriteId insertWriteId, const TSnapshot& snapshot) const;
    void CommitPortionOnComplete(const TInsertWriteId insertWriteId, IColumnEngine& engine);

    void AbortPortionOnExecute(
        NTabletFlatExecutor::TTransactionContext& txc, const TInsertWriteId insertWriteId, const TSnapshot ssRemove) const {
        auto it = InsertedPortions.find(insertWriteId);
        AFL_VERIFY(it != InsertedPortions.end());
        AFL_VERIFY(InsertedPortionsById.contains(it->second->GetPortionId()));
        it->second->SetCommitSnapshot(ssRemove);
        it->second->SetRemoveSnapshot(ssRemove);
        TDbWrapper wrapper(txc.DB, nullptr);
        it->second->CommitToDatabase(wrapper);
    }

    void AbortPortionOnComplete(const TInsertWriteId insertWriteId, IColumnEngine& engine) {
        CommitPortionOnComplete(insertWriteId, engine);
    }

    void CommitImmediateOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TSnapshot& snapshot, const TPortionDataAccessor& portion,
        const ui64 firstPKColumnId) const;
    void CommitImmediateOnComplete(const std::shared_ptr<TPortionInfo> portion, IColumnEngine& engine);

    std::vector<NStorageOptimizer::TTaskDescription> GetOptimizerTasksDescription() const {
        return OptimizerPlanner->GetTasksDescription();
    }

    void ResetOptimizer(const std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor>& constructor,
        std::shared_ptr<IStoragesManager>& storages, const std::shared_ptr<arrow::Schema>& pkSchema);
    void ResetAccessorsManager(const std::shared_ptr<NDataAccessorControl::IManagerConstructor>& constructor,
        const NDataAccessorControl::TManagerConstructionContext& context);

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

    std::shared_ptr<TColumnEngineChanges> GetOptimizationTask(
        std::shared_ptr<TGranuleMeta> self, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
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
            OnAfterChangePortion(i.second, &g, true);
        }
        if (MetadataMemoryManager->NeedPrefetch() && Portions.size()) {
            auto request = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::FETCH_ON_LOAD);
            for (auto&& p : Portions) {
                request->AddPortion(p.second);
            }
            request->RegisterSubscriber(std::make_shared<TFakeDataAccessorsSubscriber>());

            DataAccessorsManager->AskData(request);
        }
        if (ActualizationIndex->IsStarted()) {
            RefreshScheme();
        }
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

    void AppendPortion(const TPortionDataAccessor& info);
    void AppendPortion(const std::shared_ptr<TPortionInfo>& info);

    TString DebugString() const {
        return TStringBuilder() << "(granule:" << GetPathId() << ";"
                                << "path_id:" << GetPathId() << ";"
                                << "size:" << GetAdditiveSummary().GetGranuleSize() << ";"
                                << "portions_count:" << Portions.size() << ";"
                                << ")";
    }

    void UpsertPortionOnLoad(const std::shared_ptr<TPortionInfo>& portion);

    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }

    const THashMap<TInsertWriteId, std::shared_ptr<TWrittenPortionInfo>>& GetInsertedPortions() const {
        return InsertedPortions;
    }

    const std::shared_ptr<TWrittenPortionInfo>& GetInsertedPortionVerifiedPtr(const TInsertWriteId portionId) const {
        auto it = InsertedPortions.find(portionId);
        AFL_VERIFY(it != InsertedPortions.end());
        return it->second;
    }

    std::vector<std::shared_ptr<TPortionInfo>> GetPortionsVector() const {
        std::vector<std::shared_ptr<TPortionInfo>> result;
        for (auto&& i : Portions) {
            result.emplace_back(i.second);
        }
        return result;
    }

    TInternalPathId GetPathId() const {
        return PathId;
    }

    const TPortionInfo& GetPortionVerified(const ui64 portion) const {
        auto it = Portions.find(portion);
        AFL_VERIFY(it != Portions.end())("portion_id", portion)("count", Portions.size());
        return *it->second;
    }

    TPortionInfo::TPtr GetPortionVerifiedPtr(const ui64 portion, const bool committedOnly = true) const {
        {
            auto it = Portions.find(portion);
            if (it != Portions.end()) {
                return it->second;
            }
        }
        AFL_VERIFY(!committedOnly);
        {
            auto it = InsertedPortionsById.find(portion);
            AFL_VERIFY(it != InsertedPortionsById.end());
            return it->second;
        }
    }

    std::shared_ptr<TPortionInfo> GetPortionOptional(const ui64 portion) const {
        auto it = Portions.find(portion);
        if (it == Portions.end()) {
            return nullptr;
        }
        return it->second;
    }

    bool ErasePortion(const ui64 portion);

    explicit TGranuleMeta(const TInternalPathId pathId, const TGranulesStorage& owner, const NColumnShard::TGranuleDataCounters& counters,
        const TVersionedIndex& versionedIndex);

    bool Empty() const noexcept {
        return Portions.empty();
    }
};

}   // namespace NKikimr::NOlap
