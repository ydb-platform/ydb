#pragma once
#include "counters.h"

#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>
#include <util/system/types.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLevels {

class TLevelInfo {
private:
    THashMap<ui64, i64> Counters;
    YDB_READONLY(i64, CriticalWeight, 0);
    YDB_READONLY(i64, NormalizedWeight, 0);
    THashSet<ui64> PortionIds;
    std::shared_ptr<TCounters> Signals;
public:
    TLevelInfo(std::shared_ptr<TCounters> counters)
        : Signals(counters)
    {

    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p, const ui32 refCount) {
        if (p->GetBlobBytes() < (1 << 20)) {
            Signals->OnAddSmallPortion();
        }
        auto it = Counters.find(p->GetPortion());
        i64 refCountPred = 0;
        if (it == Counters.end()) {
            it = Counters.emplace(p->GetPortion(), refCount).first;
        } else {
            refCountPred = it->second;
            it->second += refCount;
        }
        if (it->second == 1 && refCountPred == 0) {
            NormalizedWeight += p->NumRows();
            Signals->OnAddNormalCount(p->NumRows());
        } else if (it->second >= 2 && refCountPred == 1) {
            CriticalWeight += p->NumRows();
            Signals->OnAddCriticalCount(p->NumRows());

            NormalizedWeight -= p->NumRows();
            Y_ABORT_UNLESS(NormalizedWeight >= 0);

            Signals->OnRemoveNormalCount(p->NumRows());
        } else if (it->second >= 2 && refCountPred == 0) {
            CriticalWeight += p->NumRows();
            Signals->OnAddCriticalCount(p->NumRows());
        } else if (it->second >= 2 && refCountPred >= 2) {
        } else {
            Y_ABORT_UNLESS(false);
        }
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& p, const ui32 refCount) {
        if (p->GetBlobBytes() < (1 << 20)) {
            Signals->OnRemoveSmallPortion();
        }
        auto it = Counters.find(p->GetPortion());
        Y_ABORT_UNLESS(it != Counters.end());
        const i64 refCountPred = it->second;
        it->second -= refCount;
        Y_ABORT_UNLESS(it->second >= 0);
        if (it->second >= 2) {
        } else if (it->second == 1) {
            Y_ABORT_UNLESS(refCountPred >= 2);
            CriticalWeight -= p->NumRows();
            Y_ABORT_UNLESS(CriticalWeight >= 0);
            Signals->OnRemoveCriticalCount(p->NumRows());
            Y_ABORT_UNLESS(CriticalWeight >= 0);
            NormalizedWeight += p->NumRows();
            Signals->OnAddNormalCount(p->NumRows());
        } else if (it->second == 0) {
            if (refCountPred >= 2) {
                Y_ABORT_UNLESS(refCountPred >= 2);
                CriticalWeight -= p->NumRows();
                Y_ABORT_UNLESS(CriticalWeight >= 0);
                Signals->OnRemoveCriticalCount(p->NumRows());
            } else if (refCountPred == 1) {
                NormalizedWeight -= p->NumRows();
                Y_ABORT_UNLESS(NormalizedWeight >= 0);
                Signals->OnRemoveNormalCount(p->NumRows());
            } else {
                Y_ABORT_UNLESS(false);
            }
            Counters.erase(it);
        }
    }

};

class TBorderPoint {
public:
    using TBorderPortions = THashMap<ui64, std::shared_ptr<TPortionInfo>>;
private:
    THashMap<ui64, ui32> MiddleWeight;
    YDB_READONLY_DEF(TBorderPortions, StartPortions);
    YDB_READONLY_DEF(TBorderPortions, MiddlePortions);
    YDB_READONLY_DEF(TBorderPortions, FinishPortions);
    std::shared_ptr<TLevelInfo> LevelInfo;
public:
    void InitInternalPoint(const TBorderPoint& predPoint) {
        Y_ABORT_UNLESS(predPoint.MiddleWeight.size() == predPoint.MiddlePortions.size());
        for (auto&& i : predPoint.MiddlePortions) {
            auto it = predPoint.MiddleWeight.find(i.first);
            if (it->second != 2) {
                AddMiddle(i.second, 1);
            }
        }
    }

    std::shared_ptr<TPortionInfo> GetOnlyPortion() const {
        Y_ABORT_UNLESS(MiddlePortions.size() == 1);
        Y_ABORT_UNLESS(!IsCritical());
        return MiddlePortions.begin()->second;
    }

    bool IsSmall() const {
        if (!IsCritical() && MiddlePortions.size() == 1 && MiddlePortions.begin()->second->GetBlobBytes() < (1 << 20)) {
            return true;
        }
        return false;
    }

    bool IsCritical() const {
        if (StartPortions.size() && FinishPortions.size()) {
            return true;
        }
        if (MiddlePortions.size() > 1 || StartPortions.size() > 1 || FinishPortions.size() > 1) {
            return true;
        }
        return false;
    }

    TBorderPoint(const std::shared_ptr<TLevelInfo>& info)
        : LevelInfo(info) {

    }

    ~TBorderPoint() {
        for (auto&& i : MiddlePortions) {
            if (i.second->IndexKeyStart() == i.second->IndexKeyEnd()) {
                LevelInfo->RemovePortion(i.second, 2);
            } else {
                LevelInfo->RemovePortion(i.second, 1);
            }
        }
    }

    void AddStart(const std::shared_ptr<TPortionInfo>& p) {
        Y_ABORT_UNLESS(StartPortions.emplace(p->GetPortion(), p).second);
    }
    void RemoveStart(const std::shared_ptr<TPortionInfo>& p) {
        Y_ABORT_UNLESS(StartPortions.erase(p->GetPortion()));
    }

    void AddMiddle(const std::shared_ptr<TPortionInfo>& p, const ui32 portionCriticalWeight) {
        Y_ABORT_UNLESS(MiddleWeight.emplace(p->GetPortion(), portionCriticalWeight).second);
        Y_ABORT_UNLESS(MiddlePortions.emplace(p->GetPortion(), p).second);
        LevelInfo->AddPortion(p, portionCriticalWeight);
    }
    void RemoveMiddle(const std::shared_ptr<TPortionInfo>& p, const ui32 portionCriticalWeight) {
        Y_ABORT_UNLESS(MiddleWeight.erase(p->GetPortion()));
        Y_ABORT_UNLESS(MiddlePortions.erase(p->GetPortion()));
        LevelInfo->RemovePortion(p, portionCriticalWeight);
    }

    void AddFinish(const std::shared_ptr<TPortionInfo>& p) {
        Y_ABORT_UNLESS(FinishPortions.emplace(p->GetPortion(), p).second);
    }
    void RemoveFinish(const std::shared_ptr<TPortionInfo>& p) {
        Y_ABORT_UNLESS(FinishPortions.erase(p->GetPortion()));
    }

    bool IsEmpty() const {
        return StartPortions.empty() && FinishPortions.empty();
    }
};

class TPortionsPlacement {
private:
    THashSet<ui64> PortionIds;
    std::map<NArrow::TReplaceKey, TBorderPoint> Borders;
    std::shared_ptr<TLevelInfo> LevelInfo;
public:
    TPortionsPlacement(const std::shared_ptr<TLevelInfo>& levelInfo)
        : LevelInfo(levelInfo)
    {

    }

    class TPortionsScanner {
    private:
        THashMap<ui64, std::shared_ptr<TPortionInfo>> CurrentPortions;
        const std::shared_ptr<NDataLocks::TManager> DataLocksManager;
    public:

        TPortionsScanner(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager)
            : DataLocksManager(dataLocksManager)
        {

        }

        const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetCurrentPortions() const {
            return CurrentPortions;
        }

        bool AddBorderPoint(const TBorderPoint& p, bool& hasBusy) {
            hasBusy = false;
            for (auto&& [_, portionInfo] : p.GetStartPortions()) {
                if (DataLocksManager->IsLocked(*portionInfo)) {
                    hasBusy = true;
                    continue;
                }
                AFL_VERIFY(CurrentPortions.emplace(portionInfo->GetPortion(), portionInfo).second);
            }

            for (auto&& [_, portionInfo] : p.GetFinishPortions()) {
                if (DataLocksManager->IsLocked(*portionInfo)) {
                    continue;
                }
                AFL_VERIFY(CurrentPortions.erase(portionInfo->GetPortion()));
            }
            return CurrentPortions.size();
        }
    };

    enum class EChainProblem {
        NoProblem,
        SmallChunks,
        MergeChunks
    };

    std::vector<std::vector<std::shared_ptr<TPortionInfo>>> GetPortionsToCompact(const ui64 sizeLimit, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        std::vector<std::vector<std::shared_ptr<TPortionInfo>>> result;
        THashSet<ui64> readyPortionIds;
        ui64 resultSize = 0;

        TPortionsScanner buffer(locksManager);
        THashMap<ui64, std::shared_ptr<TPortionInfo>> portionsCurrentChain;
        ui64 chainSize = 0;
        EChainProblem problemType = EChainProblem::NoProblem;
        for (auto&& i : Borders) {
            bool hasBusy = false;
            if (!buffer.AddBorderPoint(i.second, hasBusy)) {
                if (hasBusy && problemType == EChainProblem::SmallChunks) {
                    chainSize = 0;
                    portionsCurrentChain.clear();
                    problemType = EChainProblem::NoProblem;
                } else if (chainSize > (1 << 20)) {
                    resultSize += chainSize;
                    std::vector<std::shared_ptr<TPortionInfo>> chain;
                    for (auto&& i : portionsCurrentChain) {
                        chain.emplace_back(i.second);
                    }
                    result.emplace_back(chain);
                    chainSize = 0;
                    portionsCurrentChain.clear();
                    problemType = EChainProblem::NoProblem;
                }
            } else {
                if (buffer.GetCurrentPortions().size() > 1) {
                    problemType = EChainProblem::MergeChunks;
                } else if (buffer.GetCurrentPortions().begin()->second->GetBlobBytes() < (1 << 20) && problemType == EChainProblem::NoProblem) {
                    problemType = EChainProblem::SmallChunks;
                }
                if (problemType != EChainProblem::NoProblem) {
                    for (auto&& i : buffer.GetCurrentPortions()) {
                        if (portionsCurrentChain.emplace(i.second->GetPortion(), i.second).second) {
                            chainSize += i.second->GetBlobBytes();
                        }
                    }
                }
            }
            if (resultSize + chainSize > sizeLimit) {
                break;
            }
        }
        if (portionsCurrentChain.size() > 1) {
            std::vector<std::shared_ptr<TPortionInfo>> chain;
            for (auto&& i : portionsCurrentChain) {
                chain.emplace_back(i.second);
            }
            result.emplace_back(chain);
        }

        return result;
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
        Y_ABORT_UNLESS(PortionIds.erase(portion->GetPortion()));
        auto itStart = Borders.find(portion->IndexKeyStart());
        AFL_VERIFY(itStart != Borders.end());
        auto itFinish = Borders.find(portion->IndexKeyEnd());
        AFL_VERIFY(itFinish != Borders.end());

        itStart->second.RemoveStart(portion);
        itFinish->second.RemoveFinish(portion);
        if (itStart != itFinish) {
            for (auto it = itStart; it != itFinish; ++it) {
                it->second.RemoveMiddle(portion, 1);
            }
            if (itFinish->second.IsEmpty()) {
                Y_ABORT_UNLESS(Borders.erase(portion->IndexKeyEnd()));
            }
            if (itStart->second.IsEmpty()) {
                Y_ABORT_UNLESS(Borders.erase(portion->IndexKeyStart()));
            }
        } else {
            itStart->second.RemoveMiddle(portion, 2);
            if (itStart->second.IsEmpty()) {
                Borders.erase(itStart);
            }
        }
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& portion) {
        Y_ABORT_UNLESS(PortionIds.emplace(portion->GetPortion()).second);
        auto itStartInfo = Borders.emplace(portion->IndexKeyStart(), TBorderPoint(LevelInfo));
        auto itStart = itStartInfo.first;
        if (itStartInfo.second && itStart != Borders.begin()) {
            auto itStartCopy = itStart;
            --itStartCopy;
            itStart->second.InitInternalPoint(itStartCopy->second);
        }
        auto itFinishInfo = Borders.emplace(portion->IndexKeyEnd(), TBorderPoint(LevelInfo));
        auto itFinish = itFinishInfo.first;
        if (itFinishInfo.second) {
            Y_ABORT_UNLESS(itFinish != Borders.begin());
            auto itFinishCopy = itFinish;
            --itFinishCopy;
            itFinish->second.InitInternalPoint(itFinishCopy->second);
        }

        itStart->second.AddStart(portion);
        itFinish->second.AddFinish(portion);
        if (itStart != itFinish) {
            for (auto it = itStart; it != itFinish; ++it) {
                it->second.AddMiddle(portion, 1);
            }
        } else {
            itStart->second.AddMiddle(portion, 2);
        }
    }
};

class TLevel {
private:
    YDB_READONLY(TDuration, CriticalAge, TDuration::Zero());
    YDB_READONLY(ui64, CriticalSize, 0);
    std::shared_ptr<TLevelInfo> LevelInfo;
    TPortionsPlacement PortionsPlacement;
    std::shared_ptr<TLevel> NextLevel;
    std::map<NArrow::TReplaceKey, TBorderPoint> Borders;
    std::map<TSnapshot, THashMap<ui64, std::shared_ptr<TPortionInfo>>> PortionByAge;
    const ui64 PortionsSizeLimit = (ui64)250 * 1024 * 1024;
    TCompactionLimits CompactionLimits;
    THashSet<ui64> PortionIds;
    const std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;
public:
    TLevel(const TDuration criticalAge, const ui64 criticalSize, std::shared_ptr<TLevel> nextLevel, const std::shared_ptr<IStoragesManager>& storagesManager, std::shared_ptr<TCounters> counters,
        const std::shared_ptr<arrow::Schema>& primaryKeysSchema)
        : CriticalAge(criticalAge)
        , CriticalSize(criticalSize)
        , LevelInfo(std::make_shared<TLevelInfo>(counters))
        , PortionsPlacement(LevelInfo)
        , NextLevel(nextLevel)
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
    {
        CompactionLimits.GranuleSizeForOverloadPrevent = CriticalSize * 0.5;
    }

    ui64 GetWeight() const {
        return LevelInfo->GetCriticalWeight();
    }

    void ProvidePortionsNextLevel(const TInstant currentInstant) {
        if (!NextLevel) {
            return;
        }
        std::vector<std::shared_ptr<TPortionInfo>> portionsForProviding;
        for (auto&& i : PortionByAge) {
            if (TInstant::MilliSeconds(i.first.GetPlanStep()) + CriticalAge < currentInstant) {
                for (auto&& p : i.second) {
                    portionsForProviding.emplace_back(p.second);
                }
            } else {
                break;
            }
        }
        for (auto&& i : portionsForProviding) {
            RemovePortion(i, currentInstant);
            NextLevel->AddPortion(i, currentInstant);
        }
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& portionInfo, const TInstant addInstant) {
        if (TInstant::MilliSeconds(portionInfo->RecordSnapshotMax().GetPlanStep()) + CriticalAge < addInstant) {
            Y_ABORT_UNLESS(!PortionIds.contains(portionInfo->GetPortion()));
            if (NextLevel) {
                return NextLevel->AddPortion(portionInfo, addInstant);
            }
        }
        PortionsPlacement.AddPortion(portionInfo);
        Y_ABORT_UNLESS(PortionByAge[portionInfo->RecordSnapshotMax()].emplace(portionInfo->GetPortion(), portionInfo).second);
        ProvidePortionsNextLevel(addInstant);
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portionInfo, const TInstant removeInstant) {
        PortionsPlacement.RemovePortion(portionInfo);
        {
            auto it = PortionByAge.find(portionInfo->RecordSnapshotMax());
            Y_ABORT_UNLESS(it != PortionByAge.end());
            Y_ABORT_UNLESS(it->second.erase(portionInfo->GetPortion()));
            if (it->second.empty()) {
                PortionByAge.erase(it);
            }
        }
        ProvidePortionsNextLevel(removeInstant);
    }

    std::shared_ptr<TColumnEngineChanges> BuildOptimizationTask(const TCompactionLimits& /*limits*/, std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager, const TInstant /*currentInstant*/) const {
        std::vector<std::vector<std::shared_ptr<TPortionInfo>>> portionGroups = PortionsPlacement.GetPortionsToCompact(PortionsSizeLimit, locksManager);
        if (portionGroups.empty()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "optimization_task_skipped");
            return nullptr;
        }
        std::vector<std::shared_ptr<TPortionInfo>> portions;
        std::vector<NIndexedReader::TSortableBatchPosition> positions;
        for (auto&& i : portionGroups) {
            portions.insert(portions.end(), i.begin(), i.end());
            std::optional<NIndexedReader::TSortableBatchPosition> position;
            for (auto&& p : i) {
                NIndexedReader::TSortableBatchPosition pos(p->IndexKeyEnd().ToBatch(PrimaryKeysSchema), 0, PrimaryKeysSchema->field_names(), {}, false);
                if (!position || position->Compare(pos) == std::partial_ordering::less) {
                    position = pos;
                }
            }
            Y_ABORT_UNLESS(position);
            positions.emplace_back(*position);
        }
        TSaverContext saverContext(StoragesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(CompactionLimits.GetSplitSettings(), granule, portions, saverContext);
        for (auto&& i : positions) {
            result->AddCheckPoint(i);
        }
        return result;
    }

};

class TLevelsOptimizerPlanner: public IOptimizerPlanner {
private:
    using TBase = IOptimizerPlanner;
    std::shared_ptr<TLevel> L3;
    std::shared_ptr<TLevel> LMax;
    std::shared_ptr<TLevel> LStart;
    const std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<TCounters> Counters;
protected:
    virtual std::vector<NIndexedReader::TSortableBatchPosition> GetBucketPositions() const override {
        return {};
    }

    virtual void DoModifyPortions(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& add, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& remove) override {
        const TInstant currentInstant = TInstant::Now();
        for (auto&& [_, i] : remove) {
            if (i->GetMeta().GetTierName() != IStoragesManager::DefaultStorageId && i->GetMeta().GetTierName() != "") {
                continue;
            }
            if (!i->GetMeta().RecordSnapshotMax) {
                LMax->RemovePortion(i, currentInstant);
            } else {
                LStart->RemovePortion(i, currentInstant);
            }
        }
        for (auto&& [_, i] : add) {
            if (i->GetMeta().GetTierName() != IStoragesManager::DefaultStorageId && i->GetMeta().GetTierName() != "") {
                continue;
            }
            if (!i->GetMeta().RecordSnapshotMax) {
                LMax->AddPortion(i, currentInstant);
            } else {
                LStart->AddPortion(i, currentInstant);
            }
        }
    }
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        return LStart->BuildOptimizationTask(limits, granule, locksManager, TInstant::Now());

    }
    virtual TOptimizationPriority DoGetUsefulMetric() const override {
        return TOptimizationPriority::Critical(LStart->GetWeight());
    }
    virtual TString DoDebugString() const override {
        return "";
    }
    virtual void DoActualize(const TInstant /*currentInstant*/) override {

    }
public:
    TLevelsOptimizerPlanner(const ui64 pathId, const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<arrow::Schema>& primaryKeysSchema)
        : TBase(pathId)
        , StoragesManager(storagesManager)
        , Counters(std::make_shared<TCounters>())
    {
        L3 = std::make_shared<TLevel>(TDuration::Seconds(120), 24 << 20, nullptr, StoragesManager, Counters, primaryKeysSchema);
        LMax = L3;
        LStart = L3;
    }
};

} // namespace NKikimr::NOlap
