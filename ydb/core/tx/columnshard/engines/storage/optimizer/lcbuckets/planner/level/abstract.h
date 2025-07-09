#pragma once
#include "counters.h"

#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector/abstract.h>

#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TOrderedPortion {
private:
    TPortionInfo::TConstPtr Portion;
    NArrow::TSimpleRow Start;

public:
    const TPortionInfo::TConstPtr& GetPortion() const {
        AFL_VERIFY(Portion);
        return Portion;
    }

    const NArrow::TSimpleRow& GetStart() const {
        return Start;
    }

    TOrderedPortion(const TPortionInfo::TConstPtr& portion)
        : Portion(portion)
        , Start(portion->IndexKeyStart()) {
    }

    TOrderedPortion(const TPortionInfo::TPtr& portion)
        : Portion(portion)
        , Start(portion->IndexKeyStart()) {
    }

    friend bool operator<(const NArrow::TSimpleRow& item, const TOrderedPortion& portion) {
        auto cmp = item.CompareNotNull(portion.Start);
        if (cmp == std::partial_ordering::equivalent) {
            return false;
        } else {
            return cmp == std::partial_ordering::less;
        }
    }

    bool operator<(const NArrow::TSimpleRow& item) const {
        auto cmp = Start.CompareNotNull(item);
        if (cmp == std::partial_ordering::equivalent) {
            return true;
        } else {
            return cmp == std::partial_ordering::less;
        }
    }

    bool operator<(const TOrderedPortion& item) const {
        AFL_VERIFY(Portion->GetPathId() == item.Portion->GetPathId());
        auto cmp = Start.CompareNotNull(item.Start);
        if (cmp == std::partial_ordering::equivalent) {
            return Portion->GetPortionId() < item.Portion->GetPortionId();
        } else {
            return cmp == std::partial_ordering::less;
        }
    }

    bool operator==(const TOrderedPortion& item) const {
        AFL_VERIFY(Portion->GetPathId() == item.Portion->GetPathId());
        return Portion->GetPortionId() == item.Portion->GetPortionId();
    }
};

class TChainAddress {
private:
    YDB_READONLY(ui64, FromPortionId, 0);
    YDB_READONLY(ui64, ToPortionId, 0);
    bool LastIsSeparator = false;

public:
    TChainAddress(const ui64 from, const ui64 to, const bool lastIsSeparator)
        : FromPortionId(from)
        , ToPortionId(to)
        , LastIsSeparator(lastIsSeparator) {
    }

    bool operator<(const TChainAddress& item) const {
        return std::tie(FromPortionId, ToPortionId, LastIsSeparator) < std::tie(item.FromPortionId, item.ToPortionId, item.LastIsSeparator);
    }

    TString DebugString() const {
        return TStringBuilder() << FromPortionId << "-" << ToPortionId << ":" << LastIsSeparator;
    }
};

class TPortionsChain {
private:
    std::vector<TPortionInfo::TConstPtr> Portions;

    TPortionInfo::TConstPtr NotIncludedNextPortion;

public:
    const std::vector<TPortionInfo::TConstPtr>& GetPortions() const {
        return Portions;
    }

    const TPortionInfo::TConstPtr& GetNotIncludedNextPortion() const {
        return NotIncludedNextPortion;
    }

    TChainAddress GetAddress() const {
        if (Portions.size()) {
            return TChainAddress(Portions.front()->GetPortionId(),
                NotIncludedNextPortion ? NotIncludedNextPortion->GetPortionId() : Portions.back()->GetPortionId(), !!NotIncludedNextPortion);
        } else {
            AFL_VERIFY(NotIncludedNextPortion);
            return TChainAddress(NotIncludedNextPortion->GetPortionId(), NotIncludedNextPortion->GetPortionId(), true);
        }
    }

    TPortionsChain(const std::vector<TPortionInfo::TConstPtr>& portions, const TPortionInfo::TConstPtr& notIncludedNextPortion)
        : Portions(portions)
        , NotIncludedNextPortion(notIncludedNextPortion) {
        AFL_VERIFY(Portions.size() || !!NotIncludedNextPortion);
    }
};

class TCompactionTaskData {
private:
    YDB_ACCESSOR_DEF(std::vector<TPortionInfo::TConstPtr>, Portions);
    const TPositiveControlInteger TargetCompactionLevel;
    std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges::IMemoryPredictor> Predictor =
        NCompaction::TGeneralCompactColumnEngineChanges::BuildMemoryPredictor();
    ui64 MemoryUsage = 0;
    THashSet<ui64> UsedPortionIds;
    THashSet<ui64> RepackPortionIds;

    TSimplePortionsGroupInfo CurrentLevelPortionsInfo;
    TSimplePortionsGroupInfo TargetLevelPortionsInfo;

    std::set<TChainAddress> NextLevelChainIds;
    THashSet<ui64> NextLevelPortionIds;
    THashSet<ui64> CurrentLevelPortionIds;
    std::vector<TPortionsChain> Chains;
    std::optional<NArrow::TSimpleRow> StopSeparation;

public:
    ui64 GetTargetCompactionLevel() const {
        if (MemoryUsage > ((ui64)1 << 30)) {
            return TargetCompactionLevel.GetDec();
        } else {
            return TargetCompactionLevel;
        }
    }

    void SetStopSeparation(const NArrow::TSimpleRow& point) {
        AFL_VERIFY(!StopSeparation);
        StopSeparation = point;
    }

    std::vector<TPortionInfo::TConstPtr> GetRepackPortions(const ui32 /*levelIdx*/) const {
        std::vector<TPortionInfo::TConstPtr> result;
        if (MemoryUsage > ((ui64)1 << 30)) {
            auto predictor = NCompaction::TGeneralCompactColumnEngineChanges::BuildMemoryPredictor();
            for (auto&& i : Portions) {
                if (CurrentLevelPortionIds.contains(i->GetPortionId())) {
                    if (predictor->AddPortion(i) < MemoryUsage || result.size() < 2) {
                        result.emplace_back(i);
                    } else {
                        break;
                    }
                }
            }
            return result;
        } else {
            return Portions;
        }
        auto moveIds = GetMovePortionIds();
        for (auto&& i : Portions) {
            if (!moveIds.contains(i->GetPortionId())) {
                result.emplace_back(i);
            }
        }
        return result;
    }

    std::vector<TPortionInfo::TConstPtr> GetMovePortions() const {
        if (MemoryUsage > ((ui64)1 << 30)) {
            return {};
        }
        auto moveIds = GetMovePortionIds();
        std::vector<TPortionInfo::TConstPtr> result;
        for (auto&& i : Portions) {
            if (moveIds.contains(i->GetPortionId())) {
                result.emplace_back(i);
            }
        }
        return result;
    }

    ui64 GetRepackPortionsVolume() const {
        return TargetLevelPortionsInfo.GetRawBytes();
    }

    THashSet<ui64> GetMovePortionIds() const {
        auto movePortionIds = CurrentLevelPortionIds;
        for (auto&& i : RepackPortionIds) {
            movePortionIds.erase(i);
        }
        return movePortionIds;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "target_level_chains:[";
        for (auto&& i : NextLevelChainIds) {
            sb << i.DebugString() << ",";
        }
        sb << "];target_level_portions:[" << JoinSeq(",", NextLevelPortionIds) << "];current_level_portions_info:{"
           << CurrentLevelPortionsInfo.DebugString() << "};target_level_portions_info:{" << TargetLevelPortionsInfo.DebugString() << "};";
        sb << "move_portion_ids:[" << JoinSeq(",", GetMovePortionIds()) << "]";
        return sb;
    }

    TCompactionTaskData() = default;

    const THashSet<ui64>& GetPortionIds() const {
        return UsedPortionIds;
    }

    bool Contains(const ui64 portionId) const {
        return UsedPortionIds.contains(portionId);
    }

    bool IsEmpty() const {
        return !Portions.size();
    }

    NArrow::NMerger::TIntervalPositions GetCheckPositions(const std::shared_ptr<arrow::Schema>& pkSchema, const bool withMoved);
    std::vector<NArrow::NMerger::TSortableBatchPosition> GetFinishPoints(const bool withMoved);

    void AddCurrentLevelPortion(const TPortionInfo::TConstPtr& portion, std::optional<TPortionsChain>&& chain, const bool repackMoved) {
        AFL_VERIFY(UsedPortionIds.emplace(portion->GetPortionId()).second);
        AFL_VERIFY(CurrentLevelPortionIds.emplace(portion->GetPortionId()).second);
        Portions.emplace_back(portion);
        CurrentLevelPortionsInfo.AddPortion(portion);
        if (repackMoved || (chain && chain->GetPortions().size())) {
            MemoryUsage = Predictor->AddPortion(portion);
        }

        if (chain) {
            if (chain->GetPortions().size()) {
                RepackPortionIds.emplace(portion->GetPortionId());
            }
            if (NextLevelChainIds.emplace(chain->GetAddress()).second) {
                Chains.emplace_back(std::move(*chain));
                for (auto&& i : Chains.back().GetPortions()) {
                    if (!UsedPortionIds.emplace(i->GetPortionId()).second) {
                        AFL_VERIFY(NextLevelPortionIds.contains(i->GetPortionId()));
                        continue;
                    }
                    TargetLevelPortionsInfo.AddPortion(i);
                    Portions.emplace_back(i);
                    MemoryUsage = Predictor->AddPortion(i);
                    AFL_VERIFY(NextLevelPortionIds.emplace(i->GetPortionId()).second);
                }
            }
        }
    }

    bool CanTakeMore() const {
        if (Portions.size() <= 1) {
            return true;
        }
        return MemoryUsage < (((ui64)512) << 20) && CurrentLevelPortionsInfo.GetCount() + TargetLevelPortionsInfo.GetCount() < 1000 &&
               Portions.size() < 10000;
    }

    TCompactionTaskData(const ui64 targetCompactionLevel)
        : TargetCompactionLevel(targetCompactionLevel) {
    }
};

class IOverloadChecker {
private:
    virtual bool DoIsOverloaded(const TSimplePortionsGroupInfo& portionsData) const = 0;

public:
    virtual ~IOverloadChecker() = default;

    bool IsOverloaded(const TSimplePortionsGroupInfo& portionsData) const {
        return DoIsOverloaded(portionsData);
    }
};

class TNoOverloadChecker: public IOverloadChecker {
private:
    virtual bool DoIsOverloaded(const TSimplePortionsGroupInfo& /*portionsData*/) const override {
        return false;
    }
};

class TLimitsOverloadChecker: public IOverloadChecker {
private:
    const std::optional<ui64> PortionsCountLimit;
    const std::optional<ui64> PortionBlobsSizeLimit;
    virtual bool DoIsOverloaded(const TSimplePortionsGroupInfo& portionsData) const override {
        if (PortionsCountLimit && *PortionsCountLimit < (ui64)portionsData.GetCount()) {
            return true;
        }
        if (PortionBlobsSizeLimit && *PortionBlobsSizeLimit < (ui64)portionsData.GetBlobBytes()) {
            return true;
        }
        return false;
    }

public:
    TLimitsOverloadChecker(const std::optional<ui64> portionsCountLimit, const std::optional<ui64> portionBlobsSizeLimit)
        : PortionsCountLimit(portionsCountLimit)
        , PortionBlobsSizeLimit(portionBlobsSizeLimit) {
    }
};

class IPortionsLevel {
private:
    virtual std::vector<TPortionInfo::TPtr> DoModifyPortions(
        const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) = 0;
    virtual ui64 DoGetWeight() const = 0;
    virtual TInstant DoGetWeightExpirationInstant() const = 0;
    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& pkSchema) const = 0;
    virtual TCompactionTaskData DoGetOptimizationTask() const = 0;
    virtual std::optional<TPortionsChain> DoGetAffectedPortions(const NArrow::TSimpleRow& from, const NArrow::TSimpleRow& to) const = 0;
    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TSimpleRow& from, const NArrow::TSimpleRow& to) const = 0;

    virtual NJson::TJsonValue DoSerializeToJson() const {
        return NJson::JSON_MAP;
    }

    virtual TString DoDebugString() const {
        return "";
    }

    YDB_READONLY(ui64, LevelId, 0);
    std::vector<std::shared_ptr<IPortionsSelector>> Selectors;
    std::vector<TSimplePortionsGroupInfo> SelectivePortionsInfo;

    TSimplePortionsGroupInfo* PortionsInfo = nullptr;
    std::shared_ptr<IOverloadChecker> OverloadChecker = std::make_shared<TNoOverloadChecker>();
    std::shared_ptr<IPortionsSelector> DefaultPortionsSelector;
    const TLevelCounters LevelCounters;

protected:
    std::shared_ptr<IPortionsLevel> NextLevel;
    mutable std::optional<TInstant> PredOptimization = TInstant::Now();

public:
    virtual ui64 GetExpectedPortionSize() const = 0;

    virtual bool IsAppropriatePortionToMove(const TPortionInfoForCompaction& /*info*/) const {
        return false;
    }

    virtual bool IsAppropriatePortionToStore(const TPortionInfoForCompaction& /*info*/) const {
        return false;
    }

    const TSimplePortionsGroupInfo& GetPortionsInfo() const {
        AFL_VERIFY(PortionsInfo);
        return *PortionsInfo;
    }

    TSimplePortionsGroupInfo& MutablePortionsInfo() {
        AFL_VERIFY(PortionsInfo);
        return *PortionsInfo;
    }

    bool IsOverloaded() const {
        return NextLevel && OverloadChecker->IsOverloaded(GetPortionsInfo());
    }

    bool HasData() const {
        return GetPortionsInfo().GetCount();
    }

    virtual std::optional<double> GetPackKff() const {
        if (GetPortionsInfo().GetRawBytes()) {
            return 1.0 * GetPortionsInfo().GetBlobBytes() / GetPortionsInfo().GetRawBytes();
        } else if (!NextLevel) {
            return std::nullopt;
        } else {
            return NextLevel->GetPackKff();
        }
    }

    const std::shared_ptr<IPortionsLevel>& GetNextLevel() const {
        return NextLevel;
    }

    virtual ~IPortionsLevel() {
    }
    IPortionsLevel(const ui64 levelId, const std::shared_ptr<IPortionsLevel>& nextLevel,
        const std::shared_ptr<IOverloadChecker>& overloadChecker, const TLevelCounters levelCounters,
        const std::vector<std::shared_ptr<IPortionsSelector>>& selectors, const TString& defaultSelectorName)
        : LevelId(levelId)
        , Selectors(selectors)
        , OverloadChecker(overloadChecker ? overloadChecker : std::make_shared<TNoOverloadChecker>())
        , LevelCounters(levelCounters)
        , NextLevel(nextLevel) {
        SelectivePortionsInfo.resize(Selectors.size());
        ui32 idx = 0;
        for (auto&& i : selectors) {
            if (i->GetName() == defaultSelectorName) {
                AFL_VERIFY(!DefaultPortionsSelector);
                DefaultPortionsSelector = i;
                PortionsInfo = &SelectivePortionsInfo[idx];
            }
            ++idx;
        }
        AFL_VERIFY(DefaultPortionsSelector);
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const = 0;

    virtual TTaskDescription GetTaskDescription() const {
        TTaskDescription result(0);
        result.SetWeight(GetWeight());
        result.SetDetails(SerializeToJson().GetStringRobust());
        return result;
    }

    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("level", LevelId);
        result.InsertValue("weight", GetWeight());
        result.InsertValue("details", DoSerializeToJson());
        auto& selectiveJson = result.InsertValue("selectivity", NJson::JSON_MAP);
        ui32 idx = 0;
        for (auto&& i : SelectivePortionsInfo) {
            selectiveJson.InsertValue(Selectors[idx]->GetName(), i.SerializeToJson());
            ++idx;
        }

        return result;
    }

    TString DebugString() const {
        return DoDebugString();
    }

    std::optional<TPortionsChain> GetAffectedPortions(const NArrow::TSimpleRow& from, const NArrow::TSimpleRow& to) const {
        return DoGetAffectedPortions(from, to);
    }

    ui64 GetAffectedPortionBytes(const NArrow::TSimpleRow& from, const NArrow::TSimpleRow& to) const {
        return DoGetAffectedPortionBytes(from, to);
    }

    [[nodiscard]] std::vector<TPortionInfo::TPtr> ModifyPortions(
        const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) {
        std::vector<TPortionInfo::TPtr> addSelective;
        std::vector<TPortionInfo::TPtr> removeSelective;
        for (ui32 idx = 0; idx < Selectors.size(); ++idx) {
            const std::shared_ptr<IPortionsSelector>& selector = Selectors[idx];
            const bool isDefaultSelector = ((ui64)selector.get() == (ui64)DefaultPortionsSelector.get());
            for (auto&& i : remove) {
                if (selector && !selector->IsAppropriate(i)) {
                    continue;
                }
                if (isDefaultSelector) {
                    removeSelective.emplace_back(i);
                    LevelCounters.Portions->RemovePortion(i);
                }
                SelectivePortionsInfo[idx].RemovePortion(*i);
            }
            for (auto&& i : add) {
                if (selector && !selector->IsAppropriate(i)) {
                    continue;
                }
                if (isDefaultSelector) {
                    addSelective.emplace_back(i);
                    LevelCounters.Portions->AddPortion(i);
                }
                SelectivePortionsInfo[idx].AddPortion(*i);
                i->InitRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized, !NextLevel);
            }
        }
        return DoModifyPortions(addSelective, removeSelective);
    }

    ui64 GetWeight() const {
        return DoGetWeight();
    }

    TInstant GetWeightExpirationInstant() const {
        return DoGetWeightExpirationInstant();
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions(const std::shared_ptr<arrow::Schema>& pkSchema) const {
        return DoGetBucketPositions(pkSchema);
    }

    TCompactionTaskData GetOptimizationTask() const {
        AFL_VERIFY(NextLevel);
        TCompactionTaskData result = DoGetOptimizationTask();
        AFL_VERIFY(!result.IsEmpty());
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
