#pragma once
#include "counters.h"

#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/formats/arrow/replace_key.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TOrderedPortion {
private:
    TPortionInfo::TConstPtr Portion;
    NArrow::TReplaceKey Start;
    ui64 PortionId;
    NArrow::NMerger::TSortableBatchPosition StartPosition;

public:
    const TPortionInfo::TConstPtr& GetPortion() const {
        AFL_VERIFY(Portion);
        return Portion;
    }

    const NArrow::TReplaceKey& GetStart() const {
        return Start;
    }

    const NArrow::NMerger::TSortableBatchPosition& GetStartPosition() const {
        AFL_VERIFY(Portion);
        return StartPosition;
    }

    TOrderedPortion(const TPortionInfo::TConstPtr& portion)
        : Portion(portion)
        , Start(portion->IndexKeyStart())
        , PortionId(portion->GetPortionId())
        , StartPosition(Portion->GetMeta().GetFirstLastPK().GetBatch(), 0, false) {
    }

    TOrderedPortion(const TPortionInfo::TPtr& portion)
        : Portion(portion)
        , Start(portion->IndexKeyStart())
        , PortionId(portion->GetPortionId())
        , StartPosition(Portion->GetMeta().GetFirstLastPK().GetBatch(), 0, false) {
    }

    TOrderedPortion(const NArrow::TReplaceKey& start)
        : Start(start)
        , PortionId(Max<ui64>()) {
    }

    bool operator<(const TOrderedPortion& item) const {
        auto cmp = Start.CompareNotNull(item.Start);
        if (cmp == std::partial_ordering::equivalent) {
            return PortionId < item.PortionId;
        } else {
            return cmp == std::partial_ordering::less;
        }
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
    std::optional<NArrow::TReplaceKey> StopSeparation;

public:
    ui64 GetTargetCompactionLevel() const {
        if (MemoryUsage > ((ui64)1 << 30)) {
            return TargetCompactionLevel.GetDec();
        } else {
            return TargetCompactionLevel;
        }
    }

    void SetStopSeparation(const NArrow::TReplaceKey& point) { 
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
    std::vector<NArrow::TReplaceKey> GetFinishPoints(const bool withMoved);

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
        return MemoryUsage < (((ui64)512) << 20) && CurrentLevelPortionsInfo.GetCount() + TargetLevelPortionsInfo.GetCount() < 1000
            && Portions.size() < 10000;
    }

    TCompactionTaskData(const ui64 targetCompactionLevel)
        : TargetCompactionLevel(targetCompactionLevel) {
    }
};

class IPortionsLevel {
private:
    virtual void DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) = 0;
    virtual ui64 DoGetWeight() const = 0;
    virtual TInstant DoGetWeightExpirationInstant() const = 0;
    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& pkSchema) const = 0;
    virtual TCompactionTaskData DoGetOptimizationTask() const = 0;
    virtual std::optional<TPortionsChain> DoGetAffectedPortions(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const = 0;
    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const = 0;

    virtual NJson::TJsonValue DoSerializeToJson() const {
        return NJson::JSON_MAP;
    }

    virtual TString DoDebugString() const {
        return "";
    }

    YDB_READONLY(ui64, LevelId, 0);

protected:
    std::shared_ptr<IPortionsLevel> NextLevel;
    TSimplePortionsGroupInfo PortionsInfo;
    mutable std::optional<TInstant> PredOptimization = TInstant::Now();

public:
    bool HasData() const {
        return PortionsInfo.GetCount();
    }

    virtual std::optional<double> GetPackKff() const {
        if (PortionsInfo.GetRawBytes()) {
            return 1.0 * PortionsInfo.GetBlobBytes() / PortionsInfo.GetRawBytes();
        } else if (!NextLevel) {
            return std::nullopt;
        } else {
            return NextLevel->GetPackKff();
        }
    }

    const TSimplePortionsGroupInfo& GetPortionsInfo() const {
        return PortionsInfo;
    }

    const std::shared_ptr<IPortionsLevel>& GetNextLevel() const {
        return NextLevel;
    }

    virtual ~IPortionsLevel() = default;
    IPortionsLevel(const ui64 levelId, const std::shared_ptr<IPortionsLevel>& nextLevel)
        : LevelId(levelId)
        , NextLevel(nextLevel) {
    }

    bool CanTakePortion(const TPortionInfo::TConstPtr& portion) const {
        auto chain = GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd());
        if (chain && chain->GetPortions().size()) {
            return false;
        }
        return true;
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
        result.InsertValue("portions", PortionsInfo.SerializeToJson());
        result.InsertValue("details", DoSerializeToJson());
        return result;
    }

    TString DebugString() const {
        return DoDebugString();
    }

    std::optional<TPortionsChain> GetAffectedPortions(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const {
        return DoGetAffectedPortions(from, to);
    }

    ui64 GetAffectedPortionBytes(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const {
        return DoGetAffectedPortionBytes(from, to);
    }

    void ModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) {
        return DoModifyPortions(add, remove);
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
