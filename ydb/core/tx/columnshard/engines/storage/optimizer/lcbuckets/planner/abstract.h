#pragma once
#include "counters.h"

#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/formats/arrow/replace_key.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TChainAddress {
private:
    YDB_READONLY(ui64, FromPortionId, 0);
    YDB_READONLY(ui64, ToPortionId, 0);
    bool LastIsSeparator = false;

public:
    TChainAddress(const ui64 from, const ui64 to, const bool lastIsSeparator)
        : FromPortionId(from)
        , ToPortionId(to)
        , LastIsSeparator(lastIsSeparator)
    {
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
    std::vector<std::shared_ptr<TPortionInfo>> Portions;

    std::shared_ptr<TPortionInfo> NotIncludedNextPortion;

public:
    const std::vector<std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }

    const std::shared_ptr<TPortionInfo>& GetNotIncludedNextPortion() const {
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

    TPortionsChain(const std::vector<std::shared_ptr<TPortionInfo>>& portions, const std::shared_ptr<TPortionInfo>& notIncludedNextPortion)
        : Portions(portions)
        , NotIncludedNextPortion(notIncludedNextPortion)
    {
        AFL_VERIFY(Portions.size() || !!NotIncludedNextPortion);
    }
};

class TCompactionTaskData {
private:
    YDB_ACCESSOR_DEF(std::vector<std::shared_ptr<TPortionInfo>>, Portions);
    YDB_ACCESSOR(ui64, TargetCompactionLevel, 0);
    std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges::IMemoryPredictor> Predictor =
        NCompaction::TGeneralCompactColumnEngineChanges::BuildMemoryPredictor();
    ui64 MemoryUsage = 0;
    THashSet<ui64> UsedPortionIds;

    TSimplePortionsGroupInfo CurrentLevelPortionsInfo;
    TSimplePortionsGroupInfo TargetLevelPortionsInfo;

    std::set<TChainAddress> NextLevelChainIds;
    THashSet<ui64> NextLevelPortionIds;
    std::vector<TPortionsChain> Chains;

public:
    ui64 GetRepackPortionsVolume() const {
        return TargetLevelPortionsInfo.GetRawBytes();
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "target_level_chains:[";
        for (auto&& i : NextLevelChainIds) {
            sb << i.DebugString() << ",";
        }
        sb << "];target_level_portions:[";
        for (auto&& i : NextLevelPortionIds) {
            sb << i << ",";
        }
        sb << "];current_level_portions_info:{" << CurrentLevelPortionsInfo.DebugString() << "};target_level_portions_info:{"
           << TargetLevelPortionsInfo.DebugString() << "}";
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

    NArrow::NMerger::TIntervalPositions GetCheckPositions(const std::shared_ptr<arrow::Schema>& pkSchema);
    std::vector<NArrow::TReplaceKey> GetFinishPoints();

    void AddCurrentLevelPortion(const std::shared_ptr<TPortionInfo>& portion) {
        AFL_VERIFY(UsedPortionIds.emplace(portion->GetPortionId()).second);
        Portions.emplace_back(portion);
        CurrentLevelPortionsInfo.AddPortion(portion);
        MemoryUsage = Predictor->AddPortion(*portion);
    }

    void AddNextLevelPortionsSequence(TPortionsChain&& chain) {
        if (NextLevelChainIds.emplace(chain.GetAddress()).second) {
            Chains.emplace_back(std::move(chain));
        }
        for (auto&& i : Chains.back().GetPortions()) {
            if (!UsedPortionIds.emplace(i->GetPortionId()).second) {
                AFL_VERIFY(NextLevelPortionIds.contains(i->GetPortionId()));
                continue;
            }
            TargetLevelPortionsInfo.AddPortion(i);
            Portions.emplace_back(i);
            MemoryUsage = Predictor->AddPortion(*i);
            AFL_VERIFY(NextLevelPortionIds.emplace(i->GetPortionId()).second);
        }
    }

    bool CanTakeMore() const {
        return MemoryUsage < (((ui64)512) << 20);
    }

    TCompactionTaskData(const ui64 targetCompactionLevel)
        : TargetCompactionLevel(targetCompactionLevel) {
    }
};

class IPortionsLevel {
private:
    virtual void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) = 0;
    virtual ui64 DoGetWeight() const = 0;
    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& pkSchema) const = 0;
    virtual TCompactionTaskData DoGetOptimizationTask() const = 0;
    virtual std::optional<TPortionsChain> DoGetAffectedPortions(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const = 0;

    virtual NJson::TJsonValue DoSerializeToJson() const {
        return NJson::JSON_MAP;
    }

    virtual TString DoDebugString() const {
        return "";
    }

    YDB_READONLY(ui64, LevelId, 0);

protected:
    const ui64 RawBytesLimit;
    const ui64 BlobBytesLimit;
    std::shared_ptr<IPortionsLevel> NextLevel;
    TSimplePortionsGroupInfo PortionsInfo;
    mutable TInstant PredOptimization = TInstant::Now();

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

    ui64 GetBlobBytesLimit() const {
        return BlobBytesLimit;
    }

    const std::shared_ptr<IPortionsLevel>& GetNextLevel() const {
        return NextLevel;
    }

    virtual ~IPortionsLevel() = default;
    IPortionsLevel(const ui64 levelId, const ui64 blobBytesLimit, const ui64 rawBytesLimit, const std::shared_ptr<IPortionsLevel>& nextLevel)
        : LevelId(levelId)
        , RawBytesLimit(rawBytesLimit)
        , BlobBytesLimit(blobBytesLimit)
        , NextLevel(nextLevel) {
    }

    bool CanTakePortion(const std::shared_ptr<TPortionInfo>& portion) const {
        auto chain = GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd());
        if (chain && chain->GetPortions().size()) {
            return false;
        }
        return PortionsInfo.GetBlobBytes() + portion->GetTotalBlobBytes() < BlobBytesLimit * 0.5 || !NextLevel;
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
        result.InsertValue("raw_bytes_limit", RawBytesLimit);
        result.InsertValue("blob_bytes_limit", BlobBytesLimit);
        result.InsertValue("details", DoSerializeToJson());
        return result;
    }

    TString DebugString() const {
        return DoDebugString();
    }

    std::optional<TPortionsChain> GetAffectedPortions(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const {
        return DoGetAffectedPortions(from, to);
    }

    void ModifyPortions(const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) {
        return DoModifyPortions(add, remove);
    }

    ui64 GetWeight() const {
        return DoGetWeight();
    }

    NArrow::NMerger::TIntervalPositions GetBucketPositions(const std::shared_ptr<arrow::Schema>& pkSchema) const {
        return DoGetBucketPositions(pkSchema);
    }

    TCompactionTaskData GetOptimizationTask() const {
        TCompactionTaskData result = DoGetOptimizationTask();
        AFL_VERIFY(!result.IsEmpty());
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
