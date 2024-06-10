#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/logic/abstract/logic.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

class TLeftBucketBorder {
private:
    std::optional<NArrow::TReplaceKey> Value;
public:
    bool operator<(const TLeftBucketBorder& item) const {
        if (item.Value && Value) {
            return *Value < *item.Value;
        } else if (item.Value) {
            return true;
        } else {
            return false;
        }
    }

    TString DebugString() const {
        if (!Value) {
            return "NO_VALUE";
        } else {
            return Value->DebugString();
        }
    }

    bool operator==(const TLeftBucketBorder& item) const {
        if (!!item.Value != !!Value) {
            return false;
        }
        if (Value) {
            return *Value < *item.Value;
        }
        return true;
    }

    bool HasValue() const {
        return !!Value;
    }

    const NArrow::TReplaceKey& GetValueVerified() const {
        AFL_VERIFY(!!Value);
        return *Value;
    }

    TLeftBucketBorder() = default;
    TLeftBucketBorder(const NArrow::TReplaceKey& value)
        : Value(value) {

    }
};

class TRightBucketBorder {
private:
    std::optional<NArrow::TReplaceKey> Value;
public:
    bool IsOpen() const {
        return !Value;
    }

    TString DebugString() const {
        return Value ? Value->DebugString() : "NO_VALUE";
    }

    bool operator<(const TRightBucketBorder& item) const {
        if (item.Value && Value) {
            return *Value < *item.Value;
        } else if (Value) {
            return true;
        } else {
            return false;
        }
    }

    bool operator==(const TRightBucketBorder& item) const {
        if (!!item.Value != !!Value) {
            return false;
        }
        if (Value) {
            return *Value < *item.Value;
        }
        return true;
    }

    bool HasValue() const {
        return !!Value;
    }

    const NArrow::TReplaceKey& GetValueVerified() const {
        AFL_VERIFY(!!Value);
        return *Value;
    }

    TRightBucketBorder() = default;
    TRightBucketBorder(const NArrow::TReplaceKey& value)
        : Value(value) {

    }
};

class TPortionsBucket: public TBucketInfo, public TMoveOnly {
private:
    TLeftBucketBorder Start;
    TRightBucketBorder Finish;
    std::shared_ptr<IOptimizationLogic> Logic;
    TSimplePortionsGroupInfo PortionsInfo;
    THashMap<ui64, TBucketPortionInfo> Portions;
    TInstant NextActualizeInstant = TInstant::Zero();
    std::optional<ui64> LastWeight;
    const std::shared_ptr<TCounters> Counters;

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        AFL_VERIFY(Portions.size() == (ui64)PortionsInfo.GetCount());
        result.InsertValue("portions_count", Portions.size());
        result.InsertValue("records_count", PortionsInfo.GetRecordsCount());
        result.InsertValue("bytes", PortionsInfo.GetBytes());
        return result;
    }

    void RebuildOptimizedFeature(const TInstant currentInstant) const;

public:
    const TLeftBucketBorder& GetStart() const {
        return Start;
    }
    const TRightBucketBorder& GetFinish() const {
        return Finish;
    }

    void ResetLogic(const std::shared_ptr<IOptimizationLogic>& logic) {
        Logic = logic;
    }

    i64 ResetWeight(const TInstant currentInstant) {
        auto result = Logic->CalcWeight(currentInstant, *this);
        LastWeight = result.GetWeight();
        NextActualizeInstant = result.GetNextInstant();
        return GetWeight();
    }

    TTaskDescription GetTaskDescription() const {
        TTaskDescription result(Portions.size() ? Portions.begin()->first : 0);
        result.SetStart(Start.DebugString());
        result.SetFinish(Finish.DebugString());
        result.SetWeight(GetWeight());
        result.SetDetails(DebugJson().GetStringRobust());
        return result;
    }

    bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const;

    void AddPortion(const std::shared_ptr<TPortionInfo>& portionInfo) {
        AFL_VERIFY(Portions.emplace(portionInfo->GetPortionId(), TBucketPortionInfo(portionInfo)).second);
        PKPortions[portionInfo->IndexKeyStart()].AddStart(TBucketPortionInfo(portionInfo));
        PKPortions[portionInfo->IndexKeyEnd()].AddFinish(TBucketPortionInfo(portionInfo));
        SnapshotPortions[portionInfo->RecordSnapshotMax().GetPlanInstant()].emplace(portionInfo->GetPortionId(), portionInfo);
        NextActualizeInstant = TInstant::Zero();
        LastWeight = {};
        PortionsInfo.AddPortion(portionInfo);
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portionInfo) {
        AFL_VERIFY(Portions.erase(portionInfo->GetPortionId()));
        {
            auto it = PKPortions.find(portionInfo->IndexKeyStart());
            AFL_VERIFY(it != PKPortions.end());
            it->second.RemoveStart(TBucketPortionInfo(portionInfo));
            if (it->second.IsEmpty()) {
                PKPortions.erase(it);
            }
        }
        {
            auto it = PKPortions.find(portionInfo->IndexKeyEnd());
            AFL_VERIFY(it != PKPortions.end());
            it->second.RemoveFinish(TBucketPortionInfo(portionInfo));
            if (it->second.IsEmpty()) {
                PKPortions.erase(it);
            }
        }
        {
            auto it = SnapshotPortions.find(portionInfo->RecordSnapshotMax().GetPlanInstant());
            AFL_VERIFY(it != SnapshotPortions.end());
            AFL_VERIFY(it->second.erase(portionInfo->GetPortionId()));
            if (it->second.empty()) {
                SnapshotPortions.erase(it);
            }
        }

        NextActualizeInstant = TInstant::Zero();
        LastWeight = {};
        PortionsInfo.RemovePortion(portionInfo);
    }

    void MergeFrom(TPortionsBucket&& bucket) {
        AFL_VERIFY(Finish.HasValue());
        AFL_VERIFY(bucket.Start.HasValue());
        AFL_VERIFY(Finish.GetValueVerified() == bucket.Start.GetValueVerified());
        for (auto&& i : bucket.Portions) {
            AddPortion(i.second.GetPortionInfo());
        }
        Finish = bucket.Finish;
    }

    std::vector<std::shared_ptr<TPortionsBucket>> Split() const {
        THashMap<ui64, TBucketPortionInfo> currentPortions;
        THashMap<ui64, TBucketPortionInfo> partPortions;
        std::optional<NArrow::TReplaceKey> partStart;
        std::optional<NArrow::TReplaceKey> partFinish;
        std::vector<std::shared_ptr<TPortionsBucket>> result;
        ui64 currentSize = 0;
        for (auto&& [pk, portions] : PKPortions) {
            if (currentPortions.empty() && currentSize >= MinBucketSize) {
                if (partPortions.size()) {
                    AFL_VERIFY(partStart);
                    result.emplace_back(std::make_shared<TPortionsBucket>(Counters, std::move(partPortions), Logic, TLeftBucketBorder(*partStart), TRightBucketBorder(pk)));
                    partPortions.clear();
                }
                AFL_VERIFY(portions.GetStart().size());
                AFL_VERIFY(!portions.GetFinish().size());
                partStart = pk;
                currentSize = 0;
            }
            for (auto&& [_, p] : portions.GetStart()) {
                AFL_VERIFY(currentPortions.emplace(p->GetPortionId(), p).second);
            }
            for (auto&& [_, p] : portions.GetFinish()) {
                AFL_VERIFY(currentPortions.erase(p->GetPortionId()));
                AFL_VERIFY(partPortions.emplace(p->GetPortionId(), p).second);
                currentSize += p->GetTotalBlobBytes();
            }
        }
        AFL_VERIFY(currentPortions.empty());
        if (partPortions.size()) {
            AFL_VERIFY(partStart);
            result.emplace_back(std::make_shared<TPortionsBucket>(Counters, std::move(partPortions), Logic, TLeftBucketBorder(*partStart), Finish));
        }
        return result;
    }

    bool IsEmpty() const {
        return Portions.empty();
    }

    TPortionsBucket(const std::shared_ptr<TCounters>& counters, THashMap<ui64, TBucketPortionInfo>&& portions, const std::shared_ptr<IOptimizationLogic>& logic,
        const TLeftBucketBorder& l, const TRightBucketBorder& r)
        : Start(l)
        , Finish(r)
        , Logic(logic)
        , Counters(counters)
    {
        for (auto&& i : portions) {
            AddPortion(i.second.GetPortionInfo());
        }
    }

    TPortionsBucket(const std::shared_ptr<TCounters>& counters, const std::shared_ptr<IOptimizationLogic>& logic, const TLeftBucketBorder& l, const TRightBucketBorder& r)
        : Start(l)
        , Finish(r)
        , Logic(logic)
        , Counters(counters) {
    }

    ~TPortionsBucket() {

    }

    i64 GetWeight() const {
        AFL_VERIFY(LastWeight);
        return *LastWeight;
    }

    ui64 GetMemLimit() const;

    std::shared_ptr<TColumnEngineChanges> BuildOptimizationTask(std::shared_ptr<TGranuleMeta> granule,
        const std::shared_ptr<NDataLocks::TManager>& locksManager, const std::shared_ptr<arrow::Schema>& primaryKeysSchema,
        const std::shared_ptr<IStoragesManager>& storagesManager) const;

    bool NeedActualization(const TInstant currentInstant) const {
        return NextActualizeInstant <= currentInstant;
    }

    void Actualize(const TInstant currentInstant) {
        if (!NeedActualization(currentInstant)) {
            AFL_VERIFY(!!LastWeight);
            return;
        }
        ResetWeight(currentInstant);
        RebuildOptimizedFeature(currentInstant);
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
