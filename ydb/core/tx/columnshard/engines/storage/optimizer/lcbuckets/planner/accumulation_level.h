#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TAccumulationLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    const TLevelCounters LevelCounters;

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(const NArrow::TReplaceKey& /*from*/, const NArrow::TReplaceKey& /*to*/) const override {
        return std::nullopt;
    }

    virtual ui64 DoGetWeight() const override {
        if (PortionsInfo.GetBlobBytes() > (612 << 20) || PortionsInfo.GetRawBytes() > (256 << 20)) {
            return std::max<ui32>(500, PortionsInfo.GetCount());
        } else {
            return 0;
        }
    }

public:
    TAccumulationLevelPortions(const ui64 levelId, const ui64 blobBytesLimit, const ui64 rawBytesLimit,
        const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters)
        : TBase(levelId, blobBytesLimit, rawBytesLimit, nextLevel)
        , LevelCounters(levelCounters)
    {
    
    }

    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("portions", PortionsInfo.SerializeToJson());
        return result;
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.second)) {
                return true;
            }
        }
        return false;
    }

    virtual void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
        for (auto&& i : remove) {
            auto it = Portions.find(i->GetPortionId());
            AFL_VERIFY(it != Portions.end());
            AFL_VERIFY(it->second->GetPortionId() == i->GetPortionId());
            PortionsInfo.RemovePortion(i);
            Portions.erase(it);
            LevelCounters.Portions->RemovePortion(i);
        }
        for (auto&& i : add) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "add_accum")("portion_id", i->GetPortionId())("blob_size", i->GetTotalBlobBytes());
            AFL_VERIFY(Portions.emplace(i->GetPortionId(), i).second);
            PortionsInfo.AddPortion(i);
            LevelCounters.Portions->AddPortion(i);
        }
    }

    virtual TCompactionTaskData DoGetOptimizationTask() const override {
        AFL_VERIFY(Portions.size());
        std::optional<TCompactionTaskData> result;
        std::shared_ptr<IPortionsLevel> targetLevel = GetNextLevel();
        AFL_VERIFY(targetLevel);
        while (targetLevel) {
            TCompactionTaskData resultLocal(targetLevel->GetLevelId());
            for (auto&& i : Portions) {
                resultLocal.AddCurrentLevelPortion(i.second);
                auto nextLevelPortions = targetLevel->GetAffectedPortions(i.second->IndexKeyStart(), i.second->IndexKeyEnd());
                if (nextLevelPortions) {
                    resultLocal.AddNextLevelPortionsSequence(std::move(*nextLevelPortions));
                }
                if (!resultLocal.CanTakeMore()) {
                    break;
                }
            }
            targetLevel = targetLevel->GetNextLevel();
            if (!result || resultLocal.GetRepackPortionsVolume() < result->GetRepackPortionsVolume()) {
                result = resultLocal;
            }
        }
        AFL_VERIFY(result);
        return *result;
    }

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        NArrow::NMerger::TIntervalPositions result;
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
