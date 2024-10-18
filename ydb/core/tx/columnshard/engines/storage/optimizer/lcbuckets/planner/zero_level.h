#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TZeroLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;
    const TLevelCounters LevelCounters;
    class TOrderedPortion {
    private:
        YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, Portion);

    public:
        TOrderedPortion(const std::shared_ptr<TPortionInfo>& portion)
            : Portion(portion) {
        }

        bool operator<(const TOrderedPortion& item) const {
            if (Portion->RecordSnapshotMax() == item.Portion->RecordSnapshotMax()) {
                return Portion->GetPortionId() < item.Portion->GetPortionId();
            } else {
                return Portion->RecordSnapshotMax() < item.Portion->RecordSnapshotMax();
            }
        }
    };
    std::set<TOrderedPortion> Portions;

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        AFL_VERIFY(false);
        return NArrow::NMerger::TIntervalPositions();
    }

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(
        const NArrow::TReplaceKey& /*from*/, const NArrow::TReplaceKey& /*to*/) const override {
        AFL_VERIFY(false);
        return std::nullopt;
    }

    virtual void DoModifyPortions(
        const std::vector<std::shared_ptr<TPortionInfo>>& add, const std::vector<std::shared_ptr<TPortionInfo>>& remove) override {
        for (auto&& i : add) {
            AFL_VERIFY(Portions.emplace(i).second);
            PortionsInfo.AddPortion(i);
            LevelCounters.Portions->AddPortion(i);
            i->RemoveRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        }
        for (auto&& i : remove) {
            AFL_VERIFY(Portions.erase(i));
            LevelCounters.Portions->RemovePortion(i);
            PortionsInfo.RemovePortion(i);
        }
        if (remove.size()) {
            PredOptimization = TInstant::Now();
        }
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.GetPortion())) {
                return true;
            }
        }
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        if (PortionsInfo.GetBlobBytes() > (10 << 20) || PortionsInfo.GetRawBytes() > (512 << 20) || PortionsInfo.GetCount() > 1000 || 
            (TInstant::Now() - PredOptimization > TDuration::Seconds(180) && PortionsInfo.GetCount() > 5)) {
            return PortionsInfo.GetCount();
        } else {
            return 0;
        }
    }

    virtual TCompactionTaskData DoGetOptimizationTask() const override {
        AFL_VERIFY(Portions.size());
        std::optional<TCompactionTaskData> result;
        std::shared_ptr<IPortionsLevel> targetLevel = GetNextLevel();
        AFL_VERIFY(targetLevel);
        bool onceLevel = false;
        ui64 packedSize = PortionsInfo.GetBlobBytes();
        auto kff = targetLevel->GetPackKff();
        if (kff) {
            packedSize = PortionsInfo.GetRawBytes() * *kff;
        }
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "zero_opt")("packed_size", packedSize)("kff", kff)("info", PortionsInfo.DebugString());
        if (packedSize < (1 << 20) && PortionsInfo.GetRawBytes() < (512 << 20)) {
            onceLevel = true;
        } else {
            targetLevel = targetLevel->GetNextLevel();
        }
        AFL_VERIFY(targetLevel);
        while (targetLevel) {
            TCompactionTaskData resultLocal(targetLevel->GetLevelId());
            for (auto&& i : Portions) {
                resultLocal.AddCurrentLevelPortion(i.GetPortion());
                auto nextLevelPortions = targetLevel->GetAffectedPortions(i.GetPortion()->IndexKeyStart(), i.GetPortion()->IndexKeyEnd());
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
            if (onceLevel || !result->GetRepackPortionsVolume()) {
                break;
            }
        }
        AFL_VERIFY(result);
        return *result;
    }

    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("portions", PortionsInfo.SerializeToJson());
        return result;
    }

public:
    TZeroLevelPortions(const ui64 blobBytesLimit, const ui64 rawBytesLimit, const std::shared_ptr<IPortionsLevel>& nextLevel,
        const TLevelCounters& levelCounters)
        : TBase(0, blobBytesLimit, rawBytesLimit, nextLevel)
        , LevelCounters(levelCounters)
    {
        AFL_VERIFY(nextLevel);
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
