#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TZeroLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;
    const TDuration DurationToDrop;
    const ui64 ExpectedBlobsSize;
    const ui64 PortionsCountAvailable;

    std::set<TOrderedPortion> Portions;

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        return NArrow::NMerger::TIntervalPositions();
    }

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(
        const NArrow::TSimpleRow& /*from*/, const NArrow::TSimpleRow& /*to*/) const override {
        return std::nullopt;
    }

    virtual bool IsAppropriatePortionToMove(const TPortionInfoForCompaction& info) const override {
        return info.GetTotalBlobBytes() > NextLevel->GetExpectedPortionSize();
    }

    virtual bool IsAppropriatePortionToStore(const TPortionInfoForCompaction& info) const override {
        return info.GetTotalBlobBytes() > GetExpectedPortionSize();
    }

    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TSimpleRow& /*from*/, const NArrow::TSimpleRow& /*to*/) const override {
        return 0;
    }

    virtual std::vector<TPortionInfo::TPtr> DoModifyPortions(
        const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override {
        std::vector<TPortionInfo::TPtr> problems;
        const bool constructionFlag = Portions.empty();
        if (constructionFlag) {
            std::vector<TOrderedPortion> ordered;
            ordered.reserve(add.size());
            for (auto&& i : add) {
                ordered.emplace_back(i);
            }
            std::sort(ordered.begin(), ordered.end());
            AFL_VERIFY(std::unique(ordered.begin(), ordered.end()) == ordered.end());
            Portions = std::set<TOrderedPortion>(ordered.begin(), ordered.end());
        }
        for (auto&& i : add) {
            if (!constructionFlag) {
                AFL_VERIFY(Portions.emplace(i).second);
            }
        }
        for (auto&& i : remove) {
            AFL_VERIFY(Portions.erase(i));
        }
        return problems;
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.GetPortion(), NDataLocks::ELockCategory::Compaction)) {
                return true;
            }
        }
        return false;
    }

    virtual ui64 DoGetWeight() const override;
    virtual TInstant DoGetWeightExpirationInstant() const override;

    virtual TCompactionTaskData DoGetOptimizationTask() const override;
    virtual ui64 GetExpectedPortionSize() const override {
        return ExpectedBlobsSize;
    }

public:
    TZeroLevelPortions(const ui32 levelIdx, const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters,
        const std::shared_ptr<IOverloadChecker>& overloadChecker, const TDuration durationToDrop, const ui64 expectedBlobsSize,
        const ui64 portionsCountAvailable, const std::vector<std::shared_ptr<IPortionsSelector>>& selectors, const TString& defaultSelectorName);
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
