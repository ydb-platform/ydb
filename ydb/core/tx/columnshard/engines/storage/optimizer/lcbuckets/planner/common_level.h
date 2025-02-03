#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;

    std::set<TOrderedPortion> Portions;
    const TLevelCounters LevelCounters;
    const double BytesLimitFraction = 1;
    const ui64 ExpectedPortionSize = (1 << 20);
    const bool StrictOneLayer = true;
    std::shared_ptr<TSimplePortionsGroupInfo> SummaryPortionsInfo;

    ui64 GetLevelBlobBytesLimit() const {
        const ui32 discrete = SummaryPortionsInfo->GetBlobBytes() / (150 << 20);
        return (discrete + 1) * (150 << 20) * BytesLimitFraction;
    }

    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("expected_portion_size", ExpectedPortionSize);
        result.InsertValue("bytes_limit", GetLevelBlobBytesLimit());
        result.InsertValue("total_bytes", SummaryPortionsInfo->GetBlobBytes());
        result.InsertValue("fraction", BytesLimitFraction);
        return result;
    }

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const override {
        if (Portions.empty()) {
            return std::nullopt;
        }
        std::vector<TPortionInfo::TConstPtr> result;
        auto itFrom = Portions.upper_bound(from);
        auto itTo = Portions.upper_bound(to);
        if (itFrom != Portions.begin()) {
            auto it = itFrom;
            --it;
            if (from <= it->GetPortion()->IndexKeyEnd()) {
                result.insert(result.begin(), it->GetPortion());
            }
        }
        for (auto it = itFrom; it != itTo; ++it) {
            result.emplace_back(it->GetPortion());
        }
        if (itTo != Portions.end()) {
            return TPortionsChain(std::move(result), itTo->GetPortion());
        } else if (result.size()) {
            return TPortionsChain(std::move(result), nullptr);
        } else {
            return std::nullopt;
        }
    }

    virtual ui64 DoGetWeight() const override {
        if (!GetNextLevel()) {
            return 0;
        }
        if ((ui64)PortionsInfo.GetBlobBytes() > GetLevelBlobBytesLimit() && PortionsInfo.GetCount() > 2 &&
            (ui64)PortionsInfo.GetBlobBytes() > ExpectedPortionSize * 2) {
            return ((ui64)GetLevelId() << 48) + PortionsInfo.GetBlobBytes() - GetLevelBlobBytesLimit();
        } else {
            return 0;
        }
    }

    virtual TInstant DoGetWeightExpirationInstant() const override {
        return TInstant::Max();
    }

public:
    TLevelPortions(const ui64 levelId, const double bytesLimitFraction, const ui64 expectedPortionSize,
        const std::shared_ptr<IPortionsLevel>& nextLevel, const std::shared_ptr<TSimplePortionsGroupInfo>& summaryPortionsInfo,
        const TLevelCounters& levelCounters, const bool strictOneLayer = true)
        : TBase(levelId, nextLevel)
        , LevelCounters(levelCounters)
        , BytesLimitFraction(bytesLimitFraction)
        , ExpectedPortionSize(expectedPortionSize)
        , StrictOneLayer(strictOneLayer)
        , SummaryPortionsInfo(summaryPortionsInfo)
    {
    }

    ui64 GetExpectedPortionSize() const {
        return ExpectedPortionSize;
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.GetPortion(), NDataLocks::ELockCategory::Compaction)) {
                return true;
            }
        }
        return false;
    }

    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const override {
        if (Portions.empty()) {
            return 0;
        }
        ui64 result = 0;
        auto itFrom = Portions.upper_bound(from);
        auto itTo = Portions.upper_bound(to);
        if (itFrom != Portions.begin()) {
            auto it = itFrom;
            --it;
            if (from <= it->GetPortion()->IndexKeyEnd()) {
                result += it->GetPortion()->GetTotalRawBytes();
            }
        }
        for (auto it = itFrom; it != itTo; ++it) {
            result += it->GetPortion()->GetTotalRawBytes();
        }
        return result;
    }

    virtual void DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override;

    virtual TCompactionTaskData DoGetOptimizationTask() const override;

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& pkSchema) const override {
        NArrow::NMerger::TIntervalPositions result;
        const auto& sortingColumns = pkSchema->field_names();
        for (auto&& i : Portions) {
            result.AddPosition(i.GetStartPosition(), false);
        }
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
