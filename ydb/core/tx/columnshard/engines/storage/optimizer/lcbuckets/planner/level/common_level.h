#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TOneLayerPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;

    std::set<TOrderedPortion, std::less<>> Portions;
    const double BytesLimitFraction = 1;
    const ui64 ExpectedPortionSize = (1 << 20);
    const ui64 SizeLimitGuarantee = 0;
    const bool StrictOneLayer = true;
    std::shared_ptr<TSimplePortionsGroupInfo> SummaryPortionsInfo;

    virtual ui64 GetExpectedPortionSize() const override {
        return ExpectedPortionSize;
    }

    ui64 GetLevelBlobBytesLimit() const {
        return std::max<ui64>(SizeLimitGuarantee, SummaryPortionsInfo->GetBlobBytes() * BytesLimitFraction);
    }

    virtual NJson::TJsonValue DoSerializeToJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("expected_portion_size", ExpectedPortionSize);
        result.InsertValue("bytes_limit", GetLevelBlobBytesLimit());
        result.InsertValue("total_bytes", SummaryPortionsInfo->GetBlobBytes());
        result.InsertValue("fraction", BytesLimitFraction);
        result.InsertValue("size_limit_guarantee", SizeLimitGuarantee);
        return result;
    }

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(const NArrow::TSimpleRow& from, const NArrow::TSimpleRow& to) const override {
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
        if ((ui64)GetPortionsInfo().GetBlobBytes() < GetLevelBlobBytesLimit()) {
            return 0;
        }
        if (GetPortionsInfo().GetCount() < 2) {
            return 0;
        }
        if ((ui64)GetPortionsInfo().GetBlobBytes() < std::max(GetNextLevel()->GetExpectedPortionSize(), GetExpectedPortionSize())) {
            return 0;
        }
        return ((ui64)GetLevelId() << 48) + GetPortionsInfo().GetBlobBytes() - GetLevelBlobBytesLimit();
    }

    virtual bool IsAppropriatePortionToStore(const TPortionInfoForCompaction& info) const override {
        if (info.GetTotalBlobBytes() < GetExpectedPortionSize()) {
            return false;
        }
        return !GetAffectedPortionBytes(info.GetFirstPK(), info.GetLastPK());
    }

    virtual bool IsAppropriatePortionToMove(const TPortionInfoForCompaction& /*info*/) const override {
        return true;
    }

    virtual TInstant DoGetWeightExpirationInstant() const override {
        return TInstant::Max();
    }

public:
    TOneLayerPortions(const ui64 levelId, const double bytesLimitFraction, const ui64 expectedPortionSize,
        const std::shared_ptr<IPortionsLevel>& nextLevel, const std::shared_ptr<TSimplePortionsGroupInfo>& summaryPortionsInfo,
        const TLevelCounters& levelCounters, const ui64 sizeLimitGuarantee, const std::vector<std::shared_ptr<IPortionsSelector>>& selectors,
        const TString& defaultSelectorName, const bool strictOneLayer = true)
        : TBase(levelId, nextLevel, nullptr, levelCounters, selectors, defaultSelectorName)
        , BytesLimitFraction(bytesLimitFraction)
        , ExpectedPortionSize(expectedPortionSize)
        , SizeLimitGuarantee(sizeLimitGuarantee)
        , StrictOneLayer(strictOneLayer)
        , SummaryPortionsInfo(summaryPortionsInfo)
    {
    }

    virtual bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        for (auto&& i : Portions) {
            if (locksManager->IsLocked(*i.GetPortion(), NDataLocks::ELockCategory::Compaction)) {
                return true;
            }
        }
        return false;
    }

    virtual ui64 DoGetAffectedPortionBytes(const NArrow::TSimpleRow& from, const NArrow::TSimpleRow& to) const override {
        if (Portions.empty()) {
            return 0;
        }
        AFL_VERIFY(from <= to);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("from", from.DebugString())("to", to.DebugString());
        ui64 result = 0;
        ui64 resultPacked = 0;
        auto itFrom = Portions.upper_bound(from);
        auto itTo = Portions.upper_bound(to);
        ui32 count = 0;
        if (itFrom != Portions.begin()) {
            auto it = itFrom;
            --it;
            if (from <= it->GetPortion()->IndexKeyEnd()) {
                result += it->GetPortion()->GetTotalRawBytes();
                resultPacked += it->GetPortion()->GetTotalBlobBytes();
                ++count;
            }
        }
        for (auto it = itFrom; it != itTo; ++it) {
            result += it->GetPortion()->GetTotalRawBytes();
            resultPacked += it->GetPortion()->GetTotalBlobBytes();
            ++count;
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("itFrom", itFrom == Portions.end())("itTo", itTo == Portions.end())("raw", result)(
            "count", count)("packed", resultPacked);
        return result;
    }

    virtual std::vector<TPortionInfo::TPtr> DoModifyPortions(
        const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override;

    virtual TCompactionTaskData DoGetOptimizationTask() const override;

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& /*pkSchema*/) const override {
        NArrow::NMerger::TIntervalPositions result;
        for (auto&& i : Portions) {
            result.AddPosition(i.GetStart().BuildSortablePosition(), false);
        }
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
