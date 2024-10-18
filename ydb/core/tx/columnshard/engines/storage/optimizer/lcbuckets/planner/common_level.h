#pragma once
#include "abstract.h"
#include "counters.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TLevelPortions: public IPortionsLevel {
private:
    using TBase = IPortionsLevel;
    std::map<NArrow::TReplaceKey, std::shared_ptr<TPortionInfo>> Portions;
    const TLevelCounters LevelCounters;

    class TOrderedPortionInfo {
    private:
        YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, Portion);
        const ui64 RawBytes = 0;
    public:
        TOrderedPortionInfo(const std::shared_ptr<TPortionInfo>& portion)
            : Portion(portion)
            , RawBytes(Portion->GetTotalRawBytes()) {
        }

        bool operator<(const TOrderedPortionInfo& item) const {
            if (item.RawBytes == RawBytes) {
                return Portion->GetPortionId() < item.Portion->GetPortionId();
            } else {
                return item.RawBytes < RawBytes;
            }
        }
    };

    std::set<TOrderedPortionInfo> OrderedPortions;

    virtual std::optional<TPortionsChain> DoGetAffectedPortions(const NArrow::TReplaceKey& from, const NArrow::TReplaceKey& to) const override {
        if (Portions.empty()) {
            return std::nullopt;
        }
        std::vector<std::shared_ptr<TPortionInfo>> result;
        auto itFrom = Portions.upper_bound(from);
        auto itTo = Portions.upper_bound(to);
        if (itFrom != Portions.begin()) {
            auto it = itFrom;
            --it;
            if (from <= it->second->IndexKeyEnd()) {
                result.insert(result.begin(), it->second);
            }
        }
        for (auto it = itFrom; it != itTo; ++it) {
            result.emplace_back(it->second);
        }
        if (itTo != Portions.end()) {
            return TPortionsChain(std::move(result), itTo->second);
        } else if (result.size()) {
            return TPortionsChain(std::move(result), nullptr);
        } else {
            return std::nullopt;
        }
    }

    virtual ui64 DoGetWeight() const override {
        if (GetLevelId() > 1) {
            return 0;
        }
        if (PortionsInfo.GetBlobBytes() > (612 << 20) || PortionsInfo.GetRawBytes() > (256 << 20)) {
            return std::max<ui32>(500, PortionsInfo.GetCount());
        } else {
            return 0;
        }
    }

public:
    TLevelPortions(const ui64 levelId, const ui64 blobBytesLimit, const ui64 rawBytesLimit, const std::shared_ptr<IPortionsLevel>& nextLevel, const TLevelCounters& levelCounters)
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
            auto it = Portions.find(i->IndexKeyStart());
            AFL_VERIFY(it != Portions.end());
            AFL_VERIFY(it->second->GetPortionId() == i->GetPortionId());
            AFL_VERIFY(OrderedPortions.erase(i));
            PortionsInfo.RemovePortion(i);
            Portions.erase(it);
            LevelCounters.Portions->RemovePortion(i);
        }
        TStringBuilder sb;
        for (auto&& i : add) {
            sb << i->GetPortionId() << ",";
            auto info = Portions.emplace(i->IndexKeyStart(), i);
            i->AddRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
            AFL_VERIFY(info.second);
            AFL_VERIFY(OrderedPortions.emplace(i).second);
            PortionsInfo.AddPortion(i);
            {
                auto it = info.first;
                ++it;
                if (it != Portions.end()) {
                    AFL_VERIFY(i->IndexKeyEnd() < it->first)("start", i->IndexKeyStart().DebugString())("end", i->IndexKeyEnd().DebugString())(
                                                      "next", it->first.DebugString())("next1", it->second->IndexKeyStart().DebugString())(
                                                      "next2", it->second->IndexKeyEnd().DebugString())(
                                                      "level_id", GetLevelId())("portion_id_new", i->GetPortionId())("portion_id_old", it->second->GetPortionId())(
                                                      "portion_old", it->second->DebugString())("add", sb);
                }
            }
            {
                auto it = info.first;
                if (it != Portions.begin()) {
                    --it;
                    AFL_VERIFY(it->second->IndexKeyEnd() < i->IndexKeyStart())
                    ("start", i->IndexKeyStart().DebugString())("finish", i->IndexKeyEnd().DebugString())(
                        "pred_start", it->second->IndexKeyStart().DebugString())("pred_finish", it->second->IndexKeyEnd().DebugString())(
                        "level_id", GetLevelId())("portion_id_new", i->GetPortionId())("portion_id_old", it->second->GetPortionId())("add", sb);
                }
            }
            LevelCounters.Portions->AddPortion(i);
        }
    }

    virtual TCompactionTaskData DoGetOptimizationTask() const override {
        if (GetLevelId() == 1) {
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
        } else {
            AFL_VERIFY(false);
            return TCompactionTaskData(0);
        }
        /*
        AFL_VERIFY(OrderedPortions.size());
        ui64 compactedData = 0;
        TCompactionTaskData result(targetLevel->GetLevelId());
        auto itOrdered = OrderedPortions.begin();
        auto itFwd = Portions.find(itOrdered->GetPortion()->IndexKeyStart());
        AFL_VERIFY(itFwd != Portions.end());
        auto itBkwd = itFwd;
        if (itFwd != Portions.begin()) {
            --itBkwd;
        }
        while (BlobBytesLimit + compactedData < (ui64)PortionsInfo.GetBlobBytes() && (itBkwd != Portions.begin() || itFwd != Portions.end()) &&
               result.CanTakeMore()) {
            if (itFwd != Portions.end() && (itBkwd == Portions.begin() || itBkwd->second->GetTotalBlobBytes() <= itFwd->second->GetTotalBlobBytes())) {
                auto portion = itFwd->second;
                compactedData += portion->GetTotalBlobBytes();
                result.AddCurrentLevelPortion(portion);
                auto nextLevelPortions = targetLevel->GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd());
                if (nextLevelPortions) {
                    result.AddNextLevelPortionsSequence(std::move(*nextLevelPortions));
                }
                ++itFwd;
            } else if (itBkwd != Portions.begin() &&
                (itFwd == Portions.end() || itFwd->second->GetTotalBlobBytes() < itBkwd->second->GetTotalBlobBytes())) {
                auto portion = itBkwd->second;
                compactedData += portion->GetTotalBlobBytes();
                result.AddCurrentLevelPortion(portion);
                auto nextLevelPortions = targetLevel->GetAffectedPortions(portion->IndexKeyStart(), portion->IndexKeyEnd());
                if (nextLevelPortions) {
                    result.AddNextLevelPortionsSequence(std::move(*nextLevelPortions));
                }
                --itBkwd;
            }
        }
        return result;
*/
    }

    virtual NArrow::NMerger::TIntervalPositions DoGetBucketPositions(const std::shared_ptr<arrow::Schema>& pkSchema) const override {
        NArrow::NMerger::TIntervalPositions result;
        for (auto&& [_, p] : Portions) {
            result.AddPosition(
                NArrow::NMerger::TSortableBatchPosition(p->IndexKeyStart().ToBatch(pkSchema), 0, pkSchema->field_names(), {}, false), false);
            result.AddPosition(
                NArrow::NMerger::TSortableBatchPosition(p->IndexKeyEnd().ToBatch(pkSchema), 0, pkSchema->field_names(), {}, false), true);
        }
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
