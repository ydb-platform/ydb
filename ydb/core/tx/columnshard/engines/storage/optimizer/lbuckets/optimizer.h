#pragma once
#include "counters.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NStorageOptimizer::NBuckets {

static const ui64 SmallPortionDetectSizeLimit = 1 << 20;

TDuration GetCommonFreshnessCheckDuration() {
    static const TDuration CommonFreshnessCheckDuration = TDuration::Seconds(300);
    return NYDBTest::TControllers::GetColumnShardController()->GetOptimizerFreshnessCheckDuration(CommonFreshnessCheckDuration);
}

class TSimplePortionsGroupInfo {
private:
    YDB_READONLY(i64, Bytes, 0);
    YDB_READONLY(i64, Count, 0);
    YDB_READONLY(i64, RecordsCount, 0);

public:
    NJson::TJsonValue SerializeToJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("bytes", Bytes);
        result.InsertValue("count", Count);
        result.InsertValue("records_count", RecordsCount);
        return result;
    }

    TString DebugString() const {
        return TStringBuilder() << "{bytes=" << Bytes << ";count=" << Count << ";records=" << RecordsCount << "}";
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        Bytes += p->GetTotalBlobBytes();
        Count += 1;
        RecordsCount += p->NumRows();
    }
    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        Bytes -= p->GetTotalBlobBytes();
        Count -= 1;
        RecordsCount -= p->NumRows();
        AFL_VERIFY(Bytes >= 0);
        AFL_VERIFY(Count >= 0);
        AFL_VERIFY(RecordsCount >= 0);
    }
};

class TPortionsGroupInfo: public TSimplePortionsGroupInfo {
private:
    using TBase = TSimplePortionsGroupInfo;
    std::shared_ptr<TPortionCategoryCounters> Signals;

public:
    TPortionsGroupInfo(const std::shared_ptr<TPortionCategoryCounters>& signals)
        : Signals(signals) {
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        TBase::AddPortion(p);
        Signals->AddPortion(p);
    }
    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        TBase::RemovePortion(p);
        Signals->RemovePortion(p);
    }
};

class TPortionsPool {
private:
    THashMap<ui64, std::shared_ptr<TPortionInfo>> PreActuals;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Actuals;
    std::map<TInstant, THashMap<ui64, std::shared_ptr<TPortionInfo>>> Futures;
    TSimplePortionsGroupInfo BucketInfo;
    std::shared_ptr<TCounters> Counters;
    const TDuration FutureDetector;
    bool AddActual(const std::shared_ptr<TPortionInfo>& portion) {
        if (Actuals.emplace(portion->GetPortionId(), portion).second) {
            BucketInfo.AddPortion(portion);
            Counters->PortionsForMerge->AddPortion(portion);
            Counters->ActualPortions->AddPortion(portion);
            return true;
        } else {
            return false;
        }
    }

    bool AddPreActual(const std::shared_ptr<TPortionInfo>& portion) {
        if (PreActuals.emplace(portion->GetPortionId(), portion).second) {
            return true;
        } else {
            return false;
        }
    }

    bool RemoveActual(const std::shared_ptr<TPortionInfo>& portion) {
        if (Actuals.erase(portion->GetPortionId())) {
            BucketInfo.RemovePortion(portion);
            Counters->PortionsForMerge->RemovePortion(portion);
            Counters->ActualPortions->RemovePortion(portion);
            return true;
        } else {
            return false;
        }
    }

    bool RemovePreActual(const std::shared_ptr<TPortionInfo>& portion) {
        if (PreActuals.erase(portion->GetPortionId())) {
            return true;
        } else {
            return false;
        }
    }

    bool RemoveFutures(const TInstant instant) {
        auto itFutures = Futures.find(instant);
        if (itFutures == Futures.end()) {
            return false;
        }
        for (auto&& i : itFutures->second) {
            Counters->FuturePortions->RemovePortion(i.second);
        }
        Futures.erase(itFutures);
        return true;
    }

    bool AddFuture(const std::shared_ptr<TPortionInfo>& portion) {
        auto portionMaxSnapshotInstant = TInstant::MilliSeconds(portion->RecordSnapshotMax().GetPlanStep());
        if (Futures[portionMaxSnapshotInstant].emplace(portion->GetPortionId(), portion).second) {
            Counters->FuturePortions->AddPortion(portion);
            return true;
        } else {
            return false;
        }
    }

    bool AddFutures(const TInstant instant, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions) {
        if (portions.empty()) {
            return true;
        }
        auto& futures = Futures[instant];
        bool hasDuplications = false;
        for (auto&& i : portions) {
            if (!futures.emplace(i.second->GetPortionId(), i.second).second) {
                hasDuplications = true;
            } else {
                Counters->FuturePortions->AddPortion(i.second);
            }
        }
        return !hasDuplications;
    }

    bool RemoveFuture(const std::shared_ptr<TPortionInfo>& portion) {
        auto portionMaxSnapshotInstant = TInstant::MilliSeconds(portion->RecordSnapshotMax().GetPlanStep());
        auto it = Futures.find(portionMaxSnapshotInstant);
        if (it == Futures.end()) {
            return false;
        }
        if (!it->second.erase(portion->GetPortionId())) {
            return false;
        }
        Counters->FuturePortions->RemovePortion(portion);
        if (it->second.empty()) {
            Futures.erase(it);
        }
        return true;
    }

    bool RemoveFutures(const TInstant instant, const std::vector<std::shared_ptr<TPortionInfo>>& portions) {
        if (portions.empty()) {
            return true;
        }
        auto itFutures = Futures.find(instant);
        if (itFutures == Futures.end()) {
            return false;
        }
        bool hasAbsent = false;
        for (auto&& i : portions) {
            if (!itFutures->second.erase(i->GetPortionId())) {
                hasAbsent = true;
            } else {
                Counters->FuturePortions->RemovePortion(i);
            }
        }
        if (itFutures->second.empty()) {
            Futures.erase(itFutures);
        }
        return !hasAbsent;
    }

public:
    void InitRuntimeFeature() const {
        for (auto&& f : Futures) {
            for (auto&& p : f.second) {
                p.second->RemoveRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
            }
        }
        for (auto&& i : PreActuals) {
            i.second->RemoveRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        }
        for (auto&& i : Actuals) {
            i.second->RemoveRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        }
    }

    bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
        for (auto&& f : Futures) {
            for (auto&& p : f.second) {
                if (auto lockInfo = dataLocksManager->IsLocked(*p.second)) {
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "optimization_locked")("reason", *lockInfo);
                    return true;
                }
            }
        }
        for (auto&& i : PreActuals) {
            if (auto lockInfo = dataLocksManager->IsLocked(*i.second)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "optimization_locked")("reason", *lockInfo);
                return true;
            }
        }
        for (auto&& i : Actuals) {
            if (auto lockInfo = dataLocksManager->IsLocked(*i.second)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "optimization_locked")("reason", *lockInfo);
                return true;
            }
        }
        return false;
    }

    bool Validate(const std::shared_ptr<TPortionInfo>& portion) const {
        if (portion) {
            AFL_VERIFY(!PreActuals.contains(portion->GetPortionId()));
            AFL_VERIFY(!Actuals.contains(portion->GetPortionId()));
            for (auto&& f : Futures) {
                AFL_VERIFY(!f.second.contains(portion->GetPortionId()));
            }
        }
//        auto b = GetFutureBorder();
//        if (!b) {
//            AFL_VERIFY(PreActuals.empty());
//        }// else {
//            for (auto&& i : PreActuals) {
//                AFL_VERIFY(*b <= i.second->IndexKeyEnd());
//            }
//            for (auto&& i : Actuals) {
//                AFL_VERIFY(i.second->IndexKeyEnd() < *b);
//            }
//        }
        for (auto&& f : Futures) {
            for (auto&& p : f.second) {
                AFL_VERIFY(!Actuals.contains(p.first));
                AFL_VERIFY(!PreActuals.contains(p.first));
            }
        }
        for (auto&& i : PreActuals) {
            AFL_VERIFY(!Actuals.contains(i.first));
            for (auto&& f : Futures) {
                AFL_VERIFY(!f.second.contains(i.first));
            }
        }
        for (auto&& i : Actuals) {
            AFL_VERIFY(!PreActuals.contains(i.first));
            for (auto&& f : Futures) {
                AFL_VERIFY(!f.second.contains(i.first));
            }
        }
        return true;
    }

    bool IsEmpty() const {
        return Actuals.empty() && Futures.empty() && PreActuals.empty();
    }

    TPortionsPool(const std::shared_ptr<TCounters>& counters, const TDuration futureDetector)
        : Counters(counters)
        , FutureDetector(futureDetector) {
    }

    ~TPortionsPool() {
        for (auto&& i : Actuals) {
            Counters->PortionsForMerge->RemovePortion(i.second);
            Counters->ActualPortions->RemovePortion(i.second);
        }
        for (auto&& f : Futures) {
            for (auto&& i : f.second) {
                Counters->FuturePortions->RemovePortion(i.second);
            }
        }
    }

    std::shared_ptr<TPortionInfo> GetOldestPortion(const bool withFutures) const {
        std::shared_ptr<TPortionInfo> result;
        std::optional<TSnapshot> snapshot;
        for (auto&& i : Actuals) {
            if (!snapshot || *snapshot > i.second->RecordSnapshotMax()) {
                snapshot = i.second->RecordSnapshotMax();
                result = i.second;
            }
        }
        for (auto&& i : PreActuals) {
            if (!snapshot || *snapshot > i.second->RecordSnapshotMax()) {
                snapshot = i.second->RecordSnapshotMax();
                result = i.second;
            }
        }
        if (withFutures) {
            for (auto&& f : Futures) {
                for (auto&& i : f.second) {
                    if (!snapshot || *snapshot > i.second->RecordSnapshotMax()) {
                        snapshot = i.second->RecordSnapshotMax();
                        result = i.second;
                    }
                }
            }
        }
        return result;
    }

    std::shared_ptr<TPortionInfo> GetYoungestPortion(const bool withFutures) const {
        std::shared_ptr<TPortionInfo> result;
        std::optional<TSnapshot> snapshot;
        for (auto&& i : Actuals) {
            if (!snapshot || *snapshot < i.second->RecordSnapshotMax()) {
                snapshot = i.second->RecordSnapshotMax();
                result = i.second;
            }
        }
        for (auto&& i : PreActuals) {
            if (!snapshot || *snapshot < i.second->RecordSnapshotMax()) {
                snapshot = i.second->RecordSnapshotMax();
                result = i.second;
            }
        }
        if (withFutures) {
            for (auto&& f : Futures) {
                for (auto&& i : f.second) {
                    if (!snapshot || *snapshot < i.second->RecordSnapshotMax()) {
                        snapshot = i.second->RecordSnapshotMax();
                        result = i.second;
                    }
                }
            }
        }
        return result;
    }

    bool ActualsEmpty() const {
        return Actuals.empty();
    }

    std::optional<TInstant> GetFutureStartInstant() const {
        if (Futures.empty()) {
            return {};
        }
        return Futures.begin()->first;
    }

    std::optional<NArrow::TReplaceKey> GetFutureBorder() const {
        if (Futures.empty()) {
            return {};
        }
        std::optional<NArrow::TReplaceKey> result;
        for (auto&& s : Futures) {
            for (auto&& p : s.second) {
                if (!result || p.second->IndexKeyStart() < *result) {
                    result = p.second->IndexKeyStart();
                }
            }
        }
        return result;
    }

    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetActualsInfo() const {
        return Actuals;
    }

    std::vector<std::shared_ptr<TPortionInfo>> GetOptimizerTaskPortions(const ui64 sizeLimit, std::optional<NArrow::TReplaceKey>& separatePoint) const {
        std::vector<std::shared_ptr<TPortionInfo>> sorted;
        for (auto&& i : Actuals) {
            sorted.emplace_back(i.second);
        }
        for (auto&& i : PreActuals) {
            sorted.emplace_back(i.second);
        }
        const auto pred = [](const std::shared_ptr<TPortionInfo>& l, const std::shared_ptr<TPortionInfo>& r) {
            return l->IndexKeyStart() < r->IndexKeyStart();
        };
        std::sort(sorted.begin(), sorted.end(), pred);

        std::vector<std::shared_ptr<TPortionInfo>> result;
        std::shared_ptr<NCompaction::TGeneralCompactColumnEngineChanges::IMemoryPredictor> predictor = NCompaction::TGeneralCompactColumnEngineChanges::BuildMemoryPredictor();
        ui64 txSizeLimit = 0;
        for (auto&& i : sorted) {
            result.emplace_back(i);
            if (txSizeLimit + i->GetTxVolume() > TGlobalLimits::TxWriteLimitBytes / 2) {
                break;
            }
            if (predictor->AddPortion(*i) > sizeLimit && result.size() > 1) {
                break;
            }
        }
        if (result.size() < sorted.size()) {
            separatePoint = sorted[result.size()]->IndexKeyStart();
        }
        return result;
    }

    void Add(const std::shared_ptr<TPortionInfo>& portion, const TInstant now) {
        portion->RemoveRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        auto portionMaxSnapshotInstant = TInstant::MilliSeconds(portion->RecordSnapshotMax().GetPlanStep());
        if (now - portionMaxSnapshotInstant < FutureDetector) {
            AFL_VERIFY(AddFuture(portion));
        } else {
            auto b = GetFutureBorder();
            if (!b || portion->IndexKeyEnd() < *b) {
                AFL_VERIFY(AddActual(portion));
            } else {
                AFL_VERIFY(AddPreActual(portion));
            }
        }
    }

    [[nodiscard]] bool Remove(const std::shared_ptr<TPortionInfo>& portion) {
        portion->AddRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        if (RemovePreActual(portion)) {
            return true;
        }
        if (RemoveActual(portion)) {
            return true;
        }
        if (RemoveFuture(portion)) {
            return true;
        }
        return false;
    }

    void MergeFrom(TPortionsPool& source) {
        for (auto&& i : source.Actuals) {
            if (!PreActuals.contains(i.first)) {
                AddActual(i.second);
            }
        }
        for (auto&& i : source.PreActuals) {
            AddPreActual(i.second);
            RemoveActual(i.second);
        }
        for (auto&& i : source.Futures) {
            AddFutures(i.first, i.second);
        }
    }

    [[nodiscard]] TInstant Actualize(const TInstant currentInstant) {
        TInstant result = TInstant::Max();
        auto border = GetFutureBorder();
        if (border) {
            for (auto&& i : Futures) {
                if (currentInstant - i.first >= FutureDetector) {
                    for (auto&& p : i.second) {
                        AFL_VERIFY(AddPreActual(p.second));
                    }
                } else {
                    result = std::min(result, i.first + FutureDetector);
                }
            }
            while (Futures.size() && currentInstant - Futures.begin()->first >= FutureDetector) {
                RemoveFutures(Futures.begin()->first);
            }
        }
        border = GetFutureBorder();
        {
            std::vector<std::shared_ptr<TPortionInfo>> remove;
            for (auto&& p : PreActuals) {
                if (!border || p.second->IndexKeyEnd() < *border) {
                    AFL_VERIFY(AddActual(p.second));
                    remove.emplace_back(p.second);
                }
            }
            for (auto&& i : remove) {
                AFL_VERIFY(RemovePreActual(i));
            }
        }
        {
            std::vector<std::shared_ptr<TPortionInfo>> remove;
            for (auto&& p : Actuals) {
                if (border && *border <= p.second->IndexKeyEnd()) {
                    AFL_VERIFY(AddPreActual(p.second));
                    remove.emplace_back(p.second);
                }
            }
            for (auto&& i : remove) {
                AFL_VERIFY(RemoveActual(i));
            }
        }
        return result;
    }

    void SplitTo(TPortionsPool& dest, const NArrow::TReplaceKey& destStart) {
        THashMap<TInstant, std::vector<std::shared_ptr<TPortionInfo>>> futuresForRemove;
        for (auto&& f : Futures) {
            THashMap<ui64, std::shared_ptr<TPortionInfo>> newPortions;
            for (auto&& i : f.second) {
                if (i.second->IndexKeyEnd() < destStart) {
                    continue;
                }
                AFL_VERIFY(newPortions.emplace(i.first, i.second).second);
                if (destStart <= i.second->IndexKeyStart()) {
                    futuresForRemove[f.first].emplace_back(i.second);
                }
            }
            AFL_VERIFY(dest.AddFutures(f.first, newPortions));
        }
        for (auto&& i : futuresForRemove) {
            AFL_VERIFY(RemoveFutures(i.first, i.second));
        }
        {
            std::vector<std::shared_ptr<TPortionInfo>> portionsForRemove;
            for (auto&& i : PreActuals) {
                if (i.second->IndexKeyEnd() < destStart) {
                    continue;
                }
                AFL_VERIFY(dest.AddPreActual(i.second));
                if (destStart <= i.second->IndexKeyStart()) {
                    portionsForRemove.emplace_back(i.second);
                }
            }
            for (auto&& i : portionsForRemove) {
                AFL_VERIFY(RemovePreActual(i));
            }
        }
        {
            std::vector<std::shared_ptr<TPortionInfo>> portionsForRemove;
            for (auto&& i : Actuals) {
                if (i.second->IndexKeyEnd() < destStart) {
                    continue;
                }
                AFL_VERIFY(dest.AddActual(i.second));
                if (destStart <= i.second->IndexKeyStart()) {
                    portionsForRemove.emplace_back(i.second);
                }
            }
            for (auto&& i : portionsForRemove) {
                AFL_VERIFY(RemoveActual(i));
            }
        }
    }

    i64 GetWeight(const std::shared_ptr<TPortionInfo>& mainPortion, const bool isFinal) const {
/*
        const ui64 count = BucketInfo.GetCount() + ((mainPortion && !isFinal) ? 1 : 0);
        //        const ui64 recordsCount = BucketInfo.GetRecordsCount() + ((mainPortion && !isFinal) ? mainPortion->GetRecordsCount() : 0);
        const ui64 sumBytes = BucketInfo.GetBytes() + ((mainPortion && !isFinal) ? mainPortion->GetTotalBlobBytes() : 0);
        if (count <= 1) {
            return 0;
        }
        if (isFinal) {
            if (sumBytes > 64 * 1024 * 1024) {
                return ((i64)1 << 50) + (10000000000.0 * count - sumBytes);
            }
        } else if (Futures.empty()) {
            return (10000000000.0 * count - sumBytes);
        }
        return 0;
*/

/*
        const ui64 count = BucketInfo.GetCount() + ((mainPortion && !isFinal) ? 1 : 0);
        //        const ui64 recordsCount = BucketInfo.GetRecordsCount() + ((mainPortion && !isFinal) ? mainPortion->GetRecordsCount() : 0);
        const ui64 sumBytes = BucketInfo.GetBytes() + ((mainPortion && !isFinal) ? mainPortion->GetTotalBlobBytes() : 0);
        if (count > 1 && (sumBytes > 32 * 1024 * 1024 || !isFinal || count > 100)) {
            return (10000000000.0 * count - sumBytes) * (isFinal ? 1 : 10);
        } else {
            return 0;
        }
*/
        const bool isForce = NYDBTest::TControllers::GetColumnShardController()->GetCompactionControl() == NYDBTest::EOptimizerCompactionWeightControl::Force;
        const ui64 count = BucketInfo.GetCount() + ((mainPortion && (!isFinal || isForce)) ? 1 : 0);
        const ui64 recordsCount = BucketInfo.GetRecordsCount() + ((mainPortion && !isFinal) ? mainPortion->GetRecordsCount() : 0);
        const ui64 sumBytes = BucketInfo.GetBytes() + ((mainPortion && !isFinal) ? mainPortion->GetTotalBlobBytes() : 0);
        if (NYDBTest::TControllers::GetColumnShardController()->GetCompactionControl() == NYDBTest::EOptimizerCompactionWeightControl::Disable) {
            return 0;
        }
        const ui64 weight = (10000000000.0 * count - sumBytes) * (isFinal ? 1 : 10);
        if (isForce) {
            return (count > 1) ? weight : 0;
        }

        if (count > 1 && (sumBytes > 32 * 1024 * 1024 || !isFinal || count > 100 || recordsCount > 100000)) {
            return (10000000000.0 * count - sumBytes) * (isFinal ? 1 : 10);
        } else {
            return 0;
        }
    }

    TString DebugString(const bool verbose = false) const {
        if (verbose) {
            TStringBuilder sb;
            std::shared_ptr<TPortionInfo> oldestPortion = GetOldestPortion(true);
            std::shared_ptr<TPortionInfo> youngestPortion = GetYoungestPortion(true);
            AFL_VERIFY(oldestPortion && youngestPortion);
            sb << "{"
                << "oldest="
                << "(" << oldestPortion->IndexKeyStart().DebugString() << ":" << oldestPortion->IndexKeyEnd().DebugString() << ":" << oldestPortion->RecordSnapshotMax().GetPlanStep() << ":" << oldestPortion->GetMeta().GetProduced() << ");"
                << "youngest="
                << "(" << youngestPortion->IndexKeyStart().DebugString() << ":" << youngestPortion->IndexKeyEnd().DebugString() << ":" << youngestPortion->RecordSnapshotMax().GetPlanStep() << ":" << youngestPortion->GetMeta().GetProduced() << ");"
                << "}"
                ;
            return sb;
        } else {
            return BucketInfo.DebugString();
        }
    }
};

class TPortionsBucket: public TMoveOnly {
private:
    std::shared_ptr<TPortionInfo> MainPortion;
    const std::shared_ptr<TCounters> Counters;
    mutable std::optional<i64> LastWeight;
    TPortionsPool Others;
    TInstant NextActualizeInstant = TInstant::Zero();
    std::optional<NArrow::TReplaceKey> NextBorder;

    void MoveNextBorderTo(TPortionsBucket& dest) {
        dest.NextBorder = NextBorder;
        if (dest.MainPortion) {
            NextBorder = dest.MainPortion->IndexKeyStart();
        } else {
            NextBorder = {};
        }
    }

    bool Validate() const {
        return Others.Validate(MainPortion);
    }

    void RebuildOptimizedFeature(const TInstant currentInstant) const {
        Others.InitRuntimeFeature();
        if (!MainPortion) {
            return;
        }
        MainPortion->InitRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized, Others.IsEmpty() && currentInstant > MainPortion->RecordSnapshotMax().GetPlanInstant() +
            NYDBTest::TControllers::GetColumnShardController()->GetLagForCompactionBeforeTierings(TDuration::Minutes(60)));
    }
public:
    class TModificationGuard: TNonCopyable {
    private:
        TPortionsBucket& Owner;
        const bool IsEmptyOthers = false;
        const bool HasNextBorder = false;

    public:
        TModificationGuard(TPortionsBucket& owner)
            : Owner(owner)
            , IsEmptyOthers(Owner.Others.ActualsEmpty())
            , HasNextBorder(Owner.NextBorder) {
            AFL_VERIFY_DEBUG(Owner.Validate());
        }

        ~TModificationGuard() {
            AFL_VERIFY_DEBUG(Owner.Validate());
            Owner.NextActualizeInstant = TInstant::Zero();
            if (!Owner.MainPortion) {
                return;
            }
            if (Owner.Others.ActualsEmpty()) {
                if (!IsEmptyOthers) {
                    Owner.Counters->PortionsForMerge->RemovePortion(Owner.MainPortion);
                    Owner.Counters->BucketsForMerge->Remove(1);
                    Owner.Counters->PortionsAlone->AddPortion(Owner.MainPortion);
                }
            } else if (IsEmptyOthers) {
                Owner.Counters->PortionsAlone->RemovePortion(Owner.MainPortion);
                Owner.Counters->BucketsForMerge->Add(1);
                Owner.Counters->PortionsForMerge->AddPortion(Owner.MainPortion);
            }
        }
    };

    bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
        if (MainPortion) {
            if (auto lockInfo = dataLocksManager->IsLocked(*MainPortion)) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "optimization_locked")("reason", *lockInfo);
                return true;
            }
        }
        return Others.IsLocked(dataLocksManager);
    }

    bool IsEmpty() const {
        return !MainPortion && Others.IsEmpty();
    }

    TModificationGuard StartModificationGuard() {
        return TModificationGuard(*this);
    }

    TPortionsBucket(const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TCounters>& counters)
        : MainPortion(portion)
        , Counters(counters)
        , Others(Counters, GetCommonFreshnessCheckDuration()) {
        if (MainPortion) {
            Counters->PortionsAlone->AddPortion(MainPortion);
        }
    }

    std::shared_ptr<TPortionInfo> GetYoungestPortion(const bool withFutures) const {
        auto otherPortion = Others.GetYoungestPortion(withFutures);
        if (MainPortion && otherPortion) {
            if (MainPortion->RecordSnapshotMax() > otherPortion->RecordSnapshotMax()) {
                return MainPortion;
            } else {
                return otherPortion;
            }
        } else if (MainPortion) {
            return MainPortion;
        } else if (otherPortion) {
            return otherPortion;
        }
        return nullptr;
    }

    std::shared_ptr<TPortionInfo> GetOldestPortion(const bool withFutures) const {
        auto otherPortion = Others.GetOldestPortion(withFutures);
        if (MainPortion && otherPortion) {
            if (MainPortion->RecordSnapshotMax() < otherPortion->RecordSnapshotMax()) {
                return MainPortion;
            } else {
                return otherPortion;
            }
        } else if (MainPortion) {
            return MainPortion;
        } else if (otherPortion) {
            return otherPortion;
        }
        return nullptr;
    }

    ~TPortionsBucket() {
        if (!MainPortion) {
            return;
        }
        if (Others.ActualsEmpty()) {
            Counters->PortionsAlone->RemovePortion(MainPortion);
        } else {
            Counters->PortionsForMerge->RemovePortion(MainPortion);
            Counters->BucketsForMerge->Remove(1);
        }
    }

    const std::shared_ptr<TPortionInfo>& GetPortion() const {
        AFL_VERIFY(MainPortion);
        return MainPortion;
    }

    i64 GetWeight() const {
        LastWeight = Others.GetWeight(MainPortion, !NextBorder);
        return *LastWeight;
    }

    i64 GetLastWeight() const {
        if (LastWeight) {
            return *LastWeight;
        } else {
            return GetWeight();
        }
    }

    std::shared_ptr<TColumnEngineChanges> BuildOptimizationTask(std::shared_ptr<TGranuleMeta> granule,
        const std::shared_ptr<NDataLocks::TManager>& locksManager, const NArrow::TReplaceKey* nextBorder, const std::shared_ptr<arrow::Schema>& primaryKeysSchema,
        const std::shared_ptr<IStoragesManager>& storagesManager) const
    {
        auto youngestPortion = GetYoungestPortion(nextBorder);
        auto oldestPortion = GetOldestPortion(nextBorder);
        AFL_VERIFY(youngestPortion && oldestPortion);
        Counters->OnNewTask(!NextBorder, TInstant::MilliSeconds(youngestPortion->RecordSnapshotMax().GetPlanStep()), TInstant::MilliSeconds(oldestPortion->RecordSnapshotMax().GetPlanStep()));
        AFL_VERIFY(!!NextBorder == !!nextBorder);
        if (nextBorder) {
            AFL_VERIFY(NextBorder);
            AFL_VERIFY(*nextBorder == *NextBorder);
        }
        std::optional<NArrow::TReplaceKey> stopPoint;
        std::optional<TInstant> stopInstant;
        const ui64 memLimit = HasAppData() ? AppDataVerified().ColumnShardConfig.GetCompactionMemoryLimit() : 512 * 1024 * 1024;
        std::vector<std::shared_ptr<TPortionInfo>> portions = Others.GetOptimizerTaskPortions(memLimit, stopPoint);
        bool forceMergeForTests = false;
        if (nextBorder) {
            if (MainPortion) {
                portions.emplace_back(MainPortion);
            }
            if (!stopPoint) {
                stopPoint = *nextBorder;
            }
        } else {
            if (MainPortion) {
                if (portions.size() == 1) {
                    AFL_VERIFY(NYDBTest::TControllers::GetColumnShardController()->GetCompactionControl() == NYDBTest::EOptimizerCompactionWeightControl::Force);
                    forceMergeForTests = true;
                    portions.emplace_back(MainPortion);
                } else {
                    for (auto&& i : portions) {
                        if (MainPortion->CrossPKWith(*i)) {
                            portions.emplace_back(MainPortion);
                            break;
                        }
                    }
                }
            }
            if (!stopPoint) {
                stopPoint = Others.GetFutureBorder();
                stopInstant = Others.GetFutureStartInstant();
            }
        }
        AFL_VERIFY(portions.size() > 1)("size", portions.size());
        ui64 size = 0;
        for (auto&& i : portions) {
            size += i->GetTotalBlobBytes();
            if (locksManager->IsLocked(*i)) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("info", Others.DebugString())("event", "skip_optimization")("reason", "busy");
                return nullptr;
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("stop_instant", stopInstant)("size", size)("next", NextBorder ? NextBorder->DebugString() : "")
            ("count", portions.size())("info", Others.DebugString())("event", "start_optimization")("stop_point", stopPoint ? stopPoint->DebugString() : "");
        TSaverContext saverContext(storagesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portions, saverContext);
        if (MainPortion) {
            NArrow::NMerger::TSortableBatchPosition pos(MainPortion->IndexKeyStart().ToBatch(primaryKeysSchema), 0, primaryKeysSchema->field_names(), {}, false);
            result->AddCheckPoint(pos, false, false);
        }
        if (!nextBorder && MainPortion && !forceMergeForTests) {
            NArrow::NMerger::TSortableBatchPosition pos(MainPortion->IndexKeyEnd().ToBatch(primaryKeysSchema), 0, primaryKeysSchema->field_names(), {}, false);
            result->AddCheckPoint(pos, true, false);
        }
        if (stopPoint) {
            NArrow::NMerger::TSortableBatchPosition pos(stopPoint->ToBatch(primaryKeysSchema), 0, primaryKeysSchema->field_names(), {}, false);
            result->AddCheckPoint(pos, false, false);
        }
        return result;
    }

    void AddOther(const std::shared_ptr<TPortionInfo>& portion, const TInstant now) {
        auto gChartsThis = StartModificationGuard();
        if (NextBorder && MainPortion) {
            AFL_VERIFY(portion->CrossPKWith(MainPortion->IndexKeyStart(), *NextBorder));
#ifndef NDEBUG
            auto oldPortionInfo = GetOldestPortion(true);
            auto youngPortionInfo = GetYoungestPortion(true);
            AFL_VERIFY(oldPortionInfo && youngPortionInfo);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)
            ("event", "other_not_final")("delta", youngPortionInfo->RecordSnapshotMax().GetPlanStep() - oldPortionInfo->RecordSnapshotMax().GetPlanStep())
                ("main", MainPortion->DebugString(true))("current", portion->DebugString(true))("oldest", oldPortionInfo->DebugString(true))
                ("young", youngPortionInfo->DebugString(true))("bucket_from", MainPortion->IndexKeyStart().DebugString())("bucket_to", NextBorder->DebugString());
#endif
        }
        Others.Add(portion, now);
    }

    void RemoveOther(const std::shared_ptr<TPortionInfo>& portion) {
        auto gChartsThis = StartModificationGuard();
        AFL_VERIFY(Others.Remove(portion))("portion", portion->DebugString())("bucket_start", MainPortion ? MainPortion->DebugString(true) : "-inf")("bucket_finish", NextBorder ? NextBorder->DebugString() : "undef");
    }

    void MergeOthersFrom(TPortionsBucket& dest) {
        auto gChartsDest = dest.StartModificationGuard();
        auto gChartsThis = StartModificationGuard();
        Others.MergeFrom(dest.Others);
        dest.MoveNextBorderTo(*this);
    }

    void Actualize(const TInstant currentInstant) {
        if (currentInstant < NextActualizeInstant) {
            return;
        }
        auto gChartsThis = StartModificationGuard();
        NextActualizeInstant = Others.Actualize(currentInstant);
        RebuildOptimizedFeature(currentInstant);
    }

    void SplitOthersWith(TPortionsBucket& dest) {
        auto gChartsDest = dest.StartModificationGuard();
        auto gChartsThis = StartModificationGuard();
        MoveNextBorderTo(dest);
        AFL_VERIFY(dest.MainPortion);
        if (MainPortion) {
            AFL_VERIFY(MainPortion->IndexKeyEnd() < dest.MainPortion->IndexKeyStart());
        }
        Others.SplitTo(dest.Others, dest.MainPortion->IndexKeyStart());
    }
};

class TPortionBuckets {
private:
    const std::shared_ptr<arrow::Schema> PrimaryKeysSchema;
    const std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<TPortionsBucket> LeftBucket;
    std::map<NArrow::TReplaceKey, std::shared_ptr<TPortionsBucket>> Buckets;
    std::map<i64, THashSet<TPortionsBucket*>> BucketsByWeight;
    std::shared_ptr<TCounters> Counters;
    std::vector<std::shared_ptr<TPortionsBucket>> GetAffectedBuckets(const NArrow::TReplaceKey& fromInclude, const NArrow::TReplaceKey& toInclude) {
        std::vector<std::shared_ptr<TPortionsBucket>> result;
        auto itFrom = Buckets.upper_bound(fromInclude);
        auto itTo = Buckets.upper_bound(toInclude);
        if (itFrom == Buckets.begin()) {
            result.emplace_back(LeftBucket);
        } else {
            --itFrom;
        }
        for (auto it = itFrom; it != itTo; ++it) {
            result.emplace_back(it->second);
        }
        return result;
    }

    void RemoveBucketFromRating(const std::shared_ptr<TPortionsBucket>& bucket) {
        auto it = BucketsByWeight.find(bucket->GetLastWeight());
        AFL_VERIFY(it != BucketsByWeight.end());
        AFL_VERIFY(it->second.erase(bucket.get()));
        if (it->second.empty()) {
            BucketsByWeight.erase(it);
        }
    }

    void AddBucketToRating(const std::shared_ptr<TPortionsBucket>& bucket) {
        AFL_VERIFY(BucketsByWeight[bucket->GetWeight()].emplace(bucket.get()).second);
    }

    void RemoveOther(const std::shared_ptr<TPortionInfo>& portion) {
        auto buckets = GetAffectedBuckets(portion->IndexKeyStart(), portion->IndexKeyEnd());
        AFL_VERIFY(buckets.size());
        for (auto&& i : buckets) {
            RemoveBucketFromRating(i);
            i->RemoveOther(portion);
            AddBucketToRating(i);
        }
    }
    void AddOther(const std::shared_ptr<TPortionInfo>& portion, const TInstant now) {
        auto buckets = GetAffectedBuckets(portion->IndexKeyStart(), portion->IndexKeyEnd());
        for (auto&& i : buckets) {
            RemoveBucketFromRating(i);
            i->AddOther(portion, now);
            AddBucketToRating(i);
        }
    }
    bool RemoveBucket(const std::shared_ptr<TPortionInfo>& portion) {
        auto it = Buckets.find(portion->IndexKeyStart());
        if (it == Buckets.end()) {
            return false;
        }
        if (it->second->GetPortion()->GetPortionId() != portion->GetPortionId()) {
            return false;
        }
        RemoveBucketFromRating(it->second);
        if (it == Buckets.begin()) {
            RemoveBucketFromRating(LeftBucket);
            LeftBucket->MergeOthersFrom(*it->second);
            AddBucketToRating(LeftBucket);
        } else {
            auto itPred = it;
            --itPred;
            RemoveBucketFromRating(itPred->second);
            itPred->second->MergeOthersFrom(*it->second);
            AddBucketToRating(itPred->second);
        }
        Buckets.erase(it);
        return true;
    }

    void AddBucket(const std::shared_ptr<TPortionInfo>& portion) {
        auto insertInfo = Buckets.emplace(portion->IndexKeyStart(), std::make_shared<TPortionsBucket>(portion, Counters));
        AFL_VERIFY(insertInfo.second);
        if (insertInfo.first == Buckets.begin()) {
            RemoveBucketFromRating(LeftBucket);
            LeftBucket->SplitOthersWith(*insertInfo.first->second);
            AddBucketToRating(LeftBucket);
        } else {
            auto it = insertInfo.first;
            --it;
            RemoveBucketFromRating(it->second);
            it->second->SplitOthersWith(*insertInfo.first->second);
            AddBucketToRating(it->second);
        }
        AddBucketToRating(insertInfo.first->second);
    }

public:
    TPortionBuckets(const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<TCounters>& counters)
        : PrimaryKeysSchema(primaryKeysSchema)
        , StoragesManager(storagesManager)
        , LeftBucket(std::make_shared<TPortionsBucket>(nullptr, counters))
        , Counters(counters) {
        AddBucketToRating(LeftBucket);
    }

    bool IsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const {
        if (BucketsByWeight.empty()) {
            return false;
        }
        if (BucketsByWeight.rbegin()->second.empty()) {
            return false;
        }
        const TPortionsBucket* bucketForOptimization = *BucketsByWeight.rbegin()->second.begin();
        return bucketForOptimization->IsLocked(dataLocksManager);
    }

    bool IsEmpty() const {
        return Buckets.empty() && LeftBucket->IsEmpty();
    }
    TString DebugString() const {
        return "";
    }
    NJson::TJsonValue SerializeToJson() const {
        return NJson::JSON_NULL;
    }

    void Actualize(const TInstant currentInstant) {
        RemoveBucketFromRating(LeftBucket);
        LeftBucket->Actualize(currentInstant);
        AddBucketToRating(LeftBucket);
        for (auto&& i : Buckets) {
            RemoveBucketFromRating(i.second);
            i.second->Actualize(currentInstant);
            AddBucketToRating(i.second);
        }
    }

    i64 GetWeight() const {
        AFL_VERIFY(BucketsByWeight.size());
        return BucketsByWeight.rbegin()->first;
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
        if (portion->GetTotalBlobBytes() < NYDBTest::TControllers::GetColumnShardController()->GetSmallPortionSizeDetector(SmallPortionDetectSizeLimit)) {
            Counters->SmallPortions->RemovePortion(portion);
        }
        if (!RemoveBucket(portion)) {
            RemoveOther(portion);
        }
    }

    std::shared_ptr<TColumnEngineChanges> BuildOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        AFL_VERIFY(BucketsByWeight.size());
        if (!BucketsByWeight.rbegin()->first) {
            return nullptr;
        }
        AFL_VERIFY(BucketsByWeight.rbegin()->second.size());
        const TPortionsBucket* bucketForOptimization = *BucketsByWeight.rbegin()->second.begin();
        if (bucketForOptimization == LeftBucket.get()) {
            if (Buckets.size()) {
                return bucketForOptimization->BuildOptimizationTask(granule, locksManager, &Buckets.begin()->first, PrimaryKeysSchema, StoragesManager);
            } else {
                return bucketForOptimization->BuildOptimizationTask(granule, locksManager, nullptr, PrimaryKeysSchema, StoragesManager);
            }
        } else {
            auto it = Buckets.find(bucketForOptimization->GetPortion()->IndexKeyStart());
            AFL_VERIFY(it != Buckets.end());
            ++it;
            if (it != Buckets.end()) {
                return bucketForOptimization->BuildOptimizationTask(granule, locksManager, &it->first, PrimaryKeysSchema, StoragesManager);
            } else {
                return bucketForOptimization->BuildOptimizationTask(granule, locksManager, nullptr, PrimaryKeysSchema, StoragesManager);
            }
        }
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& portion, const TInstant now) {
        if (portion->GetTotalBlobBytes() < NYDBTest::TControllers::GetColumnShardController()->GetSmallPortionSizeDetector(SmallPortionDetectSizeLimit)) {
            Counters->SmallPortions->AddPortion(portion);
            AddOther(portion, now);
            return;
        }

        auto itFrom = Buckets.upper_bound(portion->IndexKeyStart());
        auto itTo = Buckets.upper_bound(portion->IndexKeyEnd());
        if (itFrom != itTo) {
            AddOther(portion, now);
        } else if (itFrom == Buckets.begin()) {
            AddBucket(portion);
        } else {
            if (itFrom == Buckets.end()) {
                const TDuration freshness = now - TInstant::MilliSeconds(portion->RecordSnapshotMax().GetPlanStep());
                if (Y_LIKELY(!NYDBTest::TControllers::GetColumnShardController()->NeedForceCompactionBacketsConstruction())) {
                    if (freshness < GetCommonFreshnessCheckDuration() || portion->GetMeta().GetProduced() == NPortion::EProduced::INSERTED) {
                        AddOther(portion, now);
                        return;
                    }
                }
                if (Buckets.empty()) {
                    AddBucket(portion);
                    return;
                }
            }
            --itFrom;
            if (!itFrom->second->GetPortion()->CrossPKWith(*portion)) {
                AddBucket(portion);
            } else {
                AddOther(portion, now);
            }
        }
    }

    std::vector<NArrow::NMerger::TSortableBatchPosition> GetBucketPositions() const {
        std::vector<NArrow::NMerger::TSortableBatchPosition> result;
        for (auto&& i : Buckets) {
            NArrow::NMerger::TSortableBatchPosition pos(i.second->GetPortion()->IndexKeyStart().ToBatch(PrimaryKeysSchema), 0, PrimaryKeysSchema->field_names(), {}, false);
            result.emplace_back(pos);
        }
        return result;
    }
};

class TOptimizerPlanner: public IOptimizerPlanner {
private:
    using TBase = IOptimizerPlanner;
    std::shared_ptr<TCounters> Counters;
    TPortionBuckets Buckets;
    const std::shared_ptr<IStoragesManager> StoragesManager;

protected:
    virtual bool DoIsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        return Buckets.IsLocked(dataLocksManager);
    }

    virtual void DoModifyPortions(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& add, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& remove) override {
        const TInstant now = TInstant::Now();
        for (auto&& [_, i] : remove) {
            if (i->GetMeta().GetTierName() != IStoragesManager::DefaultStorageId && i->GetMeta().GetTierName() != "") {
                continue;
            }
            Buckets.RemovePortion(i);
            if (Buckets.IsEmpty()) {
                Counters->OptimizersCount->Sub(1);
            }
        }
        for (auto&& [_, i] : add) {
            if (i->GetMeta().GetTierName() != IStoragesManager::DefaultStorageId && i->GetMeta().GetTierName() != "") {
                continue;
            }
            if (Buckets.IsEmpty()) {
                Counters->OptimizersCount->Add(1);
            }
            Buckets.AddPortion(i, now);
        }
    }
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        return Buckets.BuildOptimizationTask(granule, locksManager);
    }
    virtual void DoActualize(const TInstant currentInstant) override {
        Buckets.Actualize(currentInstant);
    }

    virtual TOptimizationPriority DoGetUsefulMetric() const override {
        if (Buckets.GetWeight()) {
            return TOptimizationPriority::Critical(Buckets.GetWeight());
        } else {
            return TOptimizationPriority::Zero();
        }
    }
    virtual TString DoDebugString() const override {
        return Buckets.DebugString();
    }
    virtual NJson::TJsonValue DoSerializeToJsonVisual() const override {
        return Buckets.SerializeToJson();
    }

public:
    virtual std::vector<NArrow::NMerger::TSortableBatchPosition> GetBucketPositions() const override {
        return Buckets.GetBucketPositions();
    }

    TOptimizerPlanner(const ui64 pathId, const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<arrow::Schema>& primaryKeysSchema)
        : TBase(pathId)
        , Counters(std::make_shared<TCounters>())
        , Buckets(primaryKeysSchema, storagesManager, Counters)
        , StoragesManager(storagesManager) {
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NBuckets
