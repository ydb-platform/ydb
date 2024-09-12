#pragma once
#include <ydb/core/formats/arrow/process_columns.h>
#include <ydb/library/formats/arrow/replace_key.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/validator.h>

#include <util/generic/hash.h>

namespace NKikimr::NOlap {
class TPredicateContainer;
}

namespace NKikimr::NOlap::NTxInteractions {

class TPointTxCounters {
private:
    YDB_READONLY(ui32, CountIncludes, 0);
    YDB_READONLY(ui32, CountNotIncludes, 0);

public:
    void Inc(const bool include) {
        if (include) {
            IncInclude();
        } else {
            IncNotInclude();
        }
    }
    bool Dec(const bool include) {
        if (include) {
            return DecInclude();
        } else {
            return DecNotInclude();
        }
    }
    void IncInclude() {
        ++CountIncludes;
    }
    [[nodiscard]] bool DecInclude() {
        AFL_VERIFY(CountIncludes);
        return --CountIncludes == 0;
    }
    void IncNotInclude() {
        ++CountNotIncludes;
    }
    [[nodiscard]] bool DecNotInclude() {
        AFL_VERIFY(CountNotIncludes);
        return --CountNotIncludes == 0;
    }
    bool IsEmpty() const {
        return !CountIncludes && !CountNotIncludes;
    }
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        if (CountIncludes) {
            result.InsertValue("count_include", CountIncludes);
        }
        if (CountNotIncludes) {
            result.InsertValue("count_not_include", CountNotIncludes);
        }
        return result;
    }
    ui32 GetCountSum() const {
        return CountIncludes + CountNotIncludes;
    }
};

class TIntervalTxCounters {
private:
    YDB_READONLY(ui32, Count, 0);

public:
    void Inc(const ui32 count = 1) {
        Count += count;
    }
    [[nodiscard]] bool Dec(const ui32 count = 1) {
        AFL_VERIFY(Count);
        Count -= count;
        return Count == 0;
    }
    bool IsEmpty() const {
        return !Count;
    }
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("count", Count);
        return result;
    }

    void ProvideFrom(const TIntervalTxCounters& counters) {
        Count += counters.Count;
        AFL_VERIFY(counters.Count);
    }
};

class TPointInfo {
private:
    THashMap<ui64, TPointTxCounters> StartTxIds;
    THashMap<ui64, TPointTxCounters> FinishTxIds;
    THashMap<ui64, TIntervalTxCounters> IntervalTxIds;

public:
    void InsertCurrentTxs(THashSet<ui64>& txIds, const bool includePoint) const {
        for (auto&& i : IntervalTxIds) {
            txIds.emplace(i.first);
        }
        if (includePoint) {
            for (auto&& i : FinishTxIds) {
                if (!i.second.GetCountIncludes()) {
                    continue;
                }
                auto it = StartTxIds.find(i.first);
                if (it != StartTxIds.end() && it->second.GetCountIncludes()) {
                    txIds.emplace(i.first);
                }
            }
        }
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& starts = result.InsertValue("starts", NJson::JSON_ARRAY);
        for (auto&& i : StartTxIds) {
            auto& data = starts.AppendValue(NJson::JSON_MAP);
            data.InsertValue("id", i.first);
            data.InsertValue("inc", i.second.DebugJson());
        }
        auto& finish = result.InsertValue("finishes", NJson::JSON_ARRAY);
        for (auto&& i : FinishTxIds) {
            auto& data = finish.AppendValue(NJson::JSON_MAP);
            data.InsertValue("id", i.first);
            data.InsertValue("inc", i.second.DebugJson());
        }
        auto& txs = result.InsertValue("txs", NJson::JSON_ARRAY);
        for (auto&& i : IntervalTxIds) {
            auto& data = txs.AppendValue(NJson::JSON_MAP);
            data.InsertValue("id", i.first);
            data.InsertValue("inc", i.second.DebugJson());
        }
        return result;
    }

    void AddStart(const ui64 txId, const bool include) {
        StartTxIds[txId].Inc(include);
    }
    void RemoveStart(const ui64 txId, const bool include) {
        if (StartTxIds[txId].Dec(include)) {
            StartTxIds.erase(txId);
        }
    }
    void AddFinish(const ui64 txId, const bool include) {
        FinishTxIds[txId].Inc(include);
    }
    void RemoveFinish(const ui64 txId, const bool include) {
        if (FinishTxIds[txId].Dec(include)) {
            FinishTxIds.erase(txId);
        }
    }
    void AddIntervalTx(const ui64 txId) {
        IntervalTxIds[txId].Inc();
    }
    void RemoveIntervalTx(const ui64 txId) {
        if (IntervalTxIds[txId].Dec()) {
            IntervalTxIds.erase(txId);
        }
    }
    bool TryRemoveTx(const ui64 txId, const bool include) {
        bool result = false;
        if (StartTxIds[txId].Dec(include)) {
            StartTxIds.erase(txId);
            result = true;
        }
        if (FinishTxIds[txId].Dec(include)) {
            FinishTxIds.erase(txId);
            result = true;
        }
        if (IntervalTxIds[txId].Dec(txId)) {
            IntervalTxIds.erase(txId);
            result = true;
        }
        return result;
    }

    bool IsEmpty() const {
        return StartTxIds.empty() && FinishTxIds.empty() && IntervalTxIds.empty();
    }

    void ProvideTxIdsFrom(const TPointInfo& previouse) {
        for (auto&& i : previouse.IntervalTxIds) {
            auto provided = i.second;
            {
                auto it = StartTxIds.find(i.first);
                if (it != StartTxIds.end()) {
                    provided.Inc(it->second.GetCountSum());
                }
            }
            {
                auto it = FinishTxIds.find(i.first);
                if (it != FinishTxIds.end()) {
                    if (provided.Dec(it->second.GetCountSum())) {
                        return;
                    }
                }
            }
            IntervalTxIds[i.first].ProvideFrom(provided);
        }
    }
};

class TIntervalPoint {
private:
    i32 IncludeState = 0;
    std::optional<NArrow::TReplaceKey> PrimaryKey;

    TIntervalPoint(const NArrow::TReplaceKey& primaryKey, const int includeState)
        : IncludeState(includeState)
        , PrimaryKey(primaryKey) {
    }

    TIntervalPoint(const std::shared_ptr<NArrow::TReplaceKey>& primaryKey, const int includeState)
        : IncludeState(includeState) {
        if (primaryKey) {
            PrimaryKey = *primaryKey;
        }
    }

public:
    static TIntervalPoint Equal(const NArrow::TReplaceKey& replaceKey) {
        return TIntervalPoint(replaceKey, 0);
    }
    static TIntervalPoint From(const TPredicateContainer& container, const std::shared_ptr<arrow::Schema>& pkSchema);
    static TIntervalPoint To(const TPredicateContainer& container, const std::shared_ptr<arrow::Schema>& pkSchema);

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("include", IncludeState);
        if (PrimaryKey) {
            result.InsertValue("pk", PrimaryKey->DebugString());
        }
        return result;
    }

    bool IsIncluded() const {
        return IncludeState == 0;
    }

    bool operator==(const TIntervalPoint& item) const {
        if (!PrimaryKey && !item.PrimaryKey) {
            return IncludeState == item.IncludeState;
        } else if (!PrimaryKey && item.PrimaryKey) {
            return false;
        } else if (PrimaryKey && !item.PrimaryKey) {
            return false;
        } else if (IncludeState == item.IncludeState) {
            if (PrimaryKey->Size() != item.PrimaryKey->Size()) {
                return false;
            }
            return *PrimaryKey == *item.PrimaryKey;
        } else {
            return false;
        }
    }

    bool operator<=(const TIntervalPoint& point) const {
        return !(point < *this);
    }

    bool operator<(const TIntervalPoint& point) const {
        if (!PrimaryKey && !point.PrimaryKey) {
            return IncludeState < point.IncludeState;
        } else if (!PrimaryKey && point.PrimaryKey) {
            return IncludeState < 0;
        } else if (PrimaryKey && !point.PrimaryKey) {
            return 0 < point.IncludeState;
        } else {
            const ui32 sizeMin = std::min<ui32>(PrimaryKey->Size(), point.PrimaryKey->Size());
            const std::partial_ordering compareResult = PrimaryKey->ComparePartNotNull(*point.PrimaryKey, sizeMin);
            if (compareResult == std::partial_ordering::less) {
                return true;
            } else if (compareResult == std::partial_ordering::greater) {
                return false;
            } else {
                AFL_VERIFY(compareResult == std::partial_ordering::equivalent);
                if (PrimaryKey->Size() == point.PrimaryKey->Size()) {
                    return IncludeState < point.IncludeState;
                } else if (PrimaryKey->Size() < point.PrimaryKey->Size()) {
                    if (IncludeState <= 1) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    if (point.IncludeState <= 1) {
                        return false;
                    } else {
                        return true;
                    }
                }
                return false;
            }
        }
    }
};

class TReadIntervals {
private:
    std::map<TIntervalPoint, TPointInfo> IntervalsInfo;

public:
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& jsonIntervals = result.InsertValue("intervals", NJson::JSON_ARRAY);
        for (auto&& i : IntervalsInfo) {
            auto& pointInfo = jsonIntervals.AppendValue(NJson::JSON_MAP);
            pointInfo.InsertValue("p", i.first.DebugJson());
            pointInfo.InsertValue("i", i.second.DebugJson());
        }
        return result;
    }

    bool IsEmpty() const {
        return IntervalsInfo.empty();
    }

    std::map<TIntervalPoint, TPointInfo>::iterator Erase(const std::map<TIntervalPoint, TPointInfo>::iterator& it) {
        return IntervalsInfo.erase(it);
    }

    std::map<TIntervalPoint, TPointInfo>::iterator GetPointIterator(const TIntervalPoint& intervalPoint) {
        auto it = IntervalsInfo.find(intervalPoint);
        AFL_VERIFY(it != IntervalsInfo.end());
        return it;
    }

    std::map<TIntervalPoint, TPointInfo>::iterator InsertPoint(const TIntervalPoint& intervalPoint) {
        auto it = IntervalsInfo.lower_bound(intervalPoint);
        if (it == IntervalsInfo.end() || it == IntervalsInfo.begin()) {
            return IntervalsInfo.emplace(intervalPoint, TPointInfo()).first;
        } else if (it->first == intervalPoint) {
            return it;
        } else {
            --it;
            auto result = IntervalsInfo.emplace(intervalPoint, TPointInfo()).first;
            result->second.ProvideTxIdsFrom(it->second);
            return result;
        }
    }

    THashSet<ui64> GetAffectedTxIds(const std::shared_ptr<arrow::RecordBatch>& writtenPrimaryKeys) const {
        AFL_VERIFY(writtenPrimaryKeys);
        auto it = IntervalsInfo.begin();
        THashSet<ui64> affectedTxIds;
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("batch", writtenPrimaryKeys->ToString())("info", DebugJson().GetStringRobust());
        for (ui32 i = 0; i < writtenPrimaryKeys->num_rows();) {
            if (it == IntervalsInfo.end()) {
                return affectedTxIds;
            }
            auto rKey = NArrow::TReplaceKey::FromBatch(writtenPrimaryKeys, writtenPrimaryKeys->schema(), i);
            auto pkIntervalPoint = TIntervalPoint::Equal(rKey);
            while (it != IntervalsInfo.end() && it->first < pkIntervalPoint) {
                ++it;
            }
            if (it == IntervalsInfo.end()) {
                return affectedTxIds;
            }
            auto itPred = it;
            bool equal = false;
            if (pkIntervalPoint < it->first) {
                if (it == IntervalsInfo.begin()) {
                    ++i;
                    continue;
                }
                if (pkIntervalPoint < it->first) {
                    --itPred;
                }
            } else {
                equal = true;
                ++it;
            }

            itPred->second.InsertCurrentTxs(affectedTxIds, equal);
            if (it == IntervalsInfo.end()) {
                return affectedTxIds;
            }
            while (i < writtenPrimaryKeys->num_rows()) {
                auto rKey = NArrow::TReplaceKey::FromBatch(writtenPrimaryKeys, writtenPrimaryKeys->schema(), i);
                if (TIntervalPoint::Equal(rKey) < it->first) {
                    ++i;
                } else {
                    break;
                }
            }
        }
        return affectedTxIds;
    }
};

class TInteractionsContext {
private:
    THashMap<ui64, TReadIntervals> ReadIntervalsByPathId;

public:
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        for (auto&& i : ReadIntervalsByPathId) {
            result.InsertValue(::ToString(i.first), i.second.DebugJson());
        }
        return result;
    }

    THashSet<ui64> GetAffectedTxIds(const ui64 pathId, const std::shared_ptr<arrow::RecordBatch>& batch) const {
        auto it = ReadIntervalsByPathId.find(pathId);
        if (it == ReadIntervalsByPathId.end()) {
            return {};
        }
        return it->second.GetAffectedTxIds(batch);
    }

    void AddInterval(const ui64 txId, const ui64 pathId, const TIntervalPoint& from, const TIntervalPoint& to) {
        auto& intervals = ReadIntervalsByPathId[pathId];
        auto itFrom = intervals.InsertPoint(from);
        auto itTo = intervals.InsertPoint(to);
        itFrom->second.AddStart(txId, from.IsIncluded());
        for (auto it = itFrom; it != itTo; ++it) {
            it->second.AddIntervalTx(txId);
        }
        itTo->second.AddFinish(txId, to.IsIncluded());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "add_interval")("interactions_info", DebugJson().GetStringRobust());
    }

    void RemoveInterval(const ui64 txId, const ui64 pathId, const TIntervalPoint& from, const TIntervalPoint& to) {
        auto itIntervals = ReadIntervalsByPathId.find(pathId);
        AFL_VERIFY(itIntervals != ReadIntervalsByPathId.end())("path_id", pathId);
        auto& intervals = itIntervals->second;
        auto itFrom = intervals.GetPointIterator(from);
        auto itTo = intervals.GetPointIterator(to);
        itFrom->second.RemoveStart(txId, from.IsIncluded());
        for (auto it = itFrom; it != itTo; ++it) {
            it->second.RemoveIntervalTx(txId);
        }
        itTo->second.RemoveFinish(txId, to.IsIncluded());
        for (auto&& it = itFrom; it != itTo;) {
            if (it->second.IsEmpty()) {
                it = intervals.Erase(it);
            } else {
                ++it;
            }
        }
        if (itTo->second.IsEmpty()) {
            intervals.Erase(itTo);
        }
        if (intervals.IsEmpty()) {
            ReadIntervalsByPathId.erase(itIntervals);
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "remove_interval")("interactions_info", DebugJson().GetStringRobust());
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
