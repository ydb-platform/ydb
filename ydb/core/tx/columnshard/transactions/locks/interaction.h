#pragma once
#include <ydb/core/formats/arrow/replace_key.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/validator.h>

#include <util/generic/hash.h>

namespace NKikimr::NOlap::NTxInteractions {

class TPointInfo {
private:
    THashMap<ui64, bool> StartTxIds;
    THashMap<ui64, bool> FinishTxIds;
    YDB_READONLY_DEF(THashSet<ui64>, IntervalTxIds);

public:
    void AddStart(const ui64 txId, const bool include) {
        AFL_VERIFY(StartTxIds.emplace(txId, include).second);
    }
    void RemoveStart(const ui64 txId) {
        AFL_VERIFY(StartTxIds.erase(txId));
    }
    void AddFinish(const ui64 txId, const bool include) {
        AFL_VERIFY(FinishTxIds.emplace(txId, include).second);
    }
    void RemoveFinish(const ui64 txId) {
        AFL_VERIFY(FinishTxIds.erase(txId));
    }
    void AddIntervalTx(const ui64 txId) {
        AFL_VERIFY(IntervalTxIds.emplace(txId).second);
    }
    void RemoveIntervalTx(const ui64 txId) {
        AFL_VERIFY(IntervalTxIds.erase(txId));
    }
    bool TryRemoveTx(const ui64 txId) {
        return StartTxIds.erase(txId) || FinishTxIds.erase(txId) || IntervalTxIds.erase(txId);
    }

    bool IsEmpty() const {
        return StartTxIds.empty() && FinishTxIds.empty() && IntervalTxIds.empty();
    }

    void ProvideTxIdsFrom(const TPointInfo& previouse) {
        for (auto&& i : previouse.IntervalTxIds) {
            if (!FinishTxIds.contains(i)) {
                AFL_VERIFY(IntervalTxIds.emplace(i).second);
            }
        }
    }
};

class TIntervalPoint {
private:
    int IncludeState = 0;
    NArrow::TReplaceKey PrimaryKey;

public:
    TIntervalPoint(const NArrow::TReplaceKey& primaryKey, const int includeState)
        : IncludeState(includeState)
        , PrimaryKey(primaryKey) {
    }

    TIntervalPoint(const std::shared_ptr<NArrow::TReplaceKey>& primaryKey, const int includeState)
        : IncludeState(includeState)
        , PrimaryKey(*TValidator::CheckNotNull(primaryKey)) {
    }

    bool IsIncluded() const {
        return IncludeState == 0;
    }

    bool operator==(const TIntervalPoint& item) const {
        return IncludeState == item.IncludeState && PrimaryKey == item.PrimaryKey;
    }

    const NArrow::TReplaceKey& GetPrimaryKey() const {
        return PrimaryKey;
    }

    bool operator<=(const TIntervalPoint& point) const {
        return !(point < *this);
    }

    bool operator<(const TIntervalPoint& point) const {
        const ui32 sizeMin = std::min<ui32>(PrimaryKey.Size(), point.PrimaryKey.Size());
        const std::partial_ordering compareResult = PrimaryKey.ComparePartNotNull(point.PrimaryKey, sizeMin);
        if (compareResult == std::partial_ordering::less) {
            return true;
        } else if (compareResult == std::partial_ordering::greater) {
            return false;
        } else {
            AFL_VERIFY(compareResult == std::partial_ordering::equivalent);
            if (PrimaryKey.Size() == point.PrimaryKey.Size()) {
                return IncludeState < point.IncludeState;
            } else if (PrimaryKey.Size() < point.PrimaryKey.Size()) {
                if (IncludeState <= 0) {
                    return true;
                } else {
                    return false;
                }
            } else {
                if (point.IncludeState <= 0) {
                    return false;
                } else {
                    return true;
                }
            }
            return false;
        }
    }
};

class TReadIntervals {
private:
    std::map<TIntervalPoint, TPointInfo> IntervalsInfo;

public:
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
            auto result = IntervalsInfo.emplace(intervalPoint, TPointInfo()).first;
            --it;
            result->second.ProvideTxIdsFrom(it->second);
            return result;
        }
    }

    THashSet<ui64> GetAffectedTxIds(const std::shared_ptr<arrow::RecordBatch>& writtenPrimaryKeys) const {
        AFL_VERIFY(writtenPrimaryKeys);
        auto it = IntervalsInfo.begin();
        THashSet<ui64> affectedTxIds;
        for (ui32 i = 0; i < writtenPrimaryKeys->num_rows();) {
            if (it == IntervalsInfo.end()) {
                return affectedTxIds;
            }
            auto rKey = NArrow::TReplaceKey::FromBatch(writtenPrimaryKeys, writtenPrimaryKeys->schema(), i);
            while (it != IntervalsInfo.end() && it->first <= TIntervalPoint(rKey, 0)) {
                ++it;
            }
            if (it == IntervalsInfo.end()) {
                return affectedTxIds;
            }
            if (it == IntervalsInfo.begin()) {
                ++i;
                continue;
            }
            auto itPred = it;
            --itPred;
            affectedTxIds.insert(itPred->second.GetIntervalTxIds().begin(), itPred->second.GetIntervalTxIds().end());
            while (i < writtenPrimaryKeys->num_rows()) {
                auto rKey = NArrow::TReplaceKey::FromBatch(writtenPrimaryKeys, writtenPrimaryKeys->schema(), i);
                if (TIntervalPoint(rKey, 0) < it->first) {
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
    THashSet<ui64> GetAffectedTxIds(const ui64 pathId, const std::shared_ptr<arrow::RecordBatch>& batch) const {
        auto it = ReadIntervalsByPathId.find(pathId);
        if (it == ReadIntervalsByPathId.end()) {
            return {};
        }
        return it->second.GetAffectedTxIds(batch);
    }

    void AddInterval(const ui64 txId, const ui64 pathId, const TIntervalPoint& from, const TIntervalPoint& to) {
        auto& intervals = ReadIntervalsByPathId[pathId];
        auto it = intervals.InsertPoint(from);
        auto toIt = intervals.InsertPoint(to);
        it->second.AddStart(txId, from.IsIncluded());
        for (; it != toIt; ++it) {
            it->second.AddIntervalTx(txId);
        }
        it->second.AddFinish(txId, to.IsIncluded());
    }

    void RemoveInterval(const ui64 txId, const ui64 pathId, const TIntervalPoint& from, const TIntervalPoint& to) {
        auto itIntervals = ReadIntervalsByPathId.find(pathId);
        AFL_VERIFY(itIntervals != ReadIntervalsByPathId.end())("path_id", pathId);
        auto& intervals = itIntervals->second;
        auto fromIt = intervals.GetPointIterator(from);
        auto toIt = intervals.GetPointIterator(to);
        fromIt->second.RemoveStart(txId);
        for (auto it = fromIt; it != toIt; ++it) {
            it->second.RemoveIntervalTx(txId);
        }
        toIt->second.RemoveFinish(txId);
        for (auto&& it = fromIt; it != toIt;) {
            if (it->second.IsEmpty()) {
                it = intervals.Erase(it);
            } else {
                ++it;
            }
        }
        if (toIt->second.IsEmpty()) {
            intervals.Erase(toIt);
        }
        if (intervals.IsEmpty()) {
            ReadIntervalsByPathId.erase(itIntervals);
        }
    }
};

}   // namespace NKikimr::NOlap::NTxInteractions
