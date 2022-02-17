#pragma once

#include "flat_part_laid.h"
#include "flat_stat_part.h"
#include "flat_table_subset.h"

#include <util/generic/queue.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NTable {

// Iterates over all parts and maintains total row count and data size
class TStatsIterator {
public:
    explicit TStatsIterator(TIntrusiveConstPtr<TKeyCellDefaults> keyColumns)
        : KeyColumns(keyColumns)
        , Heap(TIterKeyGreater{ this })
    {}

    void Add(THolder<TScreenedPartIndexIterator> pi) {
        Iterators.PushBack(std::move(pi));
        TScreenedPartIndexIterator* it = Iterators.back();
        if (it->IsValid()) {
            NextRowCount += it->GetRowCountDelta();
            NextDataSize += it->GetDataSizeDelta();
            Heap.push(it);
        }
    }

    bool IsValid() const {
        return !Heap.empty() || CurrentKeyValid;
    }

    void Next() {
        ui64 lastRowCount = RowCount;
        ui64 lastDataSize = DataSize;
        Y_VERIFY(IsValid());

        while (!Heap.empty()) {
            RowCount = NextRowCount;
            DataSize = NextDataSize;
            TScreenedPartIndexIterator* it = Heap.top();
            Heap.pop();
            TDbTupleRef key = it->GetCurrentKey();
            TString serialized = TSerializedCellVec::Serialize({key.Columns, key.ColumnCount});
            CurrentKey = TSerializedCellVec(serialized);
            CurrentKeyValid = true;
            TDbTupleRef currentKeyTuple(KeyColumns->BasicTypes().data(), CurrentKey.GetCells().data(), CurrentKey.GetCells().size());

            if (MoveIterator(it))
                Heap.push(it);

            while (!Heap.empty() && CompareKeys(currentKeyTuple, Heap.top()->GetCurrentKey()) == 0) {
                it = Heap.top();
                Heap.pop();

                if (MoveIterator(it))
                    Heap.push(it);
            }

            if (RowCount != lastRowCount && DataSize != lastDataSize) {
                return;
            }
        }

        RowCount = NextRowCount;
        DataSize = NextDataSize;
        CurrentKeyValid = false;
    }

    TDbTupleRef GetCurrentKey() const {
        return TDbTupleRef(KeyColumns->BasicTypes().data(), CurrentKey.GetCells().data(), CurrentKey.GetCells().size());
    }

    ui64 GetCurrentRowCount() const {
        return RowCount;
    }

    ui64 GetCurrentDataSize() const {
        return DataSize;
    }

private:
    int CompareKeys(const TDbTupleRef& a, const TDbTupleRef& b) const noexcept {
        return ComparePartKeys(a.Cells(), b.Cells(), *KeyColumns);
    }

    struct TIterKeyGreater {
        const TStatsIterator* Self;

        bool operator ()(const TScreenedPartIndexIterator* a, const TScreenedPartIndexIterator* b) const {
            return Self->CompareKeys(a->GetCurrentKey(), b->GetCurrentKey()) > 0;
        }
    };

    bool MoveIterator(TScreenedPartIndexIterator* it) {
        it->Next();
        NextRowCount += it->GetRowCountDelta();
        NextDataSize += it->GetDataSizeDelta();

        return it->IsValid();
    }

    TIntrusiveConstPtr<TKeyCellDefaults> KeyColumns;
    THolderVector<TScreenedPartIndexIterator> Iterators;
    TPriorityQueue<TScreenedPartIndexIterator*, TSmallVec<TScreenedPartIndexIterator*>, TIterKeyGreater> Heap;
    TSerializedCellVec CurrentKey;
    ui64 RowCount = 0;
    ui64 DataSize = 0;
    ui64 NextRowCount = 0;
    ui64 NextDataSize = 0;
    bool CurrentKeyValid = false;
};

struct TBucket {
    TString EndKey;
    ui64 Value;
};

using THistogram = TVector<TBucket>;

struct TStats {
    ui64 RowCount = 0;
    ui64 DataSize = 0;
    THistogram RowCountHistogram;
    THistogram DataSizeHistogram;

    void Clear() {
        RowCount = 0;
        DataSize = 0;
        RowCountHistogram.clear();
        DataSizeHistogram.clear();
    }

    void Swap(TStats& other) {
        std::swap(RowCount, other.RowCount);
        std::swap(DataSize, other.DataSize);
        RowCountHistogram.swap(other.RowCountHistogram);
        DataSizeHistogram.swap(other.DataSizeHistogram);
    }
};

class TKeyAccessSample {
public:
    enum class EAccessKind {
        Read = 1,
        Update = 2,
        Delete = 3
    };

    using TSample = TVector<std::pair<TString, EAccessKind>>;

public:
    explicit TKeyAccessSample(ui64 sampleCount = 100)
        : SampleCount(sampleCount)
        , TotalCount(0)
    {}

    void Add(TArrayRef<const TCell> key, EAccessKind accessKind = EAccessKind::Read) {
        ui64 idx = TotalCount;
        ++TotalCount;
        if (idx >= SampleCount) {
            idx = RandomNumber<ui64>(TotalCount) ;
        }

        if (idx >= SampleCount) {
            return;
        }

        TSerializedCellVec saved(TSerializedCellVec::Serialize(key));

        auto it = KeyRefCount.find(saved.GetBuffer());
        if (it != KeyRefCount.end()) {
            // Add a reference for existing key
            saved = it->second.first;
            ++it->second.second;
        } else {
            KeyRefCount[saved.GetBuffer()] = std::make_pair(saved, 1);
        }

        if (Sample.size() < SampleCount) {
            Sample.emplace_back(std::make_pair(saved.GetBuffer(), accessKind));
        } else {
            TString old = Sample[idx].first;
            auto oit = KeyRefCount.find(old);
            Y_VERIFY(oit != KeyRefCount.end());

            // Delete the key if this was the last reference
            if (oit->second.second == 1) {
                KeyRefCount.erase(oit);
            } else {
                --oit->second.second;
            }

            Sample[idx] = std::make_pair(saved.GetBuffer(), accessKind);
        }
    }

    const TSample& GetSample() const {
        return Sample;
    }

    void Clear() {
        Sample.clear();
        TotalCount = 0;
        KeyRefCount.clear();
    }

private:
    TSample Sample;
    const ui64 SampleCount;
    ui64 TotalCount;
    // Store only unique keys and their ref counts to save memory
    THashMap<TString, std::pair<TSerializedCellVec, ui64>> KeyRefCount;
};

void BuildStats(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, const IPages* env);
void GetPartOwners(const TSubset& subset, THashSet<ui64>& partOwners);

}}
