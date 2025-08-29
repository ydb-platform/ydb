#pragma once

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/range_treap/range_treap.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

struct TPortionIntervalTreeValueTraits: NRangeTreap::TDefaultValueTraits<std::shared_ptr<TPortionInfo>> {
    struct TValueHash {
        ui64 operator()(const std::shared_ptr<TPortionInfo>& value) const {
            return THash<TPortionAddress>()(value->GetAddress());
        }
    };

    static bool Less(const std::shared_ptr<TPortionInfo>& a, const std::shared_ptr<TPortionInfo>& b) noexcept {
        return a->GetAddress() < b->GetAddress();
    }

    static bool Equal(const std::shared_ptr<TPortionInfo>& a, const std::shared_ptr<TPortionInfo>& b) noexcept {
        return a->GetAddress() == b->GetAddress();
    }
};

using TPortionIntervalTree =
    NRangeTreap::TRangeTreap<NArrow::TSimpleRow, std::shared_ptr<TPortionInfo>, NArrow::TSimpleRow, TPortionIntervalTreeValueTraits>;

class TRowRange {
private:
    YDB_READONLY_DEF(ui64, Begin);
    YDB_READONLY_DEF(ui64, End);

public:
    TRowRange(const ui64 begin, const ui64 end)
        : Begin(begin)
        , End(end)
    {
        AFL_VERIFY(end >= begin);
    }

    std::partial_ordering operator<=>(const TRowRange& other) const {
        return std::tie(Begin, End) <=> std::tie(other.Begin, other.End);
    }
    bool operator==(const TRowRange& other) const {
        return (*this <=> other) == std::partial_ordering::equivalent;
    }

    ui64 NumRows() const {
        return End - Begin;
    }

    operator size_t() const {
        return CombineHashes(Begin, End);
    }

    TString DebugString() const {
        return TStringBuilder() << "[" << Begin << ";" << End << ")";
    }
};

class TDuplicateMapInfo {
private:
    TSnapshot MaxVersion;
    TRowRange Rows;
    YDB_READONLY_DEF(ui64, SourceId);

public:
    TDuplicateMapInfo(const TSnapshot& maxVersion, const TRowRange& rows, const ui64 sourceId)
        : MaxVersion(maxVersion)
        , Rows(rows)
        , SourceId(sourceId)
    {
    }

    operator size_t() const {
        size_t h = (size_t)MaxVersion;
        h = CombineHashes(h, (size_t)Rows);
        h = CombineHashes(h, SourceId);
        return h;
    }
    bool operator==(const TDuplicateMapInfo& other) const {
        return std::tie(MaxVersion, Rows, SourceId) == std::tie(other.MaxVersion, other.Rows, other.SourceId);
    }

    TString DebugString() const {
        return TStringBuilder() << "MaxVersion=" << MaxVersion.DebugString() << ";Rows=" << Rows.DebugString() << ";SourceId=" << SourceId;
    }

    const TRowRange& GetRows() const {
        return Rows;
    }
};

class TBorder {
private:
    YDB_READONLY_DEF(bool, IsLast);
    NArrow::NMerger::TSortableBatchPosition Key;

    TBorder(const bool isLast, const NArrow::TSimpleRow key)
        : IsLast(isLast)
        , Key(NArrow::NMerger::TSortableBatchPosition(key.ToBatch(), 0, false)) {
    }

public:
    static TBorder First(NArrow::TSimpleRow&& key) {
        return TBorder(false, std::move(key));
    }
    static TBorder Last(NArrow::TSimpleRow&& key) {
        return TBorder(true, std::move(key));
    }

    std::partial_ordering operator<=>(const TBorder& other) const {
        return std::tie(Key, IsLast) <=> std::tie(other.Key, other.IsLast);
    };
    bool operator==(const TBorder& other) const {
        return (*this <=> other) == std::partial_ordering::equivalent;
    };

    const NArrow::NMerger::TSortableBatchPosition& GetKey() const {
        return Key;
    }

    TString DebugString() const {
        return TStringBuilder() << (IsLast ? "Last:" : "First:") << Key.GetSorting()->DebugJson(0);
    }
};

class TPortionsSlice {
private:
    THashMap<ui64, TRowRange> RangeByPortion;
    TBorder IntervalEnd;

public:
    TPortionsSlice(const TBorder& end)
        : IntervalEnd(end) {
    }

    void Reserve(const ui64 portionCount) {
        RangeByPortion.reserve(portionCount);
    }

    void AddRange(const ui64 portion, const TRowRange& range) {
        if (range.NumRows() == 0) {
            return;
        }
        AFL_VERIFY(RangeByPortion.emplace(portion, range).second);
    }

    const TRowRange* GetRangeOptional(const ui64 portion) const {
        return RangeByPortion.FindPtr(portion);
    }
    THashMap<ui64, TRowRange> GetRanges() const {
        return RangeByPortion;
    }
    const TBorder& GetEnd() const {
        return IntervalEnd;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
