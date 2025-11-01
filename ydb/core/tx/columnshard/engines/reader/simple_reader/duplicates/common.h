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

class TPortionStore;

class TPortionBorderView {
private:
    enum class EBorder {
        FIRST,
        LAST,
    };

private:
    YDB_READONLY_DEF(ui64, PortionId);
    EBorder Border;

private:
    TPortionBorderView(const ui64 portionId, const EBorder border)
        : PortionId(portionId)
        , Border(border)
    {
    }

public:
    NArrow::TSimpleRow GetIndexKey(const TPortionInfo& portion) const {
        AFL_VERIFY(PortionId == portion.GetPortionId());
        switch (Border) {
            case EBorder::FIRST:
                return portion.GetMeta().IndexKeyStart();
            case EBorder::LAST:
                return portion.GetMeta().IndexKeyEnd();
        }
    }

    NArrow::TSimpleRow GetIndexKeyVerified(const TPortionStore& portions) const;

    static TPortionBorderView First(const ui64 portionId) {
        return TPortionBorderView(portionId, EBorder::FIRST);
    }
    static TPortionBorderView Last(const ui64 portionId) {
        return TPortionBorderView(portionId, EBorder::LAST);
    }

    operator size_t() const {
        return CombineHashes(PortionId, (ui64)Border);
    }

    bool IsLast() const {
        switch (Border) {
            case EBorder::FIRST:
                return false;
            case EBorder::LAST:
                return true;
        }
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << (IsLast() ? "Last:" : "First:") << PortionId;
        return sb;
    }
};

class TPortionStore: TMoveOnly {
private:
    THashMap<ui64, TPortionInfo::TConstPtr> Portions;

public:
    TPortionStore(THashMap<ui64, TPortionInfo::TConstPtr>&& portions)
        : Portions(std::move(portions))
    {
    }

    TPortionInfo::TConstPtr GetPortionVerified(const ui64 portionId) const {
        auto* findPortion = Portions.FindPtr(portionId);
        AFL_VERIFY(findPortion)("portion", portionId);
        return *findPortion;
    }
};

class TIntervalBordersView {
private:
    TPortionBorderView Begin;
    TPortionBorderView End;

public:
    TIntervalBordersView(const TPortionBorderView& begin, const TPortionBorderView& end)
        : Begin(begin)
        , End(end)
    {
    }

    const TPortionBorderView& GetBegin() const {
        return Begin;
    }
    const TPortionBorderView& GetEnd() const {
        return End;
    }

    operator size_t() const {
        return CombineHashes((size_t)Begin, (size_t)End);
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{Begin=" << Begin.DebugString() << ";End=" << End.DebugString() << "}";
        return sb;
    }
};

class TSortableBorders {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>, Begin);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>, End);

public:
    TSortableBorders(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& begin,
        const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& end)
        : Begin(begin)
        , End(end)
    {
        AFL_VERIFY(Begin->Compare(*End) != std::partial_ordering::greater)("borders", DebugString());
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "[" << Begin->DebugJson() << ";" << End->DebugJson() << "]";
        return sb;
    }
};

class TDuplicateMapInfo {
private:
    TSnapshot MaxVersion;
    TIntervalBordersView Interval;
    YDB_READONLY_DEF(ui64, SourceId);

public:
    TDuplicateMapInfo(const TSnapshot& maxVersion, const TIntervalBordersView& interval, const ui64 sourceId)
        : MaxVersion(maxVersion)
        , Interval(interval)
        , SourceId(sourceId)
    {
    }

    operator size_t() const {
        size_t h = (size_t)MaxVersion;
        h = CombineHashes(h, (size_t)Interval);
        h = CombineHashes(h, SourceId);
        return h;
    }
    bool operator==(const TDuplicateMapInfo& other) const {
        return std::tie(MaxVersion, Interval, SourceId) == std::tie(other.MaxVersion, other.Interval, other.SourceId);
    }

    TString DebugString() const {
        return TStringBuilder() << "MaxVersion=" << MaxVersion.DebugString() << ";SourceId=" << SourceId;
    }

    const TIntervalBordersView& GetInterval() const {
        return Interval;
    }
};

class TIntervalBorder {
private:
    YDB_READONLY_DEF(bool, IsLast);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>, Key);
    YDB_READONLY_DEF(ui64, PortionId);

    TIntervalBorder(const bool isLast, const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const ui64 portionId)
        : IsLast(isLast)
        , Key(key)
        , PortionId(portionId)
    {
    }

public:
    static TIntervalBorder First(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const ui64 portionId) {
        return TIntervalBorder(false, key, portionId);
    }
    static TIntervalBorder Last(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const ui64 portionId) {
        return TIntervalBorder(true, key, portionId);
    }

    bool operator<(const TIntervalBorder& other) const {
        return std::tie(*Key, IsLast, PortionId) < std::tie(*other.Key, other.IsLast, other.PortionId);
    }
    bool operator==(const TIntervalBorder& other) = delete;
    bool IsEquivalent(const TIntervalBorder& other) const {
        return *Key == *other.Key && IsLast == other.IsLast;
    };

    TString DebugString() const {
        return TStringBuilder() << "{" << (IsLast ? "Last:" : "First:") << "Portion=" << PortionId << ";Data=" << Key->GetSorting()->DebugJson(0)
                                << "}";
    }

    TPortionBorderView MakeView() const {
        return IsLast ? TPortionBorderView::Last(PortionId) : TPortionBorderView::First(PortionId);
    }
};

class TIntervalInfo {
private:
    TIntervalBorder Begin;
    TIntervalBorder End;
    YDB_READONLY_DEF(ui64, IntersectingPortionsCount);

public:
    TIntervalInfo(const TIntervalBorder& begin, const TIntervalBorder& end, const ui64 intersectingPortionsCount)
        : Begin(begin)
        , End(end)
        , IntersectingPortionsCount(intersectingPortionsCount)
    {
    }

    const TIntervalBorder& GetBegin() const {
        return Begin;
    }

    const TIntervalBorder& GetEnd() const {
        return End;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
