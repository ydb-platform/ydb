#pragma once

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/range_treap/range_treap.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TPortionStore;

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
    
    operator size_t() const {
        return CombineHashes((size_t)*Begin, (size_t)*End);
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "[" << Begin->DebugJson() << ";" << End->DebugJson() << "]";
        return sb;
    }
};

class TIntervalBorders {
private:
    TSortableBorders Interval;

public:
    TIntervalBorders(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& begin, const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& end)
        : Interval(begin, end)
    {
    }

    const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& GetBegin() const {
        return Interval.GetBegin();
    }
    const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& GetEnd() const {
        return Interval.GetEnd();
    }
    
    operator size_t() const {
        return (size_t)Interval;
    }
    
    bool operator==(const TIntervalBorders& other) const {
        return Interval.GetBegin() == other.Interval.GetBegin() && Interval.GetEnd() == other.Interval.GetEnd();
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{Begin=" << GetBegin()->DebugString() << ";End=" << GetEnd()->DebugString() << "}";
        return sb;
    }
};

class TDuplicateMapInfo {
private:
    TSnapshot MaxVersion;
    TIntervalBorders Interval;
    YDB_READONLY_DEF(ui64, PortionId);

public:
    TDuplicateMapInfo(const TSnapshot& maxVersion, const TIntervalBorders& interval, const ui64 portionId)
        : MaxVersion(maxVersion)
        , Interval(interval)
        , PortionId(portionId)
    {
    }

    explicit operator size_t() const {
        size_t h = (size_t)MaxVersion;
        h = CombineHashes(h, (size_t)Interval);
        h = CombineHashes(h, PortionId);
        return h;
    }
    bool operator==(const TDuplicateMapInfo& other) const {
        return std::tie(MaxVersion, Interval, PortionId) == std::tie(other.MaxVersion, other.Interval, other.PortionId);
    }

    TString DebugString() const {
        return TStringBuilder() << "MaxVersion=" << MaxVersion.DebugString() << ";PortionId=" << PortionId;
    }

    const TIntervalBorders& GetInterval() const {
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
};

class TIntervalInfo {
private:
    TIntervalBorder Begin;
    TIntervalBorder End;
    YDB_READONLY_DEF(ui64, IntersectingPortionsCount);
    ui64 ExclusivePortionId;

public:
    TIntervalInfo(const TIntervalBorder& begin, const TIntervalBorder& end, const THashSet<ui64>& portionIds)
        : Begin(begin)
        , End(end)
        , IntersectingPortionsCount(portionIds.size())
        , ExclusivePortionId(portionIds.size() == 1 ? *portionIds.begin() : 0)
    {
    }

    const TIntervalBorder& GetBegin() const {
        return Begin;
    }

    const TIntervalBorder& GetEnd() const {
        return End;
    }

    ui64 GetExclusivePortionId() const {
        AFL_VERIFY(IsExclusive());
        return ExclusivePortionId;
    }
    bool IsExclusive() const {
        return IntersectingPortionsCount == 1;
    }
    bool IsEmpty() const {
        return IntersectingPortionsCount == 0;
    }

    TIntervalBorders MakeInterval() const {
        return TIntervalBorders(Begin.GetKey(), End.GetKey());
    }
};

struct TPortionColumnFilter {
    ui64 Offset = 0;
    NArrow::TColumnFilter Filter;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
