#pragma once

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/range_treap/range_treap.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <algorithm>
#include <compare>
#include <memory>
#include <variant>
#include <vector>

#include <util/generic/hash_set.h>

namespace NKikimr::NOlap::NPortionIntervalTree {

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


class TPositionView {
    enum EPositionType { LeftInf = 0, RightInf = 1, StartPortionInfo = 2, EndPortionInfo = 3, SimpleRow = 4 };

    using TPositionVariant = std::variant<std::monostate, std::monostate, std::shared_ptr<TPortionInfo>, std::shared_ptr<TPortionInfo>, NArrow::TSimpleRow>;

    TPositionVariant Position;

public:
    enum class EPortionInfoIndexPosition {
        Start,
        End
    };

    enum class EInfinityType {
        Left,
        Right
    };

    explicit TPositionView(NArrow::TSimpleRow&& simpleRow);
    explicit TPositionView(EInfinityType infType);
    TPositionView(std::shared_ptr<TPortionInfo> portionInfo, EPortionInfoIndexPosition keyPosition);

    static TPositionView FromPortionInfoIndexStart(std::shared_ptr<TPortionInfo> portionInfo);
    static TPositionView FromPortionInfoIndexEnd(std::shared_ptr<TPortionInfo> portionInfo);

    static TPositionView MakeLeftInf();
    static TPositionView MakeRightInf();

    std::partial_ordering Compare(const TPositionView& rhs) const;
};


class TPositionViewBorderComparator {
    using TBorder = NRangeTreap::TBorder<TPositionView>;

public:
    static int Compare(const TBorder& lhs, const TBorder& rhs);
    static void ValidateKey(const TPositionView& /*key*/);
};


class TPortionIntervalTree {
private:
    using TImpl = NRangeTreap::TRangeTreap<
        TPositionView,
        std::shared_ptr<TPortionInfo>,
        TPositionView,
        TPortionIntervalTreeValueTraits,
        TPositionViewBorderComparator>;

public:
    using TOwnedRange = TImpl::TOwnedRange;
    using TRange = TImpl::TRange;
    using TBorder = TImpl::TBorder;

    explicit TPortionIntervalTree(bool useHeap,
        NMonitoring::TDynamicCounters::TCounterPtr maxIntersectionsCounter)
        : UseHeap(useHeap)
        , MaxIntersectionsCounter(std::move(maxIntersectionsCounter))
    {
        AFL_VERIFY(MaxIntersectionsCounter);
    }

    void AddRange(TOwnedRange range, const std::shared_ptr<TPortionInfo>& value) {
        ui32 intersectionsCount = 0;
        Impl.EachIntersection(range, [&intersectionsCount](const TRange&, const std::shared_ptr<TPortionInfo>& p) {
            p->AddIntervalTreeRangesCount(1);
            ++intersectionsCount;
            return true;
        });
        Impl.AddRange(std::move(range), value);
        value->AddIntervalTreeRangesCount(intersectionsCount);
        SetHeapDirty();
    }

    void RemoveRanges(const std::shared_ptr<TPortionInfo>& value) {
        std::vector<TRange> rangesOfValue;
        Impl.EachRange([&value, &rangesOfValue](const TRange& r, const std::shared_ptr<TPortionInfo>& p) {
            if (p == value) {
                rangesOfValue.push_back(r);
            }
            return true;
        });
        for (const auto& r : rangesOfValue) {
            Impl.EachIntersection(r, [&value](const TRange&, const std::shared_ptr<TPortionInfo>& p) {
                if (p != value) {
                    p->DecrementIntervalTreeRangesCount(1);
                }
                return true;
            });
        }
        value->ResetIntervalTreeRangesCount();
        Impl.RemoveRanges(value);
        SetHeapDirty();
    }

    /** Порция с максимальным числом пересечений (вершина кучи). nullptr если дерево пусто или куча отключена. */
    std::shared_ptr<TPortionInfo> GetPortionWithMaxIntersections() const {
        if (!UseHeap) {
            return nullptr;
        }
        if (HeapDirty) {
            RebuildHeap();
        }
        return Heap.empty() ? nullptr : Heap.front();
    }

    template <class TCallback>
    bool EachRange(TCallback&& callback) const {
        return Impl.EachRange(std::forward<TCallback>(callback));
    }

    template <class TCallback>
    bool EachIntersection(const TRange& range, TCallback&& callback) const {
        return Impl.EachIntersection(range, std::forward<TCallback>(callback));
    }

    template <class TCallback>
    bool EachIntersection(const TBorder& left, const TBorder& right, TCallback&& callback) const {
        return Impl.EachIntersection(left, right, std::forward<TCallback>(callback));
    }

    size_t Size() const noexcept {
        return Impl.Size();
    }

private:
    void SetHeapDirty() const {
        if (!UseHeap) {
            return;
        }
        HeapDirty = true;
    }

    void RebuildHeap() const {
        std::vector<std::shared_ptr<TPortionInfo>> portions;
        THashSet<TPortionAddress> seen;
        Impl.EachRange([&seen, &portions](const TRange&, const std::shared_ptr<TPortionInfo>& p) {
            if (seen.insert(p->GetAddress()).second) {
                portions.push_back(p);
            }
            return true;
        });
        Heap = std::move(portions);
        std::make_heap(Heap.begin(), Heap.end(), [](const std::shared_ptr<TPortionInfo>& a, const std::shared_ptr<TPortionInfo>& b) {
            return a->GetIntervalTreeRangesCount() < b->GetIntervalTreeRangesCount();
        });
        HeapDirty = false;
        AFL_VERIFY(MaxIntersectionsCounter);
        MaxIntersectionsCounter->Set(Heap.empty() ? 0 : Heap.front()->GetIntervalTreeRangesCount());
    }

private:
    TImpl Impl;
    const bool UseHeap;
    NMonitoring::TDynamicCounters::TCounterPtr MaxIntersectionsCounter;
    mutable std::vector<std::shared_ptr<TPortionInfo>> Heap;
    mutable bool HeapDirty = true;
};

} // namespace NKikimr::NOlap::NPortionIntervalTree
