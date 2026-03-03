#pragma once

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/range_treap/range_treap.h>

#include <compare>
#include <memory>
#include <variant>
#include <vector>

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

    void AddRange(TOwnedRange range, const std::shared_ptr<TPortionInfo>& value) {
        ui32 intersectionsCount = 0;
        Impl.EachIntersection(range, [&intersectionsCount](const TRange&, const std::shared_ptr<TPortionInfo>& p) {
            p->AddIntervalTreeRangesCount(1);
            ++intersectionsCount;
            return true;
        });
        Impl.AddRange(std::move(range), value);
        value->AddIntervalTreeRangesCount(intersectionsCount);
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
    TImpl Impl;
};

} // namespace NKikimr::NOlap::NPortionIntervalTree
