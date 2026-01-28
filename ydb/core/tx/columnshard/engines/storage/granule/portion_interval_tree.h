#pragma once

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/range_treap/range_treap.h>

#include <compare>
#include <memory>
#include <variant>

namespace NKikimr::NOlap::PortionIntervalTree {

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
    enum EPositionType { LeftInf = 0, RightInf = 1, StartSimpleRow = 2, EndSimpleRow = 3, SortableBatchPosition = 4 };

    using TPositionVariant = std::variant<std::monostate, std::monostate, std::shared_ptr<TPortionInfo>, std::shared_ptr<TPortionInfo>, const NArrow::NMerger::TSortableBatchPosition*>;

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

    explicit TPositionView(const NArrow::NMerger::TSortableBatchPosition* sortableBatchPosition);
    explicit TPositionView(EInfinityType infType);
    TPositionView(std::shared_ptr<TPortionInfo> portionInfo, EPortionInfoIndexPosition keyPosition);

    static TPositionView FromPortionInfoIndexStart(std::shared_ptr<TPortionInfo> portionInfo);
    static TPositionView FromPortionInfoIndexEnd(std::shared_ptr<TPortionInfo> portionInfo);

    static TPositionView MakeLeftInf();
    static TPositionView MakeRightInf();

    NArrow::NMerger::TSortableBatchPosition GetSortableBatchPosition() const;
    std::partial_ordering Compare(const TPositionView& rhs) const;
};


class TPositionViewBorderComparator {
    using TBorder = NRangeTreap::TBorder<TPositionView>;

public:
    static int Compare(const TBorder& lhs, const TBorder& rhs);
    static void ValidateKey(const TPositionView& /*key*/);
};


using TPortionIntervalTree = NRangeTreap::TRangeTreap<TPositionView, std::shared_ptr<TPortionInfo>, TPositionView, TPortionIntervalTreeValueTraits, TPositionViewBorderComparator>;

} // namespace NKikimr::NOlap::PortionIntervalTree
