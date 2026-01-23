#include "portion_interval_tree.h"

namespace NKikimr::NOlap::PortionIntervalTree {

TPositionView::TPositionView(const NArrow::NMerger::TSortableBatchPosition* sortableBatchPosition)
    : Position(sortableBatchPosition) {
}

TPositionView::TPositionView(std::shared_ptr<TPortionInfo> portionInfo, bool isLeft)
    : Position(isLeft ? TPositionVariant(std::in_place_index<LeftSimpleRow>, portionInfo)
                      : TPositionVariant(std::in_place_index<RightSimpleRow>, portionInfo)) {
}

NArrow::NMerger::TSortableBatchPosition TPositionView::GetSortableBatchPosition() const {
    if (auto val = std::get_if<LeftSimpleRow>(&Position); val) {
        return (*val)->IndexKeyStart().BuildSortablePosition();
    } else if (auto val = std::get_if<RightSimpleRow>(&Position); val) {
        return (*val)->IndexKeyEnd().BuildSortablePosition();
    } else if (auto val = std::get_if<SortableBatchPosition>(&Position); val) {
        return **val;
    } else {
        AFL_VERIFY(false)("error", "invalid type in TPositionView variant for GetSortableBatchPosition")("type", Position.index());
    }
}

std::partial_ordering TPositionView::Compare(const TPositionView& rhs) const {
    AFL_VERIFY(Position.index() > Infinity && Position.index() <= SortableBatchPosition &&
               rhs.Position.index() > Infinity && rhs.Position.index() <= SortableBatchPosition)
              ("lhs.index", Position.index())("rhs.index", rhs.Position.index());

    if (Position.index() == SortableBatchPosition || rhs.Position.index() == SortableBatchPosition) {
        return GetSortableBatchPosition().ComparePartial(rhs.GetSortableBatchPosition());
    } else if (auto val = std::get_if<LeftSimpleRow>(&Position); val) {
        return (*val)->IndexKeyStart().CompareNotNull(
            rhs.Position.index() == LeftSimpleRow ? std::get<LeftSimpleRow>(rhs.Position)->IndexKeyStart() : std::get<RightSimpleRow>(rhs.Position)->IndexKeyEnd());
    } else if (auto val = std::get_if<RightSimpleRow>(&Position); val) {
        return (*val)->IndexKeyEnd().CompareNotNull(
            rhs.Position.index() == LeftSimpleRow ? std::get<LeftSimpleRow>(rhs.Position)->IndexKeyStart() : std::get<RightSimpleRow>(rhs.Position)->IndexKeyEnd());
    }

    AFL_VERIFY(false)("error", "invalid type in TPositionView variant for Compare")("type", Position.index());
}


int TPositionViewBorderComparator::Compare(const TBorder& lhs, const TBorder& rhs) {
    if (lhs.GetMode() == NRangeTreap::EBorderMode::LeftInf || rhs.GetMode() == NRangeTreap::EBorderMode::RightInf) {
        return lhs.GetMode() == rhs.GetMode() ? 0 : -1;
    } else if (lhs.GetMode() == NRangeTreap::EBorderMode::RightInf || rhs.GetMode() == NRangeTreap::EBorderMode::LeftInf) {
        return lhs.GetMode() == rhs.GetMode() ? 0 : 1;
    }

    auto comp = lhs.GetKey().Compare(rhs.GetKey());
    if (comp == std::partial_ordering::less) {
        return -1;
    } else if (comp == std::partial_ordering::greater) {
        return 1;
    }

    return NRangeTreap::TBorderModeTraits::CompareEqualPoint(lhs.GetMode(), rhs.GetMode());
}

void TPositionViewBorderComparator::ValidateKey(const TPositionView& /*key*/) {
    // Do nothing
}

} // namespace NKikimr::NOlap::PortionIntervalTree
