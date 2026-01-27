#include "portion_interval_tree.h"

namespace NKikimr::NOlap::PortionIntervalTree {

TPositionView::TPositionView(const NArrow::NMerger::TSortableBatchPosition* sortableBatchPosition)
    : Position(sortableBatchPosition) {
}

TPositionView::TPositionView(std::shared_ptr<TPortionInfo> portionInfo, EPortionInfoIndexPosition keyPosition)
    : Position(keyPosition == EPortionInfoIndexPosition::Start ? TPositionVariant(std::in_place_index<StartSimpleRow>, std::move(portionInfo))
                                                               : TPositionVariant(std::in_place_index<EndSimpleRow>, std::move(portionInfo))) {
}

TPositionView TPositionView::FromPortionInfoIndexStart(std::shared_ptr<TPortionInfo> portionInfo) {
    return TPositionView(std::move(portionInfo), EPortionInfoIndexPosition::Start);
}

TPositionView TPositionView::FromPortionInfoIndexEnd(std::shared_ptr<TPortionInfo> portionInfo) {
    return TPositionView(std::move(portionInfo), EPortionInfoIndexPosition::End);
}

NArrow::NMerger::TSortableBatchPosition TPositionView::GetSortableBatchPosition() const {
    if (auto val = std::get_if<StartSimpleRow>(&Position); val) {
        return (*val)->IndexKeyStart().BuildSortablePosition();
    } else if (auto val = std::get_if<EndSimpleRow>(&Position); val) {
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
    } else if (auto val = std::get_if<StartSimpleRow>(&Position); val) {
        const auto& lhsMeta = (*val)->GetMeta();
        auto rhsIndexKeyView = rhs.Position.index() == StartSimpleRow ? std::get<StartSimpleRow>(rhs.Position)->GetMeta().IndexKeyViewStart()
                                                                      : std::get<EndSimpleRow>(rhs.Position)->GetMeta().IndexKeyViewEnd();

        return lhsMeta.IndexKeyViewStart().Compare(rhsIndexKeyView, lhsMeta.GetPkSchema()).GetResult();
    } else if (auto val = std::get_if<EndSimpleRow>(&Position); val) {
        const auto& lhsMeta = (*val)->GetMeta();
        auto rhsIndexKeyView = rhs.Position.index() == StartSimpleRow ? std::get<StartSimpleRow>(rhs.Position)->GetMeta().IndexKeyViewStart()
                                                                      : std::get<EndSimpleRow>(rhs.Position)->GetMeta().IndexKeyViewEnd();

        return lhsMeta.IndexKeyViewEnd().Compare(rhsIndexKeyView, lhsMeta.GetPkSchema()).GetResult();
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
