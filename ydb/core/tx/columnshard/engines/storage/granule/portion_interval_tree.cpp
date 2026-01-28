#include "portion_interval_tree.h"

namespace NKikimr::NOlap::PortionIntervalTree {

TPositionView::TPositionView(const NArrow::NMerger::TSortableBatchPosition* sortableBatchPosition)
    : Position(sortableBatchPosition) {
}

TPositionView::TPositionView(EInfinityType infType)
    : Position(infType == EInfinityType::Left ? TPositionVariant(std::in_place_index<LeftInf>)
                                              : TPositionVariant(std::in_place_index<RightInf>)) {
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

TPositionView TPositionView::MakeLeftInf() {
    return TPositionView(EInfinityType::Left);
}

TPositionView TPositionView::MakeRightInf() {
    return TPositionView(EInfinityType::Right);
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
    if (Position.index() == LeftInf || rhs.Position.index() == RightInf) {
        return Position.index() == rhs.Position.index() ? std::partial_ordering::equivalent : std::partial_ordering::less;
    } else if (Position.index() == RightInf || rhs.Position.index() == LeftInf) {
        return std::partial_ordering::greater;
    }

    if (Position.index() == SortableBatchPosition) {
        return std::get<SortableBatchPosition>(Position)->ComparePartial(rhs.Position.index() == SortableBatchPosition ? *std::get<SortableBatchPosition>(rhs.Position)
                                                                                                                       : rhs.GetSortableBatchPosition());
    } else if (rhs.Position.index() == SortableBatchPosition) {
        return GetSortableBatchPosition().ComparePartial(*std::get<SortableBatchPosition>(rhs.Position));
    }

    AFL_VERIFY(Position.index() == StartSimpleRow || Position.index() == EndSimpleRow)("Position.index()", Position.index());
    AFL_VERIFY(rhs.Position.index() == StartSimpleRow || rhs.Position.index() == EndSimpleRow)("rhs.Position.index()", rhs.Position.index());

    const auto& lhsMeta = Position.index() == StartSimpleRow ? std::get<StartSimpleRow>(Position)->GetMeta()
                                                             : std::get<EndSimpleRow>(Position)->GetMeta();
    auto lhsIndexKeyView = Position.index() == StartSimpleRow ? lhsMeta.IndexKeyViewStart()
                                                              : lhsMeta.IndexKeyViewEnd();
    auto rhsIndexKeyView = rhs.Position.index() == StartSimpleRow ? std::get<StartSimpleRow>(rhs.Position)->GetMeta().IndexKeyViewStart()
                                                                  : std::get<EndSimpleRow>(rhs.Position)->GetMeta().IndexKeyViewEnd();

    return lhsIndexKeyView.Compare(rhsIndexKeyView, lhsMeta.GetPkSchema()).GetResult();
}

int TPositionViewBorderComparator::Compare(const TBorder& lhs, const TBorder& rhs) {
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
