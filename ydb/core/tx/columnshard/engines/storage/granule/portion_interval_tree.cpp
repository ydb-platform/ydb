#include "portion_interval_tree.h"

namespace NKikimr::NOlap::PortionIntervalTree {

TPositionView::TPositionView(NArrow::TSimpleRow&& simpleRow)
    : Position(std::move(simpleRow)) {
}

TPositionView::TPositionView(EInfinityType infType)
    : Position(infType == EInfinityType::Left ? TPositionVariant(std::in_place_index<LeftInf>)
                                              : TPositionVariant(std::in_place_index<RightInf>)) {
}

TPositionView::TPositionView(std::shared_ptr<TPortionInfo> portionInfo, EPortionInfoIndexPosition keyPosition)
    : Position(keyPosition == EPortionInfoIndexPosition::Start ? TPositionVariant(std::in_place_index<StartPortionInfo>, std::move(portionInfo))
                                                               : TPositionVariant(std::in_place_index<EndPortionInfo>, std::move(portionInfo))) {
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

std::partial_ordering TPositionView::Compare(const TPositionView& rhs) const {
    if (Position.index() == LeftInf || rhs.Position.index() == RightInf) {
        return Position.index() == rhs.Position.index() ? std::partial_ordering::equivalent : std::partial_ordering::less;
    } else if (Position.index() == RightInf || rhs.Position.index() == LeftInf) {
        return std::partial_ordering::greater;
    }

    auto getKeyView = [](const TPositionVariant& pos) -> NArrow::TSimpleRowViewV0 {
        switch (pos.index()) {
            case StartPortionInfo:
                return std::get<StartPortionInfo>(pos)->GetMeta().IndexKeyViewStart();
            case EndPortionInfo:
                return std::get<EndPortionInfo>(pos)->GetMeta().IndexKeyViewEnd();
            case SimpleRow:
                return NArrow::TSimpleRowViewV0(std::get<SimpleRow>(pos).GetData());
            default:
                AFL_VERIFY(false)("pos.index()", pos.index());
                break;
        }
    };

    auto getSchema = [&]() -> const std::shared_ptr<arrow::Schema> {
        switch (Position.index()) {
            case StartPortionInfo:
                return std::get<StartPortionInfo>(Position)->GetMeta().GetPkSchema();
            case EndPortionInfo:
                return std::get<EndPortionInfo>(Position)->GetMeta().GetPkSchema();
            case SimpleRow:
                return std::get<SimpleRow>(Position).GetSchema();
            default:
                AFL_VERIFY(false)("Position.index()", Position.index());
                break;
        }
    };

    return getKeyView(Position).Compare(getKeyView(rhs.Position), getSchema()).GetResult();
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
