#include "portion_interval_tree.h"

namespace NKikimr::NOlap::PortionIntervalTree {

TPositionView::TPositionView(NArrow::TSimpleRow&& simpleRow)
    : Position(std::move(simpleRow))
{
}

TPositionView::TPositionView(EInfinityType infType)
    : Position(infType == EInfinityType::Left ? TPositionVariant(std::in_place_index<LeftInf>) : TPositionVariant(std::in_place_index<RightInf>))
{
}

TPositionView::TPositionView(std::shared_ptr<TPortionInfo> portionInfo, EPortionInfoIndexPosition keyPosition)
    : Position(keyPosition == EPortionInfoIndexPosition::Start ? TPositionVariant(std::in_place_index<StartPortionInfo>, std::move(portionInfo))
                                                               : TPositionVariant(std::in_place_index<EndPortionInfo>, std::move(portionInfo)))
{
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

    auto getSchema = [](const TPositionVariant& pos) -> const std::shared_ptr<arrow::Schema>& {
        switch (pos.index()) {
            case StartPortionInfo:
                return std::get<StartPortionInfo>(pos)->GetMeta().GetPkSchema();
            case EndPortionInfo:
                return std::get<EndPortionInfo>(pos)->GetMeta().GetPkSchema();
            case SimpleRow:
                return std::get<SimpleRow>(pos).GetSchema();
            default:
                AFL_VERIFY(false)("pos.index()", pos.index());
                break;
        }
    };

    auto getLogicalTypes = [](const TPositionVariant& pos) -> const std::vector<NScheme::TTypeInfo>& {
        switch (pos.index()) {
            case StartPortionInfo:
                return std::get<StartPortionInfo>(pos)->GetMeta().GetPKTypes();
            case EndPortionInfo:
                return std::get<EndPortionInfo>(pos)->GetMeta().GetPKTypes();
            case SimpleRow:
                return std::get<SimpleRow>(pos).GetLogicalTypes();
            default:
                AFL_VERIFY(false)("pos.index()", pos.index());
        }
    };

    auto getComparator = [](const TPositionVariant& pos) -> const NArrow::TComparator& {
        switch (pos.index()) {
            case StartPortionInfo:
                return std::get<StartPortionInfo>(pos)->GetMeta().GetComparator();
            case EndPortionInfo:
                return std::get<EndPortionInfo>(pos)->GetMeta().GetComparator();
            case SimpleRow:
                return std::get<SimpleRow>(pos).GetComparator();
            default:
                AFL_VERIFY(false)("pos.index()", pos.index());
        }
    };

    const auto& lhsSchema = getSchema(Position);
    const auto& rhsSchema = getSchema(rhs.Position);

    const auto& lhsTypes = getLogicalTypes(Position);
    const auto& rhsTypes = getLogicalTypes(rhs.Position);
    const auto& comparator = getComparator(Position);
    const bool lhsShorter = lhsSchema->num_fields() <= rhsSchema->num_fields();
    AFL_VERIFY_DEBUG(comparator.IsCompatibleWith(getComparator(rhs.Position), lhsShorter ? lhsTypes : rhsTypes));
    AFL_VERIFY_DEBUG(std::equal(lhsTypes.begin(), lhsTypes.begin() + std::min(lhsTypes.size(), rhsTypes.size()), rhsTypes.begin()));
    return getKeyView(Position)
        .Compare(getKeyView(rhs.Position), lhsShorter ? *lhsSchema : *rhsSchema, lhsShorter ? lhsTypes : rhsTypes, comparator)
        .GetResult();
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

}   // namespace NKikimr::NOlap::PortionIntervalTree
