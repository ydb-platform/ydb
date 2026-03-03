#include "portion_interval_tree.h"

#include <algorithm>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap::NPortionIntervalTree {

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

    const auto& lhsSchema = getSchema(Position);
    const auto& rhsSchema = getSchema(rhs.Position);

    return getKeyView(Position).Compare(getKeyView(rhs.Position), lhsSchema->num_fields() <= rhsSchema->num_fields() ? lhsSchema : rhsSchema).GetResult();
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

TPortionIntervalTree::TPortionIntervalTree(bool countPortionIntersections,
    NMonitoring::TDynamicCounters::TCounterPtr maxPortionIntersectionsCounter)
    : CountPortionIntersections(countPortionIntersections)
    , MaxPortionIntersectionsCounter(std::move(maxPortionIntersectionsCounter))
{
    AFL_VERIFY(MaxPortionIntersectionsCounter);
}

void TPortionIntervalTree::AddRange(TOwnedRange range, const std::shared_ptr<TPortionInfo>& value) {
    if (CountPortionIntersections) {
        Impl.EachIntersection(range, [&value](const TRange&, const std::shared_ptr<TPortionInfo>& p) {
            p->IncrementPortionIntersections();
            value->IncrementPortionIntersections();
            return true;
        });
        SetHeapDirty();
    }
    Impl.AddRange(std::move(range), value);
}

void TPortionIntervalTree::RemoveRanges(const std::shared_ptr<TPortionInfo>& value) {
    if (CountPortionIntersections) {
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
                    p->DecrementPortionIntersections();
                    value->DecrementPortionIntersections();
                }
                return true;
            });
        }
        SetHeapDirty();
    }
    Impl.RemoveRanges(value);
}

std::shared_ptr<TPortionInfo> TPortionIntervalTree::GetPortionWithMaxIntersections() const {
    if (!CountPortionIntersections) {
        return nullptr;
    }
    if (HeapDirty) {
        RebuildHeap();
    }
    return Heap.empty() ? nullptr : Heap.front();
}

size_t TPortionIntervalTree::Size() const noexcept {
    return Impl.Size();
}

void TPortionIntervalTree::SetHeapDirty() const {
    if (!CountPortionIntersections) {
        return;
    }
    HeapDirty = true;
}

void TPortionIntervalTree::RebuildHeap() const {
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
        return a->GetPortionIntersections() < b->GetPortionIntersections();
    });
    HeapDirty = false;
    MaxPortionIntersectionsCounter->Set(Heap.empty() ? 0 : Heap.front()->GetPortionIntersections());
}

} // namespace NKikimr::NOlap::NPortionIntervalTree
