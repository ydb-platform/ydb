#include "interval_counter.h"

namespace NKikimr::NOlap::NReader::NSimple {

void TIntervalCounter::PropagateDelta(const TPosition& node) {
    AFL_VERIFY(node.GetIndex() < PropagatedDeltas.size())("node", DebugString(node));
    if (!PropagatedDeltas[node.GetIndex()]) {
        return;
    }

    const ui64 left = node.GetIndex() * 2 + 1;
    const ui64 right = node.GetIndex() * 2 + 2;
    if (left < PropagatedDeltas.size()) {
        AFL_VERIFY(right < PropagatedDeltas.size());
        PropagatedDeltas[left] += PropagatedDeltas[node.GetIndex()];
        PropagatedDeltas[right] += PropagatedDeltas[node.GetIndex()];
    } else {
        AFL_VERIFY((i64)Count[left] >= -PropagatedDeltas[node.GetIndex()]);
        AFL_VERIFY((i64)Count[right] >= -PropagatedDeltas[node.GetIndex()]);
        Count[left] += PropagatedDeltas[node.GetIndex()];
        Count[right] += PropagatedDeltas[node.GetIndex()];
        MinValue[left] += PropagatedDeltas[node.GetIndex()];
        MinValue[right] += PropagatedDeltas[node.GetIndex()];
    }
    const i64 delta = PropagatedDeltas[node.GetIndex()] * (node.IntervalSize());
    AFL_VERIFY((i64)Count[node.GetIndex()] >= -delta);
    Count[node.GetIndex()] += delta;
    MinValue[node.GetIndex()] += PropagatedDeltas[node.GetIndex()];
    PropagatedDeltas[node.GetIndex()] = 0;
}

void TIntervalCounter::Update(const TPosition& node, const TModification& modification, TZeroCollector* callback) {
    AFL_VERIFY(modification.GetDelta());
    if (modification.GetLeft() <= node.GetLeft() && modification.GetRight() >= node.GetRight()) {
        if (node.GetLeft() == node.GetRight()) {
            AFL_VERIFY((i64)Count[node.GetIndex()] >= -modification.GetDelta());
            Count[node.GetIndex()] += modification.GetDelta();
            MinValue[node.GetIndex()] += modification.GetDelta();
        } else {
            AFL_VERIFY(node.GetIndex() < PropagatedDeltas.size());
            PropagatedDeltas[node.GetIndex()] += modification.GetDelta();
        }
        OnNodeUpdated(node, callback);
    } else {
        PropagateDelta(node);
        if (modification.GetLeft() <= node.LeftChild().GetRight()) {
            Update(node.LeftChild(), modification, callback);
        }
        if (modification.GetRight() >= node.RightChild().GetLeft()) {
            Update(node.RightChild(), modification, callback);
        }
        Count[node.GetIndex()] = GetCount(node.LeftChild()) + GetCount(node.RightChild());
        MinValue[node.GetIndex()] = Min(GetMinValue(node.LeftChild()), GetMinValue(node.RightChild()));
    }

    if (IsLeaf(node)) {
        AFL_VERIFY(Count[node.GetIndex()] == MinValue[node.GetIndex()]);
    } else {
        AFL_VERIFY(Count[node.GetIndex()] == GetCount(node.LeftChild()) + GetCount(node.RightChild()));
        AFL_VERIFY(MinValue[node.GetIndex()] == Min(GetMinValue(node.LeftChild()), GetMinValue(node.RightChild())));
    }
}

void TIntervalCounter::Inc(const ui32 l, const ui32 r) {
    Update(GetRoot(), TModification(l, r, 1), nullptr);
}

ui64 TIntervalCounter::GetCount(const TPosition& node, const ui32 l, const ui32 r) {
    if (l <= node.GetLeft() && r >= node.GetRight()) {
        return GetCount(node);
    }
    PropagateDelta(node);
    bool needLeft = node.LeftChild().GetRight() >= l;
    bool needRight = node.RightChild().GetLeft() <= r;
    AFL_VERIFY(needLeft || needRight);
    return (needLeft ? GetCount(node.LeftChild(), l, r) : 0) + (needRight ? GetCount(node.RightChild(), l, r) : 0);
}

void TIntervalCounter::OnNodeUpdated(const TPosition& node, TZeroCollector* callback) {
    if (GetCount(node) == 0) {
        AFL_VERIFY(callback);
        callback->OnNewZeros(node);
    } else if (GetMinValue(node) == 0) {
        AFL_VERIFY(callback);
        PropagateDelta(node);
        for (const auto& child : { node.LeftChild(), node.RightChild() }) {
            OnNodeUpdated(child, callback);
        }
    }
}

TIntervalCounter::TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals) {
    ui32 maxValue = 0;
    for (const auto& [l, r] : intervals) {
        AFL_VERIFY(l <= r);
        if (r > maxValue) {
            maxValue = r;
        }
    }
    if (maxValue == std::bit_ceil(maxValue)) {
        MaxIndex = maxValue * 2 - 1;
    } else {
        MaxIndex = std::bit_ceil(maxValue) - 1;
    }
    Count.resize(MaxIndex * 2 + 1);
    MinValue.resize(Count.size());
    PropagatedDeltas.resize(MaxIndex);

    for (const auto& [l, r] : intervals) {
        Inc(l, r);
    }
}

bool TIntervalCounter::IsAllZeros() const {
    return GetCount(GetRoot()) == 0;
}

std::vector<ui32> TIntervalCounter::DecAndGetZeros(const ui32 l, const ui32 r) {
    TZeroCollector callback;
    Update(GetRoot(), TModification(l, r, -1), &callback);
    return callback.ExtractNewZeroPositions();
}

}