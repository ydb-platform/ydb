#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TIntervalCounter {
private:
    class TModification {
    private:
        YDB_READONLY_DEF(ui32, Left);
        YDB_READONLY_DEF(ui32, Right);
        YDB_READONLY_DEF(i64, Delta);

    public:
        TModification(const ui32 l, const ui32 r, const i64 delta)
            : Left(l)
            , Right(r)
            , Delta(delta) {
        }
    };

    class TPosition {
    private:
        YDB_READONLY_DEF(ui32, Index);
        YDB_READONLY_DEF(ui32, Left);
        YDB_READONLY_DEF(ui32, Right);

        TPosition(const ui32 i, const ui32 l, const ui32 r)
            : Index(i)
            , Left(l)
            , Right(r) {
        }

    public:
        explicit TPosition(const ui32 maxIndex)
            : Index(0)
            , Left(0)
            , Right(maxIndex) {
        }

        TPosition LeftChild() const {
            AFL_VERIFY(Right > Left);
            return TPosition(Index * 2 + 1, Left, (Left + Right - 1) / 2);
        }

        TPosition RightChild() const {
            AFL_VERIFY(Right > Left);
            return TPosition(Index * 2 + 2, (Left + Right + 1) / 2, Right);
        }

        ui32 IntervalSize() const {
            return Right - Left + 1;
        }
    };

    class TZeroCollector {
    private:
        std::vector<ui32> Positions;

    public:
        void OnNewZeros(const TPosition& node) {
            for (ui32 i = node.GetLeft(); i <= node.GetRight(); ++i) {
                Positions.emplace_back(i);
            }
        }

        std::vector<ui32> ExtractNewZeroPositions() {
            return std::move(Positions);
        }
    };

    // Segment tree: Count[i] = GetCount(i * 2 + 1) + GetCount(i * 2 + 2)
    std::vector<ui64> Count;
    std::vector<ui64> MinValue;
    std::vector<i64> PropagatedDeltas;
    ui32 MaxIndex = 0;

private:
    void PropagateDelta(const TPosition& node);
    void Update(const TPosition& node, const TModification& modification, TZeroCollector* callback);
    void Inc(const ui32 l, const ui32 r);
    ui64 GetCount(const TPosition& node, const ui32 l, const ui32 r);
    ui64 GetCount(const TPosition& node) const {
        AFL_VERIFY(node.GetIndex() < Count.size());
        return Count[node.GetIndex()] +
               (node.GetIndex() < PropagatedDeltas.size() ? PropagatedDeltas[node.GetIndex()] : 0) * node.IntervalSize();
    }
    ui64 GetMinValue(const TPosition& node) const {
        AFL_VERIFY(node.GetIndex() < MinValue.size());
        return MinValue[node.GetIndex()] + (node.GetIndex() < PropagatedDeltas.size() ? PropagatedDeltas[node.GetIndex()] : 0);
    }
    TPosition GetRoot() const {
        return TPosition(MaxIndex);
    }
    TString DebugString(const TPosition& node) const {
        return TStringBuilder() << "node=" << node.GetIndex() << ";max_index=" << MaxIndex << ";count=" << GetCount(node)
                                << ";min=" << GetMinValue(node);
    }
    void OnNodeUpdated(const TPosition& node, TZeroCollector* callback);
    bool IsLeaf(const TPosition& node) const {
        return node.GetIndex() >= MaxIndex;
    }

public:
    TIntervalCounter(const std::vector<std::pair<ui32, ui32>>& intervals);
    std::vector<ui32> DecAndGetZeros(const ui32 l, const ui32 r);
    ui64 GetCount(const ui32 l, const ui32 r) {
        return GetCount(GetRoot(), l, r);
    }
    bool IsAllZeros() const;
};

}