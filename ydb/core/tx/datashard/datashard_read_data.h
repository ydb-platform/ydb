#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr::NDataShard {

// Borrowed cells and equivalent serialized bytes; backing owners must outlive the view.
class TReadCellVecView {
public:
    TReadCellVecView() = default;

    TReadCellVecView(TConstArrayRef<TCell> cells, const TString* buffer = nullptr)
        : Cells(cells)
        , Buffer(buffer)
    {}

    TReadCellVecView(const TSerializedCellVec& cells)
        : Cells(cells.GetCells())
        , Buffer(&cells.GetBuffer())
    {}

    TReadCellVecView(TSerializedCellVec&&) = delete;

    TConstArrayRef<TCell> GetCells() const {
        return Cells;
    }

    explicit operator bool() const {
        return !Cells.empty();
    }

    TSerializedCellVec ToOwned() const {
        return Buffer ? TSerializedCellVec(*Buffer) : TSerializedCellVec(Cells);
    }

private:
    TConstArrayRef<TCell> Cells;
    const TString* Buffer = nullptr;
};

struct TReadRangeView {
    TReadCellVecView From;
    TReadCellVecView To;
    bool FromInclusive = false;
    bool ToInclusive = false;
    bool Point = false;

    TReadRangeView() = default;

    TReadRangeView(const TSerializedTableRange& range)
        : From(range.From)
        , To(range.To)
        , FromInclusive(range.FromInclusive)
        , ToInclusive(range.ToInclusive)
        , Point(range.Point)
    {}

    TReadRangeView(TSerializedTableRange&&) = delete;

    TTableRange ToTableRange() const {
        return TTableRange(From.GetCells(), FromInclusive, To.GetCells(), ToInclusive, Point);
    }
};

} // namespace NKikimr::NDataShard
