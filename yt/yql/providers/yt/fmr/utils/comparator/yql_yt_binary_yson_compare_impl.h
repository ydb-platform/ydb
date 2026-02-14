#pragma once

#include <util/generic/strbuf.h>
#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/zigzag.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>


#include <cstring>

namespace NYql::NFmr {

struct TColumnOffsetRange {
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;

    bool IsValid() const {
        return EndOffset > StartOffset;
    }
};

enum class ESortOrder {
    Ascending = 0,
    Descending = 1
};

// Row markup: key columns ranges + last range is full row boundary [rowStart,rowEnd).
using TRowIndexMarkup = std::vector<TColumnOffsetRange>;



struct TYsonReader {
    TStringBuf Data;
    const char* Pos;

    explicit TYsonReader(TStringBuf data)
        : Data(data)
        , Pos(data.data())
    {}

    bool HasData() const;
    ui64 Remaining() const;
    char PeekByte() const;
    char ReadByte();
    TStringBuf GetView(ui64 length);
    ui64 ReadVarUint64();
    i64 ReadVarInt64();
    ui32 ReadVarUint32();
    TStringBuf ReadString();
    double ReadDouble();
};

int CompareYsonCurrentValuesFromReaders(TYsonReader& lhs, TYsonReader& rhs);

int CompareYsonScalars(char type, TYsonReader& lhs, TYsonReader& rhs);

// Compare two binary YSON values stored in separate buffers.
// Returns: -1 if lhs < rhs, 0 if lhs == rhs, +1 if lhs > rhs.
int CompareYsonValuesImpl(TStringBuf lhs, TStringBuf rhs);

Y_FORCE_INLINE TStringBuf SliceRange(TStringBuf blob, const TColumnOffsetRange& r) {
    Y_ENSURE(r.IsValid(), "Invalid column offset range");
    Y_ENSURE(r.EndOffset <= blob.size(), "Offset out of bounds");
    return TStringBuf(blob.data() + r.StartOffset, r.EndOffset - r.StartOffset);
}

int CompareKeyRowsAcrossYsonBlocks(
    TStringBuf lhsBlob,
    const TRowIndexMarkup& lhsRow,
    TStringBuf rhsBlob,
    const TRowIndexMarkup& rhsRow,
    const std::vector<ESortOrder>& sortOrders
);

} // namespace NYql::NFmr

