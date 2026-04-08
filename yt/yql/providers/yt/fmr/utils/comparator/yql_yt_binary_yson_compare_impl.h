#pragma once

#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <util/generic/strbuf.h>
#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/zigzag.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>


#include <cstring>
#include <variant>

namespace NYql::NFmr {

using TSmallKeyValue = std::variant<std::monostate, bool, i64, ui64, double>;

TMaybe<TSmallKeyValue> TryExtractSmallYsonValue(TStringBuf ysonData);

struct TExtractedKey {
    TMaybe<TSmallKeyValue> Small;
    TStringBuf RawYson;
};

int CompareExtractedKeys(const TExtractedKey& lhs, const TExtractedKey& rhs);

enum class ESortOrder {
    Ascending = 0,
    Descending = 1
};

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

