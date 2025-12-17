#pragma once

#include <library/cpp/yson/detail.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>

using namespace NYson::NDetail;

namespace NYql::NFmr {
// Represents a byte range in the source data for a column value
struct TColumnOffsetRange {
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;

    bool IsValid() const {
        return EndOffset > StartOffset;
    }
};

using TRowIndexMarkup = TVector<TColumnOffsetRange>;

// Binary YSON parser for ListFragment format that extracts byte offsets for key columns
// Usage:
//   TParserFragmentListIndex parser(data, keyColumns);
//   parser.Parse();
//   const auto& rows = parser.GetRows();
// Expected input format: ListFragment of maps (without surrounding '[' and ']')
// Each map represents a row with key-value pairs
// Parser records byte offsets (StartOffset, EndOffset) for values of specified key columns
// Last (StartOffset, EndOffset) defines boundaries of row
class TParserFragmentListIndex {
public:
    TParserFragmentListIndex(TStringBuf data, const TVector<TString>& keyColumns);

    void Parse();

    const TVector<TRowIndexMarkup>& GetRows() const {
        return RowOffsets_;
    }

private:
    void DoParse();
    void ParseListFragment();
    void ParseValue();
    void ParseMapFragment(bool isTrackingMap, char endSymbol);
    void ParseMap(bool isTopLevel = false);
    void ParseList();
    void SkipScalar();
    char PeekChar();
    char ReadChar();
    void Advance(ui64 bytes);
    ui32 ReadVarUint32();
    ui64 ReadVarUint64();
    i64 ReadVarInt64();
    void ReadBinaryString(TStringBuf* value);
    void SkipBinaryString();
    void SkipBinaryDouble();
    void SkipAttributes();
    static i32 ZigZagDecode32(ui32 value);
    static i64 ZigZagDecode64(ui64 value);
    ui64 GetOffset() const;
    void EnsureAvailable(size_t bytes) const;

    const char* Pos_;
    const char* const DataStart_;
    const char* const DataEnd_;
    THashMap<TString, ui64> KeyColumnsMap_;
    TVector<TRowIndexMarkup> RowOffsets_;
    THashMap<TString, TColumnOffsetRange> CurrentRow_;
};

} // namespace NYql::NFmr

