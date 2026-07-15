#pragma once

#include <library/cpp/yson/detail.h>
#include <util/system/compiler.h>

using namespace NYson::NDetail;

namespace NYql::NFmr {

struct TColumnOffsetRange {
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;

    bool IsValid() const {
        return EndOffset > StartOffset;
    }
};

// Row markup: key columns ranges + last range is full row boundary [rowStart,rowEnd).
using TRowIndexMarkup = std::vector<TColumnOffsetRange>;

struct TColumnEntry {
    TStringBuf Name;
    TColumnOffsetRange KeyValueRange;
    TColumnOffsetRange ValueRange;
};

struct TRowFullMarkup {
    TColumnOffsetRange RowBoundary;
    std::vector<TColumnEntry> Columns;
};

enum class EParserMode {
    SelectedColumnsIdexes,
    AllColumnsEntries,
};

// Binary YSON parser for ListFragment format that extracts byte offsets for key columns
// Usage:
//   TParserFragmentListIndex parser(data, keyColumns);
//   parser.Parse();
//   const auto& rows = parser.GetRows();
// Expected input format: ListFragment of maps (without surrounding '[' and ']')
// Each map represents a row with key-value pairs
//
// SelectedColumnsIndexes MODE (for sorting by key columns):
// Parser records byte offsets (StartOffset, EndOffset) for values of specified key columns
// Last (StartOffset, EndOffset) defines boundaries of row
//
// AllColumnsEntries MODE: (for column group splitting):
//   TParserFragmentListIndex parser(data);
//   parser.Parse();
//   const auto& rows = parser.GetAllColumnsRows();

class TParserFragmentListIndex {
public:
    TParserFragmentListIndex(TStringBuf data, const std::vector<TString>& keyColumns);
    explicit TParserFragmentListIndex(TStringBuf data); // all-columns mode

    void Parse();

    const std::vector<TRowIndexMarkup>& GetRows() const {
        return RowOffsets_;
    }

    const std::vector<TRowFullMarkup>& GetAllColumnsRows() const {
        return AllColumnsRowOffsets_;
    }

    std::vector<TRowFullMarkup> MoveAllColumnsRows() {
        return std::move(AllColumnsRowOffsets_);
    }

private:
    void DoParse();
    void ParseListFragment();
    void ParseValue();
    void ParseMapFragment(bool isTrackingMap, char endSymbol);
    void ParseMap(bool isTopLevel = false);
    void ParseList();
    void SkipScalar();
    ui32 ReadVarUint32();
    ui64 ReadVarUint64();
    i64 ReadVarInt64();
    void ReadBinaryString(TStringBuf* value);
    void SkipBinaryString();
    void SkipBinaryDouble();
    void SkipAttributes();
    static i32 ZigZagDecode32(ui32 value);
    static i64 ZigZagDecode64(ui64 value);

    [[noreturn]] void ThrowUnexpectedEnd(size_t bytes) const;

    Y_FORCE_INLINE void EnsureAvailable(size_t bytes) const {
        if (Y_UNLIKELY(Pos_ + bytes > DataEnd_)) {
            ThrowUnexpectedEnd(bytes);
        }
    }

    Y_FORCE_INLINE char PeekChar() {
        EnsureAvailable(1);
        return *Pos_;
    }

    Y_FORCE_INLINE char ReadChar() {
        EnsureAvailable(1);
        return *Pos_++;
    }

    Y_FORCE_INLINE void Advance(ui64 bytes) {
        EnsureAvailable(bytes);
        Pos_ += bytes;
    }

    Y_FORCE_INLINE ui64 GetOffset() const {
        return static_cast<ui64>(Pos_ - DataStart_);
    }

    const char* Pos_;
    const char* const DataStart_;
    const char* const DataEnd_;
    THashMap<TString, ui64> KeyColumnsMap_;
    EParserMode Mode_ = EParserMode::SelectedColumnsIdexes;
    std::vector<TRowIndexMarkup> RowOffsets_;
    std::vector<TRowFullMarkup> AllColumnsRowOffsets_;
    THashMap<TString, TColumnOffsetRange> CurrentRow_;
    std::vector<TColumnEntry> CurrentRowAllColumns_;
    ui64 LastColumnCount_ = 0; // For reserve optimization
};

} // namespace NYql::NFmr

