#include "yql_yt_parser_fragment_list_index.h"

#include <library/cpp/yson/zigzag.h>
#include <util/string/builder.h>

namespace NYql::NFmr {

namespace {
    // Binary YSON type markers from library/cpp/yson/detail.h
    constexpr char MARKER_STRING = StringMarker;    // '\x01'
    constexpr char MARKER_INT64 = Int64Marker;      // '\x02'
    constexpr char MARKER_DOUBLE = DoubleMarker;    // '\x03'
    constexpr char MARKER_FALSE = FalseMarker;      // '\x04'
    constexpr char MARKER_TRUE = TrueMarker;        // '\x05'
    constexpr char MARKER_UINT64 = Uint64Marker;    // '\x06'

    // Structural symbols
    constexpr char SYMBOL_BEGIN_LIST = BeginListSymbol;                   // '['
    constexpr char SYMBOL_END_LIST = EndListSymbol;                       // ']'
    constexpr char SYMBOL_BEGIN_MAP = BeginMapSymbol;                     // '{'
    constexpr char SYMBOL_END_MAP = EndMapSymbol;                         // '}'
    constexpr char SYMBOL_BEGIN_ATTRIBUTES = BeginAttributesSymbol;       // '<'
    constexpr char SYMBOL_END_ATTRIBUTES = EndAttributesSymbol;           // '>'
    constexpr char SYMBOL_LIST_SEPARATOR = ListItemSeparatorSymbol;       // ';'
    constexpr char SYMBOL_KEY_VALUE_SEPARATOR = KeyValueSeparatorSymbol;  // '='
    constexpr char SYMBOL_ENTITY = EntitySymbol;                          // '#'
    constexpr char SYMBOL_END = EndSymbol;                                // '\0'

    // Varint encoding constants (Protobuf-compatible)
    constexpr ui8 VARINT_DATA_MASK = 0x7F;          // Mask to extract 7 data bits
    constexpr ui8 VARINT_CONTINUE_BIT = 0x80;       // Continuation bit (if set, more bytes follow)
    constexpr int VARINT_MAX_SHIFT = 70;            // Maximum shift for 64-bit varint (10 bytes * 7 bits)
} // anonymous namespace


TParserFragmentListIndex::TParserFragmentListIndex(TStringBuf data, const std::vector<TString>& keyColumns)
    : Pos_(data.begin())
    , DataStart_(data.begin())
    , DataEnd_(data.end())
    , KeyColumnsMap_(
        [&keyColumns]() {
            THashMap<TString, ui64> map;
            for (size_t i = 0; i < keyColumns.size(); ++i) {
                map[keyColumns[i]] = i;
            }
            return map;
    }())
{
}


void TParserFragmentListIndex::Parse() {
    Y_ENSURE(!KeyColumnsMap_.empty(), "Vector of key columns is empty");
    DoParse();
}


void TParserFragmentListIndex::DoParse() {
    ParseListFragment();
}

void TParserFragmentListIndex::ParseListFragment() {
    while (Pos_ < DataEnd_) {
        char ch = PeekChar();
        // TODO: Probably we need to implement support for # with attributes. Yt can send such data.
        Y_ENSURE(ch == SYMBOL_BEGIN_MAP, "Expected begin map symbol");
        ParseMap(true);
        if (Pos_ < DataEnd_) {
            ch = PeekChar();
            if (ch == SYMBOL_LIST_SEPARATOR) {
                Advance(1);
            } else if (ch == SYMBOL_END) {
                break;
            }
        }
    }
}

void TParserFragmentListIndex::ParseValue() {
    char ch = PeekChar();
    switch (ch) {
        case SYMBOL_BEGIN_ATTRIBUTES:
            SkipAttributes();
            ParseValue();
            break;

        case SYMBOL_BEGIN_MAP:
            ParseMap();
            break;

        case SYMBOL_BEGIN_LIST:
            ParseList();
            break;

        default:
            SkipScalar();
            break;
    }
}



void TParserFragmentListIndex::ParseMapFragment(bool isTrackingMap, char endSymbol) {
    const ui64 rowStartOffset = GetOffset();
    Advance(1);
    while (true) {
        char ch = PeekChar();

        if (ch == endSymbol) {
            Advance(1);
            break;
        }

        // Read key (always a string in binary YSON)
        Y_ENSURE(ch == MARKER_STRING,
                 TStringBuilder() << "Expected string marker byte for map key, but got: 0x"
                 << IntToString<16>(static_cast<unsigned char>(ch)));
        Advance(1);

        TStringBuf key;
        ReadBinaryString(&key);

        ch = ReadChar();
        Y_ENSURE(ch == SYMBOL_KEY_VALUE_SEPARATOR,
                 TStringBuilder() << "Expected '" << SYMBOL_KEY_VALUE_SEPARATOR
                 << "' (key-value separator) after map key, but got: 0x"
                 << IntToString<16>(static_cast<unsigned char>(ch)));

        const bool shouldTrack = isTrackingMap && KeyColumnsMap_.contains(key);
        const ui64 valueStartOffset = shouldTrack ? GetOffset() : 0;

        ParseValue();

        if (shouldTrack) {
            const ui64 valueEndOffset = GetOffset();
            CurrentRow_[TString(key)] = TColumnOffsetRange{valueStartOffset, valueEndOffset};
        }

        if (Pos_ < DataEnd_) {
            ch = PeekChar();
            if (ch == SYMBOL_LIST_SEPARATOR) {
                Advance(1);
            }
        }
    }
    const ui64 rowEndOffset = GetOffset();
    // When exiting top-level map, save the row
    if (isTrackingMap && !CurrentRow_.empty()) {
        TRowIndexMarkup row(KeyColumnsMap_.size());
        row.reserve(KeyColumnsMap_.size());
        for (const auto& [key, value] : CurrentRow_) {
            row[KeyColumnsMap_[key]] = value;
        }
        row.push_back(TColumnOffsetRange{rowStartOffset, rowEndOffset});
        RowOffsets_.push_back(std::move(row));
        CurrentRow_.clear();
    }
}

void TParserFragmentListIndex::ParseMap(bool isTopLevel) {
    ParseMapFragment(isTopLevel, SYMBOL_END_MAP);
}

void TParserFragmentListIndex::ParseList() {
    Advance(1);
    while (true) {
        char ch = PeekChar();
        if (ch == SYMBOL_END_LIST) {
            Advance(1);
            break;
        }
        ParseValue();
        if (Pos_ < DataEnd_) {
            ch = PeekChar();
            if (ch == SYMBOL_LIST_SEPARATOR) {
                Advance(1);
            }
        }
    }
}


void TParserFragmentListIndex::SkipScalar() {
    char ch = ReadChar();

    switch (ch) {
        case MARKER_STRING:
            SkipBinaryString();
            break;

        case MARKER_INT64:
            ReadVarInt64();
            break;

        case MARKER_UINT64:
            ReadVarUint64();
            break;

        case MARKER_DOUBLE:
            SkipBinaryDouble();
            break;

        case MARKER_FALSE:
        case MARKER_TRUE:
            // Already consumed by ReadChar()
            break;

        case SYMBOL_ENTITY:
            // Already consumed by ReadChar()
            break;

        default:
            Y_ENSURE(false, TStringBuilder() << "Unexpected YSON marker: 0x"
                     << IntToString<16>(static_cast<unsigned char>(ch)));
    }
}


char TParserFragmentListIndex::PeekChar() {
    EnsureAvailable(1);
    return *Pos_;
}

char TParserFragmentListIndex::ReadChar() {
    EnsureAvailable(1);
    return *Pos_++;
}

void TParserFragmentListIndex::Advance(ui64 bytes) {
    EnsureAvailable(bytes);
    Pos_ += bytes;
}

ui32 TParserFragmentListIndex::ReadVarUint32() {
    ui32 result = 0;
    int shift = 0;

    while (true) {
        EnsureAvailable(1);
        const ui8 byte = static_cast<ui8>(*Pos_++);
        result |= static_cast<ui32>(byte & VARINT_DATA_MASK) << shift;

        if ((byte & VARINT_CONTINUE_BIT) == 0) {
            break;
        }

        shift += 7;
        Y_ENSURE(shift < 35, "Varint32 too long (>5 bytes)");
    }

    return result;
}

ui64 TParserFragmentListIndex::ReadVarUint64() {
    ui64 result = 0;
    int shift = 0;

    while (true) {
        EnsureAvailable(1);
        const ui8 byte = static_cast<ui8>(*Pos_++);

        result |= static_cast<ui64>(byte & VARINT_DATA_MASK) << shift;

        if ((byte & VARINT_CONTINUE_BIT) == 0) {
            break;
        }

        shift += 7;
        Y_ENSURE(shift < VARINT_MAX_SHIFT, "Varint64 too long (>10 bytes)");
    }
    return result;
}

i64 TParserFragmentListIndex::ReadVarInt64() {
    const ui64 encoded = ReadVarUint64();
    return ZigZagDecode64(encoded);
}

void TParserFragmentListIndex::ReadBinaryString(TStringBuf* value) {
    const ui32 ulength = ReadVarUint32();
    const i32 length = ZigZagDecode32(ulength);

    Y_ENSURE(length >= 0, TStringBuilder() << "Negative binary string length: " << length);

    *value = TStringBuf(Pos_, static_cast<size_t>(length));
    Advance(static_cast<ui64>(length));
}

void TParserFragmentListIndex::SkipBinaryString() {
    TStringBuf dummy;
    ReadBinaryString(&dummy);
}

void TParserFragmentListIndex::SkipBinaryDouble() {
    Advance(sizeof(double));
}

void TParserFragmentListIndex::SkipAttributes() {
    ParseMapFragment(false, SYMBOL_END_ATTRIBUTES);
}

i32 TParserFragmentListIndex::ZigZagDecode32(ui32 value) {
    return NYson::ZigZagDecode32(value);
}

i64 TParserFragmentListIndex::ZigZagDecode64(ui64 value) {
    return NYson::ZigZagDecode64(value);
}


ui64 TParserFragmentListIndex::GetOffset() const {
    return static_cast<ui64>(Pos_ - DataStart_);
}

void TParserFragmentListIndex::EnsureAvailable(size_t bytes) const {
    Y_ENSURE(Pos_ + bytes <= DataEnd_,
             TStringBuilder() << "Unexpected end of data: need " << bytes
                              << " bytes, but only " << (DataEnd_ - Pos_) << " available");
}

} // namespace NYql::NFmr

