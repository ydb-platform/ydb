// yql_yt_binary_yson_comparator.cpp
#include "yql_yt_binary_yson_comparator.h"
#include <library/cpp/yson/zigzag.h>
#include <library/cpp/yson/detail.h>
#include <cmath>

using namespace NYson::NDetail;

namespace NYql::NFmr {

namespace {
    constexpr char MARKER_STRING = StringMarker;     // 0x01
    constexpr char MARKER_INT64 = Int64Marker;       // 0x02
    constexpr char MARKER_DOUBLE = DoubleMarker;     // 0x03
    constexpr char MARKER_FALSE = FalseMarker;       // 0x04
    constexpr char MARKER_TRUE = TrueMarker;         // 0x05
    constexpr char MARKER_UINT64 = Uint64Marker;     // 0x06

    constexpr char SYMBOL_BEGIN_LIST = BeginListSymbol;               // '['
    constexpr char SYMBOL_END_LIST = EndListSymbol;                   // ']'
    constexpr char SYMBOL_LIST_SEPARATOR = ListItemSeparatorSymbol;   // ';'
    constexpr char SYMBOL_ENTITY = EntitySymbol;                      // '#'

    constexpr ui8 VARINT_DATA_MASK = 0x7F;
    constexpr ui8 VARINT_CONTINUE_BIT = 0x80;

    enum class ETypeOrder : ui8 {
        Null = 0,
        Boolean = 1,
        Int64 = 2,
        Uint64 = 3,
        Double = 4,
        String = 5,
        Any = 6,
    };

    ETypeOrder GetTypeOrder(char marker) {
        switch (marker) {
            case SYMBOL_ENTITY: return ETypeOrder::Null;
            case MARKER_FALSE:
            case MARKER_TRUE: return ETypeOrder::Boolean;
            case MARKER_INT64: return ETypeOrder::Int64;
            case MARKER_UINT64: return ETypeOrder::Uint64;
            case MARKER_DOUBLE: return ETypeOrder::Double;
            case MARKER_STRING: return ETypeOrder::String;
            case SYMBOL_BEGIN_LIST: return ETypeOrder::Any;
            default:
                ythrow yexception() << "Unknown YSON marker: 0x"
                    << IntToString<16>(static_cast<unsigned char>(marker));
        }
    }

    template<typename T>
    Y_FORCE_INLINE int TernaryCompare(T lhs, T rhs) {
        if (lhs < rhs) return -1;
        if (lhs > rhs) return 1;
        return 0;
    }

    template <class T>
    requires std::is_floating_point_v<T>
    Y_FORCE_INLINE int NaNSafeCompareFloatingPoint(const T& lhs, const T& rhs) {
        if (std::isnan(lhs) && std::isnan(rhs)) return 0;
        if (std::isnan(lhs)) return 1;
        if (std::isnan(rhs)) return -1;
        return TernaryCompare(lhs, rhs);
    }
}

ui64 TBinaryYsonComparator::TYsonReader::ReadVarUint64() {
    ui64 result = 0;
    int shift = 0;

    while (true) {
        ui8 byte = static_cast<ui8>(ReadByte());
        result |= static_cast<ui64>(byte & VARINT_DATA_MASK) << shift;

        if ((byte & VARINT_CONTINUE_BIT) == 0) {
            break;
        }

        shift += 7;
        Y_ENSURE(shift < 70, "Varint64 too long");
    }

    return result;
}

i64 TBinaryYsonComparator::TYsonReader::ReadVarInt64() {
    return NYson::ZigZagDecode64(ReadVarUint64());
}

ui32 TBinaryYsonComparator::TYsonReader::ReadVarUint32() {
    ui32 result = 0;
    int shift = 0;

    while (true) {
        ui8 byte = static_cast<ui8>(ReadByte());
        result |= static_cast<ui32>(byte & VARINT_DATA_MASK) << shift;

        if ((byte & VARINT_CONTINUE_BIT) == 0) {
            break;
        }

        shift += 7;
        Y_ENSURE(shift < 35, "Varint32 too long");
    }

    return result;
}

TStringBuf TBinaryYsonComparator::TYsonReader::ReadString() {
    ui32 ulength = ReadVarUint32();
    i32 length = NYson::ZigZagDecode32(ulength);
    Y_ENSURE(length >= 0, "Negative string length");

    return GetView(length);
}

double TBinaryYsonComparator::TYsonReader::ReadDouble() {
    Y_ENSURE(Remaining() >= sizeof(double), "Not enough data for double");
    double value;
    memcpy(&value, Pos, sizeof(double));
    Pos += sizeof(double);
    return value;
}

int TBinaryYsonComparator::CompareYsonValuesImpl(
    TYsonReader& lhs,
    TYsonReader& rhs
) const {
    char lhsType = lhs.PeekByte();
    char rhsType = rhs.PeekByte();

    auto lhsTypeOrder = GetTypeOrder(lhsType);
    auto rhsTypeOrder = GetTypeOrder(rhsType);

    if (lhsTypeOrder != rhsTypeOrder) {
        ythrow yexception() << "Types mismatch: " << lhsType << " vs " << rhsType;
    }

    return CompareYsonScalars(lhsType, lhs, rhs);
}

int TBinaryYsonComparator::CompareYsonScalars(
    char type,
    TYsonReader& lhs,
    TYsonReader& rhs
) const {
    switch (type) {
        case SYMBOL_ENTITY:
            lhs.ReadByte();
            rhs.ReadByte();
            return 0;

        case MARKER_FALSE:
        case MARKER_TRUE: {
            bool lhsVal = (lhs.ReadByte() == MARKER_TRUE);
            bool rhsVal = (rhs.ReadByte() == MARKER_TRUE);
            return TernaryCompare(lhsVal, rhsVal);
        }

        case MARKER_INT64: {
            lhs.ReadByte();
            rhs.ReadByte();
            i64 lhsVal = lhs.ReadVarInt64();
            i64 rhsVal = rhs.ReadVarInt64();
            return TernaryCompare(lhsVal, rhsVal);
        }

        case MARKER_UINT64: {
            lhs.ReadByte();
            rhs.ReadByte();
            ui64 lhsVal = lhs.ReadVarUint64();
            ui64 rhsVal = rhs.ReadVarUint64();
            return TernaryCompare(lhsVal, rhsVal);
        }

        case MARKER_DOUBLE: {
            lhs.ReadByte();
            rhs.ReadByte();
            double lhsVal = lhs.ReadDouble();
            double rhsVal = rhs.ReadDouble();
            return NaNSafeCompareFloatingPoint(lhsVal, rhsVal);
        }

        case MARKER_STRING: {
            lhs.ReadByte();
            rhs.ReadByte();
            TStringBuf lhsStr = lhs.ReadString();
            TStringBuf rhsStr = rhs.ReadString();
            return TernaryCompare(lhsStr, rhsStr);
        }

        case SYMBOL_BEGIN_LIST: {
            lhs.ReadByte();
            rhs.ReadByte();

            while (true) {
                if (!lhs.HasData() || !rhs.HasData()) {
                    break;
                }

                char lhsNext = lhs.PeekByte();
                char rhsNext = rhs.PeekByte();

                if (lhsNext == SYMBOL_END_LIST && rhsNext == SYMBOL_END_LIST) {
                    return 0;
                }

                if (lhsNext == SYMBOL_END_LIST) {
                    return -1;
                }

                if (rhsNext == SYMBOL_END_LIST) {
                    return 1;
                }

                int cmp = CompareYsonValuesImpl(lhs, rhs);
                if (cmp != 0) {
                    return cmp;
                }

                if (lhs.HasData() && lhs.PeekByte() == SYMBOL_LIST_SEPARATOR) {
                    lhs.ReadByte();
                }
                if (rhs.HasData() && rhs.PeekByte() == SYMBOL_LIST_SEPARATOR) {
                    rhs.ReadByte();
                }
            }
            return 0;
        }

        default:
            ythrow yexception() << "Unsupported YSON type: 0x"
                << IntToString<16>(static_cast<unsigned char>(type));
    }
}

int TBinaryYsonComparator::CompareYsonValues(TColumnOffsetRange lhs, TColumnOffsetRange rhs) const {
    TYsonReader lhsReader = GetColumnReader(lhs);
    TYsonReader rhsReader = GetColumnReader(rhs);
    return CompareYsonValuesImpl(lhsReader, rhsReader);
}

int TBinaryYsonComparator::CompareRows(
    const TRowIndexMarkup& lhsRow,
    const TRowIndexMarkup& rhsRow
) const {
    Y_ENSURE(lhsRow.size() == rhsRow.size(), "Row sizes mismatch");
    Y_ENSURE(lhsRow.size() - 1 == SortOrders_.size(), "Row size doesn't match sort orders. Expected "
                                                      "last column to be the row boundary.");

    // The last column is the row boundary, so we don't need to compare it
    for (ui64 colIdx = 0; colIdx < lhsRow.size() - 1; ++colIdx) {
        const auto& lhsRange = lhsRow[colIdx];
        const auto& rhsRange = rhsRow[colIdx];

        bool lhsIsNull = !lhsRange.IsValid();
        bool rhsIsNull = !rhsRange.IsValid();

        if (lhsIsNull && rhsIsNull) {
            continue;
        }
        if (lhsIsNull) {
            return -1;
        }
        if (rhsIsNull) {
            return 1;
        }

        int result = CompareYsonValues(lhsRange, rhsRange);

        if (SortOrders_[colIdx] == ESortOrder::Descending) {
            result = -result;
        }

        if (result != 0) {
            return result;
        }
    }

    return 0;
}

} // namespace NYql::NFmr
