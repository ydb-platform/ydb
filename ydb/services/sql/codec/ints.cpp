#include "codec.h"

#include <util/string/cast.h>

#include <cstring>
#include <limits>

namespace NKikimr::NSql {

namespace {

TCodecValue Fail(const char* sqlState, TString message) {
    TCodecValue value;
    value.SqlState = sqlState;
    value.Message = std::move(message);
    return value;
}

TCodecValue Unsupported() {
    return Fail("0A000", "not supported");
}

TCodecValue OkBytes(TString bytes) {
    TCodecValue value;
    value.Ok = true;
    value.Bytes = std::move(bytes);
    return value;
}

TCodecValue OkNull() {
    TCodecValue value;
    value.Ok = true;
    value.Null = true;
    return value;
}

template <class T>
T ReadNative(TStringBuf cell) {
    T value{};
    memcpy(&value, cell.data(), sizeof(T));
    return value;
}

template <class T>
TString WriteNative(T value) {
    TString bytes(sizeof(T), '\0');
    memcpy(bytes.begin(), &value, sizeof(T));
    return bytes;
}

void AppendBe16(TString& out, ui16 value) {
    char bytes[2] = {static_cast<char>(value >> 8), static_cast<char>(value)};
    out.append(bytes, 2);
}

ui16 ReadBe16(const char* data) {
    return (static_cast<ui16>(static_cast<ui8>(data[0])) << 8) | static_cast<ui8>(data[1]);
}

i16 ReadBeI16(const char* data) {
    return static_cast<i16>(ReadBe16(data));
}

i32 ReadBeI32(const char* data) {
    ui32 value = 0;
    for (int i = 0; i < 4; ++i) {
        value = (value << 8) | static_cast<ui8>(data[i]);
    }
    return static_cast<i32>(value);
}

i64 ReadBeI64(const char* data) {
    ui64 value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | static_cast<ui8>(data[i]);
    }
    return static_cast<i64>(value);
}

TString WriteBe16(i16 value) {
    TString out;
    AppendBe16(out, static_cast<ui16>(value));
    return out;
}

TString WriteBe32(i32 value) {
    TString out(4, '\0');
    const ui32 bits = static_cast<ui32>(value);
    out[0] = static_cast<char>(bits >> 24);
    out[1] = static_cast<char>(bits >> 16);
    out[2] = static_cast<char>(bits >> 8);
    out[3] = static_cast<char>(bits);
    return out;
}

TString WriteBe64(i64 value) {
    TString out(8, '\0');
    const ui64 bits = static_cast<ui64>(value);
    for (int i = 0; i < 8; ++i) {
        out[i] = static_cast<char>(bits >> (8 * (7 - i)));
    }
    return out;
}

enum class EWireInt {
    Int2,
    Int4,
    Int8,
    Numeric,
};

EWireInt WireOf(NScheme::TTypeId type) {
    switch (type) {
        case NScheme::NTypeIds::Int8:
        case NScheme::NTypeIds::Int16:
        case NScheme::NTypeIds::Uint8:
            return EWireInt::Int2;
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Uint16:
            return EWireInt::Int4;
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Uint32:
            return EWireInt::Int8;
        case NScheme::NTypeIds::Uint64:
            return EWireInt::Numeric;
        default:
            return EWireInt::Int8;
    }
}

bool Supported(NScheme::TTypeId type) {
    switch (type) {
        case NScheme::NTypeIds::Bool:
        case NScheme::NTypeIds::Int8:
        case NScheme::NTypeIds::Int16:
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Uint8:
        case NScheme::NTypeIds::Uint16:
        case NScheme::NTypeIds::Uint32:
        case NScheme::NTypeIds::Uint64:
            return true;
        default:
            return false;
    }
}

size_t CellSize(NScheme::TTypeId type) {
    switch (type) {
        case NScheme::NTypeIds::Bool:
        case NScheme::NTypeIds::Int8:
        case NScheme::NTypeIds::Uint8:
            return 1;
        case NScheme::NTypeIds::Int16:
        case NScheme::NTypeIds::Uint16:
            return 2;
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Uint32:
            return 4;
        default:
            return 8;
    }
}

TCodecValue EncodeNumeric(ui64 value) {
    TString out;
    if (value == 0) {
        AppendBe16(out, 0);
        AppendBe16(out, 0);
        AppendBe16(out, 0);
        AppendBe16(out, 0);
        return OkBytes(std::move(out));
    }
    ui16 digits[5];
    int count = 0;
    while (value > 0) {
        digits[count++] = static_cast<ui16>(value % 10000);
        value /= 10000;
    }
    int first = 0;
    while (first < count - 1 && digits[count - 1 - first] == 0) {
        ++first;
    }
    const int used = count - first;
    AppendBe16(out, static_cast<ui16>(used));
    AppendBe16(out, static_cast<ui16>(count - 1));
    AppendBe16(out, 0);
    AppendBe16(out, 0);
    for (int i = count - 1; i >= first; --i) {
        AppendBe16(out, digits[i]);
    }
    return OkBytes(std::move(out));
}

TCodecValue DecodeNumeric(TStringBuf wire) {
    if (wire.size() < 8 || (wire.size() % 2) != 0) {
        return Fail("22P03", "invalid numeric");
    }
    const i16 ndigits = ReadBeI16(wire.data());
    const i16 weight = ReadBeI16(wire.data() + 2);
    const i16 sign = ReadBeI16(wire.data() + 4);
    const i16 scale = ReadBeI16(wire.data() + 6);
    if (ndigits < 0 || wire.size() != static_cast<size_t>(8 + ndigits * 2)) {
        return Fail("22P03", "invalid numeric");
    }
    if (sign != 0 || scale != 0) {
        return Fail("22003", "numeric is out of range");
    }
    if (ndigits == 0) {
        return OkBytes(WriteNative<ui64>(0));
    }
    ui64 value = 0;
    int power = weight;
    for (int i = 0; i < ndigits; ++i) {
        const ui16 digit = ReadBe16(wire.data() + 8 + i * 2);
        if (digit >= 10000) {
            return Fail("22P03", "invalid numeric");
        }
        if (value > (std::numeric_limits<ui64>::max() - digit) / 10000) {
            return Fail("22003", "numeric is out of range");
        }
        value = value * 10000 + digit;
        --power;
    }
    if (power < -1) {
        return Fail("22003", "numeric is out of range");
    }
    while (power >= 0) {
        if (value > std::numeric_limits<ui64>::max() / 10000) {
            return Fail("22003", "numeric is out of range");
        }
        value *= 10000;
        --power;
    }
    return OkBytes(WriteNative<ui64>(value));
}

template <class T>
TCodecValue FitSigned(i64 value) {
    if (value < static_cast<i64>(std::numeric_limits<T>::min())
        || value > static_cast<i64>(std::numeric_limits<T>::max()))
    {
        return Fail("22003", "integer is out of range");
    }
    return OkBytes(WriteNative(static_cast<T>(value)));
}

template <class T>
TCodecValue FitUnsigned(i64 value) {
    if (value < 0 || static_cast<ui64>(value) > static_cast<ui64>(std::numeric_limits<T>::max())) {
        return Fail("22003", "integer is out of range");
    }
    return OkBytes(WriteNative(static_cast<T>(value)));
}

TCodecValue StoreWireInteger(NScheme::TTypeId type, i64 value) {
    switch (type) {
        case NScheme::NTypeIds::Int8:
            return FitSigned<i8>(value);
        case NScheme::NTypeIds::Int16:
            return FitSigned<i16>(value);
        case NScheme::NTypeIds::Int32:
            return FitSigned<i32>(value);
        case NScheme::NTypeIds::Int64:
            return FitSigned<i64>(value);
        case NScheme::NTypeIds::Uint8:
            return FitUnsigned<ui8>(value);
        case NScheme::NTypeIds::Uint16:
            return FitUnsigned<ui16>(value);
        case NScheme::NTypeIds::Uint32:
            return FitUnsigned<ui32>(value);
        default:
            return Unsupported();
    }
}

i64 CellAsSigned(NScheme::TTypeId type, TStringBuf cell) {
    switch (type) {
        case NScheme::NTypeIds::Int8:
            return ReadNative<i8>(cell);
        case NScheme::NTypeIds::Int16:
            return ReadNative<i16>(cell);
        case NScheme::NTypeIds::Int32:
            return ReadNative<i32>(cell);
        case NScheme::NTypeIds::Int64:
            return ReadNative<i64>(cell);
        case NScheme::NTypeIds::Uint8:
            return ReadNative<ui8>(cell);
        case NScheme::NTypeIds::Uint16:
            return ReadNative<ui16>(cell);
        case NScheme::NTypeIds::Uint32:
            return ReadNative<ui32>(cell);
        default:
            return 0;
    }
}

TCodecValue EncodeBool(TStringBuf cell, EWireFormat format) {
    const bool on = ReadNative<ui8>(cell) != 0;
    if (format == EWireFormat::Text) {
        return OkBytes(on ? "t" : "f");
    }
    return OkBytes(TString(1, on ? '\1' : '\0'));
}

TCodecValue DecodeBool(TStringBuf wire, EWireFormat format) {
    bool on = false;
    if (format == EWireFormat::Binary) {
        if (wire.size() != 1) {
            return Fail("22P03", "invalid bool");
        }
        on = static_cast<ui8>(wire[0]) != 0;
    } else if (wire == "t" || wire == "true" || wire == "1") {
        on = true;
    } else if (wire == "f" || wire == "false" || wire == "0") {
        on = false;
    } else {
        return Fail("22P02", "invalid bool");
    }
    return OkBytes(WriteNative<ui8>(on ? 1 : 0));
}

TCodecValue EncodeInteger(NScheme::TTypeId type, TStringBuf cell, EWireFormat format) {
    if (type == NScheme::NTypeIds::Uint64) {
        const ui64 value = ReadNative<ui64>(cell);
        if (format == EWireFormat::Text) {
            return OkBytes(ToString(value));
        }
        return EncodeNumeric(value);
    }
    const i64 value = CellAsSigned(type, cell);
    if (format == EWireFormat::Text) {
        return OkBytes(ToString(value));
    }
    switch (WireOf(type)) {
        case EWireInt::Int2:
            return OkBytes(WriteBe16(static_cast<i16>(value)));
        case EWireInt::Int4:
            return OkBytes(WriteBe32(static_cast<i32>(value)));
        case EWireInt::Int8:
            return OkBytes(WriteBe64(value));
        case EWireInt::Numeric:
            break;
    }
    return Unsupported();
}

TCodecValue DecodeInteger(NScheme::TTypeId type, TStringBuf wire, EWireFormat format) {
    if (type == NScheme::NTypeIds::Uint64) {
        if (format == EWireFormat::Binary) {
            return DecodeNumeric(wire);
        }
        ui64 value = 0;
        if (!TryFromString(wire, value)) {
            return Fail("22P02", "invalid numeric");
        }
        return OkBytes(WriteNative(value));
    }
    i64 value = 0;
    if (format == EWireFormat::Text) {
        if (!TryFromString(wire, value)) {
            return Fail("22P02", "invalid integer");
        }
    } else {
        switch (WireOf(type)) {
            case EWireInt::Int2:
                if (wire.size() != 2) {
                    return Fail("22P03", "invalid integer");
                }
                value = ReadBeI16(wire.data());
                break;
            case EWireInt::Int4:
                if (wire.size() != 4) {
                    return Fail("22P03", "invalid integer");
                }
                value = ReadBeI32(wire.data());
                break;
            case EWireInt::Int8:
                if (wire.size() != 8) {
                    return Fail("22P03", "invalid integer");
                }
                value = ReadBeI64(wire.data());
                break;
            case EWireInt::Numeric:
                return Unsupported();
        }
    }
    return StoreWireInteger(type, value);
}

} // namespace

TCodecValue EncodeCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format) {
    if (!Supported(type)) {
        return Unsupported();
    }
    if (null || cell.empty()) {
        return OkNull();
    }
    if (cell.size() != CellSize(type)) {
        return Fail("22P03", "invalid cell");
    }
    if (type == NScheme::NTypeIds::Bool) {
        return EncodeBool(cell, format);
    }
    return EncodeInteger(type, cell, format);
}

TCodecValue DecodeCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format) {
    if (!Supported(type)) {
        return Unsupported();
    }
    if (null) {
        return OkNull();
    }
    if (type == NScheme::NTypeIds::Bool) {
        return DecodeBool(wire, format);
    }
    return DecodeInteger(type, wire, format);
}

} // namespace NKikimr::NSql
