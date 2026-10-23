#include "codec.h"

#include <util/generic/vector.h>

#include <cstring>
#include <limits>

namespace NKikimr::NSql {

namespace {

using TInt128 = __int128;
using TUint128 = unsigned __int128;

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

bool IsDecimal(NScheme::TTypeId type) {
    return type == NScheme::NTypeIds::Decimal;
}

bool IsDocument(NScheme::TTypeId type) {
    return type == NScheme::NTypeIds::Json || type == NScheme::NTypeIds::JsonDocument || type == NScheme::NTypeIds::Uuid;
}

TInt128 Read128(TStringBuf cell) {
    TInt128 value = 0;
    memcpy(&value, cell.data(), sizeof(value));
    return value;
}

TString Write128(TInt128 value) {
    TString bytes(sizeof(value), '\0');
    memcpy(bytes.begin(), &value, sizeof(value));
    return bytes;
}

TString DigitsOf(TUint128 value) {
    if (value == 0) {
        return "0";
    }
    TString digits;
    while (value > 0) {
        digits.push_back(static_cast<char>('0' + static_cast<int>(value % 10)));
        value /= 10;
    }
    for (size_t i = 0; i < digits.size() / 2; ++i) {
        const char tmp = digits[i];
        digits[i] = digits[digits.size() - 1 - i];
        digits[digits.size() - 1 - i] = tmp;
    }
    return digits;
}

size_t SignificantDigits(const TString& digits) {
    size_t first = 0;
    while (first + 1 < digits.size() && digits[first] == '0') {
        ++first;
    }
    return digits.size() - first;
}

TString FormatDecimal(TInt128 value, ui8 scale) {
    const bool negative = value < 0;
    const TString digits = DigitsOf(negative ? static_cast<TUint128>(-value) : static_cast<TUint128>(value));
    TString body = digits;
    if (body.size() <= scale) {
        body = TString(scale + 1 - body.size(), '0') + body;
    }
    if (scale == 0) {
        return (negative ? "-" : "") + body;
    }
    body.insert(body.size() - scale, ".");
    return (negative ? "-" : "") + body;
}

void AppendBe16(TString& out, ui16 value) {
    out.push_back(static_cast<char>(value >> 8));
    out.push_back(static_cast<char>(value));
}

i16 ReadBeI16(const char* data) {
    return static_cast<i16>((static_cast<ui16>(static_cast<ui8>(data[0])) << 8) | static_cast<ui8>(data[1]));
}

TCodecValue EncodeNumeric(TInt128 value, ui8 scale) {
    const bool negative = value < 0;
    TString digits = DigitsOf(negative ? static_cast<TUint128>(-value) : static_cast<TUint128>(value));
    if (digits.size() <= scale) {
        digits = TString(scale + 1 - digits.size(), '0') + digits;
    }
    TString integer = scale == 0 ? digits : digits.substr(0, digits.size() - scale);
    TString fraction = scale == 0 ? TString() : digits.substr(digits.size() - scale);
    while (fraction.size() % 4 != 0) {
        fraction.push_back('0');
    }
    while (integer.size() % 4 != 0) {
        integer = "0" + integer;
    }
    TVector<ui16> groups;
    auto append = [&](const TString& part) {
        for (size_t i = 0; i < part.size(); i += 4) {
            ui16 group = 0;
            for (size_t j = 0; j < 4; ++j) {
                group = static_cast<ui16>(group * 10 + (part[i + j] - '0'));
            }
            groups.push_back(group);
        }
    };
    append(integer);
    const int integerGroups = static_cast<int>(groups.size());
    append(fraction);
    int first = 0;
    while (first < static_cast<int>(groups.size()) && groups[first] == 0) {
        ++first;
    }
    TString out;
    if (first == static_cast<int>(groups.size())) {
        AppendBe16(out, 0);
        AppendBe16(out, 0);
        AppendBe16(out, 0);
        AppendBe16(out, scale);
        return OkBytes(std::move(out));
    }
    const int weight = integerGroups - first - 1;
    AppendBe16(out, static_cast<ui16>(groups.size() - first));
    AppendBe16(out, static_cast<ui16>(static_cast<i16>(weight)));
    AppendBe16(out, negative ? 0x4000 : 0);
    AppendBe16(out, scale);
    for (size_t i = first; i < groups.size(); ++i) {
        AppendBe16(out, groups[i]);
    }
    return OkBytes(std::move(out));
}

TCodecValue DecodeNumeric(TStringBuf wire, ui8 precision, ui8 scale) {
    if (wire.size() < 8) {
        return Fail("22P03", "invalid numeric");
    }
    const i16 ndigits = ReadBeI16(wire.data());
    const i16 weight = ReadBeI16(wire.data() + 2);
    const i16 sign = ReadBeI16(wire.data() + 4);
    const i16 dscale = ReadBeI16(wire.data() + 6);
    if (sign == static_cast<i16>(0xC000) || sign == static_cast<i16>(0xD000) || sign == static_cast<i16>(0xF000)) {
        return Unsupported();
    }
    if (ndigits < 0 || wire.size() != static_cast<size_t>(8 + ndigits * 2) || dscale < 0) {
        return Fail("22P03", "invalid numeric");
    }
    TUint128 magnitude = 0;
    for (int i = 0; i < ndigits; ++i) {
        const ui16 digit = static_cast<ui16>(ReadBeI16(wire.data() + 8 + i * 2));
        if (digit >= 10000 || magnitude > (std::numeric_limits<TUint128>::max() - digit) / 10000) {
            return Fail("22003", "numeric is out of range");
        }
        magnitude = magnitude * 10000 + digit;
    }
    int fracGroups = ndigits == 0 ? 0 : ndigits - (weight + 1);
    while (fracGroups < 0) {
        if (magnitude > std::numeric_limits<TUint128>::max() / 10000) {
            return Fail("22003", "numeric is out of range");
        }
        magnitude *= 10000;
        ++fracGroups;
    }
    int drop = fracGroups * 4 - dscale;
    while (drop > 0) {
        if (magnitude % 10 != 0) {
            return Fail("22003", "numeric is out of range");
        }
        magnitude /= 10;
        --drop;
    }
    int adjust = scale - dscale;
    if (adjust < 0) {
        return Fail("22003", "numeric is out of range");
    }
    while (adjust > 0) {
        if (magnitude > std::numeric_limits<TUint128>::max() / 10) {
            return Fail("22003", "numeric is out of range");
        }
        magnitude *= 10;
        --adjust;
    }
    const TString probe = DigitsOf(magnitude);
    if (SignificantDigits(probe) > precision) {
        return Fail("22003", "numeric is out of range");
    }
    TInt128 signedValue = magnitude;
    if (sign != 0) {
        signedValue = -signedValue;
    }
    return OkBytes(Write128(signedValue));
}

TString FormatUuid(TStringBuf cell) {
    static const char* hex = "0123456789abcdef";
    TString out;
    out.reserve(36);
    for (size_t i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            out.push_back('-');
        }
        const ui8 byte = static_cast<ui8>(cell[i]);
        out.push_back(hex[byte >> 4]);
        out.push_back(hex[byte & 0x0f]);
    }
    return out;
}

int HexValue(char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }
    return -1;
}

TCodecValue ParseUuid(TStringBuf text) {
    TString out(16, '\0');
    size_t byte = 0;
    for (size_t i = 0; i < text.size() && byte < 16; ++i) {
        if (text[i] == '-') {
            continue;
        }
        if (i + 1 >= text.size()) {
            return Fail("22P02", "invalid uuid");
        }
        const int hi = HexValue(text[i]);
        const int lo = HexValue(text[++i]);
        if (hi < 0 || lo < 0) {
            return Fail("22P02", "invalid uuid");
        }
        out[byte++] = static_cast<char>((hi << 4) | lo);
    }
    if (byte != 16) {
        return Fail("22P02", "invalid uuid");
    }
    return OkBytes(std::move(out));
}

TCodecValue ParseDecimal(TStringBuf text, ui8 precision, ui8 scale) {
    if (text == "NaN" || text == "Infinity" || text == "-Infinity") {
        return Unsupported();
    }
    const bool negative = text.SkipPrefix("-");
    const size_t point = text.find('.');
    TStringBuf whole = point == TStringBuf::npos ? text : text.substr(0, point);
    TStringBuf fraction = point == TStringBuf::npos ? TStringBuf() : text.substr(point + 1);
    if (whole.empty() || fraction.size() > scale) {
        return Fail("22003", "numeric is out of range");
    }
    TUint128 magnitude = 0;
    auto take = [&](TStringBuf part) {
        for (char ch : part) {
            if (ch < '0' || ch > '9') {
                return false;
            }
            magnitude = magnitude * 10 + static_cast<TUint128>(ch - '0');
        }
        return true;
    };
    if (!take(whole) || !take(fraction)) {
        return Fail("22P02", "invalid numeric");
    }
    for (size_t i = fraction.size(); i < scale; ++i) {
        magnitude *= 10;
    }
    if (SignificantDigits(DigitsOf(magnitude)) > precision) {
        return Fail("22003", "numeric is out of range");
    }
    TInt128 value = magnitude;
    if (negative) {
        value = -value;
    }
    return OkBytes(Write128(value));
}

} // namespace

TCodecValue EncodeDecimalCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format, ui8 precision, ui8 scale) {
    if (type == NScheme::NTypeIds::DyNumber) {
        return Unsupported();
    }
    if (!IsDecimal(type) && !IsDocument(type)) {
        return Unsupported();
    }
    if (IsDecimal(type) && (precision < 1 || precision > 35 || scale > precision)) {
        return Unsupported();
    }
    if (null || (IsDecimal(type) && cell.empty())) {
        return OkNull();
    }
    if (type == NScheme::NTypeIds::Uuid) {
        if (cell.size() != 16) {
            return Fail("22P03", "invalid uuid");
        }
        if (format == EWireFormat::Binary) {
            return OkBytes(TString(cell));
        }
        return OkBytes(FormatUuid(cell));
    }
    if (type == NScheme::NTypeIds::Json || type == NScheme::NTypeIds::JsonDocument) {
        return OkBytes(TString(cell));
    }
    if (cell.size() != 16) {
        return Fail("22P03", "invalid numeric");
    }
    const TInt128 value = Read128(cell);
    if (value == std::numeric_limits<TInt128>::min()) {
        return Fail("22003", "numeric is out of range");
    }
    const TString digits = DigitsOf(value < 0 ? static_cast<TUint128>(-value) : static_cast<TUint128>(value));
    if (SignificantDigits(digits) > precision) {
        return Fail("22003", "numeric is out of range");
    }
    if (format == EWireFormat::Text) {
        return OkBytes(FormatDecimal(value, scale));
    }
    return EncodeNumeric(value, scale);
}

TCodecValue DecodeDecimalCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format, ui8 precision, ui8 scale) {
    if (type == NScheme::NTypeIds::DyNumber) {
        return Unsupported();
    }
    if (!IsDecimal(type) && !IsDocument(type)) {
        return Unsupported();
    }
    if (IsDecimal(type) && (precision < 1 || precision > 35 || scale > precision)) {
        return Unsupported();
    }
    if (null) {
        return OkNull();
    }
    if (type == NScheme::NTypeIds::Uuid) {
        if (format == EWireFormat::Binary) {
            if (wire.size() != 16) {
                return Fail("22P03", "invalid uuid");
            }
            return OkBytes(TString(wire));
        }
        return ParseUuid(wire);
    }
    if (type == NScheme::NTypeIds::Json || type == NScheme::NTypeIds::JsonDocument) {
        return OkBytes(TString(wire));
    }
    if (format == EWireFormat::Text) {
        return ParseDecimal(wire, precision, scale);
    }
    return DecodeNumeric(wire, precision, scale);
}

} // namespace NKikimr::NSql
