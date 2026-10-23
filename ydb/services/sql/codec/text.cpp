#include "codec.h"

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

bool IsUtf8(NScheme::TTypeId type) {
    return type == NScheme::NTypeIds::Utf8;
}

bool IsBytes(NScheme::TTypeId type) {
    return type == NScheme::NTypeIds::String
        || type == NScheme::NTypeIds::String4k
        || type == NScheme::NTypeIds::String2m;
}

char HexDigit(ui8 value) {
    return static_cast<char>(value < 10 ? '0' + value : 'a' + value - 10);
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

TString EncodeHex(TStringBuf bytes) {
    TString out;
    out.reserve(2 + bytes.size() * 2);
    out.append("\\x");
    for (unsigned char byte : bytes) {
        out.push_back(HexDigit(byte >> 4));
        out.push_back(HexDigit(byte & 0x0f));
    }
    return out;
}

TCodecValue DecodeHex(TStringBuf wire) {
    if (wire.size() < 2 || wire[0] != '\\' || (wire[1] != 'x' && wire[1] != 'X')) {
        return Fail("22P02", "invalid bytea");
    }
    if ((wire.size() % 2) != 0) {
        return Fail("22P02", "invalid bytea");
    }
    TString out;
    out.reserve((wire.size() - 2) / 2);
    for (size_t i = 2; i < wire.size(); i += 2) {
        const int hi = HexValue(wire[i]);
        const int lo = HexValue(wire[i + 1]);
        if (hi < 0 || lo < 0) {
            return Fail("22P02", "invalid bytea");
        }
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return OkBytes(std::move(out));
}

} // namespace

TCodecValue EncodeTextCell(NScheme::TTypeId type, bool null, TStringBuf cell, EWireFormat format) {
    if (!IsUtf8(type) && !IsBytes(type)) {
        return Unsupported();
    }
    if (null || cell.empty()) {
        return OkNull();
    }
    if (IsUtf8(type) || format == EWireFormat::Binary) {
        return OkBytes(TString(cell));
    }
    return OkBytes(EncodeHex(cell));
}

TCodecValue DecodeTextCell(NScheme::TTypeId type, bool null, TStringBuf wire, EWireFormat format) {
    if (!IsUtf8(type) && !IsBytes(type)) {
        return Unsupported();
    }
    if (null) {
        return OkNull();
    }
    if (IsUtf8(type) || format == EWireFormat::Binary) {
        return OkBytes(TString(wire));
    }
    return DecodeHex(wire);
}

} // namespace NKikimr::NSql
