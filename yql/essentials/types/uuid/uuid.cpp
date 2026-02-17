#include "uuid.h"

#include <util/stream/str.h>

namespace NKikimr::NUuid {

namespace {

void WriteHexDigit(ui8 digit, IOutputStream& out) {
    if (digit <= 9) {
        out << char('0' + digit);
    } else {
        out << char('a' + digit - 10);
    }
}

void WriteHex(ui16 bytes, IOutputStream& out, bool reverseBytes = false) {
    if (reverseBytes) {
        WriteHexDigit((bytes >> 4) & 0x0f, out);
        WriteHexDigit(bytes & 0x0f, out);
        WriteHexDigit((bytes >> 12) & 0x0f, out);
        WriteHexDigit((bytes >> 8) & 0x0f, out);
    } else {
        WriteHexDigit((bytes >> 12) & 0x0f, out);
        WriteHexDigit((bytes >> 8) & 0x0f, out);
        WriteHexDigit((bytes >> 4) & 0x0f, out);
        WriteHexDigit(bytes & 0x0f, out);
    }
}

} // namespace

TString UuidBytesToString(const TString& in) {
    TStringStream ss;

    UuidBytesToString(in, ss);

    return ss.Str();
}

void UuidBytesToString(const TString& in, IOutputStream& out) {
    ui16 dw[8];
    std::memcpy(dw, in.data(), sizeof(dw));
    NUuid::UuidToString(dw, out);
}

void UuidHalfsToString(ui64 low, ui64 hi, IOutputStream& out) {
    union {
        ui16 Dw[8];
        ui64 Half[2];
    } buf;
    buf.Half[0] = low;
    buf.Half[1] = hi;
    NUuid::UuidToString(buf.Dw, out);
}

void UuidToString(ui16 dw[8], IOutputStream& out) {
    WriteHex(dw[1], out);
    WriteHex(dw[0], out);
    out << '-';
    WriteHex(dw[2], out);
    out << '-';
    WriteHex(dw[3], out);
    out << '-';
    WriteHex(dw[4], out, true);
    out << '-';
    WriteHex(dw[5], out, true);
    WriteHex(dw[6], out, true);
    WriteHex(dw[7], out, true);
}

void UuidHalfsToByteString(ui64 low, ui64 hi, IOutputStream& out) {
    union {
        char Bytes[16];
        ui64 Half[2];
    } buf;
    buf.Half[0] = low;
    buf.Half[1] = hi;
    out.Write(buf.Bytes, 16);
}

} // namespace NKikimr::NUuid
