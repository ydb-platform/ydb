#include "uuid.h"

#include <util/stream/str.h>

namespace NKikimr {
namespace NUuid {

static void WriteHexDigit(ui8 digit, IOutputStream& out) {
    if (digit <= 9) {
        out << char('0' + digit);
    }
    else {
        out << char('a' + digit - 10);
    }
}

static void WriteHex(ui16 bytes, IOutputStream& out, bool reverseBytes = false) {
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

TString UuidBytesToString(const TString& in) {
    TStringStream ss;

    UuidBytesToString(in, ss);

    return ss.Str();
}

void UuidBytesToString(const TString& in, IOutputStream& out) {
    ui16 dw[8];
    std::memcpy(dw, in.Data(), sizeof(dw));
    NUuid::UuidToString(dw, out);
}

void UuidHalfsToString(ui64 low, ui64 hi, IOutputStream& out) {
    union {
        ui16 dw[8];
        ui64 half[2];
    } buf;
    buf.half[0] = low;
    buf.half[1] = hi;
    NUuid::UuidToString(buf.dw, out);
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
        char bytes[16];
        ui64 half[2];
    } buf;
    buf.half[0] = low;
    buf.half[1] = hi;
    out.Write(buf.bytes, 16);
}

}
}
