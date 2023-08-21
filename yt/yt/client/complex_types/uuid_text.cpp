#include "uuid_text.h"

#include <yt/yt/core/misc/error.h>

#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

#include <util/string/hex.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

static void CheckUuidSize(TStringBuf bytes)
{
    if (bytes.size() != UuidBinarySize) {
        THROW_ERROR_EXCEPTION(
            "Invalid binary UUID length: got %v, expected: %v",
            bytes.size(),
            UuidBinarySize);
    }
}

void TextYqlUuidToBytes(TStringBuf uuid, char* ptr)
{
    // Opposite to TextYqlUuidFromBytes(). Check that function first.
    if (uuid.size() != UuidYqlTextSize) {
        THROW_ERROR_EXCEPTION(
            "Invalid text YQL UUID length: got %v, expected: %v",
            uuid.size(),
            UuidYqlTextSize);
    }
    auto parseByte = [] (const char* ptr) {
        try {
            return Char2Digit(*ptr) * 16 + Char2Digit(*(ptr + 1));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Could not parse hex byte") << ex;
        }
    };
    auto verifyDash = [] (const char* ptr) {
        if (*ptr != '-') {
            THROW_ERROR_EXCEPTION("Unexpected character: actual %Qv, expected %Qv",
                *ptr,
                '-');
        }
    };

    const char* input = uuid.Data();
    for (int i = 3; i >= 0; --i) {
        *ptr++ = parseByte(input + 2 * i);
    }
    verifyDash(input + 8);
    for (int i = 5; i >= 4; --i) {
        *ptr++ = parseByte(input + 1 + 2 * i);
    }
    verifyDash(input + 13);
    for (int i = 7; i >= 6; --i) {
        *ptr++ = parseByte(input + 2 + 2 * i);
    }
    verifyDash(input + 18);
    for (int i = 8; i <= 9; ++i) {
        *ptr++ = parseByte(input + 3 + 2 * i);
    }
    verifyDash(input + 23);
    for (int i = 10; i < 16; ++i) {
        *ptr++ = parseByte(input + 4 + 2 * i);
    }
}

char* TextYqlUuidFromBytes(TStringBuf bytes, char* ptr)
{
    // Inspired by https://a.yandex-team.ru/arc/trunk/arcadia/yql/minikql/mkql_type_ops.cpp?rev=6853830#L327
    CheckUuidSize(bytes);
    static const char* HexDigits = "0123456789abcdef";

    auto writeByte = [&ptr] (ui8 x) {
        *ptr++ = HexDigits[x >> 4];
        *ptr++ = HexDigits[x & 0b1111];
    };

    for (int i = 3; i >= 0; --i) {
        writeByte(bytes[i]);
    }
    *ptr++ = '-';
    for (int i = 5; i >= 4; --i) {
        writeByte(bytes[i]);
    }
    *ptr++ = '-';
    for (int i = 7; i >= 6; --i) {
        writeByte(bytes[i]);
    }
    *ptr++ = '-';
    for (int i = 8; i <= 9; ++i) {
        writeByte(bytes[i]);
    }
    *ptr++ = '-';
    for (int i = 10; i < 16; ++i) {
        writeByte(bytes[i]);
    }
    return ptr;
}

void GuidToBytes(TGuid guid, char* ptr)
{
    auto low = LittleToHost(guid.Parts64[0]);
    auto high = LittleToHost(guid.Parts64[1]);

    WriteUnaligned<ui64>(ptr, HostToInet(high));
    WriteUnaligned<ui64>(ptr + sizeof(ui64), HostToInet(low));
}

TGuid GuidFromBytes(TStringBuf bytes)
{
    // Doing the same way as
    // https://a.yandex-team.ru/arc/trunk/arcadia/yt/yt/client/formats/skiff_yson_converter-inl.h?rev=r8579019#L124

    CheckUuidSize(bytes);
    const char* ptr = bytes.Data();
    auto low = InetToHost(ReadUnaligned<ui64>(ptr + sizeof(ui64)));
    auto high = InetToHost(ReadUnaligned<ui64>(ptr));

    return TGuid(HostToLittle(low), HostToLittle(high));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
