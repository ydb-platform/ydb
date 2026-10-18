#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/sql/codec/codec.h>

using namespace NKikimr::NSql;
namespace NTypeIds = NKikimr::NScheme::NTypeIds;

namespace {

void RoundTrip(NKikimr::NScheme::TTypeId type, TStringBuf cell) {
    for (const EWireFormat format : {EWireFormat::Text, EWireFormat::Binary}) {
        const TCodecValue encoded = EncodeCell(type, false, cell, format);
        UNIT_ASSERT_C(encoded.Ok, encoded.Message);
        const TCodecValue decoded = DecodeCell(type, false, encoded.Bytes, format);
        UNIT_ASSERT_C(decoded.Ok, decoded.Message);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Bytes, cell);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(CodecText) {
    Y_UNIT_TEST(Utf8AndBytesKeepTheirLength) {
        TString text;
        for (int i = 0; i < 4000; ++i) {
            text.append("я");
        }
        text.append("tail");
        RoundTrip(NTypeIds::Utf8, text);
        const TString bytes = TString(5000, '\0') + TString("\xff\x00long", 6);
        RoundTrip(NTypeIds::String, bytes);
        RoundTrip(NTypeIds::String4k, bytes);
        RoundTrip(NTypeIds::String2m, bytes);
    }

    Y_UNIT_TEST(ByteaTextIsHex) {
        const TCodecValue encoded = EncodeCell(NTypeIds::String, false, "A", EWireFormat::Text);
        UNIT_ASSERT(encoded.Ok);
        UNIT_ASSERT_VALUES_EQUAL(encoded.Bytes, "\\x41");
    }
}
