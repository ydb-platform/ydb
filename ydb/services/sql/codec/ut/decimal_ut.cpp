#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/sql/codec/codec.h>

#include <cstring>

using namespace NKikimr::NSql;
namespace NTypeIds = NKikimr::NScheme::NTypeIds;

namespace {

TString Cell128(long long value) {
    __int128 wide = value;
    TString bytes(sizeof(wide), '\0');
    memcpy(bytes.begin(), &wide, sizeof(wide));
    return bytes;
}

} // namespace

Y_UNIT_TEST_SUITE(CodecDecimal) {
    Y_UNIT_TEST(DecimalUuidAndJson) {
        const TString cell = Cell128(1234);
        const TCodecValue text = EncodeCell(NTypeIds::Decimal, false, cell, EWireFormat::Text, 4, 2);
        UNIT_ASSERT_C(text.Ok, text.Message);
        UNIT_ASSERT_VALUES_EQUAL(text.Bytes, "12.34");
        const TCodecValue back = DecodeCell(NTypeIds::Decimal, false, text.Bytes, EWireFormat::Text, 4, 2);
        UNIT_ASSERT_C(back.Ok, back.Message);
        UNIT_ASSERT_VALUES_EQUAL(back.Bytes, cell);

        const TCodecValue binary = EncodeCell(NTypeIds::Decimal, false, cell, EWireFormat::Binary, 4, 2);
        UNIT_ASSERT(binary.Ok);
        const TCodecValue binaryBack = DecodeCell(NTypeIds::Decimal, false, binary.Bytes, EWireFormat::Binary, 4, 2);
        UNIT_ASSERT_C(binaryBack.Ok, binaryBack.Message);
        UNIT_ASSERT_VALUES_EQUAL(binaryBack.Bytes, cell);

        const TCodecValue nan = DecodeCell(NTypeIds::Decimal, false, "NaN", EWireFormat::Text, 4, 2);
        UNIT_ASSERT(!nan.Ok);
        UNIT_ASSERT_VALUES_EQUAL(nan.SqlState, "0A000");

        TString uuid(16, '\0');
        uuid[0] = 0x12;
        uuid[15] = static_cast<char>(0xab);
        const TCodecValue uuidText = EncodeCell(NTypeIds::Uuid, false, uuid, EWireFormat::Text);
        UNIT_ASSERT(uuidText.Ok);
        UNIT_ASSERT_VALUES_EQUAL(uuidText.Bytes, "12000000-0000-0000-0000-0000000000ab");
        const TCodecValue uuidBack = DecodeCell(NTypeIds::Uuid, false, uuidText.Bytes, EWireFormat::Text);
        UNIT_ASSERT_VALUES_EQUAL(uuidBack.Bytes, uuid);
        const TCodecValue uuidBinary = EncodeCell(NTypeIds::Uuid, false, uuid, EWireFormat::Binary);
        UNIT_ASSERT_VALUES_EQUAL(uuidBinary.Bytes, uuid);

        const TString json = "{\"a\":1}";
        for (const NKikimr::NScheme::TTypeId type : {NTypeIds::Json, NTypeIds::JsonDocument}) {
            for (const EWireFormat format : {EWireFormat::Text, EWireFormat::Binary}) {
                const TCodecValue encoded = EncodeCell(type, false, json, format);
                UNIT_ASSERT_VALUES_EQUAL(encoded.Bytes, json);
                const TCodecValue decoded = DecodeCell(type, false, encoded.Bytes, format);
                UNIT_ASSERT_VALUES_EQUAL(decoded.Bytes, json);
            }
        }
    }

    Y_UNIT_TEST(DyNumberIsUnsupported) {
        const TCodecValue encoded = EncodeCell(NTypeIds::DyNumber, false, "1", EWireFormat::Text);
        UNIT_ASSERT(!encoded.Ok);
        UNIT_ASSERT_VALUES_EQUAL(encoded.SqlState, "0A000");
    }
}
