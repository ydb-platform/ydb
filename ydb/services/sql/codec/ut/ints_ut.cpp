#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/sql/codec/codec.h>

#include <cstring>
#include <limits>

using namespace NKikimr::NSql;
namespace NTypeIds = NKikimr::NScheme::NTypeIds;

namespace {

template <class T>
TString Cell(T value) {
    TString bytes(sizeof(T), '\0');
    memcpy(bytes.begin(), &value, sizeof(T));
    return bytes;
}

template <class T>
T Uncell(const TString& bytes) {
    T value{};
    UNIT_ASSERT_VALUES_EQUAL(bytes.size(), sizeof(T));
    memcpy(&value, bytes.data(), sizeof(T));
    return value;
}

void RoundTrip(NKikimr::NScheme::TTypeId type, const TString& cell) {
    for (const EWireFormat format : {EWireFormat::Text, EWireFormat::Binary}) {
        const TCodecValue encoded = EncodeCell(type, false, cell, format);
        UNIT_ASSERT_C(encoded.Ok, encoded.Message);
        UNIT_ASSERT(!encoded.Null);
        const TCodecValue decoded = DecodeCell(type, false, encoded.Bytes, format);
        UNIT_ASSERT_C(decoded.Ok, decoded.Message);
        UNIT_ASSERT_VALUES_EQUAL(decoded.Bytes, cell);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(CodecInts) {
    Y_UNIT_TEST(NullIsAnEmptyCell) {
        const TCodecValue encoded = EncodeCell(NTypeIds::Int32, true, {}, EWireFormat::Text);
        UNIT_ASSERT(encoded.Ok);
        UNIT_ASSERT(encoded.Null);
        UNIT_ASSERT(encoded.Bytes.empty());

        const TCodecValue decoded = DecodeCell(NTypeIds::Bool, true, {}, EWireFormat::Binary);
        UNIT_ASSERT(decoded.Ok);
        UNIT_ASSERT(decoded.Null);
    }

    Y_UNIT_TEST(Boundaries) {
        RoundTrip(NTypeIds::Bool, Cell<ui8>(0));
        RoundTrip(NTypeIds::Bool, Cell<ui8>(1));
        RoundTrip(NTypeIds::Int8, Cell<i8>(std::numeric_limits<i8>::min()));
        RoundTrip(NTypeIds::Int8, Cell<i8>(std::numeric_limits<i8>::max()));
        RoundTrip(NTypeIds::Int16, Cell<i16>(std::numeric_limits<i16>::min()));
        RoundTrip(NTypeIds::Int16, Cell<i16>(std::numeric_limits<i16>::max()));
        RoundTrip(NTypeIds::Int32, Cell<i32>(std::numeric_limits<i32>::min()));
        RoundTrip(NTypeIds::Int32, Cell<i32>(std::numeric_limits<i32>::max()));
        RoundTrip(NTypeIds::Int64, Cell<i64>(std::numeric_limits<i64>::min()));
        RoundTrip(NTypeIds::Int64, Cell<i64>(std::numeric_limits<i64>::max()));
        RoundTrip(NTypeIds::Uint8, Cell<ui8>(0));
        RoundTrip(NTypeIds::Uint8, Cell<ui8>(255));
        RoundTrip(NTypeIds::Uint16, Cell<ui16>(0));
        RoundTrip(NTypeIds::Uint16, Cell<ui16>(65535));
        RoundTrip(NTypeIds::Uint32, Cell<ui32>(0));
        RoundTrip(NTypeIds::Uint32, Cell<ui32>(4294967295u));
        RoundTrip(NTypeIds::Uint64, Cell<ui64>(0));
        RoundTrip(NTypeIds::Uint64, Cell<ui64>(1ull << 63));
        RoundTrip(NTypeIds::Uint64, Cell<ui64>(std::numeric_limits<ui64>::max()));
    }

    Y_UNIT_TEST(Uint64AboveSignedMaxStaysNumeric) {
        const TString cell = Cell<ui64>(std::numeric_limits<ui64>::max());
        const TCodecValue text = EncodeCell(NTypeIds::Uint64, false, cell, EWireFormat::Text);
        UNIT_ASSERT(text.Ok);
        UNIT_ASSERT_VALUES_EQUAL(text.Bytes, "18446744073709551615");
        const TCodecValue binary = EncodeCell(NTypeIds::Uint64, false, cell, EWireFormat::Binary);
        UNIT_ASSERT(binary.Ok);
        UNIT_ASSERT(binary.Bytes.size() > 8);
    }
}
