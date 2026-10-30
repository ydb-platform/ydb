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
    memcpy(&value, bytes.data(), sizeof(T));
    return value;
}

void RoundTrip(NKikimr::NScheme::TTypeId type, const TString& cell, TStringBuf text) {
    const TCodecValue encoded = EncodeCell(type, false, cell, EWireFormat::Text);
    UNIT_ASSERT_C(encoded.Ok, encoded.Message);
    UNIT_ASSERT_VALUES_EQUAL(encoded.Bytes, text);
    const TCodecValue decoded = DecodeCell(type, false, encoded.Bytes, EWireFormat::Text);
    UNIT_ASSERT_C(decoded.Ok, decoded.Message);
    UNIT_ASSERT_VALUES_EQUAL(decoded.Bytes, cell);

    const TCodecValue binary = EncodeCell(type, false, cell, EWireFormat::Binary);
    UNIT_ASSERT_C(binary.Ok, binary.Message);
    const TCodecValue back = DecodeCell(type, false, binary.Bytes, EWireFormat::Binary);
    UNIT_ASSERT_C(back.Ok, back.Message);
    UNIT_ASSERT_VALUES_EQUAL(back.Bytes, cell);
}

} // namespace

Y_UNIT_TEST_SUITE(CodecTime) {
    Y_UNIT_TEST(DateBoundaries) {
        RoundTrip(NTypeIds::Date, Cell<ui16>(0), "1970-01-01");
        RoundTrip(NTypeIds::Date, Cell<ui16>(49672), "2105-12-31");
        RoundTrip(NTypeIds::Date32, Cell<i32>(-1), "1969-12-31");

        const TCodecValue early = DecodeCell(NTypeIds::Date, false, "1969-12-31", EWireFormat::Text);
        UNIT_ASSERT(!early.Ok);
        UNIT_ASSERT_VALUES_EQUAL(early.SqlState, "22003");
    }

    Y_UNIT_TEST(TimestampBoundaries) {
        RoundTrip(NTypeIds::Timestamp, Cell<ui64>(0), "1970-01-01 00:00:00.000000");
        const ui64 end = (49672ull + 1) * 86400ull * 1000000ull - 1;
        RoundTrip(NTypeIds::Timestamp, Cell<ui64>(end), "2105-12-31 23:59:59.999999");
        RoundTrip(NTypeIds::Timestamp64, Cell<i64>(-1), "1969-12-31 23:59:59.999999");
        RoundTrip(NTypeIds::Datetime, Cell<ui32>(0), "1970-01-01 00:00:00.000000");
        RoundTrip(NTypeIds::Datetime64, Cell<i64>(-86400), "1969-12-31 00:00:00.000000");

        const TCodecValue beforeEpoch = DecodeCell(NTypeIds::Timestamp, false, "1969-12-31 23:59:59.999999", EWireFormat::Text);
        UNIT_ASSERT(!beforeEpoch.Ok);
        UNIT_ASSERT_VALUES_EQUAL(beforeEpoch.SqlState, "22003");
    }

    Y_UNIT_TEST(IntervalIsMicrosecondsOnly) {
        RoundTrip(NTypeIds::Interval, Cell<i64>(1500000), "0:00:01.500000");
        RoundTrip(NTypeIds::Interval, Cell<i64>(-1500000), "-0:00:01.500000");
        const i64 wide = std::numeric_limits<i64>::max() / 4;
        RoundTrip(NTypeIds::Interval64, Cell<i64>(wide), EncodeCell(NTypeIds::Interval64, false, Cell<i64>(wide), EWireFormat::Text).Bytes);

        const TCodecValue days = DecodeCell(NTypeIds::Interval, false, "1 day", EWireFormat::Text);
        UNIT_ASSERT(!days.Ok);
        UNIT_ASSERT_VALUES_EQUAL(days.SqlState, "0A000");

        TString binary(16, '\0');
        binary[11] = 1;
        const TCodecValue months = DecodeCell(NTypeIds::Interval64, false, binary, EWireFormat::Binary);
        UNIT_ASSERT(!months.Ok);
        UNIT_ASSERT_VALUES_EQUAL(months.SqlState, "0A000");
    }
}
