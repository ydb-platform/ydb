#include <ydb/core/tx/columnshard/engines/scheme/tiering/tier_info.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/dynumber/dynumber.h>

namespace NKikimr::NOlap {
namespace {

std::shared_ptr<arrow::Scalar> DyNumberScalar(TStringBuf text) {
    const auto binary = NDyNumber::ParseDyNumberString(text);
    UNIT_ASSERT_C(binary, text);
    return std::make_shared<arrow::BinaryScalar>(arrow::Buffer::FromString(std::string(binary->data(), binary->size())));
}

Y_UNIT_TEST_SUITE(ColumnShardTtlTypes) {
    Y_UNIT_TEST(LegacyTypes) {
        const auto tier = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts");
        UNIT_ASSERT_VALUES_EQUAL(*tier->ScalarToInstant(std::make_shared<arrow::UInt16Scalar>(1), NScheme::NTypeIds::Date), TInstant::Days(1));
        UNIT_ASSERT_VALUES_EQUAL(
            *tier->ScalarToInstant(std::make_shared<arrow::UInt32Scalar>(1), NScheme::NTypeIds::Datetime), TInstant::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(*tier->ScalarToInstant(std::make_shared<arrow::TimestampScalar>(1, arrow::timestamp(arrow::TimeUnit::MICRO)),
                                     NScheme::NTypeIds::Timestamp), TInstant::MicroSeconds(1));
        for (const ui32 units : { 1U, 1000U, 1000000U, 1000000000U }) {
            const auto numeric = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts", units);
            UNIT_ASSERT_VALUES_EQUAL(
                *numeric->ScalarToInstant(std::make_shared<arrow::UInt32Scalar>(units), NScheme::NTypeIds::Uint32), TInstant::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(
                *numeric->ScalarToInstant(std::make_shared<arrow::UInt64Scalar>(units), NScheme::NTypeIds::Uint64), TInstant::Seconds(1));
        }
    }

    Y_UNIT_TEST(ExtendedDates) {
        const auto tier = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts");
        for (const i64 value : { -1LL, 0LL, 1LL, 1000000LL }) {
            const auto date = tier->ScalarToInstant(std::make_shared<arrow::Int32Scalar>(value), NScheme::NTypeIds::Date32);
            const auto datetime = tier->ScalarToInstant(std::make_shared<arrow::Int64Scalar>(value), NScheme::NTypeIds::Datetime64);
            const auto timestamp = tier->ScalarToInstant(std::make_shared<arrow::Int64Scalar>(value), NScheme::NTypeIds::Timestamp64);
            UNIT_ASSERT(date && datetime && timestamp);
            UNIT_ASSERT_VALUES_EQUAL(*date, TInstant::Days(Max<i64>(0, value)));
            UNIT_ASSERT_VALUES_EQUAL(*datetime, TInstant::Seconds(Max<i64>(0, value)));
            UNIT_ASSERT_VALUES_EQUAL(*timestamp, TInstant::MicroSeconds(Max<i64>(0, value)));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            *tier->ScalarToInstant(std::make_shared<arrow::Int32Scalar>(49673), NScheme::NTypeIds::Date32), TInstant::Days(49673));
        UNIT_ASSERT_VALUES_EQUAL(*tier->ScalarToInstant(std::make_shared<arrow::Int64Scalar>(4294967296LL), NScheme::NTypeIds::Datetime64),
            TInstant::Seconds(4294967296ULL));
    }

    Y_UNIT_TEST(DyNumberUnitsAndBounds) {
        for (const ui32 units : { 1U, 1000U, 1000000U, 1000000000U }) {
            const auto tier = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts", units);
            for (const auto text : { "-1e125", "-0.00001", "0" }) {
                UNIT_ASSERT_VALUES_EQUAL(*tier->ScalarToInstant(DyNumberScalar(text), NScheme::NTypeIds::DyNumber), TInstant::Zero());
            }
            UNIT_ASSERT_VALUES_EQUAL(*tier->ScalarToInstant(DyNumberScalar(ToString(units)), NScheme::NTypeIds::DyNumber), TInstant::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(*tier->ScalarToInstant(DyNumberScalar("1e-130"), NScheme::NTypeIds::DyNumber), TInstant::MicroSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(*tier->ScalarToInstant(DyNumberScalar("1e125"), NScheme::NTypeIds::DyNumber), TInstant::Max());
        }
        const auto tier = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts", 1000000);
        for (const auto& [text, expected] :
            TVector<std::pair<TString, ui64>>{ { "1000000.25", 1000001 }, { "18446744073709551614", Max<ui64>() },
                { "18446744073709551615", Max<ui64>() }, { "18446744073709551616", Max<ui64>() }, { "18446744073709551614.1", Max<ui64>() } }) {
            UNIT_ASSERT_VALUES_EQUAL(
                *tier->ScalarToInstant(DyNumberScalar(text), NScheme::NTypeIds::DyNumber), TInstant::MicroSeconds(expected));
        }
    }

    Y_UNIT_TEST(DyNumberRounding) {
        const auto seconds = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts", 1);
        for (const auto& [text, expected] : TVector<std::pair<TString, ui64>>{
                 { "0.0000001", 1 }, { "0.000001", 1 }, { "90.0000001", 90000001 }, { "1700000000.5", 1700000000500000 } }) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                *seconds->ScalarToInstant(DyNumberScalar(text), NScheme::NTypeIds::DyNumber), TInstant::MicroSeconds(expected), text);
        }
        const auto nanos = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts", 1000000000);
        UNIT_ASSERT_VALUES_EQUAL(*nanos->ScalarToInstant(DyNumberScalar("1001"), NScheme::NTypeIds::DyNumber), TInstant::MicroSeconds(2));
    }

    Y_UNIT_TEST(InvalidDyNumber) {
        const auto tier = TTierInfo::MakeTtl(TDuration::Seconds(1), "ts", 1);
        for (const TStringBuf binary : { TStringBuf(), TStringBuf("\x02", 1), TStringBuf("\x03", 1), TStringBuf("\x01\x00", 2),
                 TStringBuf("\x02\x81\xfa", 3), TStringBuf("\x00\x81\x00", 3) }) {
            const auto scalar = std::make_shared<arrow::BinaryScalar>(arrow::Buffer::FromString(std::string(binary.data(), binary.size())));
            UNIT_ASSERT(!tier->ScalarToInstant(scalar, NScheme::NTypeIds::DyNumber));
        }
    }

    Y_UNIT_TEST(ExpirationBoundary) {
        TTiering tiering;
        UNIT_ASSERT(tiering.Add(TTierInfo::MakeTtl(TDuration::Seconds(10), "ts", 1)));
        const TInstant now = TInstant::Seconds(100);
        for (const auto text : { "-1", "0", "90" }) {
            const auto context = tiering.GetTierToMove(DyNumberScalar(text), now, false, NScheme::NTypeIds::DyNumber);
            UNIT_ASSERT_VALUES_EQUAL(context.GetCurrentTierName(), TTierInfo::GetTtlTierName());
        }
        const auto future = tiering.GetTierToMove(DyNumberScalar("90.0000001"), now, false, NScheme::NTypeIds::DyNumber);
        UNIT_ASSERT_VALUES_EQUAL(future.GetNextTierNameVerified(), TTierInfo::GetTtlTierName());
        UNIT_ASSERT_VALUES_EQUAL(future.GetNextTierWaitingVerified(), TDuration::MicroSeconds(1));
        for (const auto type : { NScheme::NTypeIds::Datetime64, NScheme::NTypeIds::Timestamp64 }) {
            const auto context = tiering.GetTierToMove(std::make_shared<arrow::Int64Scalar>(-1), now, false, type);
            UNIT_ASSERT_VALUES_EQUAL(context.GetCurrentTierName(), TTierInfo::GetTtlTierName());
        }
    }
}

}   // namespace
}   // namespace NKikimr::NOlap
