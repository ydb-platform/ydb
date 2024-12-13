#include "../pg_compat.h"

#include <arrow/api.h>
#include <arrow/array.h>

#include <library/cpp/testing/unittest/registar.h>

#include "arrow.h"
#include "arrow_impl.h"

extern "C" {
#include "utils/fmgrprotos.h"
}

namespace {

template <bool IsFixedSizeReader>
void checkResult(const char ** expected, auto result, NYql::NUdf::IBlockReader* reader, auto out_fun) {
    const auto& data = result->data();

    for (int i = 0; i < data->length; i++) {
        if (result->IsNull(i)) {
            UNIT_ASSERT(expected[i] == nullptr);
        } else {
            UNIT_ASSERT(expected[i] != nullptr);

            Datum item;
            if constexpr (IsFixedSizeReader) {
                item = reader->GetItem(*data, i).template As<Datum>();
            } else {
                item = Datum(reader->GetItem(*data, i).AsStringRef().Data() + sizeof(void*));
            }
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DatumGetCString(DirectFunctionCall1(out_fun, item))),
                expected[i]
            );
        }
    }
}

} // namespace {

namespace NYql {

Y_UNIT_TEST_SUITE(TArrowUtilsTests) {

Y_UNIT_TEST(TestPgFloatToNumeric) {
    TArenaMemoryContext arena;
    auto n = PgFloatToNumeric(711.56, 1000000000000LL, 12);
    auto value = TString(DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(n))));
    UNIT_ASSERT_VALUES_EQUAL(value, "711.56");

    n = PgFloatToNumeric(-711.56, 1000000000000LL, 12);
    value = TString(DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(n))));
    UNIT_ASSERT_VALUES_EQUAL(value, "-711.56");

    n = PgFloatToNumeric(711.56f, 100000LL, 5);
    value = TString(DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(n))));
    UNIT_ASSERT_VALUES_EQUAL(value, "711.56");

    n = PgFloatToNumeric(-711.56f, 100000LL, 5);
    value = TString(DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(n))));
    UNIT_ASSERT_VALUES_EQUAL(value, "-711.56");
}


Y_UNIT_TEST(PgConvertNumericDouble) {
    TArenaMemoryContext arena;
 
    arrow::DoubleBuilder builder;
    ARROW_OK(builder.Append(1.1));
    ARROW_OK(builder.Append(31.37));
    ARROW_OK(builder.AppendNull());
    ARROW_OK(builder.Append(-1.337));
    ARROW_OK(builder.Append(0.0));

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgConvertNumeric<double>(array);
    
    const char* expected[] = {
        "1.1", "31.37", nullptr, "-1.337", "0", "1.234111"
    };

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128Scale1) {
    TArenaMemoryContext arena;

    int32_t precision = 6;
    int32_t scale = 1;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "12345.0", "-12345.0", nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromString("12345.0").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-12345.0").ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128ScaleNegative) {
    TArenaMemoryContext arena;

    int32_t precision = 8;
    int32_t scale = -3;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "12345678000", "-12345678000", nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromString("12345678").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-12345678").ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128Scale2) {
    TArenaMemoryContext arena;

    int32_t precision = 5;
    int32_t scale = 2;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "123.45", "-123.45", nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromString("123.45").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-123.45").ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128Scale3) {
    TArenaMemoryContext arena;

    int32_t precision = 3;
    int32_t scale = 3;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "0.123", "-0.123", nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromString("0.123").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-0.123").ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128Scale4) {
    TArenaMemoryContext arena;

    int32_t precision = 7;
    int32_t scale = 4;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "123.4567", "-123.4567", nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromString("123.4567").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-123.4567").ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128Scale5) {
    TArenaMemoryContext arena;

    int32_t precision = 7;
    int32_t scale = 5;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "12.34567", "-12.34567", nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromReal(12.34567, precision, scale).ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromReal(-12.34567, precision, scale).ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128BigScale3) {
    TArenaMemoryContext arena;

    int32_t precision = 20;
    int32_t scale = 3;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "36893488147419103.245", "-36893488147419103.245",
        "46116860184273879.041", "-46116860184273879.041",
        nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromString("36893488147419103.245").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-36893488147419103.245").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("46116860184273879.041").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-46116860184273879.041").ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericDecimal128BigScale1) {
    TArenaMemoryContext arena;

    int32_t precision = 26;
    int32_t scale = 1;
    std::shared_ptr<arrow::DataType> type(new arrow::Decimal128Type(precision, scale));
    arrow::Decimal128Builder builder(type);

    const char* expected[] = {
        "3868562622766813359059763.2", "-3868562622766813359059763.2", nullptr
    };

    ARROW_OK(builder.Append(arrow::Decimal128::FromString("3868562622766813359059763.2").ValueOrDie()));
    ARROW_OK(builder.Append(arrow::Decimal128::FromString("-3868562622766813359059763.2").ValueOrDie()));
    ARROW_OK(builder.AppendNull());

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgDecimal128ConvertNumeric(array, precision, scale);

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertNumericInt) {
    TArenaMemoryContext arena;
 
    arrow::Int64Builder builder;
    ARROW_OK(builder.Append(11));
    ARROW_OK(builder.Append(3137));
    ARROW_OK(builder.AppendNull());
    ARROW_OK(builder.Append(-1337));
    ARROW_OK(builder.Append(0));

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    auto result = PgConvertNumeric<i64>(array);
    const auto& data = result->data();

    const char* expected[] = {
        "11", "3137", nullptr, "-1337", "0"
    };

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    checkResult<false>(expected, result, &reader, numeric_out);
}

Y_UNIT_TEST(PgConvertDate32Date) {
    TArenaMemoryContext arena;

    arrow::Date32Builder builder;
    ARROW_OK(builder.Append(10227));
    ARROW_OK(builder.AppendNull());
    ARROW_OK(builder.Append(11323));
    ARROW_OK(builder.Append(10227));
    ARROW_OK(builder.Append(10958));
    ARROW_OK(builder.Append(11688));

    std::shared_ptr<arrow::Array> array;
    ARROW_OK(builder.Finish(&array));

    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TTypeEnvironment typeEnv(alloc);
    auto* targetType = NKikimr::NMiniKQL::TPgType::Create(DATEOID, typeEnv);

    auto converter = BuildPgColumnConverter(std::shared_ptr<arrow::DataType>(new arrow::Date32Type), targetType);
    auto result = converter(array);
    UNIT_ASSERT_VALUES_EQUAL(result->length(), 6);

    const char* expected[] = {
        "1998-01-01", nullptr, "2001-01-01", "1998-01-01", "2000-01-02", "2002-01-01"
    };

    NUdf::TFixedSizeBlockReader<ui64, true> reader;
    checkResult<true>(expected, result, &reader, date_out);
}

} // Y_UNIT_TEST_SUITE(TArrowUtilsTests)

} // namespace NYql
