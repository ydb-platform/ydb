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
        "1.1", "31.37", nullptr, "-1.337", "0"
    };

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