#include <arrow/api.h>
#include <arrow/array.h>

#include <library/cpp/testing/unittest/registar.h>

#include "arrow.h"
#include "arrow_impl.h"

extern "C" {
#include "utils/fmgrprotos.h"
}

namespace {

void checkResult(const char ** expected, std::shared_ptr<arrow::ArrayData> data) {

    NYql::NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    for (int i = 0; i < data->length; i++) {
        auto item = reader.GetItem(*data, i);
        if (!item) {
            UNIT_ASSERT(expected[i] == nullptr);
        } else {
            const char* addr = item.AsStringRef().Data() + sizeof(void*);
            UNIT_ASSERT(expected[i] != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DatumGetCString(DirectFunctionCall1(numeric_out, (Datum)addr))),
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
    builder.Append(1.1);
    builder.Append(31.37);
    builder.AppendNull();
    builder.Append(-1.337);
    builder.Append(0.0);

    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);

    auto result = PgConvertNumeric<double>(array);
    const auto& data = result->data();
    


    const char* expected[] = {
        "1.1", "31.37", nullptr, "-1.337", "0"
    };

    checkResult(expected, data);
}

Y_UNIT_TEST(PgConvertNumericInt) {
    TArenaMemoryContext arena;
 
    arrow::Int64Builder builder;
    builder.Append(11);
    builder.Append(3137);
    builder.AppendNull();
    builder.Append(-1337);
    builder.Append(0);

    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);

    auto result = PgConvertNumeric<i64>(array);
    const auto& data = result->data();

    const char* expected[] = {
        "11", "3137", nullptr, "-1337", "0"
    };

    checkResult(expected, data);
}

Y_UNIT_TEST(PgConvertDate32Date) {
    TArenaMemoryContext arena;

    arrow::Date32Builder builder;
    builder.Append(10227);
    builder.AppendNull();
    builder.Append(11323);
    builder.Append(10227);
    builder.Append(10958);
    builder.Append(11688);

    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);

    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NKikimr::NMiniKQL::TTypeEnvironment typeEnv(alloc);
    auto* targetType = NKikimr::NMiniKQL::TPgType::Create(DATEOID, typeEnv);

    auto converter = BuildPgColumnConverter(std::shared_ptr<arrow::DataType>(new arrow::Date32Type), targetType);
    auto result = converter(array);
    const auto& data = result->data();
    UNIT_ASSERT_VALUES_EQUAL(result->length(), 6);

    const char* expected[] = {
        "1998-01-01", nullptr, "2001-01-01", "1998-01-01", "2000-01-02", "2002-01-01"
    };

    NUdf::TFixedSizeBlockReader<ui64, true> reader;
    for (int i = 0; i < 6; i++) {
        if (result->IsNull(i)) {
            UNIT_ASSERT(expected[i] == nullptr);
        } else {
            auto item = reader.GetItem(*data, i).As<Datum>();
            UNIT_ASSERT(expected[i] != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DatumGetCString(DirectFunctionCall1(date_out, item))),
                expected[i]
            );
        }
    }
}

} // Y_UNIT_TEST_SUITE(TArrowUtilsTests)

} // namespace NYql