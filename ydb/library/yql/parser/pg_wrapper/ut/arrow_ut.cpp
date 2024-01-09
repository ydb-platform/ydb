#include <arrow/api.h>
#include <arrow/array.h>

#include <library/cpp/testing/unittest/registar.h>

#include "arrow.h"
#include "arrow_impl.h"

extern "C" {
#include "utils/fmgrprotos.h"
}

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
    
    NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    for (int i = 0; i < 5; i++) {
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
    
    NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
    for (int i = 0; i < 5; i++) {
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

} // Y_UNIT_TEST_SUITE(TArrowUtilsTests)

} // namespace NYql

