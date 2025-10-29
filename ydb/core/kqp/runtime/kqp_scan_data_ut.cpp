#include "kqp_scan_data.h"

#include <yql/essentials/public/udf/udf_ut_helpers.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

struct TDataRow {
    TSmallVec<TKqpComputeContextBase::TColumn> Columns() {
        return {
            {0, TTypeInfo(NTypeIds::Bool), ""},
            {1, TTypeInfo(NTypeIds::Int8), ""},
            {2, TTypeInfo(NTypeIds::Int16), ""},
            {3, TTypeInfo(NTypeIds::Int32), ""},
            {4, TTypeInfo(NTypeIds::Int64), ""},
            {5, TTypeInfo(NTypeIds::Uint8), ""},
            {6, TTypeInfo(NTypeIds::Uint16), ""},
            {7, TTypeInfo(NTypeIds::Uint32), ""},
            {8, TTypeInfo(NTypeIds::Uint64), ""},
            {9, TTypeInfo(NTypeIds::Float), ""},
            {10, TTypeInfo(NTypeIds::Double), ""},
            {11, TTypeInfo(NTypeIds::String), ""},
            {12, TTypeInfo(NTypeIds::Utf8), ""},
            {13, TTypeInfo(NTypeIds::Json), ""},
            {14, TTypeInfo(NTypeIds::Yson), ""},
            {15, TTypeInfo(NTypeIds::Date), ""},
            {16, TTypeInfo(NTypeIds::Datetime), ""},
            {17, TTypeInfo(NTypeIds::Timestamp), ""},
            {18, TTypeInfo(NTypeIds::Interval), ""},
            {19, TTypeInfo(NTypeIds::Date32), ""},
            {20, TTypeInfo(NTypeIds::Datetime64), ""},
            {21, TTypeInfo(NTypeIds::Timestamp64), ""},
            {22, TTypeInfo(NTypeIds::Interval64), ""},
            {23, TTypeInfo(NScheme::TDecimalType(22, 9)), ""},
            {24, TTypeInfo(NScheme::TDecimalType(35, 10)), ""},
            {25, TTypeInfo(NPg::TypeDescFromPgTypeName("pgint2")), ""},
            {26, TTypeInfo(NPg::TypeDescFromPgTypeName("pgint4")), ""},
            {27, TTypeInfo(NPg::TypeDescFromPgTypeName("pgint8")), ""},
            {28, TTypeInfo(NPg::TypeDescFromPgTypeName("pgfloat4")), ""},
            {29, TTypeInfo(NPg::TypeDescFromPgTypeName("pgfloat8")), ""},
        };
    }

    bool Bool;
    i8 Int8;
    i16 Int16;
    i32 Int32;
    i64 Int64;
    ui8 UInt8;
    ui16 UInt16;
    ui32 UInt32;
    ui64 UInt64;
    float Float32;
    double Float64;
    TString String;
    TString Utf8;
    TString Json;
    TString Yson;
    i32 Date;
    i64 Datetime;
    i64 Timestamp;
    i64 Interval;
    i32 Date32;
    i64 Datetime64;
    i64 Timestamp64;
    i64 Interval64;
    NYql::NDecimal::TInt128 Decimal;
    NYql::NDecimal::TInt128 Decimal35;
    i16 PgInt2;
    i32 PgInt4;
    i64 PgInt8;
    float PgFloat4;
    double PgFloat8;

    static std::shared_ptr<arrow::Schema> MakeArrowSchema() {
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("bool", arrow::boolean()),
            arrow::field("i8", arrow::int8()),
            arrow::field("i16", arrow::int16()),
            arrow::field("i32", arrow::int32()),
            arrow::field("i64", arrow::int64()),
            arrow::field("ui8", arrow::uint8()),
            arrow::field("ui16", arrow::uint16()),
            arrow::field("ui32", arrow::uint32()),
            arrow::field("ui64", arrow::uint64()),
            arrow::field("f32", arrow::float32()),
            arrow::field("f64", arrow::float64()),
            arrow::field("string", arrow::utf8()),
            arrow::field("utf8", arrow::utf8()),
            arrow::field("json", arrow::binary()),
            arrow::field("yson", arrow::binary()),
            arrow::field("date", arrow::date32()),
            arrow::field("datetime", arrow::timestamp(arrow::TimeUnit::TimeUnit::SECOND)),
            arrow::field("ts", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO)),
            arrow::field("ival", arrow::duration(arrow::TimeUnit::TimeUnit::MICRO)),
            arrow::field("date32", arrow::date32()),
            arrow::field("datetime64", arrow::date64()),
            arrow::field("timestamp64", arrow::date64()),
            arrow::field("interval64", arrow::date64()),
            arrow::field("dec", arrow::decimal(22, 9)),
            arrow::field("dec35", arrow::decimal(35, 10)),
            arrow::field("pgint2", arrow::int16()),
            arrow::field("pgint4", arrow::int32()),
            arrow::field("pgint8", arrow::int64()),
            arrow::field("pgfloat4", arrow::float32()),
            arrow::field("pgfloat8", arrow::float64()),
        };

        return std::make_shared<arrow::Schema>(std::move(fields));
    }
};

std::shared_ptr<arrow::RecordBatch> VectorToBatch(const std::vector<struct TDataRow>& rows, std::shared_ptr<arrow::Schema>&& resultSchema) {
    TString err;
    std::unique_ptr<arrow::RecordBatchBuilder> batchBuilder = nullptr;
    std::shared_ptr<arrow::RecordBatch> batch;
    auto result = arrow::RecordBatchBuilder::Make(resultSchema, arrow::default_memory_pool(), &batchBuilder);
    UNIT_ASSERT(result.ok());

    for (const TDataRow& row : rows) {
        int colIndex = 0;
        for (auto colName : resultSchema->field_names()) {
            if (colName == "bool") {
                auto result = batchBuilder->GetFieldAs<arrow::BooleanBuilder>(colIndex++)->Append(row.Bool);
                UNIT_ASSERT(result.ok());
            } else if (colName == "i8") {
                auto result = batchBuilder->GetFieldAs<arrow::Int8Builder>(colIndex++)->Append(row.Int8);
                UNIT_ASSERT(result.ok());
            } else if (colName == "i16") {
                auto result = batchBuilder->GetFieldAs<arrow::Int16Builder>(colIndex++)->Append(row.Int16);
                UNIT_ASSERT(result.ok());
            } else if (colName == "i32") {
                auto result = batchBuilder->GetFieldAs<arrow::Int32Builder>(colIndex++)->Append(row.Int32);
                UNIT_ASSERT(result.ok());
            } else if (colName == "i64") {
                auto result = batchBuilder->GetFieldAs<arrow::Int64Builder>(colIndex++)->Append(row.Int64);
                UNIT_ASSERT(result.ok());
            } else if (colName == "ui8") {
                auto result = batchBuilder->GetFieldAs<arrow::UInt8Builder>(colIndex++)->Append(row.UInt8);
                UNIT_ASSERT(result.ok());
            } else if (colName == "ui16") {
                auto result = batchBuilder->GetFieldAs<arrow::UInt16Builder>(colIndex++)->Append(row.UInt16);
                UNIT_ASSERT(result.ok());
            } else if (colName == "ui32") {
                auto result = batchBuilder->GetFieldAs<arrow::UInt32Builder>(colIndex++)->Append(row.UInt32);
                UNIT_ASSERT(result.ok());
            } else if (colName == "ui64") {
                auto result = batchBuilder->GetFieldAs<arrow::UInt64Builder>(colIndex++)->Append(row.UInt64);
                UNIT_ASSERT(result.ok());
            } else if (colName == "f32") {
                auto result = batchBuilder->GetFieldAs<arrow::FloatBuilder>(colIndex++)->Append(row.Float32);
                UNIT_ASSERT(result.ok());
            } else if (colName == "f64") {
                auto result = batchBuilder->GetFieldAs<arrow::DoubleBuilder>(colIndex++)->Append(row.Float64);
                UNIT_ASSERT(result.ok());
            } else if (colName == "string") {
                auto result = batchBuilder->GetFieldAs<arrow::StringBuilder>(colIndex++)->Append(row.String.data(), row.String.size());
                UNIT_ASSERT(result.ok());
            } else if (colName == "utf8") {
                auto result = batchBuilder->GetFieldAs<arrow::StringBuilder>(colIndex++)->Append(row.Utf8.data(), row.Utf8.size());
                UNIT_ASSERT(result.ok());
            } else if (colName == "json") {
                auto result = batchBuilder->GetFieldAs<arrow::BinaryBuilder>(colIndex++)->Append(row.Json.data(), row.Json.size());
                UNIT_ASSERT(result.ok());
            } else if (colName == "yson") {
                auto result = batchBuilder->GetFieldAs<arrow::BinaryBuilder>(colIndex++)->Append(row.Yson.data(), row.Yson.size());
                UNIT_ASSERT(result.ok());
            } else if (colName == "date") {
                auto result = batchBuilder->GetFieldAs<arrow::Date32Builder>(colIndex++)->Append(row.Date);
                UNIT_ASSERT(result.ok());
            } else if (colName == "datetime") {
                auto result = batchBuilder->GetFieldAs<arrow::TimestampBuilder>(colIndex++)->Append(row.Datetime);
                UNIT_ASSERT(result.ok());
            } else if (colName == "ts") {
                auto result = batchBuilder->GetFieldAs<arrow::TimestampBuilder>(colIndex++)->Append(row.Timestamp);
                UNIT_ASSERT(result.ok());
            } else if (colName == "ival") {
                auto result = batchBuilder->GetFieldAs<arrow::DurationBuilder>(colIndex++)->Append(row.Interval);
                UNIT_ASSERT(result.ok());
            } else if (colName == "date32") {
                auto result = batchBuilder->GetFieldAs<arrow::Date32Builder>(colIndex++)->Append(row.Date32);
                UNIT_ASSERT(result.ok());
            } else if (colName == "datetime64") {
                auto result = batchBuilder->GetFieldAs<arrow::Date64Builder>(colIndex++)->Append(row.Datetime64);
                UNIT_ASSERT(result.ok());
            } else if (colName == "timestamp64") {
                auto result = batchBuilder->GetFieldAs<arrow::Date64Builder>(colIndex++)->Append(row.Timestamp64);
                UNIT_ASSERT(result.ok());
            } else if (colName == "interval64") {
                auto result = batchBuilder->GetFieldAs<arrow::Date64Builder>(colIndex++)->Append(row.Interval64);
                UNIT_ASSERT(result.ok());
            } else if (colName == "dec") {
                auto result = batchBuilder->GetFieldAs<arrow::Decimal128Builder>(colIndex++)->Append(reinterpret_cast<const char*>(&row.Decimal));
                UNIT_ASSERT(result.ok());
            } else if (colName == "dec35") {
                auto result = batchBuilder->GetFieldAs<arrow::Decimal128Builder>(colIndex++)->Append(reinterpret_cast<const char*>(&row.Decimal35));
                UNIT_ASSERT(result.ok());
            } else if (colName == "pgint2") {
                auto result = batchBuilder->GetFieldAs<arrow::Int16Builder>(colIndex++)->Append(row.PgInt2);
                UNIT_ASSERT(result.ok());
            } else if (colName == "pgint4") {
                auto result = batchBuilder->GetFieldAs<arrow::Int32Builder>(colIndex++)->Append(row.PgInt4);
                UNIT_ASSERT(result.ok());
            } else if (colName == "pgint8") {
                auto result = batchBuilder->GetFieldAs<arrow::Int64Builder>(colIndex++)->Append(row.PgInt8);
                UNIT_ASSERT(result.ok());
            } else if (colName == "pgfloat4") {
                auto result = batchBuilder->GetFieldAs<arrow::FloatBuilder>(colIndex++)->Append(row.PgFloat4);
                UNIT_ASSERT(result.ok());
            } else if (colName == "pgfloat8") {
                auto result = batchBuilder->GetFieldAs<arrow::DoubleBuilder>(colIndex++)->Append(row.PgFloat8);
                UNIT_ASSERT(result.ok());
            }
        }
    }

    auto resultFlush = batchBuilder->Flush(&batch);
    UNIT_ASSERT(resultFlush.ok());
    return batch;
}

TVector<TDataRow> TestRows() {
    TVector<TDataRow> rows = {
        {false, -1, -1, -1, -1, 1, 1, 1, 1, -1.0f, -1.0, "s1"                       , "u1"                      , "{j:1}", "{y:1}", 0, 0, 0, 0, -1, -1, -1, -1, 111, 1111, -21,  210, -2100,  21.3f,  21.6},
        {false,  2,  2,  2,  2, 2, 2, 2, 2,  2.0f,  2.0, "s2"                       , "u2"                      , "{j:2}", "{y:2}", 0, 0, 0, 0, -2, -2, -2, -2, 222, 2222,  22, -220,  2200, -22.3f,  22.6},
        {false, -3, -3, -3, -3, 3, 3, 3, 3, -3.0f, -3.0, "s3"                       , "u3"                      , "{j:3}", "{y:3}", 0, 0, 0, 0, -3, -3, -3, -3, 333, 3333,  23,  230, -2300,  23.3f, -23.6},
        {false, -4, -4, -4, -4, 4, 4, 4, 4,  4.0f,  4.0, "s4"                       , "u4"                      , "{j:4}", "{y:4}", 0, 0, 0, 0, -4, -4, -4, -4, 444, 4444, -24,  240,  2400, -24.3f,  24.6},
        {false, -5, -5, -5, -5, 5, 5, 5, 5,  5.0f,  5.0, "long5long5long5long5long5", "utflong5utflong5utflong5", "{j:5}", "{y:5}", 0, 0, 0, 0, -5, -5, -5, -5, 555, 5555,  25, -250,  2500,  25.3f, -25.6},
    };
    return rows;
}

}

Y_UNIT_TEST_SUITE(TKqpScanData) {

    Y_UNIT_TEST(UnboxedValueSize) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        namespace NTypeIds = NScheme::NTypeIds;
        struct TTestCase {
            NUdf::TUnboxedValue Value;
            NScheme::TTypeInfo Type;
            std::pair<ui64, ui64> ExpectedSizes;
        };
        TString pattern = "This string has 26 symbols";
        NUdf::TStringValue str(pattern.size());
        std::memcpy(str.Data(), pattern.data(), pattern.size());
        NUdf::TUnboxedValue containsLongString(NUdf::TUnboxedValuePod(std::move(str)));
        NYql::NDecimal::TInt128 decimalVal = 123456789012;
        NYql::NDecimal::TInt128 decimal35Val = 987654321012;
        TVector<TTestCase> cases = {
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Bool        , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Int32       , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Uint32      , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Int64       , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Uint64      , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Double      , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Float       , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::String      , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Utf8        , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Yson        , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Json        , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NScheme::TDecimalType(22, 9), {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NScheme::TDecimalType(35, 10), {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Date        , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Datetime    , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Timestamp   , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Interval    , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Date32      , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Datetime64  , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Timestamp64 , {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), NTypeIds::Interval64  , {16, 8 } },
            {NUdf::TUnboxedValuePod(true        ), NTypeIds::Bool        , {16, 8 } },
            {NUdf::TUnboxedValuePod((i8) 1      ), NTypeIds::Int8        , {16, 8 } },
            {NUdf::TUnboxedValuePod((i16) 2     ), NTypeIds::Int16       , {16, 8 } },
            {NUdf::TUnboxedValuePod((i32) 3     ), NTypeIds::Int32       , {16, 8 } },
            {NUdf::TUnboxedValuePod((i64) 4     ), NTypeIds::Int64       , {16, 8 } },
            {NUdf::TUnboxedValuePod((ui8) 5     ), NTypeIds::Uint8       , {16, 8 } },
            {NUdf::TUnboxedValuePod((ui16) 6    ), NTypeIds::Uint16      , {16, 8 } },
            {NUdf::TUnboxedValuePod((ui32) 7    ), NTypeIds::Uint32      , {16, 8 } },
            {NUdf::TUnboxedValuePod((ui64) 8    ), NTypeIds::Uint64      , {16, 8 } },
            {NUdf::TUnboxedValuePod((float) 1.2 ), NTypeIds::Float       , {16, 8 } },
            {NUdf::TUnboxedValuePod(123456789012), NTypeIds::Date        , {16, 8 } },
            {NUdf::TUnboxedValuePod((double) 3.4), NTypeIds::Double      , {16, 8 } },
            {NUdf::TUnboxedValuePod(123456789012), NTypeIds::Datetime    , {16, 8 } },
            {NUdf::TUnboxedValuePod(123456789012), NTypeIds::Timestamp   , {16, 8 } },
            {NUdf::TUnboxedValuePod(123456789012), NTypeIds::Interval    , {16, 8 } },
            {NUdf::TUnboxedValuePod(-12345678901), NTypeIds::Date32      , {16, 8 } },
            {NUdf::TUnboxedValuePod(-12345678901), NTypeIds::Datetime64  , {16, 8 } },
            {NUdf::TUnboxedValuePod(-12345678901), NTypeIds::Timestamp64 , {16, 8 } },
            {NUdf::TUnboxedValuePod(-12345678901), NTypeIds::Interval64  , {16, 8 } },
            {NUdf::TUnboxedValuePod(decimalVal  ), NScheme::TDecimalType(22, 9), {16, 16} },
            {NUdf::TUnboxedValuePod(decimal35Val  ), NScheme::TDecimalType(35, 10), {16, 16} },
            {NUdf::TUnboxedValuePod::Embedded("12charecters"), NTypeIds::String , {16, 12 } },
            {NUdf::TUnboxedValuePod::Embedded("foooo"), NTypeIds::String , {16, 8 } },
            {NUdf::TUnboxedValuePod::Embedded("FOOD!"), NTypeIds::Utf8   , {16, 8 } },
            {NUdf::TUnboxedValuePod::Embedded("{j:0}"), NTypeIds::Json   , {16, 8 } },
            {NUdf::TUnboxedValuePod::Embedded("{y:0}"), NTypeIds::Yson   , {16, 8 } },
            {containsLongString                       , NTypeIds::String, {16 + pattern.size(), pattern.size()}},
            {NUdf::TUnboxedValuePod(            ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgint2"  )), {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgint4"  )), {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgint8"  )), {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgfloat4")), {16, 8 } },
            {NUdf::TUnboxedValuePod(            ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgfloat8")), {16, 8 } },
            {NUdf::TUnboxedValuePod((i16) 2     ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgint2"  )), {16, 2 } },
            {NUdf::TUnboxedValuePod((i32) 3     ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgint4"  )), {16, 4 } },
            {NUdf::TUnboxedValuePod((i64) 4     ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgint8"  )), {16, 8 } },
            {NUdf::TUnboxedValuePod((float) 1.2 ), TTypeInfo(NPg::TypeDescFromPgTypeName("pgfloat4")), {16, 4 } },
            {NUdf::TUnboxedValuePod((double) 3.4), TTypeInfo(NPg::TypeDescFromPgTypeName("pgfloat8")), {16, 8 } },
        };

        for (auto& testCase: cases) {
            auto sizes = GetUnboxedValueSizeForTests(testCase.Value, NScheme::TTypeInfo(testCase.Type));
            UNIT_ASSERT_EQUAL_C(sizes, testCase.ExpectedSizes, "Wrong size for type " << NScheme::TypeName(testCase.Type));
        }
    }

    Y_UNIT_TEST(ArrowToUnboxedValueConverter) {
        TVector<TDataRow> rows = TestRows();
        std::shared_ptr<arrow::RecordBatch> batch = VectorToBatch(rows, rows.front().MakeArrowSchema());
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("");
        THolderFactory factory(alloc.Ref(), memInfo);

        TKqpScanComputeContext::TScanData scanData({}, TTableRange({}), rows.front().Columns(), {}, rows.front().Columns());

        scanData.AddData(batch, {}, factory, 0, 0, 0, false);

        std::vector<NUdf::TUnboxedValue> container;
        container.resize(TDataRow::MakeArrowSchema()->num_fields());
        std::vector<NUdf::TUnboxedValue*> containerPtr;
        for (auto&& i : container) {
            containerPtr.emplace_back(&i);
        }
        for (auto& row: rows) {
            scanData.FillDataValues(containerPtr.data());
            UNIT_ASSERT_EQUAL(container[0 ].Get<bool  >(), row.Bool   );
            UNIT_ASSERT_EQUAL(container[1 ].Get<i8    >(), row.Int8   );
            UNIT_ASSERT_EQUAL(container[2 ].Get<i16   >(), row.Int16  );
            UNIT_ASSERT_EQUAL(container[3 ].Get<i32   >(), row.Int32  );
            UNIT_ASSERT_EQUAL(container[4 ].Get<i64   >(), row.Int64  );
            UNIT_ASSERT_EQUAL(container[5 ].Get<ui8   >(), row.UInt8  );
            UNIT_ASSERT_EQUAL(container[6 ].Get<ui16  >(), row.UInt16 );
            UNIT_ASSERT_EQUAL(container[7 ].Get<ui32  >(), row.UInt32 );
            UNIT_ASSERT_EQUAL(container[8 ].Get<ui64  >(), row.UInt64 );
            UNIT_ASSERT_EQUAL(container[9 ].Get<float >(), row.Float32);
            UNIT_ASSERT_EQUAL(container[10].Get<double>(), row.Float64);
            auto tmpString = container[11];
            UNIT_ASSERT_EQUAL(TString(tmpString.AsStringRef()), row.String);
            auto tmpUtf8 = container[12];
            UNIT_ASSERT_EQUAL(TString(tmpUtf8.AsStringRef()), row.Utf8);
            auto tmpJson = container[13];
            UNIT_ASSERT_EQUAL(TString(tmpJson.AsStringRef()), row.Json);
            auto tmpYson = container[14];
            UNIT_ASSERT_EQUAL(TString(tmpYson.AsStringRef()), row.Yson);
            UNIT_ASSERT_EQUAL(container[15].Get<i32 >(), row.Date     );
            UNIT_ASSERT_EQUAL(container[16].Get<i64 >(), row.Datetime );
            UNIT_ASSERT_EQUAL(container[17].Get<i64 >(), row.Timestamp);
            UNIT_ASSERT_EQUAL(container[18].Get<i64 >(), row.Interval );
            UNIT_ASSERT_EQUAL(container[19].Get<i32 >(), row.Date32   );
            UNIT_ASSERT_EQUAL(container[20].Get<i64 >(), row.Datetime64);
            UNIT_ASSERT_EQUAL(container[21].Get<i64 >(), row.Timestamp64);
            UNIT_ASSERT_EQUAL(container[22].Get<i64 >(), row.Interval64 );
            UNIT_ASSERT_EQUAL(container[23].GetInt128(), row.Decimal    );
            UNIT_ASSERT_EQUAL(container[24].GetInt128(), row.Decimal35  );
            UNIT_ASSERT_EQUAL(container[25].Get<i16 >(), row.PgInt2     );
            UNIT_ASSERT_EQUAL(container[26].Get<i32 >(), row.PgInt4     );
            UNIT_ASSERT_EQUAL(container[27].Get<i64 >(), row.PgInt8     );
            UNIT_ASSERT_EQUAL(container[28].Get<float >(), row.PgFloat4 );
            UNIT_ASSERT_EQUAL(container[29].Get<double>(), row.PgFloat8 );
        }

        UNIT_ASSERT(scanData.IsEmpty());

        scanData.Clear();
    }

    Y_UNIT_TEST(DifferentNumberOfInputAndResultColumns) {
        TVector<TDataRow> rows = TestRows();
        std::vector<std::shared_ptr<arrow::Field>> fields = { arrow::field("i8", arrow::int8()) };
        std::shared_ptr<arrow::RecordBatch> batch = VectorToBatch(rows, std::make_shared<arrow::Schema>(fields));
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("");
        THolderFactory factory(alloc.Ref(), memInfo);

        TSmallVec<TKqpComputeContextBase::TColumn> resultCols;
        auto resCol = TKqpComputeContextBase::TColumn {
            .Type = TTypeInfo(NTypeIds::Int8)
        };
        resultCols.push_back(resCol);
        TKqpScanComputeContext::TScanData scanData({}, TTableRange({}), rows.front().Columns(), {}, resultCols);

        scanData.AddData(batch, {}, factory, 0, 0, 0, false);

        std::vector<NUdf::TUnboxedValue> container;
        container.resize(1);
        std::vector<NUdf::TUnboxedValue*> containerPtr;
        for (auto&& i : container) {
            containerPtr.emplace_back(&i);
        }

        for (auto& row: rows) {
            scanData.FillDataValues(containerPtr.data());
            UNIT_ASSERT_EQUAL(container[0].Get<i8>(), row.Int8);
        }

        UNIT_ASSERT(scanData.IsEmpty());

        scanData.Clear();
    }

    Y_UNIT_TEST(EmptyColumns) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("");
        THolderFactory factory(alloc.Ref(), memInfo);

        TKqpScanComputeContext::TScanData scanData({}, TTableRange({}), {}, {}, {});
        TVector<TOwnedCellVec> emptyBatch(1000);
        auto bytes = scanData.AddData(emptyBatch, {}, factory, 0, 0, 0, false);
        UNIT_ASSERT(bytes > 0);

        std::vector<NUdf::TUnboxedValue*> containerPtr;

        for (const auto& row: emptyBatch) {
            Y_UNUSED(row);
            UNIT_ASSERT(!scanData.IsEmpty());
            UNIT_ASSERT(scanData.FillDataValues(containerPtr.data()) == 0);
        }
        UNIT_ASSERT(scanData.IsEmpty());
    }

    Y_UNIT_TEST(EmptyColumnsAndNonEmptyArrowBatch) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("");
        THolderFactory factory(alloc.Ref(), memInfo);
        TKqpScanComputeContext::TScanData scanData({}, TTableRange({}), {}, {}, {});

        TVector<TDataRow> rows = TestRows();
        std::shared_ptr<arrow::RecordBatch> anotherEmptyBatch = VectorToBatch(rows, rows.front().MakeArrowSchema());

        auto bytes = scanData.AddData(anotherEmptyBatch, {}, factory, 0, 0, 0, false);
        UNIT_ASSERT(bytes > 0);
        std::vector<NUdf::TUnboxedValue*> containerPtr;
        for (const auto& row: rows) {
            Y_UNUSED(row);
            UNIT_ASSERT(!scanData.IsEmpty());
            UNIT_ASSERT(scanData.FillDataValues(containerPtr.data()) == 0);
        }
        UNIT_ASSERT(scanData.IsEmpty());
    }

    Y_UNIT_TEST(FailOnUnsupportedPgType) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        TMemoryUsageInfo memInfo("");
        THolderFactory factory(alloc.Ref(), memInfo);

        TSmallVec<TKqpComputeContextBase::TColumn> cols{{
            .Type = TTypeInfo(NPg::TypeDescFromPgTypeName("pgtext"))
        }};
        TKqpScanComputeContext::TScanData scanData({}, TTableRange({}), cols, {}, cols);

        std::unique_ptr<arrow::RecordBatchBuilder> batchBuilder = nullptr;
        auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{arrow::field("pgtext", arrow::utf8())});
        auto batchBuilderResult = arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), &batchBuilder);
        UNIT_ASSERT(batchBuilderResult.ok());

        TString textData{"some data"};
        auto appendResult = batchBuilder->GetFieldAs<arrow::StringBuilder>(0)->Append(textData.data(), textData.size());
        UNIT_ASSERT(appendResult.ok());

        std::shared_ptr<arrow::RecordBatch> batch;
        auto flushResult = batchBuilder->Flush(&batch);
        UNIT_ASSERT(flushResult.ok());

        UNIT_ASSERT_EXCEPTION(scanData.AddData(batch, {}, factory, 0, 0, 0, false), NYql::TYqlPanic);
    }
}

} // namespace NKikimr::NMiniKQL
