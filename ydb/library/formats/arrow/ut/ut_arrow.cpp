#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/permutations.h>

#include <ydb/library/binary_json/write.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <util/random/shuffle.h>

namespace NKikimr {
namespace {

struct TDataRow {
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
    std::string String;
    std::string Utf8;
    std::string Json;
    std::string Yson;
    ui16 Date;
    ui32 Datetime;
    i64 Timestamp;
    i64 Interval;
    std::string JsonDocument;
    //ui64 Decimal[2];

    bool operator == (const TDataRow& r) const {
        return (Bool == r.Bool) &&
            (Int8 == r.Int8) &&
            (Int16 == r.Int16) &&
            (Int32 == r.Int32) &&
            (Int64 == r.Int64) &&
            (UInt8 == r.UInt8) &&
            (UInt16 == r.UInt16) &&
            (UInt32 == r.UInt32) &&
            (UInt64 == r.UInt64) &&
            (Float32 == r.Float32) &&
            (Float64 == r.Float64) &&
            (String == r.String) &&
            (Utf8 == r.Utf8) &&
            (Json == r.Json) &&
            (Yson == r.Yson) &&
            (Date == r.Date) &&
            (Datetime == r.Datetime) &&
            (Timestamp == r.Timestamp) &&
            (Interval == r.Interval) &&
            (JsonDocument == r.JsonDocument);
            //(Decimal[0] == r.Decimal[0] && Decimal[1] == r.Decimal[1]);
    }

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
            arrow::field("string", arrow::binary()),
            arrow::field("utf8", arrow::utf8()),
            arrow::field("json", arrow::utf8()),
            arrow::field("yson", arrow::binary()),
            arrow::field("date", arrow::uint16()),
            arrow::field("datetime", arrow::uint32()),
            arrow::field("ts", arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO)),
            arrow::field("ival", arrow::duration(arrow::TimeUnit::TimeUnit::MICRO)),
            arrow::field("json_doc", arrow::binary()),
            //arrow::field("dec", arrow::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE)),
        };

        return std::make_shared<arrow::Schema>(std::move(fields));
    }
};


std::shared_ptr<arrow::Array> GetColumn(const arrow::Table& table, int i, int chunk = 0) {
    return table.column(i)->chunk(chunk);
}

template <typename T>
std::vector<TDataRow> ToVector(const std::shared_ptr<T>& table) {
    std::vector<TDataRow> rows;

    auto arbool = std::static_pointer_cast<arrow::BooleanArray>(GetColumn(*table, 0));
    auto ari8 = std::static_pointer_cast<arrow::Int8Array>(GetColumn(*table, 1));
    auto ari16 = std::static_pointer_cast<arrow::Int16Array>(GetColumn(*table, 2));
    auto ari32 = std::static_pointer_cast<arrow::Int32Array>(GetColumn(*table, 3));
    auto ari64 = std::static_pointer_cast<arrow::Int64Array>(GetColumn(*table, 4));
    auto aru8 = std::static_pointer_cast<arrow::UInt8Array>(GetColumn(*table, 5));
    auto aru16 = std::static_pointer_cast<arrow::UInt16Array>(GetColumn(*table, 6));
    auto aru32 = std::static_pointer_cast<arrow::UInt32Array>(GetColumn(*table, 7));
    auto aru64 = std::static_pointer_cast<arrow::UInt64Array>(GetColumn(*table, 8));
    auto arf32 = std::static_pointer_cast<arrow::FloatArray>(GetColumn(*table, 9));
    auto arf64 = std::static_pointer_cast<arrow::DoubleArray>(GetColumn(*table, 10));

    auto arstr = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 11));
    auto arutf = std::static_pointer_cast<arrow::StringArray>(GetColumn(*table, 12));
    auto arj = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 13));
    auto ary = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 14));

    auto ard = std::static_pointer_cast<arrow::UInt16Array>(GetColumn(*table, 15));
    auto ardt = std::static_pointer_cast<arrow::UInt32Array>(GetColumn(*table, 16));
    auto arts = std::static_pointer_cast<arrow::TimestampArray>(GetColumn(*table, 17));
    auto arival = std::static_pointer_cast<arrow::DurationArray>(GetColumn(*table, 18));

    auto arjd = std::static_pointer_cast<arrow::BinaryArray>(GetColumn(*table, 19));
    //auto ardec = std::static_pointer_cast<arrow::Decimal128Array>(GetColumn(*table, 19));

    for (int64_t i = 0; i < table->num_rows(); ++i) {
        //ui64 dec[2];
        //memcpy(dec, ardec->Value(i), 16);
        TDataRow r{ arbool->Value(i),
            ari8->Value(i), ari16->Value(i), ari32->Value(i), ari64->Value(i),
            aru8->Value(i), aru16->Value(i), aru32->Value(i), aru64->Value(i),
            arf32->Value(i), arf64->Value(i),
            arstr->GetString(i), arutf->GetString(i), arj->GetString(i), ary->GetString(i),
            ard->Value(i), ardt->Value(i), arts->Value(i), arival->Value(i), arjd->GetString(i)
            //{dec[0], dec[1]}
        };
        rows.emplace_back(std::move(r));
    }

    return rows;
}

class TDataRowTableBuilder
{
public:
    TDataRowTableBuilder()
        : Bts(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool())
        , Bival(arrow::duration(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool())
        //, Bdec(arrow::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE), arrow::default_memory_pool())
    {}

    void AddRow(const TDataRow& row) {
        UNIT_ASSERT(Bbool.Append(row.Bool).ok());
        UNIT_ASSERT(Bi8.Append(row.Int8).ok());
        UNIT_ASSERT(Bi16.Append(row.Int16).ok());
        UNIT_ASSERT(Bi32.Append(row.Int32).ok());
        UNIT_ASSERT(Bi64.Append(row.Int64).ok());
        UNIT_ASSERT(Bu8.Append(row.UInt8).ok());
        UNIT_ASSERT(Bu16.Append(row.UInt16).ok());
        UNIT_ASSERT(Bu32.Append(row.UInt32).ok());
        UNIT_ASSERT(Bu64.Append(row.UInt64).ok());
        UNIT_ASSERT(Bf32.Append(row.Float32).ok());
        UNIT_ASSERT(Bf64.Append(row.Float64).ok());

        UNIT_ASSERT(Bstr.Append(row.String).ok());
        UNIT_ASSERT(Butf.Append(row.Utf8).ok());
        UNIT_ASSERT(Bj.Append(row.Json).ok());
        UNIT_ASSERT(By.Append(row.Yson).ok());

        UNIT_ASSERT(Bd.Append(row.Date).ok());
        UNIT_ASSERT(Bdt.Append(row.Datetime).ok());
        UNIT_ASSERT(Bts.Append(row.Timestamp).ok());
        UNIT_ASSERT(Bival.Append(row.Interval).ok());

        UNIT_ASSERT(Bjd.Append(row.JsonDocument).ok());
        //UNIT_ASSERT(Bdec.Append((const char *)&row.Decimal).ok());
    }

    std::shared_ptr<arrow::Table> Finish() {
        std::shared_ptr<arrow::BooleanArray> arbool;
        std::shared_ptr<arrow::Int8Array> ari8;
        std::shared_ptr<arrow::Int16Array> ari16;
        std::shared_ptr<arrow::Int32Array> ari32;
        std::shared_ptr<arrow::Int64Array> ari64;
        std::shared_ptr<arrow::UInt8Array> aru8;
        std::shared_ptr<arrow::UInt16Array> aru16;
        std::shared_ptr<arrow::UInt32Array> aru32;
        std::shared_ptr<arrow::UInt64Array> aru64;
        std::shared_ptr<arrow::FloatArray> arf32;
        std::shared_ptr<arrow::DoubleArray> arf64;

        std::shared_ptr<arrow::BinaryArray> arstr;
        std::shared_ptr<arrow::StringArray> arutf;
        std::shared_ptr<arrow::BinaryArray> arj;
        std::shared_ptr<arrow::BinaryArray> ary;

        std::shared_ptr<arrow::UInt16Array> ard;
        std::shared_ptr<arrow::UInt32Array> ardt;
        std::shared_ptr<arrow::TimestampArray> arts;
        std::shared_ptr<arrow::DurationArray> arival;

        std::shared_ptr<arrow::BinaryArray> arjd;
        //std::shared_ptr<arrow::Decimal128Array> ardec;

        UNIT_ASSERT(Bbool.Finish(&arbool).ok());
        UNIT_ASSERT(Bi8.Finish(&ari8).ok());
        UNIT_ASSERT(Bi16.Finish(&ari16).ok());
        UNIT_ASSERT(Bi32.Finish(&ari32).ok());
        UNIT_ASSERT(Bi64.Finish(&ari64).ok());
        UNIT_ASSERT(Bu8.Finish(&aru8).ok());
        UNIT_ASSERT(Bu16.Finish(&aru16).ok());
        UNIT_ASSERT(Bu32.Finish(&aru32).ok());
        UNIT_ASSERT(Bu64.Finish(&aru64).ok());
        UNIT_ASSERT(Bf32.Finish(&arf32).ok());
        UNIT_ASSERT(Bf64.Finish(&arf64).ok());

        UNIT_ASSERT(Bstr.Finish(&arstr).ok());
        UNIT_ASSERT(Butf.Finish(&arutf).ok());
        UNIT_ASSERT(Bj.Finish(&arj).ok());
        UNIT_ASSERT(By.Finish(&ary).ok());

        UNIT_ASSERT(Bd.Finish(&ard).ok());
        UNIT_ASSERT(Bdt.Finish(&ardt).ok());
        UNIT_ASSERT(Bts.Finish(&arts).ok());
        UNIT_ASSERT(Bival.Finish(&arival).ok());

        UNIT_ASSERT(Bjd.Finish(&arjd).ok());
        //UNIT_ASSERT(Bdec.Finish(&ardec).ok());

        std::shared_ptr<arrow::Schema> schema = TDataRow::MakeArrowSchema();
        return arrow::Table::Make(schema, {
            arbool,
            ari8, ari16, ari32, ari64,
            aru8, aru16, aru32, aru64,
            arf32, arf64,
            arstr, arutf, arj, ary,
            ard, ardt, arts, arival, arjd
            //ardec
        });
    }

    static std::shared_ptr<arrow::Table> Build(const std::vector<struct TDataRow>& rows) {
        TDataRowTableBuilder builder;
        for (const TDataRow& row : rows) {
            builder.AddRow(row);
        }
        return builder.Finish();
    }

private:
    arrow::BooleanBuilder Bbool;
    arrow::Int8Builder Bi8;
    arrow::Int16Builder Bi16;
    arrow::Int32Builder Bi32;
    arrow::Int64Builder Bi64;
    arrow::UInt8Builder Bu8;
    arrow::UInt16Builder Bu16;
    arrow::UInt32Builder Bu32;
    arrow::UInt64Builder Bu64;
    arrow::FloatBuilder Bf32;
    arrow::DoubleBuilder Bf64;
    arrow::BinaryBuilder Bstr;
    arrow::StringBuilder Butf;
    arrow::BinaryBuilder Bj;
    arrow::BinaryBuilder By;
    arrow::UInt16Builder Bd;
    arrow::UInt32Builder Bdt;
    arrow::TimestampBuilder Bts;
    arrow::DurationBuilder Bival;
    arrow::BinaryBuilder Bjd;
    //arrow::Decimal128Builder Bdec;
};

std::vector<TDataRow> TestRows() {
    std::vector<TDataRow> rows = {
        {false, -1, -1, -1, -1, 1, 1, 1, 1, -1.0f, -1.0, "s1", "u1", "{\"j\":1}", "{y:1}", 0, 0, 0, 0, "{\"jd\":1}" },
        {false, 2, 2, 2, 2, 2, 2, 2, 2, 2.0f, 2.0, "s2", "u2", "{\"j\":2}", "{y:2}", 0, 0, 0, 0, "{\"jd\":1}" },
        {false, -3, -3, -3, -3, 3, 3, 3, 3, -3.0f, -3.0, "s3", "u3", "{\"j\":3}", "{y:3}", 0, 0, 0, 0, "{\"jd\":1}" },
        {false, -4, -4, -4, -4, 4, 4, 4, 4, 4.0f, 4.0, "s4", "u4", "{\"j\":4}", "{y:4}", 0, 0, 0, 0, "{\"jd\":1}" },
    };
    return rows;
}

}

Y_UNIT_TEST_SUITE(ArrowTest) {
    Y_UNIT_TEST(Basic) {
        std::vector<TDataRow> rows = TestRows();

        std::shared_ptr<arrow::Table> table = TDataRowTableBuilder::Build(rows);

        auto expectedSchema = TDataRow::MakeArrowSchema();
        UNIT_ASSERT_EQUAL(expectedSchema->Equals(*table->schema()), true);

        std::vector<TDataRow> readRows = ToVector(table);

        UNIT_ASSERT_EQUAL(rows.size(), readRows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_EQUAL(rows[i], readRows[i]);
        }
    }
}

}
