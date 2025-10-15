#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/permutations.h>

#include <yql/essentials/types/binary_json/write.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <util/random/shuffle.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

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

    static std::shared_ptr<arrow20::Schema> MakeArrowSchema() {
        std::vector<std::shared_ptr<arrow20::Field>> fields = {
            arrow20::field("bool", arrow20::boolean()),
            arrow20::field("i8", arrow20::int8()),
            arrow20::field("i16", arrow20::int16()),
            arrow20::field("i32", arrow20::int32()),
            arrow20::field("i64", arrow20::int64()),
            arrow20::field("ui8", arrow20::uint8()),
            arrow20::field("ui16", arrow20::uint16()),
            arrow20::field("ui32", arrow20::uint32()),
            arrow20::field("ui64", arrow20::uint64()),
            arrow20::field("f32", arrow20::float32()),
            arrow20::field("f64", arrow20::float64()),
            arrow20::field("string", arrow20::binary()),
            arrow20::field("utf8", arrow20::utf8()),
            arrow20::field("json", arrow20::utf8()),
            arrow20::field("yson", arrow20::binary()),
            arrow20::field("date", arrow20::uint16()),
            arrow20::field("datetime", arrow20::uint32()),
            arrow20::field("ts", arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO)),
            arrow20::field("ival", arrow20::duration(arrow20::TimeUnit::TimeUnit::MICRO)),
            arrow20::field("json_doc", arrow20::binary()),
            //arrow20::field("dec", arrow20::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE)),
        };

        return std::make_shared<arrow20::Schema>(std::move(fields));
    }
};


std::shared_ptr<arrow20::Array> GetColumn(const arrow20::Table& table, int i, int chunk = 0) {
    return table.column(i)->chunk(chunk);
}

template <typename T>
std::vector<TDataRow> ToVector(const std::shared_ptr<T>& table) {
    std::vector<TDataRow> rows;

    auto arbool = std::static_pointer_cast<arrow20::BooleanArray>(GetColumn(*table, 0));
    auto ari8 = std::static_pointer_cast<arrow20::Int8Array>(GetColumn(*table, 1));
    auto ari16 = std::static_pointer_cast<arrow20::Int16Array>(GetColumn(*table, 2));
    auto ari32 = std::static_pointer_cast<arrow20::Int32Array>(GetColumn(*table, 3));
    auto ari64 = std::static_pointer_cast<arrow20::Int64Array>(GetColumn(*table, 4));
    auto aru8 = std::static_pointer_cast<arrow20::UInt8Array>(GetColumn(*table, 5));
    auto aru16 = std::static_pointer_cast<arrow20::UInt16Array>(GetColumn(*table, 6));
    auto aru32 = std::static_pointer_cast<arrow20::UInt32Array>(GetColumn(*table, 7));
    auto aru64 = std::static_pointer_cast<arrow20::UInt64Array>(GetColumn(*table, 8));
    auto arf32 = std::static_pointer_cast<arrow20::FloatArray>(GetColumn(*table, 9));
    auto arf64 = std::static_pointer_cast<arrow20::DoubleArray>(GetColumn(*table, 10));

    auto arstr = std::static_pointer_cast<arrow20::BinaryArray>(GetColumn(*table, 11));
    auto arutf = std::static_pointer_cast<arrow20::StringArray>(GetColumn(*table, 12));
    auto arj = std::static_pointer_cast<arrow20::BinaryArray>(GetColumn(*table, 13));
    auto ary = std::static_pointer_cast<arrow20::BinaryArray>(GetColumn(*table, 14));

    auto ard = std::static_pointer_cast<arrow20::UInt16Array>(GetColumn(*table, 15));
    auto ardt = std::static_pointer_cast<arrow20::UInt32Array>(GetColumn(*table, 16));
    auto arts = std::static_pointer_cast<arrow20::TimestampArray>(GetColumn(*table, 17));
    auto arival = std::static_pointer_cast<arrow20::DurationArray>(GetColumn(*table, 18));

    auto arjd = std::static_pointer_cast<arrow20::BinaryArray>(GetColumn(*table, 19));
    //auto ardec = std::static_pointer_cast<arrow20::Decimal128Array>(GetColumn(*table, 19));

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
        : Bts(arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), arrow20::default_memory_pool())
        , Bival(arrow20::duration(arrow20::TimeUnit::TimeUnit::MICRO), arrow20::default_memory_pool())
        //, Bdec(arrow20::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE), arrow20::default_memory_pool())
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

    std::shared_ptr<arrow20::Table> Finish() {
        std::shared_ptr<arrow20::BooleanArray> arbool;
        std::shared_ptr<arrow20::Int8Array> ari8;
        std::shared_ptr<arrow20::Int16Array> ari16;
        std::shared_ptr<arrow20::Int32Array> ari32;
        std::shared_ptr<arrow20::Int64Array> ari64;
        std::shared_ptr<arrow20::UInt8Array> aru8;
        std::shared_ptr<arrow20::UInt16Array> aru16;
        std::shared_ptr<arrow20::UInt32Array> aru32;
        std::shared_ptr<arrow20::UInt64Array> aru64;
        std::shared_ptr<arrow20::FloatArray> arf32;
        std::shared_ptr<arrow20::DoubleArray> arf64;

        std::shared_ptr<arrow20::BinaryArray> arstr;
        std::shared_ptr<arrow20::StringArray> arutf;
        std::shared_ptr<arrow20::BinaryArray> arj;
        std::shared_ptr<arrow20::BinaryArray> ary;

        std::shared_ptr<arrow20::UInt16Array> ard;
        std::shared_ptr<arrow20::UInt32Array> ardt;
        std::shared_ptr<arrow20::TimestampArray> arts;
        std::shared_ptr<arrow20::DurationArray> arival;

        std::shared_ptr<arrow20::BinaryArray> arjd;
        //std::shared_ptr<arrow20::Decimal128Array> ardec;

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

        std::shared_ptr<arrow20::Schema> schema = TDataRow::MakeArrowSchema();
        return arrow20::Table::Make(schema, {
            arbool,
            ari8, ari16, ari32, ari64,
            aru8, aru16, aru32, aru64,
            arf32, arf64,
            arstr, arutf, arj, ary,
            ard, ardt, arts, arival, arjd
            //ardec
        });
    }

    static std::shared_ptr<arrow20::Table> Build(const std::vector<struct TDataRow>& rows) {
        TDataRowTableBuilder builder;
        for (const TDataRow& row : rows) {
            builder.AddRow(row);
        }
        return builder.Finish();
    }

private:
    arrow20::BooleanBuilder Bbool;
    arrow20::Int8Builder Bi8;
    arrow20::Int16Builder Bi16;
    arrow20::Int32Builder Bi32;
    arrow20::Int64Builder Bi64;
    arrow20::UInt8Builder Bu8;
    arrow20::UInt16Builder Bu16;
    arrow20::UInt32Builder Bu32;
    arrow20::UInt64Builder Bu64;
    arrow20::FloatBuilder Bf32;
    arrow20::DoubleBuilder Bf64;
    arrow20::BinaryBuilder Bstr;
    arrow20::StringBuilder Butf;
    arrow20::BinaryBuilder Bj;
    arrow20::BinaryBuilder By;
    arrow20::UInt16Builder Bd;
    arrow20::UInt32Builder Bdt;
    arrow20::TimestampBuilder Bts;
    arrow20::DurationBuilder Bival;
    arrow20::BinaryBuilder Bjd;
    //arrow20::Decimal128Builder Bdec;
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

        std::shared_ptr<arrow20::Table> table = TDataRowTableBuilder::Build(rows);

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
