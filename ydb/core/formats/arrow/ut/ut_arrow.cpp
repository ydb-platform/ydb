#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>

#include <ydb/library/binary_json/write.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <util/random/shuffle.h>

namespace NKikimr {
namespace {

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

struct TDataRow {
    static const constexpr TTypeInfo Types[20] = {
        TTypeInfo(NTypeIds::Bool),
        TTypeInfo(NTypeIds::Int8),
        TTypeInfo(NTypeIds::Int16),
        TTypeInfo(NTypeIds::Int32),
        TTypeInfo(NTypeIds::Int64),
        TTypeInfo(NTypeIds::Uint8),
        TTypeInfo(NTypeIds::Uint16),
        TTypeInfo(NTypeIds::Uint32),
        TTypeInfo(NTypeIds::Uint64),
        TTypeInfo(NTypeIds::Float),
        TTypeInfo(NTypeIds::Double),
        TTypeInfo(NTypeIds::String),
        TTypeInfo(NTypeIds::Utf8),
        TTypeInfo(NTypeIds::Json),
        TTypeInfo(NTypeIds::Yson),
        TTypeInfo(NTypeIds::Date),
        TTypeInfo(NTypeIds::Datetime),
        TTypeInfo(NTypeIds::Timestamp),
        TTypeInfo(NTypeIds::Interval),
        TTypeInfo(NTypeIds::JsonDocument),
        // TODO: DyNumber, Decimal
    };

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

    static std::vector<std::pair<TString, TTypeInfo>> MakeYdbSchema() {
        std::vector<std::pair<TString, TTypeInfo>> columns = {
            {"bool", TTypeInfo(NTypeIds::Bool) },
            {"i8", TTypeInfo(NTypeIds::Int8) },
            {"i16", TTypeInfo(NTypeIds::Int16) },
            {"i32", TTypeInfo(NTypeIds::Int32) },
            {"i64", TTypeInfo(NTypeIds::Int64) },
            {"ui8", TTypeInfo(NTypeIds::Uint8) },
            {"ui16", TTypeInfo(NTypeIds::Uint16) },
            {"ui32", TTypeInfo(NTypeIds::Uint32) },
            {"ui64", TTypeInfo(NTypeIds::Uint64) },
            {"f32", TTypeInfo(NTypeIds::Float) },
            {"f64", TTypeInfo(NTypeIds::Double) },
            {"string", TTypeInfo(NTypeIds::String) },
            {"utf8", TTypeInfo(NTypeIds::Utf8) },
            {"json", TTypeInfo(NTypeIds::Json) },
            {"yson", TTypeInfo(NTypeIds::Yson) },
            {"date", TTypeInfo(NTypeIds::Date) },
            {"datetime", TTypeInfo(NTypeIds::Datetime) },
            {"ts", TTypeInfo(NTypeIds::Timestamp) },
            {"ival", TTypeInfo(NTypeIds::Interval) },
            {"json_doc", TTypeInfo(NTypeIds::JsonDocument) },
            //{"dec", TTypeInfo(NTypeIds::Decimal) }
        };
        return columns;
    }

    NKikimr::TDbTupleRef ToDbTupleRef() const {
        static TCell Cells[20];
        Cells[0] = TCell::Make<bool>(Bool);
        Cells[1] = TCell::Make<i8>(Int8);
        Cells[2] = TCell::Make<i16>(Int16);
        Cells[3] = TCell::Make<i32>(Int32);
        Cells[4] = TCell::Make<i64>(Int64);
        Cells[5] = TCell::Make<ui8>(UInt8);
        Cells[6] = TCell::Make<ui16>(UInt16);
        Cells[7] = TCell::Make<ui32>(UInt32);
        Cells[8] = TCell::Make<ui64>(UInt64);
        Cells[9] = TCell::Make<float>(Float32);
        Cells[10] = TCell::Make<double>(Float64);
        Cells[11] = TCell(String.data(), String.size());
        Cells[12] = TCell(Utf8.data(), Utf8.size());
        Cells[13] = TCell(Json.data(), Json.size());
        Cells[14] = TCell(Yson.data(), Yson.size());
        Cells[15] = TCell::Make<ui16>(Date);
        Cells[16] = TCell::Make<ui32>(Datetime);
        Cells[17] = TCell::Make<i64>(Timestamp);
        Cells[18] = TCell::Make<i64>(Interval);
        Cells[19] = TCell(JsonDocument.data(), JsonDocument.size());
        //Cells[19] = TCell((const char *)&Decimal[0], 16);

        return NKikimr::TDbTupleRef(Types, Cells, 20);
    }

    TOwnedCellVec SerializedCells() const {
        NKikimr::TDbTupleRef value = ToDbTupleRef();
        std::vector<TCell> cells(value.Cells().data(), value.Cells().data() + value.Cells().size());

        auto binaryJson = NBinaryJson::SerializeToBinaryJson(TStringBuf(JsonDocument.data(), JsonDocument.size()));
        UNIT_ASSERT(binaryJson.Defined());

        cells[19] = TCell(binaryJson->Data(), binaryJson->Size());
        return TOwnedCellVec(cells);
    }
};


std::shared_ptr<arrow::Array> GetColumn(const arrow::Table& table, int i, int chunk = 0) {
    return table.column(i)->chunk(chunk);
}

std::shared_ptr<arrow::Array> GetColumn(const arrow::RecordBatch& batch, int i) {
    return batch.column(i);
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

std::shared_ptr<arrow::RecordBatch> VectorToBatch(const std::vector<struct TDataRow>& rows) {
    TString err;
    NArrow::TArrowBatchBuilder batchBuilder;
    batchBuilder.Start(TDataRow::MakeYdbSchema(), 0, 0, err);

    for (const TDataRow& row : rows) {
        NKikimr::TDbTupleRef key;
        NKikimr::TDbTupleRef value = row.ToDbTupleRef();
        batchBuilder.AddRow(key, value);
    }

    return batchBuilder.FlushBatch(false);
}

std::vector<TDataRow> TestRows() {
    std::vector<TDataRow> rows = {
        {false, -1, -1, -1, -1, 1, 1, 1, 1, -1.0f, -1.0, "s1", "u1", "{\"j\":1}", "{y:1}", 0, 0, 0, 0, "{\"jd\":1}" },
        {false, 2, 2, 2, 2, 2, 2, 2, 2, 2.0f, 2.0, "s2", "u2", "{\"j\":2}", "{y:2}", 0, 0, 0, 0, "{\"jd\":1}" },
        {false, -3, -3, -3, -3, 3, 3, 3, 3, -3.0f, -3.0, "s3", "u3", "{\"j\":3}", "{y:3}", 0, 0, 0, 0, "{\"jd\":1}" },
        {false, -4, -4, -4, -4, 4, 4, 4, 4, 4.0f, 4.0, "s4", "u4", "{\"j\":4}", "{y:4}", 0, 0, 0, 0, "{\"jd\":1}" },
    };
    return rows;
}

bool CheckFilter(const std::vector<bool>& f, size_t count, bool value) {
    for (size_t i = 0; i < f.size(); ++i) {
        if (i < count) {
            if (f[i] != value) {
                return false;
            }
        } else {
            if (f[i] == value) {
                return false;
            }
        }
    }
    return true;
}

std::shared_ptr<arrow::Table> MakeTable1000() {
    TDataRowTableBuilder builder;

    for (int i = 0; i < 1000; ++i) {
        i8 a = i/100;
        i16 b = (i%100)/10;
        i32 c = i%10;
        builder.AddRow(TDataRow{false, a, b, c, i, 1, 1, 1, 1, 1.0f, 1.0, "", "", "", "", 0, 0, 0, 0, {0,0} });
    }

    auto table = builder.Finish();
    auto schema = table->schema();
    auto tres = table->SelectColumns(std::vector<int>{
        schema->GetFieldIndex("i8"),
        schema->GetFieldIndex("i16"),
        schema->GetFieldIndex("i32")
    });
    UNIT_ASSERT(tres.ok());
    return *tres;
}

std::shared_ptr<arrow::Table> Shuffle(std::shared_ptr<arrow::Table> table) {
    std::vector<arrow::UInt64Builder::value_type> shuffle(1000);
    for (int i = 0; i < 1000; ++i) {
        shuffle[i] = i;
    }
    ShuffleRange(shuffle);

    arrow::UInt64Builder builder;
    UNIT_ASSERT(builder.AppendValues(&shuffle[0], shuffle.size()).ok());

    std::shared_ptr<arrow::UInt64Array> permutation;
    UNIT_ASSERT(builder.Finish(&permutation).ok());

    auto res = arrow::compute::Take(table, permutation);
    UNIT_ASSERT(res.ok());
    return (*res).table();
}

std::shared_ptr<arrow::RecordBatch> ExtractBatch(std::shared_ptr<arrow::Table> table) {
    std::shared_ptr<arrow::RecordBatch> batch;

    arrow::TableBatchReader reader(*table);
    auto result = reader.Next();
    Y_ABORT_UNLESS(result.ok());
    batch = *result;
    result = reader.Next();
    Y_ABORT_UNLESS(result.ok() && !(*result));
    return batch;
}

std::shared_ptr<arrow::RecordBatch> AddSnapColumn(const std::shared_ptr<arrow::RecordBatch>& batch, ui64 snap) {
    auto snapColumn = NArrow::MakeUI64Array(snap, batch->num_rows());
    auto result = batch->AddColumn(batch->num_columns(), "snap", snapColumn);
    Y_ABORT_UNLESS(result.ok());
    return *result;
}

THashMap<ui64, ui32> CountValues(const std::shared_ptr<arrow::UInt64Array>& array) {
    THashMap<ui64, ui32> out;
    for (int i = 0; i < array->length(); ++i) {
        ui64 val = array->Value(i);
        ++out[val];
    }
    return out;
}

ui32 RestoreValue(ui32 a, ui32 b, ui32 c) {
    return ui32(a) * 100 + b * 10 + c;
}

bool CheckSorted1000(const std::shared_ptr<arrow::RecordBatch>& batch, bool desc = false) {
    auto arrA = std::static_pointer_cast<arrow::Int8Array>(batch->GetColumnByName("i8"));
    auto arrB = std::static_pointer_cast<arrow::Int16Array>(batch->GetColumnByName("i16"));
    auto arrC = std::static_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("i32"));
    UNIT_ASSERT(arrA->length() == arrB->length() && arrA->length() == arrC->length());

    for (int i = 0; i < arrA->length(); ++i) {
        ui32 value = RestoreValue(arrA->Value(i), arrB->Value(i), arrC->Value(i));
        if (desc) {
            if (value != (arrA->length() - ui32(i) - 1)) {
                return false;
            }
        } else {
            if (value != ui32(i)) {
                return false;
            }
        }
    }

    return true;
}

bool CheckSorted(const std::shared_ptr<arrow::RecordBatch>& batch, bool desc = false) {
    auto arrA = std::static_pointer_cast<arrow::Int8Array>(batch->GetColumnByName("i8"));
    auto arrB = std::static_pointer_cast<arrow::Int16Array>(batch->GetColumnByName("i16"));
    auto arrC = std::static_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("i32"));
    UNIT_ASSERT(arrA->length() == arrB->length() && arrA->length() == arrC->length());

    ui32 prev = RestoreValue(arrA->Value(0), arrB->Value(0), arrC->Value(0));

    for (int i = 1; i < arrA->length(); ++i) {
        ui32 value = RestoreValue(arrA->Value(i), arrB->Value(i), arrC->Value(i));
        if ((desc && value > prev) ||
            (!desc && value < prev)) {
            return false;
        }
        prev = value;
    }

    return true;
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

    Y_UNIT_TEST(BatchBuilder) {
        std::vector<TDataRow> rows = TestRows();

        std::shared_ptr<arrow::RecordBatch> batch = VectorToBatch(rows);
        UNIT_ASSERT(batch);

        auto expectedSchema = TDataRow::MakeArrowSchema();
        //Cerr << expectedSchema->ToString() << '\n';
        //Cerr << batch->schema()->ToString() << '\n';

        UNIT_ASSERT_EQUAL(expectedSchema->Equals(*batch->schema()), true);

        std::vector<TDataRow> readRows = ToVector(batch);

        UNIT_ASSERT_EQUAL(rows.size(), readRows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_EQUAL(rows[i], readRows[i]);
        }
    }

    Y_UNIT_TEST(ArrowToYdbConverter) {
        std::vector<TDataRow> rows = TestRows();

        std::vector<TOwnedCellVec> cellRows;
        for (const TDataRow& row : rows) {
            cellRows.push_back(TOwnedCellVec(row.SerializedCells()));
        }

        std::shared_ptr<arrow::RecordBatch> batch = VectorToBatch(rows);
        UNIT_ASSERT(batch);

        auto expectedSchema = TDataRow::MakeArrowSchema();

        UNIT_ASSERT_EQUAL(expectedSchema->Equals(*batch->schema()), true);

        struct TRowWriter : public NArrow::IRowWriter {
            std::vector<TOwnedCellVec> Rows;

            void AddRow(const TConstArrayRef<TCell> &cells) override {
                Rows.push_back(TOwnedCellVec(cells));
            }
        } rowWriter;

        NArrow::TArrowToYdbConverter toYdbConverter(TDataRow::MakeYdbSchema(), rowWriter);
        TString errStr;
        bool ok = toYdbConverter.Process(*batch, errStr);
        Cerr << "Process: " << errStr << "\n";
        UNIT_ASSERT(ok);

        UNIT_ASSERT_VALUES_EQUAL(cellRows.size(), rowWriter.Rows.size());

        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT(0 == CompareTypedCellVectors(
                            cellRows[i].data(), rowWriter.Rows[i].data(),
                            TDataRow::Types,
                            cellRows[i].size(), rowWriter.Rows[i].size()));
        }
    }

    Y_UNIT_TEST(KeyComparison) {
        auto table = MakeTable1000();

        std::shared_ptr<arrow::RecordBatch> border; // {2, 3, 4}
        {
            arrow::ScalarVector scalars{
                std::make_shared<arrow::Int8Scalar>(2),
                std::make_shared<arrow::Int16Scalar>(3),
                std::make_shared<arrow::Int32Scalar>(4),
            };

            std::vector<std::shared_ptr<arrow::Array>> columns;
            for (auto scalar : scalars) {
                auto res = arrow::MakeArrayFromScalar(*scalar, 1);
                UNIT_ASSERT(res.ok());
                columns.push_back(*res);
            }

            border = arrow::RecordBatch::Make(table->schema(), 1, columns);
        }

        const NArrow::TColumnFilter lt = NArrow::TColumnFilter::MakePredicateFilter(table, border, NArrow::ECompareType::LESS);
        const NArrow::TColumnFilter le = NArrow::TColumnFilter::MakePredicateFilter(table, border, NArrow::ECompareType::LESS_OR_EQUAL);
        const NArrow::TColumnFilter gt = NArrow::TColumnFilter::MakePredicateFilter(table, border, NArrow::ECompareType::GREATER);
        const NArrow::TColumnFilter ge = NArrow::TColumnFilter::MakePredicateFilter(table, border, NArrow::ECompareType::GREATER_OR_EQUAL);

        UNIT_ASSERT(CheckFilter(lt.BuildSimpleFilter(), 234, true));
        UNIT_ASSERT(CheckFilter(le.BuildSimpleFilter(), 235, true));
        UNIT_ASSERT(CheckFilter(gt.BuildSimpleFilter(), 235, false));
        UNIT_ASSERT(CheckFilter(ge.BuildSimpleFilter(), 234, false));
    }

    Y_UNIT_TEST(SortWithCompositeKey) {
        std::shared_ptr<arrow::Table> table = Shuffle(MakeTable1000());

        // Table sort is not supported yet: we need not chunked arrays in MakeSortPermutation
        std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(table);

        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1000);
        UNIT_ASSERT(!CheckSorted1000(batch));

        auto sortPermutation = NArrow::MakeSortPermutation(batch, table->schema(), false);

        auto res = arrow::compute::Take(batch, sortPermutation);
        UNIT_ASSERT(res.ok());
        batch = (*res).record_batch();

        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1000);
        UNIT_ASSERT(CheckSorted1000(batch));
    }

    Y_UNIT_TEST(MergingSortedInputStream) {
        std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable1000());
        UNIT_ASSERT(CheckSorted1000(batch));

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        batches.push_back(batch->Slice(0, 100));    // 0..100 +100
        batches.push_back(batch->Slice(100, 200));  // 100..300 +200
        batches.push_back(batch->Slice(200, 400));  // 200..600 +300
        batches.push_back(batch->Slice(500, 50));   // 500..550 +50
        batches.push_back(batch->Slice(600, 1));    // 600..601 +1

        std::shared_ptr<arrow::RecordBatch> sorted;
        {
            NArrow::NMerger::TRecordBatchBuilder builder(batch->schema()->fields());
            const std::vector<std::string> vColumns = {batch->schema()->field(0)->name()};
            auto merger = std::make_shared<NArrow::NMerger::TMergePartialStream>(batch->schema(), batch->schema(), false, vColumns);
            for (auto&& i : batches) {
                merger->AddSource(i, nullptr);
            }
            merger->DrainAll(builder);
            sorted = builder.Finalize();
        }
        UNIT_ASSERT_VALUES_EQUAL(sorted->num_rows(), 601);
        UNIT_ASSERT(NArrow::IsSorted(sorted, batch->schema()));
        UNIT_ASSERT(CheckSorted(sorted));
    }

    Y_UNIT_TEST(MergingSortedInputStreamReversed) {
        std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable1000());
        UNIT_ASSERT(CheckSorted1000(batch));

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        batches.push_back(batch->Slice(0, 100));    // 0..100 +100
        batches.push_back(batch->Slice(100, 200));  // 100..300 +200
        batches.push_back(batch->Slice(200, 400));  // 200..600 +300
        batches.push_back(batch->Slice(500, 50));   // 500..550 +50
        batches.push_back(batch->Slice(600, 1));    // 600..601 +1

        std::shared_ptr<arrow::RecordBatch> sorted;
        {
            NArrow::NMerger::TRecordBatchBuilder builder(batch->schema()->fields());
            const std::vector<std::string> vColumns = {batch->schema()->field(0)->name()};
            auto merger = std::make_shared<NArrow::NMerger::TMergePartialStream>(batch->schema(), batch->schema(), true, vColumns);
            for (auto&& i : batches) {
                merger->AddSource(i, nullptr);
            }
            merger->DrainAll(builder);
            sorted = builder.Finalize();
        }
        UNIT_ASSERT_VALUES_EQUAL(sorted->num_rows(), 601);
        UNIT_ASSERT(NArrow::IsSorted(sorted, batch->schema(), true));
        UNIT_ASSERT(CheckSorted(sorted, true));
    }

    Y_UNIT_TEST(MergingSortedInputStreamReplace) {
        std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable1000());
        UNIT_ASSERT(CheckSorted1000(batch));

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        batches.push_back(AddSnapColumn(batch->Slice(0, 400), 0));
        batches.push_back(AddSnapColumn(batch->Slice(200, 400), 1));
        batches.push_back(AddSnapColumn(batch->Slice(400, 400), 2));
        batches.push_back(AddSnapColumn(batch->Slice(600, 400), 3));

        std::shared_ptr<arrow::RecordBatch> sorted;
        {
            NArrow::NMerger::TRecordBatchBuilder builder(batches[0]->schema()->fields());
            const std::vector<std::string> vColumns = {"snap"};
            auto merger = std::make_shared<NArrow::NMerger::TMergePartialStream>(batch->schema(), batches[0]->schema(), false, vColumns);
            for (auto&& i : batches) {
                merger->AddSource(i, nullptr);
            }
            merger->DrainAll(builder);
            sorted = builder.Finalize();
        }

        UNIT_ASSERT_VALUES_EQUAL(sorted->num_rows(), 1000);
        UNIT_ASSERT(CheckSorted1000(sorted));
        UNIT_ASSERT(NArrow::IsSortedAndUnique(sorted, batch->schema()));

        auto counts = CountValues(std::static_pointer_cast<arrow::UInt64Array>(sorted->GetColumnByName("snap")));
        UNIT_ASSERT_VALUES_EQUAL(counts[0], 200);
        UNIT_ASSERT_VALUES_EQUAL(counts[1], 200);
        UNIT_ASSERT_VALUES_EQUAL(counts[2], 200);
        UNIT_ASSERT_VALUES_EQUAL(counts[3], 400);
    }
}

}
