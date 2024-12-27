#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>

#include <yql/essentials/types/binary_json/write.h>
#include <library/cpp/testing/benchmark/bench.h>
#include <util/string/printf.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <util/random/shuffle.h>
#include <numeric>

namespace NKikimr {
namespace {

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

struct TDataRow {
    static const TTypeInfo* MakeTypeInfos() {
        static const TTypeInfo types[27] = {
            TTypeInfo(NTypeIds::Bool),
            TTypeInfo(NTypeIds::Int8),
            TTypeInfo(NTypeIds::Int16),
            TTypeInfo(NTypeIds::Int32),
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
            TTypeInfo(NPg::TypeDescFromPgTypeId(INT2OID)),
            TTypeInfo(NPg::TypeDescFromPgTypeId(INT4OID)),
            TTypeInfo(NPg::TypeDescFromPgTypeId(INT8OID)),
            TTypeInfo(NPg::TypeDescFromPgTypeId(FLOAT4OID)),
            TTypeInfo(NPg::TypeDescFromPgTypeId(FLOAT8OID)),
            TTypeInfo(NPg::TypeDescFromPgTypeId(BYTEAOID)),
            TTypeInfo(NPg::TypeDescFromPgTypeId(TEXTOID)),
            TTypeInfo(NTypeIds::Int64),
            // TODO: DyNumber, Decimal
        };
        return types;
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
    std::string String;
    std::string Utf8;
    std::string Json;
    std::string Yson;
    ui16 Date;
    ui32 Datetime;
    i64 Timestamp;
    i64 Interval;
    std::string JsonDocument;
    i16 PgInt2;
    i32 PgInt4;
    i64 PgInt8;
    float PgFloat4;
    double PgFloat8;
    std::string PgBytea;
    std::string PgText;
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
            (JsonDocument == r.JsonDocument) &&
            (PgInt2 == r.PgInt2) &&
            (PgInt4 == r.PgInt4) &&
            (PgInt8 == r.PgInt8) &&
            (PgFloat4 == r.PgFloat4) &&
            (PgFloat8 == r.PgFloat8) &&
            (PgBytea == r.PgBytea) &&
            (PgText == r.PgText);
            //(Decimal[0] == r.Decimal[0] && Decimal[1] == r.Decimal[1]);
    }

    static std::shared_ptr<arrow::Schema> MakeArrowSchema() {
        std::vector<std::shared_ptr<arrow::Field>> fields = {
            arrow::field("bool", arrow::boolean()),
            arrow::field("i8", arrow::int8()),
            arrow::field("i16", arrow::int16()),
            arrow::field("i32", arrow::int32()),
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
            arrow::field("pgint2", arrow::int16()),
            arrow::field("pgint4", arrow::int32()),
            arrow::field("pgint8", arrow::int64()),
            arrow::field("pgfloat4", arrow::float32()),
            arrow::field("pgfloat8", arrow::float64()),
            arrow::field("pgbytea", arrow::binary()),
            arrow::field("pgtext", arrow::utf8()),
            arrow::field("i64", arrow::int64()),
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
            {"pgint2", TTypeInfo(NPg::TypeDescFromPgTypeId(INT2OID)) },
            {"pgint4", TTypeInfo(NPg::TypeDescFromPgTypeId(INT4OID)) },
            {"pgint8", TTypeInfo(NPg::TypeDescFromPgTypeId(INT8OID)) },
            {"pgfloat4", TTypeInfo(NPg::TypeDescFromPgTypeId(FLOAT4OID)) },
            {"pgfloat8", TTypeInfo(NPg::TypeDescFromPgTypeId(FLOAT8OID)) },
            {"pgbytea", TTypeInfo(NPg::TypeDescFromPgTypeId(BYTEAOID)) },
            {"pgtext", TTypeInfo(NPg::TypeDescFromPgTypeId(TEXTOID)) },
            {"i64", TTypeInfo(NTypeIds::Int64) },
            //{"dec", TTypeInfo(NTypeIds::Decimal) }
        };
        return columns;
    }

    NKikimr::TDbTupleRef ToDbTupleRef() const {
        static TCell Cells[27];
        Cells[0] = TCell::Make<bool>(Bool);
        Cells[1] = TCell::Make<i8>(Int8);
        Cells[2] = TCell::Make<i16>(Int16);
        Cells[3] = TCell::Make<i32>(Int32);
        Cells[4] = TCell::Make<ui8>(UInt8);
        Cells[5] = TCell::Make<ui16>(UInt16);
        Cells[6] = TCell::Make<ui32>(UInt32);
        Cells[7] = TCell::Make<ui64>(UInt64);
        Cells[8] = TCell::Make<float>(Float32);
        Cells[9] = TCell::Make<double>(Float64);
        Cells[10] = TCell(String.data(), String.size());
        Cells[11] = TCell(Utf8.data(), Utf8.size());
        Cells[12] = TCell(Json.data(), Json.size());
        Cells[13] = TCell(Yson.data(), Yson.size());
        Cells[14] = TCell::Make<ui16>(Date);
        Cells[15] = TCell::Make<ui32>(Datetime);
        Cells[16] = TCell::Make<i64>(Timestamp);
        Cells[17] = TCell::Make<i64>(Interval);
        Cells[18] = TCell(JsonDocument.data(), JsonDocument.size());
        Cells[19] = TCell::Make<i16>(PgInt2);
        Cells[20] = TCell::Make<i32>(PgInt4);
        Cells[21] = TCell::Make<i64>(PgInt8);
        Cells[22] = TCell::Make<float>(PgFloat4);
        Cells[23] = TCell::Make<double>(PgFloat8);
        Cells[24] = TCell(PgBytea.data(), PgBytea.size());
        Cells[25] = TCell(PgText.data(), PgText.size());
        Cells[26] = TCell::Make<i64>(Int64);

        //Cells[19] = TCell((const char *)&Decimal[0], 16);

        return NKikimr::TDbTupleRef(MakeTypeInfos(), Cells, 27);
    }

    TOwnedCellVec SerializedCells() const {
        NKikimr::TDbTupleRef value = ToDbTupleRef();
        std::vector<TCell> cells(value.Cells().data(), value.Cells().data() + value.Cells().size());

        auto maybeBinaryJson = NBinaryJson::SerializeToBinaryJson(TStringBuf(JsonDocument.data(), JsonDocument.size()));
        (std::holds_alternative<NBinaryJson::TBinaryJson>(maybeBinaryJson));

        const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(maybeBinaryJson);
        cells[18] = TCell(binaryJson.Data(), binaryJson.Size());
        return TOwnedCellVec(cells);
    }
};

class TDataRowTableBuilder
{
public:
    TDataRowTableBuilder()
        : Bts(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool())
        , Bival(arrow::duration(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool())
        //, Bdec(arrow::decimal(NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE), arrow::default_memory_pool())
    {}

    void AddRow(const TDataRow& row) {
        (Bbool.Append(row.Bool).ok());
        (Bi8.Append(row.Int8).ok());
        (Bi16.Append(row.Int16).ok());
        (Bi32.Append(row.Int32).ok());
        (Bi64.Append(row.Int64).ok());
        (Bu8.Append(row.UInt8).ok());
        (Bu16.Append(row.UInt16).ok());
        (Bu32.Append(row.UInt32).ok());
        (Bu64.Append(row.UInt64).ok());
        (Bf32.Append(row.Float32).ok());
        (Bf64.Append(row.Float64).ok());

        (Bstr.Append(row.String).ok());
        (Butf.Append(row.Utf8).ok());
        (Bj.Append(row.Json).ok());
        (By.Append(row.Yson).ok());

        (Bd.Append(row.Date).ok());
        (Bdt.Append(row.Datetime).ok());
        (Bts.Append(row.Timestamp).ok());
        (Bival.Append(row.Interval).ok());

        (Bjd.Append(row.JsonDocument).ok());
        (Bpgi2.Append(row.PgInt2).ok());
        (Bpgi4.Append(row.PgInt4).ok());
        (Bpgi8.Append(row.PgInt8).ok());
        (Bpgf4.Append(row.PgFloat4).ok());
        (Bpgf8.Append(row.PgFloat8).ok());
        (Bpgb.Append(row.PgBytea).ok());
        (Bpgt.Append(row.PgText).ok());
        //(Bdec.Append((const char *)&row.Decimal).ok());
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
        std::shared_ptr<arrow::Int16Array> arpgi2;
        std::shared_ptr<arrow::Int32Array> arpgi4;
        std::shared_ptr<arrow::Int64Array> arpgi8;
        std::shared_ptr<arrow::FloatArray> arpgf4;
        std::shared_ptr<arrow::DoubleArray> arpgf8;
        std::shared_ptr<arrow::BinaryArray> arpgb;
        std::shared_ptr<arrow::StringArray> arpgt;
        //std::shared_ptr<arrow::Decimal128Array> ardec;

        (Bbool.Finish(&arbool).ok());
        (Bi8.Finish(&ari8).ok());
        (Bi16.Finish(&ari16).ok());
        (Bi32.Finish(&ari32).ok());
        (Bi64.Finish(&ari64).ok());
        (Bu8.Finish(&aru8).ok());
        (Bu16.Finish(&aru16).ok());
        (Bu32.Finish(&aru32).ok());
        (Bu64.Finish(&aru64).ok());
        (Bf32.Finish(&arf32).ok());
        (Bf64.Finish(&arf64).ok());

        (Bstr.Finish(&arstr).ok());
        (Butf.Finish(&arutf).ok());
        (Bj.Finish(&arj).ok());
        (By.Finish(&ary).ok());

        (Bd.Finish(&ard).ok());
        (Bdt.Finish(&ardt).ok());
        (Bts.Finish(&arts).ok());
        (Bival.Finish(&arival).ok());

        (Bjd.Finish(&arjd).ok());
        (Bpgi2.Finish(&arpgi2).ok());
        (Bpgi4.Finish(&arpgi4).ok());
        (Bpgi8.Finish(&arpgi8).ok());
        (Bpgf4.Finish(&arpgf4).ok());
        (Bpgf8.Finish(&arpgf8).ok());
        (Bpgb.Finish(&arpgb).ok());
        (Bpgt.Finish(&arpgt).ok());
        //(Bdec.Finish(&ardec).ok());

        std::shared_ptr<arrow::Schema> schema = TDataRow::MakeArrowSchema();
        return arrow::Table::Make(schema, {
            arbool,
            ari8, ari16, ari32, 
            aru8, aru16, aru32, aru64,
            arf32, arf64,
            arstr, arutf, arj, ary,
            ard, ardt, arts, arival, arjd,
            arpgi2, arpgi4, arpgi8, arpgf4, arpgf8,
            arpgb, arpgt, ari64
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
    arrow::Int16Builder Bpgi2;
    arrow::Int32Builder Bpgi4;
    arrow::Int64Builder Bpgi8;
    arrow::FloatBuilder Bpgf4;
    arrow::DoubleBuilder Bpgf8;
    arrow::BinaryBuilder Bpgb;
    arrow::StringBuilder Bpgt;
    //arrow::Decimal128Builder Bdec;
};

std::shared_ptr<arrow::Table> MakeTable(i64 block_number, i64 block_size) {
    std::vector<i64> all_data(block_number * block_size);
    std::iota(all_data.begin(), all_data.end(), 0);
    std::mt19937_64 rnd;
    std::shuffle(all_data.begin(),all_data.end(), rnd);

    TDataRowTableBuilder builder;
    for (i64 i = 0; i < block_number * block_size; i += block_size) {
        std::sort(all_data.begin() + i, all_data.begin() + i + block_size);
    }    

    for (int i = 0; i < block_number * block_size; ++i) {
        builder.AddRow(
            TDataRow{false, 0, 0, 0, all_data[i], 1, 1, 1, 1, 1.0f, 1.0, "", "", "", "", 0, 0, 0, 0, {0,0},
                0, 0, 0, 0.0f, 0.0, "", ""});
    }
    return builder.Finish();
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
}

Y_CPU_BENCHMARK(MergingSortedInputStream, iface) {
    i64 data_size = 1e5;
    i64 block_number = 5000;
    i64 block_size = data_size / block_number;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        std::shared_ptr<arrow::RecordBatch> batch = ExtractBatch(MakeTable(block_number, block_size));

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (i64 i = 0; i < block_number * block_size; i += block_size) {
            batches.push_back(batch->Slice( i, block_size)); 
        }

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
        auto same_size = sorted.get()->GetColumnByName("i64")->data().get()->length == block_size * block_number;
        if (!same_size) {
            throw std::runtime_error("wrong size...");
        }
    }
}
}
