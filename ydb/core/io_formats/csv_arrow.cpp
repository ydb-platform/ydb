#include "csv.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/serializer/stream.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/value_parsing.h>

namespace NKikimr::NFormats {

namespace {
class TimestampIntParser: public arrow::TimestampParser {
public:
    TimestampIntParser() {}

    bool operator()(const char* s, size_t length, arrow::TimeUnit::type out_unit,
        int64_t* out) const override {
        int64_t unitsCount;
        if (!TryFromString(TString(s, length), unitsCount)) {
            return false;
        }
        *out = unitsCount;
        switch (out_unit) {
            case arrow::TimeUnit::NANO:
                *out *= 1000000000;
                break;
            case arrow::TimeUnit::MICRO:
                *out *= 1000000;
                break;
            case arrow::TimeUnit::MILLI:
                *out *= 1000;
                break;
            case arrow::TimeUnit::SECOND:
                *out *= 1;
                break;
        }
        return true;
    }

    const char* kind() const override { return "ts_int"; }
};

}

TArrowCSV::TArrowCSV(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, bool header)
    : ReadOptions(arrow::csv::ReadOptions::Defaults())
    , ParseOptions(arrow::csv::ParseOptions::Defaults())
    , ConvertOptions(arrow::csv::ConvertOptions::Defaults())
{
    ConvertOptions.check_utf8 = false;
    ConvertOptions.timestamp_parsers.clear();
    ConvertOptions.timestamp_parsers.emplace_back(arrow::TimestampParser::MakeISO8601());
    ConvertOptions.timestamp_parsers.emplace_back(std::make_shared<TimestampIntParser>());

    ReadOptions.block_size = DEFAULT_BLOCK_SIZE;
    ReadOptions.use_threads = false;
    ReadOptions.autogenerate_column_names = false;
    if (header) {
        // !autogenerate + column_names.empty() => read from CSV
        ResultColumns.reserve(columns.size());

        for (auto& [name, type] : columns) {
            ResultColumns.push_back(name);
            std::string columnName(name.data(), name.size());
            ConvertOptions.column_types[columnName] = NArrow::GetCSVArrowType(type);
            OriginalColumnTypes[columnName] = NArrow::GetArrowType(type);
        }
    } else if (!columns.empty()) {
        // !autogenerate + !column_names.empty() => specified columns
        ReadOptions.column_names.reserve(columns.size());

        for (auto& [name, type] : columns) {
            std::string columnName(name.data(), name.size());
            ReadOptions.column_names.push_back(columnName);
            ConvertOptions.column_types[columnName] = NArrow::GetCSVArrowType(type);
            OriginalColumnTypes[columnName] = NArrow::GetArrowType(type);
        }
#if 0
    } else {
        ReadOptions.autogenerate_column_names = true;
#endif
    }

    SetNullValue(); // set default null value
}

std::shared_ptr<arrow::RecordBatch> TArrowCSV::ConvertColumnTypes(std::shared_ptr<arrow::RecordBatch> parsedBatch) const {
    if (!parsedBatch) {
        return parsedBatch;
    }
    std::shared_ptr<arrow::Schema> schema;
    {
        arrow::SchemaBuilder sBuilder;
        for (auto&& f : parsedBatch->schema()->fields()) {
            Y_VERIFY(sBuilder.AddField(std::make_shared<arrow::Field>(f->name(), f->type())).ok());

        }
        auto resultSchema = sBuilder.Finish();
        Y_VERIFY(resultSchema.ok());
        schema = *resultSchema;
    }

    std::vector<std::shared_ptr<arrow::Array>> resultColumns;
    std::set<std::string> columnsFilter(ResultColumns.begin(), ResultColumns.end());
    arrow::SchemaBuilder sBuilderFixed;
    for (auto&& f : schema->fields()) {
        auto fArr = parsedBatch->GetColumnByName(f->name());
        std::shared_ptr<arrow::DataType> originalType;
        if (columnsFilter.contains(f->name()) || columnsFilter.empty()) {
            auto it = OriginalColumnTypes.find(f->name());
            Y_VERIFY(it != OriginalColumnTypes.end());
            originalType = it->second;
            Y_VERIFY(sBuilderFixed.AddField(std::make_shared<arrow::Field>(f->name(), originalType)).ok());
        } else {
            continue;
        }
        if (fArr->type()->Equals(originalType)) {
            resultColumns.emplace_back(fArr);
        } else if (fArr->type()->id() == arrow::TimestampType::type_id) {
            arrow::Result<std::shared_ptr<arrow::Array>> arrResult;
            {
                std::shared_ptr<arrow::TimestampArray> i64Arr = std::make_shared<arrow::TimestampArray>(fArr->data());
                if (originalType->id() == arrow::UInt16Type::type_id) {
                    arrow::UInt16Builder aBuilder;
                    Y_VERIFY(aBuilder.Reserve(parsedBatch->num_rows()).ok());
                    for (long i = 0; i < parsedBatch->num_rows(); ++i) {
                        if (i64Arr->IsNull(i)) {
                            Y_VERIFY(aBuilder.AppendNull().ok());
                        } else {
                            aBuilder.UnsafeAppend(i64Arr->Value(i) / 86400ull);
                        }
                    }
                    arrResult = aBuilder.Finish();
                } else if (originalType->id() == arrow::UInt32Type::type_id) {
                    arrow::UInt32Builder aBuilder;
                    Y_VERIFY(aBuilder.Reserve(parsedBatch->num_rows()).ok());
                    for (long i = 0; i < parsedBatch->num_rows(); ++i) {
                        if (i64Arr->IsNull(i)) {
                            Y_VERIFY(aBuilder.AppendNull().ok());
                        } else {
                            aBuilder.UnsafeAppend(i64Arr->Value(i));
                        }
                    }
                    arrResult = aBuilder.Finish();
                } else {
                    Y_VERIFY(false);
                }
            }
            Y_VERIFY(arrResult.ok());
            resultColumns.emplace_back(*arrResult);
        } else {
            Y_VERIFY(false);
        }
    }

    auto resultSchemaFixed = sBuilderFixed.Finish();
    Y_VERIFY(resultSchemaFixed.ok());
    return arrow::RecordBatch::Make(*resultSchemaFixed, parsedBatch->num_rows(), resultColumns);
}

std::shared_ptr<arrow::RecordBatch> TArrowCSV::ReadNext(const TString& csv, TString& errString) {
    if (!Reader) {
        if (ConvertOptions.column_types.empty()) {
            errString = ErrorPrefix() + "no columns specified";
            return {};
        }

        auto buffer = std::make_shared<NArrow::NSerialization::TBufferOverString>(csv);
        auto input = std::make_shared<arrow::io::BufferReader>(buffer);
        auto res = arrow::csv::StreamingReader::Make(arrow::io::default_io_context(), input,
                                                     ReadOptions, ParseOptions, ConvertOptions);
        if (!res.ok()) {
            errString = ErrorPrefix() + res.status().ToString();
            return {};
        }
        Reader = *res;
    }

    if (!Reader) {
        errString = ErrorPrefix() + "cannot make reader";
        return {};
    }

    std::shared_ptr<arrow::RecordBatch> batchParsed;
    Reader->ReadNext(&batchParsed).ok();

    std::shared_ptr<arrow::RecordBatch> batch = ConvertColumnTypes(batchParsed);

    if (batch && !ResultColumns.empty()) {
        batch = NArrow::ExtractColumns(batch, ResultColumns);
        if (!batch) {
            errString = ErrorPrefix() + "not all result columns present";
        }
    }
    return batch;
}

void TArrowCSV::SetNullValue(const TString& null) {
    ConvertOptions.null_values = { std::string(null.data(), null.size()) };
    ConvertOptions.strings_can_be_null = true;
    ConvertOptions.quoted_strings_can_be_null = false;
}

std::shared_ptr<arrow::RecordBatch> TArrowCSV::ReadSingleBatch(const TString& csv, TString& errString) {
    auto batch = ReadNext(csv, errString);
    if (!batch) {
        if (errString.empty()) {
            errString = ErrorPrefix();
        }
        return {};
    }

    if (ReadNext(csv, errString)) {
        errString = ErrorPrefix() + "too big CSV data portion";
        return {};
    }
    return batch;
}

}
