#include "csv_arrow.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/serializer/stream.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/value_parsing.h>
#include <util/string/join.h>

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

arrow::Result<TArrowCSV> TArrowCSV::Create(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, bool header, const std::set<std::string>& notNullColumns) {
        TVector<TString> errors;
        TColummns convertedColumns;
        convertedColumns.reserve(columns.size());
        for (auto& [name, type] : columns) {
            const auto arrowType = NArrow::GetArrowType(type);
            if (!arrowType.ok()) {
                errors.emplace_back("column " + name + ": " + arrowType.status().ToString());
                continue;
            }
            const auto csvArrowType = NArrow::GetCSVArrowType(type);
            if (!csvArrowType.ok()) {
                errors.emplace_back("column " + name + ": " + csvArrowType.status().ToString());
                continue;
            }
            convertedColumns.emplace_back(TColumnInfo{name, *arrowType, *csvArrowType});
        }
        if (!errors.empty()) {
            return arrow::Status::TypeError(ErrorPrefix() + "columns errors: " + JoinSeq("; ", errors));
        }
        return TArrowCSV(convertedColumns, header, notNullColumns);
}

TArrowCSV::TArrowCSV(const TColummns& columns, bool header, const std::set<std::string>& notNullColumns)
    : ReadOptions(arrow::csv::ReadOptions::Defaults())
    , ParseOptions(arrow::csv::ParseOptions::Defaults())
    , ConvertOptions(arrow::csv::ConvertOptions::Defaults())
    , NotNullColumns(notNullColumns)
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

        for (const auto& col: columns) {
            ResultColumns.push_back(col.Name);
            ConvertOptions.column_types[col.Name] = col.CsvArrowType;
            OriginalColumnTypes[col.Name] = col.ArrowType;
        }
    } else if (!columns.empty()) {
        // !autogenerate + !column_names.empty() => specified columns
        ReadOptions.column_names.reserve(columns.size());

        for (const auto& col: columns) {
            ReadOptions.column_names.push_back(col.Name);
            ConvertOptions.column_types[col.Name] = col.CsvArrowType;
            OriginalColumnTypes[col.Name] = col.ArrowType;
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
        return nullptr;
    }

    const auto& schema = parsedBatch->schema();

    std::vector<std::shared_ptr<arrow::Array>> resultColumns;
    std::set<std::string> columnsFilter(ResultColumns.begin(), ResultColumns.end());
    arrow::SchemaBuilder sBuilderFixed;

    for (const auto& f : schema->fields()) {
        auto fArr = parsedBatch->GetColumnByName(f->name());
        Y_ABORT_UNLESS(fArr);
        std::shared_ptr<arrow::DataType> originalType;
        if (columnsFilter.contains(f->name()) || columnsFilter.empty()) {
            auto it = OriginalColumnTypes.find(f->name());
            Y_ABORT_UNLESS(it != OriginalColumnTypes.end());
            originalType = it->second;
            bool nullable = !NotNullColumns.contains(f->name());
            Y_ABORT_UNLESS(sBuilderFixed.AddField(std::make_shared<arrow::Field>(f->name(), originalType, nullable)).ok());
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
                    Y_ABORT_UNLESS(aBuilder.Reserve(parsedBatch->num_rows()).ok());
                    for (long i = 0; i < parsedBatch->num_rows(); ++i) {
                        if (i64Arr->IsNull(i)) {
                            Y_ABORT_UNLESS(aBuilder.AppendNull().ok());
                        } else {
                            aBuilder.UnsafeAppend(i64Arr->Value(i) / 86400ull);
                        }
                    }
                    arrResult = aBuilder.Finish();
                } else if (originalType->id() == arrow::UInt32Type::type_id) {
                    arrow::UInt32Builder aBuilder;
                    Y_ABORT_UNLESS(aBuilder.Reserve(parsedBatch->num_rows()).ok());
                    for (long i = 0; i < parsedBatch->num_rows(); ++i) {
                        if (i64Arr->IsNull(i)) {
                            Y_ABORT_UNLESS(aBuilder.AppendNull().ok());
                        } else {
                            aBuilder.UnsafeAppend(i64Arr->Value(i));
                        }
                    }
                    arrResult = aBuilder.Finish();
                } else if (originalType->id() == arrow::Int32Type::type_id) {
                    arrow::Int32Builder aBuilder;
                    Y_ABORT_UNLESS(aBuilder.Reserve(parsedBatch->num_rows()).ok());
                    for (long i = 0; i < parsedBatch->num_rows(); ++i) {
                        if (i64Arr->IsNull(i)) {
                            Y_ABORT_UNLESS(aBuilder.AppendNull().ok());
                        } else {
                            aBuilder.UnsafeAppend(i64Arr->Value(i) / 86400);
                        }
                    }
                    arrResult = aBuilder.Finish();
                } else if (originalType->id() == arrow::Int64Type::type_id) {
                    arrow::Int64Builder aBuilder;
                    Y_ABORT_UNLESS(aBuilder.Reserve(parsedBatch->num_rows()).ok());
                    for (long i = 0; i < parsedBatch->num_rows(); ++i) {
                        if (i64Arr->IsNull(i)) {
                            Y_ABORT_UNLESS(aBuilder.AppendNull().ok());
                        } else {
                            aBuilder.UnsafeAppend(i64Arr->Value(i));
                        }
                    }
                    arrResult = aBuilder.Finish();
                } else {
                    Y_ABORT_UNLESS(false);
                }
            }
            Y_ABORT_UNLESS(arrResult.ok());
            resultColumns.emplace_back(*arrResult);
        } else {
            Y_ABORT_UNLESS(false);
        }
    }

    auto resultSchemaFixed = sBuilderFixed.Finish();
    Y_ABORT_UNLESS(resultSchemaFixed.ok());
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
    auto res = Reader->ReadNext(&batchParsed);
    if (!res.ok()) {
        errString = ErrorPrefix() + res.ToString();
        return {};
    }

    if (batchParsed) {
        if (!batchParsed->Validate().ok()) {
            errString = ErrorPrefix() + batchParsed->Validate().ToString();
            return {};
        }
        if (!batchParsed->schema()->HasDistinctFieldNames()) {
            errString = ErrorPrefix() + "duplicate column names:";
            for (auto& field : batchParsed->schema()->fields()) {
                if (batchParsed->schema()->GetFieldIndex(field->name()) == -1) {
                    errString += " '" + field->name() + "'";
                }
            }
            return {};
        }
    }

    std::shared_ptr<arrow::RecordBatch> batch = ConvertColumnTypes(batchParsed);
    if (batch && !batch->Validate().ok()) {
        errString = ErrorPrefix() + batch->Validate().ToString();
        return {};
    }

    if (batch && ResultColumns.size()) {
        batch = NArrow::TColumnOperator().NullIfAbsent().Extract(batch, ResultColumns);
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
