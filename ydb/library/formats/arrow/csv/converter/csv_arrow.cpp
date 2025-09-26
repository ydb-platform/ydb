#include "csv_arrow.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/value_parsing.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
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
    auto SetOptionsForColumns = [&](const auto& col) {
        if (col.Precision > 0) {
            ConvertOptions.column_types[col.Name] = arrow::decimal128(static_cast<int32_t>(col.Precision), static_cast<int32_t>(col.Scale));
        } else {
            ConvertOptions.column_types[col.Name] = col.CsvArrowType;
        }
    };
    
    if (header) {
        // !autogenerate + column_names.empty() => read from CSV
        ResultColumns.reserve(columns.size());

        for (const auto& col: columns) {
            ResultColumns.push_back(col.Name);
            SetOptionsForColumns(col);
            OriginalColumnTypes[col.Name] = col.ArrowType;
        }
    } else if (!columns.empty()) {
        // !autogenerate + !column_names.empty() => specified columns
        ReadOptions.column_names.reserve(columns.size());

        for (const auto& col: columns) {
            ReadOptions.column_names.push_back(col.Name);
            SetOptionsForColumns(col);
            OriginalColumnTypes[col.Name] = col.ArrowType;
        }
#if 0
    } else {
        ReadOptions.autogenerate_column_names = true;
#endif
    }

    SetNullValue(); // set default null value
}

namespace {

    template<class TBuilder, class TOriginalArray>
    std::shared_ptr<arrow::Array> ConvertArray(std::shared_ptr<arrow::ArrayData> data, ui64 dev) {
        auto originalArr = std::make_shared<TOriginalArray>(data);
        TBuilder aBuilder;
        Y_ABORT_UNLESS(aBuilder.Reserve(originalArr->length()).ok());
        for (long i = 0; i < originalArr->length(); ++i) {
            if (originalArr->IsNull(i)) {
                Y_ABORT_UNLESS(aBuilder.AppendNull().ok());
            } else {
                aBuilder.UnsafeAppend(originalArr->Value(i) / dev);
            }
        }
        auto res = aBuilder.Finish();
        Y_ABORT_UNLESS(res.ok());
        return *res;
    }

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
            resultColumns.emplace_back([originalType, fArr]() {
                switch (originalType->id()) {
                case arrow::UInt16Type::type_id: // Date
                    return ConvertArray<arrow::UInt16Builder, arrow::TimestampArray>(fArr->data(), 86400);
                case arrow::UInt32Type::type_id: // Datetime
                    return ConvertArray<arrow::UInt32Builder, arrow::TimestampArray>(fArr->data(), 1);
                case arrow::Int32Type::type_id: // Date32
                    return ConvertArray<arrow::Int32Builder, arrow::TimestampArray>(fArr->data(), 86400);
                case arrow::Int64Type::type_id:// Datetime64, Timestamp64
                    return ConvertArray<arrow::Int64Builder, arrow::TimestampArray>(fArr->data(), 1);
                default:
                    Y_ABORT_UNLESS(false);
                }
            }());
        } else if (fArr->type()->id() == arrow::Decimal128Type::type_id && originalType->id() == arrow::FixedSizeBinaryType::type_id) {
            auto fixedSizeBinaryType = std::static_pointer_cast<arrow::FixedSizeBinaryType>(originalType);
            const auto& decData = fArr->data();
            auto viewData = arrow::ArrayData::Make(
                fixedSizeBinaryType,
                decData->length,
                decData->buffers,
                decData->null_count,
                decData->offset
            );
            resultColumns.emplace_back(arrow::MakeArray(viewData));
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

        auto buffer = std::make_shared<arrow::Buffer>(arrow::util::string_view(csv.c_str(), csv.length()));
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
std::shared_ptr<arrow::RecordBatch> TArrowCSV::ReadSingleBatch(const TString& csv, const Ydb::Formats::CsvSettings& csvSettings, TString& errString) {
    const auto& quoting = csvSettings.quoting();
    if (quoting.quote_char().length() > 1) {
        errString = ErrorPrefix() + "Wrong quote char '" + quoting.quote_char() + "'";
        return {};
    }

    const char qchar = quoting.quote_char().empty() ? '"' : quoting.quote_char().front();
    SetQuoting(!quoting.disabled(), qchar, !quoting.double_quote_disabled());
    if (csvSettings.delimiter()) {
        if (csvSettings.delimiter().size() != 1) {
            errString = ErrorPrefix() + "Invalid delimitr in csv: " + csvSettings.delimiter();
            return {};
        }
        SetDelimiter(csvSettings.delimiter().front());
    }
    SetSkipRows(csvSettings.skip_rows());

    if (csvSettings.null_value()) {
        SetNullValue(csvSettings.null_value());
    }

    if (csv.size() > NKikimr::NFormats::TArrowCSV::DEFAULT_BLOCK_SIZE) {
        ui32 blockSize = NKikimr::NFormats::TArrowCSV::DEFAULT_BLOCK_SIZE;
        blockSize *= csv.size() / blockSize + 1;
        SetBlockSize(blockSize);
    }
    return ReadSingleBatch(csv, errString);
}

}
