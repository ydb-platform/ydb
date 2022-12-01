#include "csv.h"
#include <ydb/core/formats/arrow_helpers.h>

namespace NKikimr::NFormats {

TArrowCSV::TArrowCSV(const TVector<std::pair<TString, NScheme::TTypeInfo>>& columns, bool header)
    : ReadOptions(arrow::csv::ReadOptions::Defaults())
    , ParseOptions(arrow::csv::ParseOptions::Defaults())
    , ConvertOptions(arrow::csv::ConvertOptions::Defaults())
{
    ConvertOptions.check_utf8 = false;
    ReadOptions.block_size = DEFAULT_BLOCK_SIZE;
    ReadOptions.use_threads = false;
    ReadOptions.autogenerate_column_names = false;
    if (header) {
        // !autogenerate + column_names.empty() => read from CSV
        ResultColumns.reserve(columns.size());

        for (auto& [name, type] : columns) {
            ResultColumns.push_back(name);
            std::string columnName(name.data(), name.size());
            ConvertOptions.column_types[columnName] = NArrow::GetArrowType(type);
        }
    } else if (!columns.empty()) {
        // !autogenerate + !column_names.empty() => specified columns
        ReadOptions.column_names.reserve(columns.size());

        for (auto& [name, type] : columns) {
            std::string columnName(name.data(), name.size());
            ReadOptions.column_names.push_back(columnName);
            ConvertOptions.column_types[columnName] = NArrow::GetArrowType(type);
        }
#if 0
    } else {
        ReadOptions.autogenerate_column_names = true;
#endif
    }

    SetNullValue(); // set default null value
}

std::shared_ptr<arrow::RecordBatch> TArrowCSV::ReadNext(const TString& csv, TString& errString) {
    if (!Reader) {
        if (ConvertOptions.column_types.empty()) {
            errString = ErrorPrefix() + "no columns specified";
            return {};
        }

        auto buffer = std::make_shared<NArrow::TBufferOverString>(csv);
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

    std::shared_ptr<arrow::RecordBatch> batch;
    Reader->ReadNext(&batch).ok();

    if (batch && !ResultColumns.empty()) {
        batch = NArrow::ExtractColumns(batch, ResultColumns);
        if (!batch) {
            errString = ErrorPrefix() + "not all result columns present";
        }
    }
    return batch;
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
