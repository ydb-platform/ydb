#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>

#include <util/generic/string.h>

namespace NTestUtils {

// Parses Parquet file content into a single-chunk arrow::Table.
inline std::shared_ptr<arrow::Table> ReadParquet(const TString& data) {
    auto input = std::make_shared<arrow::io::BufferReader>(
        reinterpret_cast<const uint8_t*>(data.data()), static_cast<int64_t>(data.size()));

    parquet::arrow::FileReaderBuilder builder;
    UNIT_ASSERT_C(builder.Open(input).ok(), "Failed to open Parquet file");

    std::unique_ptr<parquet::arrow::FileReader> reader;
    UNIT_ASSERT_C(builder.Build(&reader).ok(), "Failed to build Parquet reader");

    std::shared_ptr<arrow::Table> table;
    const auto status = reader->ReadTable(&table);
    UNIT_ASSERT_C(status.ok(), status.message());

    auto combined = table->CombineChunks();
    UNIT_ASSERT_C(combined.ok(), combined.status().message());
    return combined.ValueOrDie();
}

} // namespace NTestUtils
