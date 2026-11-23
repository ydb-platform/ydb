#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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

// A Parquet file of the table, written the way the exporter does it.
inline TString WriteParquet(const std::shared_ptr<arrow::Table>& table, i64 rowGroupSize = 16) {
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    auto arrowProperties = parquet::ArrowWriterProperties::Builder();
    arrowProperties.store_schema();
    const auto status = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        sink,
        rowGroupSize,
        parquet::default_writer_properties(),
        arrowProperties.build());
    UNIT_ASSERT_C(status.ok(), status.ToString());

    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

// A Parquet file of columns of strings, any of which may be NULL.
inline TString BuildUtf8ColumnsParquet(
    const TVector<std::pair<TString, TVector<TMaybe<TString>>>>& columns,
    i64 rowGroupSize = 16)
{
    arrow::FieldVector fields;
    arrow::ArrayVector arrays;
    for (const auto& [name, values] : columns) {
        arrow::StringBuilder builder;
        for (const auto& value : values) {
            UNIT_ASSERT((value ? builder.Append(value->data(), value->size()) : builder.AppendNull()).ok());
        }

        std::shared_ptr<arrow::Array> array;
        UNIT_ASSERT(builder.Finish(&array).ok());
        fields.push_back(arrow::field(std::string(name), arrow::utf8()));
        arrays.push_back(std::move(array));
    }

    return WriteParquet(
        arrow::Table::Make(std::make_shared<arrow::Schema>(std::move(fields)), std::move(arrays)),
        rowGroupSize);
}

// A Parquet file of a table with a Utf8 key and a Utf8 value, which may be NULL.
inline TString BuildUtf8KeyValueParquet(
    const TVector<std::pair<TString, TMaybe<TString>>>& rows,
    i64 rowGroupSize = 16)
{
    TVector<TMaybe<TString>> keys;
    TVector<TMaybe<TString>> values;
    for (const auto& [key, value] : rows) {
        keys.push_back(key);
        values.push_back(value);
    }
    return BuildUtf8ColumnsParquet({{"key", std::move(keys)}, {"value", std::move(values)}}, rowGroupSize);
}

} // namespace NTestUtils
