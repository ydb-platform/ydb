#include <ydb/library/workload/vector/vector_data_generator.h>
#include <ydb/library/testlib/helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_nested.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/csv/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>

#include <array>

using namespace NYdbWorkload;

namespace {

    struct TVectorCase {
        const char* Type;
        TString Bytes;
    };

    const TString FloatEmbedding("\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x20\x40\x01", 13);

    // Encodings of [1, 0, 2.5], including each format's tag and padding trailer.
    const std::array<TVectorCase, 10> VectorCases{{
        {"float", FloatEmbedding},
        {"float16", TString("\x00\x3c\x00\x00\x00\x41\x04", 7)},
        {"bfloat16", TString("\x80\x3f\x00\x00\x20\x40\x05", 7)},
        {"float8_e4m3", TString("\x38\x00\x42\x06", 4)},
        {"float8_e5m2", TString("\x3c\x00\x41\x07", 4)},
        {"uint8", TString("\x01\x00\x02\x02", 4)},
        {"int8", TString("\x01\x00\x02\x03", 4)},
        {"uint4", TString("\x01\x02\x01\x08", 4)},
        {"int4", TString("\x01\x02\x01\x09", 4)},
        {"bit", TString("\x05\x05\x0a", 3)},
    }};

} // namespace

Y_UNIT_TEST_SUITE(VectorWorkloadFileImport) {

    Y_UNIT_TEST_QUAD(CsvFloatListsUseSelectedFormat, Directory, Tsv) {
        TTempDir directory;
        const char* format = Tsv ? "tsv" : "csv";
        auto file = TTempFileHandle::InDir(directory.Path(), "vectors", format);
        {
            TFileOutput output(file.Name());
            output << (Tsv ? "1\t[1, 0, 2.5]\n" : "1,\"[1 0 2.5]\"\n");
        }

        for (const auto& vector : VectorCases) {
            TVectorWorkloadParams params;
            params.KeyColumns = {"id"};
            params.EmbeddingColumn = "embedding";
            TWorkloadVectorFilesDataInitializer initializer(params);
            NLastGetopt::TOpts opts;
            initializer.ConfigureOpts(opts);
            params.ConfigureOpts(opts, TWorkloadParams::ECommandType::Import, 0);
            const char* argv[] = {"files", "--input", Directory ? directory.Name().c_str() : file.Name().c_str(), "--format", format,
                "--vector-type", vector.Type, "--vector-dimension", "3"};
            NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
            const auto generators = initializer.GetBulkInitialData();
            UNIT_ASSERT_VALUES_EQUAL(generators.size(), 1);
            const auto portions = generators.front()->GenerateDataPortion();
            UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
            auto* csv = std::get_if<IBulkDataGenerator::TDataPortion::TCsv>(&portions.front()->MutableData());
            UNIT_ASSERT(csv);
            const auto input = std::make_shared<arrow::io::BufferReader>(arrow::util::string_view(csv->Data.data(), csv->Data.size()));
            auto convertOptions = arrow::csv::ConvertOptions::Defaults();
            convertOptions.column_types["embedding"] = arrow::binary();
            auto parseOptions = arrow::csv::ParseOptions::Defaults();
            parseOptions.newlines_in_values = true;
            const auto reader = arrow::csv::TableReader::Make(arrow::io::default_io_context(), input,
                arrow::csv::ReadOptions::Defaults(), parseOptions, convertOptions).ValueOrDie();
            const auto table = reader->Read().ValueOrDie();
            UNIT_ASSERT_VALUES_EQUAL(table->num_rows(), 1);
            const auto column = std::static_pointer_cast<arrow::BinaryArray>(table->GetColumnByName("embedding")->chunk(0));
            const auto bytes = column->GetView(0);
            UNIT_ASSERT_VALUES_EQUAL_C(TStringBuf(bytes.data(), bytes.size()), vector.Bytes, vector.Type);
            UNIT_ASSERT(generators.front()->GenerateDataPortion().empty());
        }
    }

    Y_UNIT_TEST_TWIN(ParquetFloatListsAndBinaryEmbeddings, Binary) {
        for (const auto& vector : VectorCases) {
            auto file = TTempFileHandle::InCurrentDir("vectors", "parquet");
            arrow::UInt64Builder ids;
            UNIT_ASSERT(ids.Append(1).ok());
            std::shared_ptr<arrow::Array> idArray, embeddings;
            UNIT_ASSERT(ids.Finish(&idArray).ok());
            if constexpr (Binary) {
                arrow::BinaryBuilder builder;
                UNIT_ASSERT(builder.Append(vector.Bytes.data(), vector.Bytes.size()).ok());
                UNIT_ASSERT(builder.Finish(&embeddings).ok());
            } else {
                auto values = std::make_shared<arrow::FloatBuilder>();
                arrow::ListBuilder lists(arrow::default_memory_pool(), values);
                UNIT_ASSERT(lists.Append().ok());
                for (float value : {1.0f, 0.0f, 2.5f}) {
                    UNIT_ASSERT(values->Append(value).ok());
                }
                UNIT_ASSERT(lists.Finish(&embeddings).ok());
            }
            const auto table = arrow::Table::Make(arrow::schema({
                arrow::field("id", arrow::uint64()), arrow::field("embedding", embeddings->type())
            }), {idArray, embeddings});
            const auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
            UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1).ok());
            const auto parquetData = sink->Finish().ValueOrDie();
            {
                TFileOutput output(file.Name());
                output.Write(parquetData->data(), parquetData->size());
            }

            TVectorWorkloadParams params;
            params.KeyColumns = {"id"};
            params.EmbeddingColumn = "embedding";
            TWorkloadVectorFilesDataInitializer initializer(params);
            NLastGetopt::TOpts opts;
            initializer.ConfigureOpts(opts);
            params.ConfigureOpts(opts, TWorkloadParams::ECommandType::Import, 0);
            const char* argv[] = {"files", "--input", file.Name().c_str(), "--vector-type", vector.Type, "--vector-dimension", "3"};
            NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
            const auto generators = initializer.GetBulkInitialData();
            UNIT_ASSERT_VALUES_EQUAL(generators.size(), 1);
            const auto portions = generators.front()->GenerateDataPortion();
            UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
            auto* data = std::get_if<IBulkDataGenerator::TDataPortion::TArrow>(&portions.front()->MutableData());
            UNIT_ASSERT(data);
            arrow::ipc::DictionaryMemo dictionary;
            arrow::io::BufferReader schemaBuffer(arrow::util::string_view(data->Schema.data(), data->Schema.size()));
            const auto schema = arrow::ipc::ReadSchema(&schemaBuffer, &dictionary).ValueOrDie();
            arrow::io::BufferReader batchBuffer(arrow::util::string_view(data->Data.data(), data->Data.size()));
            const auto batch = arrow::ipc::ReadRecordBatch(schema, &dictionary, {}, &batchBuffer).ValueOrDie();
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 1);
            const auto column = std::static_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("embedding"));
            UNIT_ASSERT(column);
            const auto bytes = column->GetView(0);
            UNIT_ASSERT_VALUES_EQUAL_C(TStringBuf(bytes.data(), bytes.size()), vector.Bytes, vector.Type);
            UNIT_ASSERT(generators.front()->GenerateDataPortion().empty());
        }
    }
}
