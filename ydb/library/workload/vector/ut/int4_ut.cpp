#include <ydb/library/workload/vector/vector_data_generator.h>
#include <ydb/library/workload/vector/vector_enums.h>
#include <ydb/library/workload/vector/vector_workload_generator.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/testing/unittest/registar.h>

#include <cmath>
#include <util/system/tempfile.h>
#include <util/folder/tempdir.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_nested.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>
#include <util/stream/file.h>

using namespace NYdbWorkload;

namespace {
    template <typename T>
    void CheckInt4Embedding(TStringBuf bytes, size_t dimension) {
        UNIT_ASSERT_VALUES_EQUAL(bytes.size(), (dimension + 1) / 2 + 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(bytes.back()), static_cast<ui8>(Format<T>));
        NKnnVectorSerialization::TDeserializer<T> decoder(bytes);
        UNIT_ASSERT_VALUES_EQUAL(decoder.GetElementCount(), dimension);
        UNIT_ASSERT_VALUES_EQUAL(decoder.GetElementCount(), dimension);
        decoder.DoDeserialize([](const T& value) {
            const float decoded = static_cast<float>(value);
            UNIT_ASSERT(std::isfinite(decoded));
            UNIT_ASSERT(T::MinValue <= decoded && decoded <= T::MaxValue);
        });
    }
}

Y_UNIT_TEST_SUITE(VectorWorkloadInt4) {
    Y_UNIT_TEST_TWIN(Int4Options, Signed) {
        NVector::TVectorOpts vectorOpts;
        NLastGetopt::TOpts opts;
        NVector::ConfigureVectorOpts(opts, &vectorOpts);
        const char* vectorType = Signed ? "int4" : "uint4";
        UNIT_ASSERT_STRING_CONTAINS(opts.GetLongOption("vector-type").GetHelp(), vectorType);
        const char* argv[] = {"vector", "--vector-type", vectorType, "--vector-dimension", "67"};
        NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
        UNIT_ASSERT_VALUES_EQUAL(vectorOpts.VectorType, vectorType);
        UNIT_ASSERT_VALUES_EQUAL(vectorOpts.VectorDimension, 67);
    }

    Y_UNIT_TEST_QUAD(Int4BulkData, Signed, Prefixed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        TVectorWorkloadParams params;
        params.KmeansTreePrefixed = Prefixed;
        params.BulkSize = 3;
        TWorkloadVectorGenerateDataInitializer initializer(params);
        NLastGetopt::TOpts opts;
        initializer.ConfigureOpts(opts);
        NVector::ConfigureVectorOpts(opts, &params.VectorOpts);
        const char* argv[] = {"generator", "--rows", "5", "--prefix-count", "2", "--seed", "42", "--vector-type", Signed ? "int4" : "uint4", "--vector-dimension", "67"};
        NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
        const auto generators = initializer.GetBulkInitialData();
        UNIT_ASSERT_VALUES_EQUAL(generators.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(generators.front()->GetSize(), 5);

        size_t rowCount = 0;
        for (const size_t expectedBatchSize : {3, 2}) {
            const auto portions = generators.front()->GenerateDataPortion();
            UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(portions.front()->GetSize(), expectedBatchSize);
            auto* data = std::get_if<IBulkDataGenerator::TDataPortion::TArrow>(&portions.front()->MutableData());
            UNIT_ASSERT(data);
            arrow::ipc::DictionaryMemo dictionary;
            arrow::io::BufferReader schemaBuffer(arrow::util::string_view(data->Schema.data(), data->Schema.size()));
            const auto schema = arrow::ipc::ReadSchema(&schemaBuffer, &dictionary).ValueOrDie();
            arrow::io::BufferReader batchBuffer(arrow::util::string_view(data->Data.data(), data->Data.size()));
            const auto batch = arrow::ipc::ReadRecordBatch(schema, &dictionary, {}, &batchBuffer).ValueOrDie();
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), expectedBatchSize);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), Prefixed ? 3 : 2);
            const auto ids = std::static_pointer_cast<arrow::UInt64Array>(batch->GetColumnByName("id"));
            const auto embeddings = std::static_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("embedding"));
            UNIT_ASSERT(ids);
            UNIT_ASSERT(embeddings);
            for (size_t i = 0; i < expectedBatchSize; ++i, ++rowCount) {
                UNIT_ASSERT_VALUES_EQUAL(ids->Value(i), rowCount);
                const auto bytes = embeddings->GetView(i);
                CheckInt4Embedding<T>(TStringBuf(bytes.data(), bytes.size()), params.VectorOpts.VectorDimension);
                if constexpr (Prefixed) {
                    const auto prefixes = std::static_pointer_cast<arrow::UInt64Array>(batch->GetColumnByName("prefix"));
                    UNIT_ASSERT(prefixes);
                    UNIT_ASSERT_VALUES_EQUAL(prefixes->Value(i), rowCount % 2);
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(rowCount, 5);
        UNIT_ASSERT(generators.front()->GenerateDataPortion().empty());
    }

    Y_UNIT_TEST_QUAD(Int4Upsert, Signed, Prefixed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        TVectorWorkloadParams params;
        params.VectorOpts.VectorType = Signed ? "int4" : "uint4";
        params.VectorOpts.VectorDimension = 67;
        params.UpsertBulkSize = 3;
        params.UpsertPrefixed = Prefixed;
        params.RunWorkloadType = static_cast<int>(EWorkloadRunType::Upsert);
        params.Metric = NYdb::NTable::TVectorIndexSettings::EMetric::Euclidean;
        TVectorWorkloadGenerator generator(&params);
        generator.Init();
        const auto queries = generator.GetWorkload(static_cast<int>(EWorkloadRunType::Upsert));
        UNIT_ASSERT_VALUES_EQUAL(queries.size(), 1);
        const auto rows = queries.front().Params.GetValue("$rows");
        UNIT_ASSERT(rows);
        NYdb::TValueParser parser(*rows);
        parser.OpenList();
        size_t count = 0;
        while (parser.TryNextListItem()) {
            parser.OpenStruct();
            bool embeddingSeen = false;
            bool prefixSeen = false;
            while (parser.TryNextMember()) {
                if (parser.GetMemberName() == "embedding") {
                    const auto& bytes = parser.GetString();
                    CheckInt4Embedding<T>(TStringBuf(bytes.data(), bytes.size()), params.VectorOpts.VectorDimension);
                    embeddingSeen = true;
                } else if (parser.GetMemberName() == "prefix") {
                    UNIT_ASSERT(parser.GetUint64() < params.UpsertPrefixCount);
                    prefixSeen = true;
                } else {
                    parser.GetUint64();
                }
            }
            parser.CloseStruct();
            UNIT_ASSERT(embeddingSeen);
            UNIT_ASSERT_VALUES_EQUAL(prefixSeen, Prefixed);
            ++count;
        }
        parser.CloseList();
        UNIT_ASSERT_VALUES_EQUAL(count, params.UpsertBulkSize);
    }

    Y_UNIT_TEST_QUAD(Int4CsvImport, Signed, Directory) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        TTempDir directory;
        auto file = TTempFileHandle::InDir(directory.Path(), "int4", "csv");
        {
            TFileOutput output(file.Name());
            output << "1,\"[-9 0 19]\"\n";
        }
        TVectorWorkloadParams params;
        params.KeyColumns = {"id"};
        params.EmbeddingColumn = "embedding";
        params.VectorOpts.VectorType = Signed ? "int4" : "uint4";
        params.VectorOpts.VectorDimension = 3;
        TWorkloadVectorFilesDataInitializer initializer(params);
        NLastGetopt::TOpts opts;
        initializer.ConfigureOpts(opts);
        const char* argv[] = {"files", "--input", Directory ? directory.Name().c_str() : file.Name().c_str(), "--format", "csv"};
        NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
        const auto generators = initializer.GetBulkInitialData();
        UNIT_ASSERT_VALUES_EQUAL(generators.size(), 1);
        const auto portions = generators.front()->GenerateDataPortion();
        UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
        auto* csv = std::get_if<IBulkDataGenerator::TDataPortion::TCsv>(&portions.front()->MutableData());
        UNIT_ASSERT(csv);
        TStringBuilder expected;
        NKnnVectorSerialization::TSerializer<T> serializer(&expected.Out);
        for (int value : {-9, 0, 19}) { serializer.HandleElement(value); }
        serializer.Finish();
        UNIT_ASSERT_STRING_CONTAINS(csv->Data, expected);
        UNIT_ASSERT(generators.front()->GenerateDataPortion().empty());
    }

    Y_UNIT_TEST_TWIN(Int4ParquetImport, Signed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        auto file = TTempFileHandle::InCurrentDir("int4", "parquet");
        arrow::UInt64Builder ids;
        UNIT_ASSERT(ids.Append(1).ok());
        auto values = std::make_shared<arrow::FloatBuilder>();
        arrow::ListBuilder lists(arrow::default_memory_pool(), values);
        UNIT_ASSERT(lists.Append().ok());
        for (float value : {-9.0f, 0.0f, 19.0f}) {
            UNIT_ASSERT(values->Append(value).ok());
        }
        std::shared_ptr<arrow::Array> idArray, embeddings;
        UNIT_ASSERT(ids.Finish(&idArray).ok());
        UNIT_ASSERT(lists.Finish(&embeddings).ok());
        const auto table = arrow::Table::Make(arrow::schema({
            arrow::field("id", arrow::uint64()), arrow::field("embedding", arrow::list(arrow::float32()))
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
        params.VectorOpts.VectorType = Signed ? "int4" : "uint4";
        params.VectorOpts.VectorDimension = 3;
        TWorkloadVectorFilesDataInitializer initializer(params);
        NLastGetopt::TOpts opts;
        initializer.ConfigureOpts(opts);
        const char* argv[] = {"files", "--input", file.Name().c_str()};
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
        NKnnVectorSerialization::TDeserializer<T> decoder(TStringBuf(bytes.data(), bytes.size()));
        const int expected[] = {T::MinValue, 0, T::MaxValue};
        size_t index = 0;
        decoder.DoDeserialize([&](const T& value) { UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(value), expected[index++]); });
        UNIT_ASSERT_VALUES_EQUAL(index, 3);
        UNIT_ASSERT(generators.front()->GenerateDataPortion().empty());
    }

    Y_UNIT_TEST_TWIN(CsvFloatAndMalformedInput, Malformed) {
        auto file = TTempFileHandle::InCurrentDir("int4", "csv");
        {
            TFileOutput output(file.Name());
            output << (Malformed ? "1,\"[broken]\"\n" : "1,\"[1.5 -2 3]\"\n");
        }
        TVectorWorkloadParams params;
        params.KeyColumns = {"id"};
        params.EmbeddingColumn = "embedding";
        TWorkloadVectorFilesDataInitializer initializer(params);
        NLastGetopt::TOpts opts;
        initializer.ConfigureOpts(opts);
        const char* argv[] = {"files", "--input", file.Name().c_str(), "--format", "csv"};
        NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
        const auto generators = initializer.GetBulkInitialData();
        UNIT_ASSERT_VALUES_EQUAL(generators.size(), 1);
        if constexpr (Malformed) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(generators.front()->GenerateDataPortion(), yexception, "Cannot parse float");
        } else {
            const auto portions = generators.front()->GenerateDataPortion();
            UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
            auto* csv = std::get_if<IBulkDataGenerator::TDataPortion::TCsv>(&portions.front()->MutableData());
            UNIT_ASSERT(csv);
            TStringBuilder expected;
            NKnnVectorSerialization::TSerializer<float> serializer(&expected.Out);
            for (float value : {1.5f, -2.0f, 3.0f}) { serializer.HandleElement(value); }
            serializer.Finish();
            UNIT_ASSERT_STRING_CONTAINS(csv->Data, expected);
        }
    }

}
