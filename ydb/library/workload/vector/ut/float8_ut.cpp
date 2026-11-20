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

using namespace NYdbWorkload;

namespace {
    template <typename T>
    void CheckFloat8Embedding(TStringBuf bytes, size_t dimension) {
        UNIT_ASSERT_VALUES_EQUAL(bytes.size(), dimension + 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(bytes.back()), static_cast<ui8>(Format<T>));
        NKnnVectorSerialization::TDeserializer<T> decoder(bytes);
        UNIT_ASSERT_VALUES_EQUAL(decoder.GetElementCount(), dimension);
        decoder.DoDeserialize([](const T& value) {
            const float decoded = static_cast<float>(value);
            UNIT_ASSERT(std::isfinite(decoded));
            UNIT_ASSERT(-1.0f <= decoded && decoded <= 1.0f);
        });
    }
}

Y_UNIT_TEST_SUITE(VectorWorkload) {
    Y_UNIT_TEST_TWIN(Float8Options, E5M2) {
        NVector::TVectorOpts vectorOpts;
        NLastGetopt::TOpts opts;
        NVector::ConfigureVectorOpts(opts, &vectorOpts);
        const char* vectorType = E5M2 ? "float8_e5m2" : "float8_e4m3";
        UNIT_ASSERT_STRING_CONTAINS(opts.GetLongOption("vector-type").GetHelp(), vectorType);
        const char* argv[] = {"vector", "--vector-type", vectorType, "--vector-dimension", "67"};
        NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
        UNIT_ASSERT_VALUES_EQUAL(vectorOpts.VectorType, vectorType);
        UNIT_ASSERT_VALUES_EQUAL(vectorOpts.VectorDimension, 67);
    }

    Y_UNIT_TEST_QUAD(Float8BulkData, E5M2, Prefixed) {
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        TVectorWorkloadParams params;
        params.VectorOpts.VectorType = E5M2 ? "float8_e5m2" : "float8_e4m3";
        params.VectorOpts.VectorDimension = 67;
        params.KmeansTreePrefixed = Prefixed;
        params.BulkSize = 3;
        TWorkloadVectorGenerateDataInitializer initializer(params);
        NLastGetopt::TOpts opts;
        initializer.ConfigureOpts(opts);
        const char* argv[] = {"generator", "--rows", "5", "--prefix-count", "2", "--seed", "42"};
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
            const auto embeddings = std::static_pointer_cast<arrow::StringArray>(batch->GetColumnByName("embedding"));
            UNIT_ASSERT(ids);
            UNIT_ASSERT(embeddings);
            for (size_t i = 0; i < expectedBatchSize; ++i, ++rowCount) {
                UNIT_ASSERT_VALUES_EQUAL(ids->Value(i), rowCount);
                const auto bytes = embeddings->GetView(i);
                CheckFloat8Embedding<T>(TStringBuf(bytes.data(), bytes.size()), params.VectorOpts.VectorDimension);
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

    Y_UNIT_TEST_QUAD(Float8Upsert, E5M2, Prefixed) {
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        TVectorWorkloadParams params;
        params.VectorOpts.VectorType = E5M2 ? "float8_e5m2" : "float8_e4m3";
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
                    CheckFloat8Embedding<T>(TStringBuf(bytes.data(), bytes.size()), params.VectorOpts.VectorDimension);
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

    Y_UNIT_TEST(Uint8Upsert) {
        TVectorWorkloadParams params;
        params.VectorOpts.VectorType = "uint8";
        params.VectorOpts.VectorDimension = 67;
        params.UpsertBulkSize = 1;
        params.RunWorkloadType = static_cast<int>(EWorkloadRunType::Upsert);
        TVectorWorkloadGenerator generator(&params);
        generator.Init();
        const auto queries = generator.GetWorkload(static_cast<int>(EWorkloadRunType::Upsert));
        UNIT_ASSERT_VALUES_EQUAL(queries.size(), 1);
        const auto rows = queries.front().Params.GetValue("$rows");
        UNIT_ASSERT(rows);
        NYdb::TValueParser parser(*rows);
        parser.OpenList();
        UNIT_ASSERT(parser.TryNextListItem());
        parser.OpenStruct();
        bool embeddingSeen = false;
        while (parser.TryNextMember()) {
            if (parser.GetMemberName() == "embedding") {
                const auto& bytes = parser.GetString();
                NKnnVectorSerialization::TDeserializer<ui8> decoder(TStringBuf(bytes.data(), bytes.size()));
                UNIT_ASSERT_VALUES_EQUAL(decoder.GetElementCount(), params.VectorOpts.VectorDimension);
                embeddingSeen = true;
            } else {
                parser.GetUint64();
            }
        }
        parser.CloseStruct();
        UNIT_ASSERT(embeddingSeen);
        UNIT_ASSERT(!parser.TryNextListItem());
        parser.CloseList();
    }
}
