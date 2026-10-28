#include <ydb/library/workload/vector/vector_enums.h>
#include <ydb/library/workload/vector/vector_workload_generator.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>

using namespace NYdbWorkload;

Y_UNIT_TEST_SUITE(VectorWorkload) {
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
                    UNIT_ASSERT_VALUES_EQUAL(bytes.size(), 68);
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(bytes.back()), static_cast<ui8>(Format<T>));
                    NKnnVectorSerialization::TDeserializer<T> decoder(TStringBuf(bytes.data(), bytes.size()));
                    decoder.DoDeserialize([](const T& value) {
                        const float decoded = static_cast<float>(value);
                        UNIT_ASSERT(std::isfinite(decoded));
                        UNIT_ASSERT(-1.0f <= decoded && decoded <= 1.0f);
                    });
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
}
