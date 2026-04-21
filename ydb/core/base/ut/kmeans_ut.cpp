#include "kmeans_clusters.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/testing/unittest/registar.h>
#include <cmath>
#include <util/stream/str.h>
#include <util/generic/xrange.h>

namespace NKikimr::NKMeans {

namespace {

    template <typename T>
    TString SerializeVector(std::initializer_list<T> values) {
        TString result;
        TStringOutput output(result);
        NKnnVectorSerialization::TSerializer<T> serializer(&output);
        for (const auto& value : values) {
            serializer.HandleElement(value);
        }
        serializer.Finish();
        return result;
    }

    template <typename T>
    TVector<T> DeserializeVector(const TStringBuf data) {
        TVector<T> result;
        NKnnVectorSerialization::TDeserializer<T> deserializer(data);
        result.reserve(deserializer.GetElementCount());
        deserializer.DoDeserialize([&](const T& value) {
            result.push_back(value);
        });
        return result;
    }

    Ydb::Table::VectorIndexSettings MakeCosineSettings(
        Ydb::Table::VectorIndexSettings::VectorType vectorType,
        ui32 dimensions)
    {
        Ydb::Table::VectorIndexSettings settings;
        settings.set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE);
        settings.set_vector_type(vectorType);
        settings.set_vector_dimension(dimensions);
        return settings;
    }

} // namespace

Y_UNIT_TEST_SUITE(NKMeans) {

    Y_UNIT_TEST(ValidateSettings) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "vector index settings should be set");

        auto& vectorIndex = *settings.mutable_settings();

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "either distance or similarity should be set");

        vectorIndex.set_metric((::Ydb::Table::VectorIndexSettings_Metric)UINT32_MAX);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid metric");

        vectorIndex.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "vector_type should be set");

        vectorIndex.set_vector_type((::Ydb::Table::VectorIndexSettings_VectorType)UINT32_MAX);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid vector_type");

        vectorIndex.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "vector_dimension should be set");

        vectorIndex.set_vector_dimension(UINT32_MAX);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid vector_dimension");

        vectorIndex.set_vector_dimension(16384);
        vectorIndex.GetReflection()->MutableUnknownFields(&vectorIndex)->AddVarint(10000, 100000);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "vector index settings contain 1 unsupported parameter(s)");

        vectorIndex.GetReflection()->MutableUnknownFields(&vectorIndex)->DeleteByNumber(10000);
        settings.GetReflection()->MutableUnknownFields(&settings)->AddVarint(10000, 100000);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "vector index settings contain 1 unsupported parameter(s)");

        settings.GetReflection()->MutableUnknownFields(&settings)->DeleteByNumber(10000);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "levels should be set");

        settings.set_levels(UINT32_MAX);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid levels");

        settings.set_levels(16);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "clusters should be set");

        settings.set_clusters(UINT32_MAX);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid clusters");

        settings.set_clusters(2048);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid clusters^levels");

        settings.set_clusters(4);
        settings.set_levels(16);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid clusters^levels");

        settings.set_clusters(1024);
        settings.set_levels(3);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid vector_dimension*clusters");

        settings.set_clusters(50);
        settings.set_levels(3);
        settings.set_overlap_clusters(51);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "overlap_clusters should be less than or equal to clusters");

        settings.set_overlap_clusters(0);
        settings.set_overlap_ratio(-1.2);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "overlap_ratio should be >= 0");

        settings.set_overlap_ratio(0);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(CosineCentroidAggregatesNormalizedFloatVectors) {
        TString error;
        auto clusters = CreateClusters(
            MakeCosineSettings(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2),
            1,
            error);

        UNIT_ASSERT_C(clusters, error);
        UNIT_ASSERT(clusters->SetClusters(TVector<TString>{clusters->GetEmptyRow()}));

        const auto first = SerializeVector<float>({100.0f, 0.0f});
        const auto second = SerializeVector<float>({1.0f, 1.0f});
        clusters->AggregateToCluster(0, TArrayRef<const char>(first.data(), first.size()));
        clusters->AggregateToCluster(0, TArrayRef<const char>(second.data(), second.size()));

        UNIT_ASSERT(clusters->NextRound());
        const auto centroid = DeserializeVector<float>(clusters->GetClusters().front());
        UNIT_ASSERT_VALUES_EQUAL(centroid.size(), 2);
        UNIT_ASSERT_GT(centroid[1], 0.0f);
        const double norm = std::sqrt(
            static_cast<double>(centroid[0]) * static_cast<double>(centroid[0]) +
            static_cast<double>(centroid[1]) * static_cast<double>(centroid[1]));
        UNIT_ASSERT_DOUBLES_EQUAL(norm, 1.0, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(centroid[0]) / static_cast<double>(centroid[1]),
            1.0 + std::sqrt(2.0),
            1e-4);
    }

    Y_UNIT_TEST(CosineCentroidKeepsDirectionForInt8Vectors) {
        TString error;
        auto clusters = CreateClusters(
            MakeCosineSettings(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8, 2),
            1,
            error);

        UNIT_ASSERT_C(clusters, error);
        UNIT_ASSERT(clusters->SetClusters(TVector<TString>{clusters->GetEmptyRow()}));

        const auto first = SerializeVector<i8>({100, 0});
        const auto second = SerializeVector<i8>({1, 1});
        clusters->AggregateToCluster(0, TArrayRef<const char>(first.data(), first.size()));
        clusters->AggregateToCluster(0, TArrayRef<const char>(second.data(), second.size()));

        UNIT_ASSERT(clusters->NextRound());
        const auto centroid = DeserializeVector<i8>(clusters->GetClusters().front());
        UNIT_ASSERT_VALUES_EQUAL(centroid.size(), 2);
        UNIT_ASSERT_UNEQUAL(centroid[1], 0);
        const auto ratio = static_cast<double>(centroid[0]) / static_cast<double>(centroid[1]);
        UNIT_ASSERT_VALUES_EQUAL(centroid[0], 127);
        UNIT_ASSERT_GT(ratio, 2.0);
        UNIT_ASSERT_LT(ratio, 3.0);
    }

    Y_UNIT_TEST(CosineCentroidUsesNormalizedBitVotes) {
        TString error;
        auto clusters = CreateClusters(
            MakeCosineSettings(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT, 5),
            1,
            error);

        UNIT_ASSERT_C(clusters, error);
        UNIT_ASSERT(clusters->SetClusters(TVector<TString>{clusters->GetEmptyRow()}));

        const auto first = SerializeVector<bool>({true, true, true, true, false});
        const auto second = SerializeVector<bool>({true, false, false, false, true});
        clusters->AggregateToCluster(0, TArrayRef<const char>(first.data(), first.size()));
        clusters->AggregateToCluster(0, TArrayRef<const char>(second.data(), second.size()));

        UNIT_ASSERT(clusters->NextRound());
        const auto centroid = DeserializeVector<bool>(clusters->GetClusters().front());
        UNIT_ASSERT_VALUES_EQUAL(centroid.size(), 5);
        UNIT_ASSERT(centroid[0]);
        UNIT_ASSERT(!centroid[1]);
        UNIT_ASSERT(!centroid[2]);
        UNIT_ASSERT(!centroid[3]);
        UNIT_ASSERT(!centroid[4]);
    }
}

}
