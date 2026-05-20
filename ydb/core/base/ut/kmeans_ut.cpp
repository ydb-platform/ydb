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

    Y_UNIT_TEST(ComputeOptimalClustersBasic) {
        // L=1: C = sqrt(T * P * N) = sqrt(10 * 1 * 10000) = sqrt(100000) ≈ 316
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(1, 10, 10000, 1.0), 316);

        // L=2: C = ((2 * 10 * 1 * 10000) / (1 + 10))^(1/3) = (200000/11)^(1/3) ≈ 26.3 → 26
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(2, 10, 10000, 1.0), 26);

        // L=3: C = ((3 * 10 * 1 * 10000) / (1 + 20))^(1/4) = (300000/21)^(1/4) ≈ 10.9 → 11
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(3, 10, 10000, 1.0), 11);
    }

    Y_UNIT_TEST(ComputeOptimalClustersWithOverlap) {
        // Overlap: L=1, T=4, P=2.5, N=10000: C = sqrt(4 * 2.5 * 10000) = sqrt(100000) ≈ 316
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(1, 4, 10000, 2.5), 316);

        // Overlap: L=2, T=4, P=2.5, N=10000: C = ((2*4*2.5*10000)/(1+4))^(1/3) = (200000/5)^(1/3) = 40000^(1/3) ≈ 34.2 → 34
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(2, 4, 10000, 2.5), 34);
    }

    Y_UNIT_TEST(ComputeOptimalClustersEdgeCases) {
        // L=1, T=10, N=1, P=1: C = sqrt(10) ≈ 3.16 → 3
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(1, 10, 1, 1.0), 3);

        // L=5, T=10, N=1, P=1: very small, clamped to MinClusters(2)
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(5, 10, 1, 1.0), 2);

        // Clamped to MaxClusters(2048) when result > 2048
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(1, 10, 1000000000, 1.0), 2048);

        // rowCount = 0: division by zero gives 0 → clamped to MinClusters
        UNIT_ASSERT_VALUES_EQUAL(ComputeOptimalClusters(1, 10, 0, 1.0), 2);
    }

    Y_UNIT_TEST(ComputeEfficiencyScoreBasic) {
        // S = C + C*T*(L-1) + T*N*P/(C^L)
        // L=1, C=100, T=10, N=1000, P=1: S = 100 + 0 + 10000/100 = 200
        UNIT_ASSERT_DOUBLES_EQUAL(ComputeEfficiencyScore(1, 100, 10, 1000, 1.0), 200.0, 0.001);

        // L=2, C=100, T=10, N=1000, P=1: S = 100 + 1000 + 10000/10000 = 1101
        UNIT_ASSERT_DOUBLES_EQUAL(ComputeEfficiencyScore(2, 100, 10, 1000, 1.0), 1101.0, 0.001);

        // L=1 with overlap: T=4, P=2.5: S = 100 + 0 + 4*1000*2.5/100 = 200
        UNIT_ASSERT_DOUBLES_EQUAL(ComputeEfficiencyScore(1, 100, 4, 1000, 2.5), 200.0, 0.001);
    }

    Y_UNIT_TEST(ComputeEfficiencyScoreMatchesWikiExamples) {
        // Wiki: N=485859, L=2, T=10 -> C=96 (rounded), S=1583
        ui64 c = ComputeOptimalClusters(2, 10, 485859, 1.0);
        UNIT_ASSERT_VALUES_EQUAL(c, 96);
        double score = ComputeEfficiencyScore(2, c, 10, 485859, 1.0);
        // S = 96 + 960 + 4858590/9216 = 1056 + 527.26... = 1583.26...
        UNIT_ASSERT_DOUBLES_EQUAL(score, 1583.26, 0.1);
    }

    Y_UNIT_TEST(AutoSelectKMeansNoneSpecified) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);

        // rowCount < 100 → levels=1, clusters=MinClusters(2)
        AutoSelectKMeansSettings(settings, 50);
        UNIT_ASSERT_VALUES_EQUAL(settings.levels(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 2);
        UNIT_ASSERT(ValidateSettings(settings, error));

        settings.clear_levels();
        settings.clear_clusters();

        // rowCount = 99 → levels=1, clusters=2
        AutoSelectKMeansSettings(settings, 99);
        UNIT_ASSERT_VALUES_EQUAL(settings.levels(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 2);
        UNIT_ASSERT(ValidateSettings(settings, error));

        settings.clear_levels();
        settings.clear_clusters();

        // rowCount = 100 → should auto-select (≥ threshold)
        AutoSelectKMeansSettings(settings, 100);
        UNIT_ASSERT(settings.levels() >= 1);
        UNIT_ASSERT(settings.clusters() >= 2);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansMissingLevels) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);
        settings.set_clusters(50);

        AutoSelectKMeansSettings(settings, 100000);
        UNIT_ASSERT(settings.has_levels() && settings.levels() >= 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 50);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansMissingClusters) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);
        settings.set_levels(2);

        AutoSelectKMeansSettings(settings, 100000);
        UNIT_ASSERT_VALUES_EQUAL(settings.levels(), 2);
        UNIT_ASSERT(settings.has_clusters() && settings.clusters() >= 2);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansBothSpecified) {
        Ydb::Table::KMeansTreeSettings settings;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);
        settings.set_levels(3);
        settings.set_clusters(50);

        AutoSelectKMeansSettings(settings, 100000);
        UNIT_ASSERT_VALUES_EQUAL(settings.levels(), 3);
        UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 50);
    }

    Y_UNIT_TEST(AutoSelectKMeansWithOverlap) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);
        settings.set_overlap_clusters(3);

        AutoSelectKMeansSettings(settings, 100000);
        UNIT_ASSERT(settings.levels() >= 1);
        UNIT_ASSERT(settings.clusters() >= 2);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansPrefixed) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);

        // rowCount = 1000000 with prefixed → N = sqrt(1000000) = 1000
        // non-prefixed with N=1000000 → C clamped to 2048
        // prefixed with N=1000 → C ≈ 100
        AutoSelectKMeansSettings(settings, 1000000, true);
        UNIT_ASSERT(settings.levels() >= 1);
        UNIT_ASSERT(settings.clusters() >= 2);
        UNIT_ASSERT(settings.clusters() < 500);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansLargeRowCount) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);

        AutoSelectKMeansSettings(settings, 1000000000);
        UNIT_ASSERT(settings.levels() >= 1 && settings.levels() <= 16);
        UNIT_ASSERT(settings.clusters() >= 2 && settings.clusters() <= 2048);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansZeroRowCount) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);

        AutoSelectKMeansSettings(settings, 0);
        UNIT_ASSERT_VALUES_EQUAL(settings.levels(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 2);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(NeedsVectorSettingsAutoSelect) {
        Ydb::Table::VectorIndexSettings settings;

        UNIT_ASSERT(NeedsVectorSettingsAutoSelect(settings));

        settings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        UNIT_ASSERT(NeedsVectorSettingsAutoSelect(settings));

        settings.set_vector_dimension(128);
        UNIT_ASSERT(!NeedsVectorSettingsAutoSelect(settings));
    }

    Y_UNIT_TEST(AutoSelectVectorSettings) {
        Ydb::Table::VectorIndexSettings settings;
        settings.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);

        UNIT_ASSERT(!AutoSelectVectorSettings(settings, TStringBuf()));

        auto embedding = SerializeVector<float>({1.0f, 2.0f, 3.0f, 4.0f});
        UNIT_ASSERT(AutoSelectVectorSettings(settings, embedding));
        UNIT_ASSERT_VALUES_EQUAL(settings.vector_type(), Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        UNIT_ASSERT_VALUES_EQUAL(settings.vector_dimension(), 4);

        settings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        settings.clear_vector_dimension();
        auto uint8embedding = SerializeVector<ui8>({10, 20, 30});
        UNIT_ASSERT(AutoSelectVectorSettings(settings, uint8embedding));
        UNIT_ASSERT_VALUES_EQUAL(settings.vector_type(), Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        UNIT_ASSERT_VALUES_EQUAL(settings.vector_dimension(), 3);
    }

    Y_UNIT_TEST(AutoSelectKMeansClampByVectorDimension) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        // Large dimension: max clusters = 4M / 16384 = 256
        settings.mutable_settings()->set_vector_dimension(16384);

        // Large row count would normally select clusters >= 257 (sqrt(10*1M)=3162, clamped to 2048)
        // But vector_dimension constraint caps at 256
        AutoSelectKMeansSettings(settings, 1000000);
        UNIT_ASSERT(settings.clusters() <= 256);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansClampByPowLevels) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(4);

        // Only levels specified → auto-select clusters. High levels cap clusters tighter.
        // For levels=4: clusters <= 2^(30/4) ≈ 181
        settings.set_levels(4);
        AutoSelectKMeansSettings(settings, 100000000);
        UNIT_ASSERT(settings.clusters() >= 2);
        UNIT_ASSERT(ValidateSettings(settings, error));

        // Reset, levels=6: clusters <= 2^(30/6) = 2^5 = 32
        settings.clear_clusters();
        settings.set_levels(6);
        AutoSelectKMeansSettings(settings, 100000000);
        UNIT_ASSERT(settings.clusters() >= 2);
        UNIT_ASSERT(settings.clusters() <= 32);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansClampBothMissing) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(16384);

        // Both levels and clusters missing → auto-select should respect vector_dimension cap
        AutoSelectKMeansSettings(settings, 1000000);
        UNIT_ASSERT(settings.clusters() <= 256); // 4M / 16384
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansClampNeverBelowMinClusters) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT);
        // Bit vector with dimension 1: extremely small per-dimension constraint
        settings.mutable_settings()->set_vector_dimension(1);

        // Both missing → clamp should not dip below MinClusters
        AutoSelectKMeansSettings(settings, 100);
        UNIT_ASSERT(settings.clusters() >= 2);
        UNIT_ASSERT(ValidateSettings(settings, error));
    }

    Y_UNIT_TEST(AutoSelectKMeansUserClustersNotClamped) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings.mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings.mutable_settings()->set_vector_dimension(16384);
        // User-specified clusters, even if exceeding vector_dimension constraint
        settings.set_clusters(500);

        // User-specified clusters should NOT be clamped
        AutoSelectKMeansSettings(settings, 100000);
        UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 500);
        // But ValidateSettings will catch the violation
        UNIT_ASSERT(!ValidateSettings(settings, error));
    }
}

}
