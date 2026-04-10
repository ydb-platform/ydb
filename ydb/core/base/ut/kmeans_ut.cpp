#include "kmeans_clusters.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

#include <cmath>

namespace NKikimr::NKMeans {

Y_UNIT_TEST_SUITE(NKMeans) {

    Y_UNIT_TEST(ComputeOptimalClustersAndLevels_ZeroRows) {
        // Empty table must return minimum valid parameters
        auto [clusters, levels] = ComputeOptimalClustersAndLevels(0, 10, 1.0, 1500);
        UNIT_ASSERT_VALUES_EQUAL(clusters, 2u);
        UNIT_ASSERT_VALUES_EQUAL(levels, 1u);
    }

    Y_UNIT_TEST(ComputeOptimalClustersAndLevels_SmallTable) {
        // A small table (< threshold) should fit into a single level
        // N=100, T=10, P=1.0, sThresh=1500: cost with l=1 is C + 0 + 10*100/C
        // minimizing C + 1000/C => C ~ sqrt(1000) ~ 31.6, clusters=32
        // s = 32 + 10*100/32 = 32 + 31.25 = 63.25 < 1500 => l=1
        auto [clusters, levels] = ComputeOptimalClustersAndLevels(100, 10, 1.0, 1500);
        UNIT_ASSERT_VALUES_EQUAL(levels, 1u);
        UNIT_ASSERT_GE(clusters, 2u);
        UNIT_ASSERT_LE(clusters, 2048u);
    }

    Y_UNIT_TEST(ComputeOptimalClustersAndLevels_LargeTableNeedsMultipleLevels) {
        // A very large table should require more than one level
        auto [clusters, levels] = ComputeOptimalClustersAndLevels(100000000, 10, 1.0, 1500);
        UNIT_ASSERT_GT(levels, 1u);
        UNIT_ASSERT_GE(clusters, 2u);
        UNIT_ASSERT_LE(clusters, 2048u);
    }

    Y_UNIT_TEST(ComputeOptimalClustersAndLevels_OverlapFactorIncreasesCost) {
        // Higher overlap factor P means more work per query => should push toward more clusters/levels
        auto [c1, l1] = ComputeOptimalClustersAndLevels(10000, 10, 1.0, 1500);
        auto [c2, l2] = ComputeOptimalClustersAndLevels(10000, 10, 5.0, 1500);
        // With P=5 the cost is higher, so it needs either more clusters or more levels to stay under threshold
        // Just verify result is still valid
        UNIT_ASSERT_GE(c1, 2u); UNIT_ASSERT_LE(c1, 2048u);
        UNIT_ASSERT_GE(c2, 2u); UNIT_ASSERT_LE(c2, 2048u);
        UNIT_ASSERT_GE(l2, l1);
    }

    Y_UNIT_TEST(ComputeOptimalClustersAndLevels_ClustersClampedToMin) {
        // Very few rows: cost formula may produce c < MinClusters=2, must be clamped
        auto [clusters, levels] = ComputeOptimalClustersAndLevels(1, 10, 1.0, 1500);
        UNIT_ASSERT_GE(clusters, 2u);
        UNIT_ASSERT_GE(levels, 1u);
    }

    Y_UNIT_TEST(ComputeOptimalClustersAndLevels_ThresholdExact) {
        // Result must satisfy S < sThresh whenever a valid (clusters, levels) exists
        // For n=0 the function returns early; for small n verify the cost is actually < sThresh
        constexpr ui64 sThresh = 1500;
        auto [clusters, levels] = ComputeOptimalClustersAndLevels(500, 10, 1.0, sThresh);
        const double s = clusters
            + static_cast<double>(clusters) * 10 * (levels - 1)
            + static_cast<double>(10) * 500 * 1.0 / std::pow(static_cast<double>(clusters), static_cast<double>(levels));
        UNIT_ASSERT_C(s < static_cast<double>(sThresh),
            "Expected S < sThresh but got S=" << s);
    }

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
        // levels and clusters are optional (auto-detected when not set)
        UNIT_ASSERT(ValidateSettings(settings, error));

        settings.set_levels(UINT32_MAX);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Invalid levels");

        settings.set_levels(16);
        // clusters still optional when levels is set
        UNIT_ASSERT(ValidateSettings(settings, error));

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

    Y_UNIT_TEST(ValidateSettings_OverlapClustersWithoutClusters) {
        // overlap_clusters without an explicit clusters value must be accepted at validation time.
        // (clusters is optional; the bound check is deferred to build time when auto-K is computed.)
        Ydb::Table::KMeansTreeSettings settings;
        TString error;

        auto& vectorIndex = *settings.mutable_settings();
        vectorIndex.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        vectorIndex.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        vectorIndex.set_vector_dimension(8);

        settings.set_overlap_clusters(500);
        // No clusters set — must pass validation
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);
    }

    Y_UNIT_TEST(ValidateSettings_OverlapClustersExceedsClustersRejected) {
        // When both overlap_clusters and clusters are explicitly set,
        // overlap_clusters > clusters must be rejected.
        Ydb::Table::KMeansTreeSettings settings;
        TString error;

        auto& vectorIndex = *settings.mutable_settings();
        vectorIndex.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        vectorIndex.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        vectorIndex.set_vector_dimension(8);

        settings.set_clusters(10);
        settings.set_overlap_clusters(11);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "overlap_clusters should be less than or equal to clusters");
    }

    Y_UNIT_TEST(ValidateSettings_OptionalClustersAndLevels) {
        // Both clusters and levels are optional after commit 1ca26b6 — neither field
        // is required when the other is absent.
        Ydb::Table::KMeansTreeSettings settings;
        TString error;

        auto& vectorIndex = *settings.mutable_settings();
        vectorIndex.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        vectorIndex.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        vectorIndex.set_vector_dimension(8);

        // Neither clusters nor levels set
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);

        // Only levels set (no clusters required)
        settings.set_levels(2);
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);

        // Only clusters set (no levels required)
        settings.clear_levels();
        settings.set_clusters(16);
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);
    }
}

}
