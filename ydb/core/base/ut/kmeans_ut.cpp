#include "kmeans_clusters.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

namespace NKikimr::NKMeans {

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
}

}
