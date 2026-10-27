#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ylimits.h>

namespace NKikimr::NKMeans {
    namespace {

        Ydb::Table::KMeansTreeSettings MakeSettings() {
            Ydb::Table::KMeansTreeSettings settings;
            settings.set_levels(1);
            settings.set_clusters(1);
            auto* vector = settings.mutable_settings();
            vector->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            vector->set_vector_dimension(4);
            return settings;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(HnswSettings) {
        Y_UNIT_TEST(IndexTypeConversion) {
            for (const bool compactFulltext : {false, true}) {
                const auto type = NTableIndex::TryConvertIndexType(
                    Ydb::Table::TableIndex::kGlobalHnswIndex, compactFulltext);
                UNIT_ASSERT(type);
                UNIT_ASSERT_VALUES_EQUAL(*type, NKikimrSchemeOp::EIndexTypeGlobalHnsw);
                UNIT_ASSERT(!NTableIndex::DoesIndexSupportTTL(*type));
            }
            UNIT_ASSERT(!NTableIndex::IsLocalTableIndex(Ydb::Table::TableIndex::kGlobalHnswIndex));
        }

        Y_UNIT_TEST(FeatureFlagDefaultsToDisabled) {
            NKikimrConfig::TFeatureFlags flags;
            UNIT_ASSERT(!flags.GetEnableHnswIndex());
        }

        Y_UNIT_TEST(SingleClusterDoesNotChangeKMeansValidation) {
            auto settings = MakeSettings();
            TString error;
            UNIT_ASSERT_C(ValidateHnswSettings(settings, error), error);
            UNIT_ASSERT(!ValidateSettings(settings, error));
            settings.set_clusters(2);
            UNIT_ASSERT_C(ValidateSettings(settings, error), error);
            settings.set_clusters(0);
            UNIT_ASSERT(!ValidateHnswSettings(settings, error));
        }

        Y_UNIT_TEST(DefaultRoutingSettings) {
            auto settings = MakeSettings();
            settings.clear_clusters();
            settings.clear_levels();
            TString error;
            UNIT_ASSERT_C(AutoSelectHnswSettings(settings, 0, error), error);
            UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(settings.levels(), 1u);
            settings.clear_clusters();
            UNIT_ASSERT_C(AutoSelectHnswSettings(settings, 3ull << 30, error), error);
            UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), 4u);
            settings.clear_clusters();
            UNIT_ASSERT(!AutoSelectHnswSettings(settings, Max<ui64>(), error));
            UNIT_ASSERT_STRING_CONTAINS(error, "clusters");
        }

        Y_UNIT_TEST(ConstructionResourceLimits) {
            auto settings = MakeSettings();
            auto* vector = settings.mutable_settings();
            TString error;
            for (const auto m : {0u, MaxHnswM + 1}) {
                vector->set_m(m);
                UNIT_ASSERT(!ValidateHnswSettings(settings, error));
                UNIT_ASSERT_STRING_CONTAINS(error, "M");
            }
            vector->set_m(MaxHnswM);
            for (const auto ef : {0u, MaxHnswEfConstruction + 1}) {
                vector->set_ef_construction(ef);
                UNIT_ASSERT(!ValidateHnswSettings(settings, error));
                UNIT_ASSERT_STRING_CONTAINS(error, "ef_construction");
            }
            vector->set_ef_construction(MaxHnswEfConstruction);
            vector->set_min_rows(0);
            vector->set_delta_rows(Max<ui64>());
            UNIT_ASSERT_C(ValidateHnswSettings(settings, error), error);
        }

        Y_UNIT_TEST(PartialSettingsStillValidateConstructionLimits) {
            Ydb::Table::KMeansTreeSettings settings;
            TString error;
            UNIT_ASSERT(!ValidateHnswSettingsPartial(settings, error));
            settings.mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            UNIT_ASSERT_C(ValidateHnswSettingsPartial(settings, error), error);
            UNIT_ASSERT(!ValidateHnswSettings(settings, error));
            settings.mutable_settings()->set_m(MaxHnswM + 1);
            UNIT_ASSERT(!ValidateHnswSettingsPartial(settings, error));
            UNIT_ASSERT_STRING_CONTAINS(error, "M");
        }
    Y_UNIT_TEST(FillSettingHnsw) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;

        UNIT_ASSERT(FillHnswSetting(settings, "min_rows", "10000", error));
        UNIT_ASSERT(FillHnswSetting(settings, "m", "24", error));
        UNIT_ASSERT(FillHnswSetting(settings, "ef_construction", "200", error));
        UNIT_ASSERT(FillHnswSetting(settings, "delta_rows", "5", error));
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().min_rows(), 10000);
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().m(), 24);
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().ef_construction(), 200);
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().delta_rows(), 5);
        UNIT_ASSERT(FillHnswSetting(settings, "delta_rows", "0", error));
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().delta_rows(), 0);
        UNIT_ASSERT(FillHnswSetting(settings, "delta_rows", "4294967296", error));
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().delta_rows(), 4294967296ULL);
        UNIT_ASSERT(FillHnswSetting(settings, "delta_rows", "18446744073709551615", error));
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().delta_rows(), Max<ui64>());
        UNIT_ASSERT(!FillHnswSetting(settings, "delta_rows", "18446744073709551616", error));
        UNIT_ASSERT(!FillHnswSetting(settings, "delta_rows", "-1", error));
        UNIT_ASSERT(!FillHnswSetting(settings, "delta_rows", "bad", error));
        for (const TString& oldName : {"hnsw_min_rows", "hnsw_connectivity", "hnsw_construction_candidates",
                                     "hnsw_rebuild_threshold_percent", "hnsw_search_candidates"}) {
            UNIT_ASSERT(!FillHnswSetting(settings, oldName, "15", error));
        }

        UNIT_ASSERT(FillHnswSetting(settings, "min_rows", "4294967296", error));
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().min_rows(), 4294967296ULL);
        UNIT_ASSERT(FillHnswSetting(settings, "min_rows", "18446744073709551615", error));
        UNIT_ASSERT_VALUES_EQUAL(settings.settings().min_rows(), Max<ui64>());
        UNIT_ASSERT(!FillHnswSetting(settings, "min_rows", "18446744073709551616", error));
        UNIT_ASSERT(!FillHnswSetting(settings, "min_rows", "-1", error));
        UNIT_ASSERT(!FillHnswSetting(settings, "min_rows", "not-a-number", error));

        UNIT_ASSERT(!FillHnswSetting(settings, "m", "0", error));
        UNIT_ASSERT(!FillHnswSetting(settings, "ef_construction", "0", error));
        UNIT_ASSERT(!FillHnswSetting(settings, "m", ToString(MaxHnswM + 1), error));
        UNIT_ASSERT(!FillHnswSetting(settings, "ef_construction", ToString(MaxHnswEfConstruction + 1), error));
    }

    Y_UNIT_TEST(KMeansDoesNotAcceptHnswConstructionSettings) {
        Ydb::Table::KMeansTreeSettings settings;
        TString error;
        for (const TString name : {"min_rows", "m", "ef_construction", "delta_rows"}) {
            UNIT_ASSERT(!FillSetting(settings, name, "1", error));
        }
    }

    } // Y_UNIT_TEST_SUITE(HnswSettings)

} // namespace NKikimr::NKMeans
