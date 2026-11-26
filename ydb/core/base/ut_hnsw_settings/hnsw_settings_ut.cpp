#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ylimits.h>
#include <util/string/builder.h>

#include <array>
#include <utility>

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

        template <typename TIndexProto>
        void CheckDescriptionRoundTrip(const NKikimrSchemeOp::THnswIndexDescription& description) {
            TIndexProto index;
            index.SetName("vidx");
            *index.MutableHnswIndexDescription() = description;

            TIndexProto restored;
            UNIT_ASSERT(restored.ParseFromString(index.SerializeAsString()));
            UNIT_ASSERT(restored.HasHnswIndexDescription());
            const auto& actual = restored.GetHnswIndexDescription();
            UNIT_ASSERT_VALUES_EQUAL(actual.GetSettings().SerializeAsString(), description.GetSettings().SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(actual.HasEmbeddingColumnId(), description.HasEmbeddingColumnId());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetEmbeddingColumnId(), description.GetEmbeddingColumnId());
            UNIT_ASSERT_VALUES_EQUAL(actual.HasTablePath(), description.HasTablePath());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetTablePath(), description.GetTablePath());
            UNIT_ASSERT_VALUES_EQUAL(actual.HasIndexPath(), description.HasIndexPath());
            UNIT_ASSERT_VALUES_EQUAL(actual.GetIndexPath(), description.GetIndexPath());
        }

    } // namespace

    Y_UNIT_TEST_SUITE(HnswSettings) {
        Y_UNIT_TEST(IndexDescriptionMetadataRoundTrip) {
            for (const bool withMetadata : {false, true}) {
                NKikimrSchemeOp::THnswIndexDescription description;
                *description.MutableSettings() = MakeSettings();
                if (withMetadata) {
                    description.SetEmbeddingColumnId(42);
                    description.SetTablePath("/Root/vector table");
                    description.SetIndexPath("/Root/vector table/vidx");
                }
                CheckDescriptionRoundTrip<NKikimrSchemeOp::TIndexCreationConfig>(description);
                CheckDescriptionRoundTrip<NKikimrSchemeOp::TIndexDescription>(description);
                CheckDescriptionRoundTrip<NKikimrSchemeOp::TIndexAlteringConfig>(description);
                NKikimrSchemeOp::TTableDescription table;
                auto* index = table.AddTableIndexes();
                index->SetType(NKikimrSchemeOp::EIndexTypeGlobalHnsw);
                *index->MutableHnswIndexDescription() = description;
                NKikimrSchemeOp::TTableDescription restored;
                UNIT_ASSERT(restored.ParseFromString(table.SerializeAsString()));
                UNIT_ASSERT_VALUES_EQUAL(restored.TableIndexesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(restored.GetTableIndexes(0).SerializeAsString(), index->SerializeAsString());
            }
            // Empty but present index metadata is the persisted HNSW disable marker.
            CheckDescriptionRoundTrip<NKikimrSchemeOp::TIndexDescription>({});
        }

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
        }

        Y_UNIT_TEST(AutoSelectedRoutingClustersAreCapped) {
            const std::array<std::pair<ui64, ui32>, 4> cases = {{
                {(2047ull << 30) - 1, 2047},
                {2047ull << 30, 2048},
                {2048ull << 30, 2048},
                {Max<ui64>(), 2048},
            }};
            for (const auto& [dataSize, expectedClusters] : cases) {
                auto settings = MakeSettings();
                settings.clear_clusters();
                TString error;
                UNIT_ASSERT_C(AutoSelectHnswSettings(settings, dataSize, error), error);
                UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), expectedClusters);
                UNIT_ASSERT_C(ValidateHnswSettings(settings, error), error);
            }
        }

        Y_UNIT_TEST(ExplicitRoutingClustersKeepValidation) {
            for (const ui32 clusters : {1u, 64u, 2048u, 2049u, Max<ui32>()}) {
                auto settings = MakeSettings();
                settings.set_clusters(clusters);
                TString error;
                const bool valid = AutoSelectHnswSettings(settings, Max<ui64>(), error);
                UNIT_ASSERT_VALUES_EQUAL(valid, clusters <= 2048);
                UNIT_ASSERT_VALUES_EQUAL(settings.clusters(), clusters);
                if (!valid) {
                    UNIT_ASSERT_STRING_CONTAINS(error, "clusters");
                }
            }
        }

        Y_UNIT_TEST(ConstructionResourceLimits) {
            auto settings = MakeSettings();
            auto* vector = settings.mutable_settings();
            TString error;
            for (const auto m : {0u, 1u, MaxHnswM + 1}) {
                vector->set_m(m);
                UNIT_ASSERT(!ValidateHnswSettings(settings, error));
                UNIT_ASSERT_STRING_CONTAINS(error, "M");
                UNIT_ASSERT(!ValidateHnswSettingsPartial(settings, error));
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
            vector->set_m(MinHnswM);
            UNIT_ASSERT_C(ValidateHnswSettings(settings, error), error);
            UNIT_ASSERT_C(ValidateHnswSettingsPartial(settings, error), error);
        }

        Y_UNIT_TEST(KMeansRejectsHnswOnlySettings) {
            auto settings = MakeSettings();
            settings.set_clusters(2);
            TString error;
            UNIT_ASSERT_C(ValidateSettings(settings, error), error);
            UNIT_ASSERT_C(ValidateSettingsPartial(settings, error), error);

            const auto check = [&](int unsupportedCount) {
                const auto expectedError = TStringBuilder()
                    << "vector index settings contain " << unsupportedCount << " unsupported parameter(s)";
                UNIT_ASSERT(!ValidateSettings(settings, error));
                UNIT_ASSERT_VALUES_EQUAL(error, expectedError);
                UNIT_ASSERT(!ValidateSettingsPartial(settings, error));
                UNIT_ASSERT_VALUES_EQUAL(error, expectedError);
                UNIT_ASSERT(!ValidateSettings(settings.settings(), error));
                UNIT_ASSERT_VALUES_EQUAL(error, expectedError);
                UNIT_ASSERT(!ValidateSettingsPartial(settings.settings(), error));
                UNIT_ASSERT_VALUES_EQUAL(error, expectedError);
                UNIT_ASSERT_C(ValidateHnswSettings(settings, error), error);
                UNIT_ASSERT_C(ValidateHnswSettingsPartial(settings, error), error);
            };

            for (const auto setField : {
                &Ydb::Table::VectorIndexSettings::set_m,
                &Ydb::Table::VectorIndexSettings::set_ef_construction,
            }) {
                (settings.mutable_settings()->*setField)(50);
                check(1);
                settings.mutable_settings()->clear_m();
                settings.mutable_settings()->clear_ef_construction();
            }
            for (const auto setField : {
                &Ydb::Table::VectorIndexSettings::set_min_rows,
                &Ydb::Table::VectorIndexSettings::set_delta_rows,
            }) {
                (settings.mutable_settings()->*setField)(0);
                check(1);
                settings.mutable_settings()->clear_min_rows();
                settings.mutable_settings()->clear_delta_rows();
            }

            settings.mutable_settings()->set_m(50);
            settings.mutable_settings()->set_ef_construction(200);
            settings.mutable_settings()->set_min_rows(0);
            settings.mutable_settings()->set_delta_rows(0);
            check(4);

            settings.mutable_settings()->GetReflection()->MutableUnknownFields(settings.mutable_settings())->AddVarint(10000, 1);
            UNIT_ASSERT(!ValidateSettings(settings, error));
            UNIT_ASSERT_VALUES_EQUAL(error, "vector index settings contain 5 unsupported parameter(s)");
            UNIT_ASSERT(!ValidateHnswSettings(settings, error));
            UNIT_ASSERT_VALUES_EQUAL(error, "vector index settings contain 1 unsupported parameter(s)");
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
            UNIT_ASSERT(!FillHnswSetting(settings, "m", "1", error));
            UNIT_ASSERT(FillHnswSetting(settings, "m", "2", error));
            UNIT_ASSERT(!FillHnswSetting(settings, "ef_construction", "0", error));
            UNIT_ASSERT(!FillHnswSetting(settings, "m", ToString(MaxHnswM + 1), error));
            UNIT_ASSERT(!FillHnswSetting(settings, "ef_construction", ToString(MaxHnswEfConstruction + 1), error));
        }

        Y_UNIT_TEST(HnswDefaultMatchesReadProtocolDefaults) {
            NKikimrKqp::TReadVectorTopK top;
            NKikimrTxDataShard::TKqpVectorSearchSettings search;
            UNIT_ASSERT_VALUES_EQUAL(top.GetHnswEfSearch(), DefaultHnswEfSearch);
            UNIT_ASSERT_VALUES_EQUAL(search.GetHnswEfSearch(), DefaultHnswEfSearch);
        }

        Y_UNIT_TEST(DistanceSettingsPreserveGraphSettingsAndUnknownFields) {
            auto routing = MakeSettings();
            auto& settings = *routing.mutable_settings();
            settings.set_m(24);
            settings.set_ef_construction(64);
            settings.set_min_rows(1);
            settings.set_delta_rows(100);
            const auto original = settings.SerializeAsString();
            const auto distance = MakeDistanceSettings(settings);
            UNIT_ASSERT_VALUES_EQUAL(settings.SerializeAsString(), original);
            UNIT_ASSERT(!distance.has_m());
            UNIT_ASSERT(!distance.has_ef_construction());
            UNIT_ASSERT(!distance.has_min_rows());
            UNIT_ASSERT(!distance.has_delta_rows());
            UNIT_ASSERT_VALUES_EQUAL(distance.metric(), settings.metric());
            UNIT_ASSERT_VALUES_EQUAL(distance.vector_type(), settings.vector_type());
            UNIT_ASSERT_VALUES_EQUAL(distance.vector_dimension(), settings.vector_dimension());
            TString error;
            UNIT_ASSERT_C(ValidateSettings(distance, error), error);
            settings.GetReflection()->MutableUnknownFields(&settings)->AddVarint(10000, 1);
            UNIT_ASSERT(!ValidateSettings(MakeDistanceSettings(settings), error));
        }

        Y_UNIT_TEST(FailedHnswParsesPreserveSettings) {
            auto settings = MakeSettings();
            settings.mutable_settings()->set_m(24);
            settings.mutable_settings()->set_ef_construction(64);
            settings.mutable_settings()->set_min_rows(1);
            settings.mutable_settings()->set_delta_rows(100);
            const auto original = settings.SerializeAsString();
            for (const TString name : {"clusters", "m", "ef_construction", "min_rows", "delta_rows"}) {
                TString error = "previous error";
                UNIT_ASSERT(!FillHnswSetting(settings, name, "bad", error));
                UNIT_ASSERT(!error.empty());
                UNIT_ASSERT_VALUES_EQUAL(settings.SerializeAsString(), original);
            }
            TString error = "previous error";
            UNIT_ASSERT(FillHnswSetting(settings, "clusters", "1", error));
            UNIT_ASSERT(error.empty());
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
