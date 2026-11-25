#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/ydb_convert/table_description.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

enum class EDescription {
    None,
    KMeans,
    Hnsw,
    Fulltext,
    Bloom,
    BloomNGram,
};

NKikimrSchemeOp::TIndexCreationConfig MakeCreationConfig(EDescription kind) {
    NKikimrSchemeOp::TIndexCreationConfig config;
    Ydb::Table::KMeansTreeSettings* settings = nullptr;
    switch (kind) {
        case EDescription::KMeans:
            settings = config.MutableVectorIndexKmeansTreeDescription()->MutableSettings();
            break;
        case EDescription::Hnsw:
            settings = config.MutableHnswIndexDescription()->MutableSettings();
            settings->mutable_settings()->set_m(16);
            break;
        case EDescription::Fulltext:
            config.MutableFulltextIndexDescription();
            break;
        case EDescription::Bloom:
            config.MutableBloomFilterDescription();
            break;
        case EDescription::BloomNGram:
            config.MutableBloomNGrammFilterDescription();
            break;
        case EDescription::None:
            break;
    }
    if (settings) {
        settings->set_clusters(2);
        settings->set_levels(1);
        settings->mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        settings->mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        settings->mutable_settings()->set_vector_dimension(2);
    }
    return config;
}

}

Y_UNIT_TEST_SUITE(IndexBuildInfoTest) {
    Y_UNIT_TEST(VectorSettingsLookupRejectsMismatchedDescriptions) {
        for (auto type : {NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree,
                          NKikimrSchemeOp::EIndexTypeGlobalHnsw,
                          NKikimrSchemeOp::EIndexTypeGlobal}) {
            TIndexBuildInfo info;
            info.IndexType = type;
            UNIT_ASSERT(!info.TryGetVectorSettings());
            info.SpecializedIndexDescription.emplace<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>();
            UNIT_ASSERT_VALUES_EQUAL(bool(info.TryGetVectorSettings()),
                type == NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree);
            info.SpecializedIndexDescription.emplace<NKikimrSchemeOp::THnswIndexDescription>();
            UNIT_ASSERT_VALUES_EQUAL(bool(info.TryGetVectorSettings()),
                type == NKikimrSchemeOp::EIndexTypeGlobalHnsw);
            info.SpecializedIndexDescription.emplace<NKikimrSchemeOp::TFulltextIndexDescription>();
            UNIT_ASSERT(!info.TryGetVectorSettings());
        }
    }

    Y_UNIT_TEST(RestoredDescriptionsRemainSerializableAfterCancellation) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        TestCreateTable(runtime, 100, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "embedding" Type: "String" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, 100);
        const auto table = DescribePath(runtime, "/MyRoot/Table");
        const TPathId tableId(TTestTxConfig::SchemeShard, table.GetPathDescription().GetSelf().GetPathId());
        auto* ss = dynamic_cast<TSchemeShard*>(runtime.FindActor(ResolveTablet(runtime, TTestTxConfig::SchemeShard)));
        UNIT_ASSERT(ss);
        struct TTypeCase {
            NKikimrSchemeOp::EIndexType Type;
            EDescription Description;
            bool Optional = false;
        };
        const TTypeCase types[] = {
            {NKikimrSchemeOp::EIndexTypeGlobal, EDescription::None},
            {NKikimrSchemeOp::EIndexTypeGlobalAsync, EDescription::None},
            {NKikimrSchemeOp::EIndexTypeGlobalUnique, EDescription::None},
            {NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, EDescription::KMeans},
            {NKikimrSchemeOp::EIndexTypeGlobalHnsw, EDescription::Hnsw},
            {NKikimrSchemeOp::EIndexTypeGlobalFulltextPlain, EDescription::Fulltext},
            {NKikimrSchemeOp::EIndexTypeGlobalFulltextRelevance, EDescription::Fulltext},
            {NKikimrSchemeOp::EIndexTypeGlobalFulltextCompact, EDescription::Fulltext},
            {NKikimrSchemeOp::EIndexTypeGlobalFulltextCompactRelevance, EDescription::Fulltext},
            {NKikimrSchemeOp::EIndexTypeGlobalJson, EDescription::Fulltext, true},
            {NKikimrSchemeOp::EIndexTypeGlobalJsonCompact, EDescription::Fulltext, true},
        };
        runtime.RunCall([&] {
            for (const auto& type : types) {
                for (auto description : {EDescription::None, EDescription::KMeans, EDescription::Hnsw,
                                         EDescription::Fulltext, EDescription::Bloom, EDescription::BloomNGram}) {
                    TIndexBuildInfo info;
                    info.IndexType = type.Type;
                    info.BuildKind = type.Description == EDescription::KMeans || type.Description == EDescription::Hnsw
                        ? TIndexBuildInfo::EBuildKind::BuildVectorIndex
                        : type.Description == EDescription::Fulltext
                            ? TIndexBuildInfo::EBuildKind::BuildFulltext
                            : TIndexBuildInfo::EBuildKind::BuildSecondaryIndex;
                    info.TablePathId = tableId;
                    info.IndexName = "idx";
                    info.IndexColumns = {"embedding"};
                    const auto config = MakeCreationConfig(description);
                    info.RestoreSpecializedIndexDescription(config);
                    const bool noDescription = description == EDescription::None || description == EDescription::Bloom
                        || description == EDescription::BloomNGram;
                    const bool compatible = description == type.Description
                        || noDescription && (type.Description == EDescription::None || type.Optional);
                    UNIT_ASSERT_VALUES_EQUAL_C(info.IsBroken, !compatible,
                        TStringBuilder() << "type=" << type.Type << " description=" << int(description));
                    if (!compatible) {
                        UNIT_ASSERT_STRING_CONTAINS(info.GetIssue(), "Persisted index type and description do not match");
                    } else if (info.TryGetVectorSettings()) {
                        const auto& settings = description == EDescription::Hnsw
                            ? config.GetHnswIndexDescription().GetSettings()
                            : config.GetVectorIndexKmeansTreeDescription().GetSettings();
                        UNIT_ASSERT_VALUES_EQUAL(info.GetVectorSettings().SerializeAsString(), settings.SerializeAsString());
                        UNIT_ASSERT(info.Clusters);
                        UNIT_ASSERT_VALUES_EQUAL(info.KMeans.K, 2u);
                        UNIT_ASSERT_VALUES_EQUAL(info.KMeans.Levels, 1u);
                    }
                    // Cancellation clears the broken flag, but still needs to serialize the build.
                    info.IsBroken = false;
                    NKikimrSchemeOp::TIndexBuildConfig serialized;
                    info.SerializeToProto(ss, &serialized);
                    UNIT_ASSERT_VALUES_EQUAL(serialized.GetIndex().GetType(), type.Type);
                    if (type.Description == EDescription::KMeans) {
                        UNIT_ASSERT(serialized.GetIndex().HasVectorIndexKmeansTreeDescription());
                        UNIT_ASSERT(info.TryGetVectorSettings());
                    } else if (type.Description == EDescription::Hnsw) {
                        UNIT_ASSERT(serialized.GetIndex().HasHnswIndexDescription());
                        UNIT_ASSERT(info.TryGetVectorSettings());
                    } else if (type.Description == EDescription::Fulltext) {
                        UNIT_ASSERT_VALUES_EQUAL(serialized.GetIndex().HasFulltextIndexDescription(),
                            !type.Optional || description == EDescription::Fulltext);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(int(serialized.GetIndex().GetSpecializedIndexDescriptionCase()),
                            int(NKikimrSchemeOp::TIndexCreationConfig::SPECIALIZEDINDEXDESCRIPTION_NOT_SET));
                    }
                }
            }
            return true;
        });
    }

    Y_UNIT_TEST(VectorMaterializationWithoutIndexColumnsIsRejected) {
        for (bool hnsw : {false, true}) {
            Ydb::Table::CreateTableRequest backup;
            auto* index = backup.add_indexes();
            index->set_name("idx");
            if (hnsw) {
                index->mutable_global_hnsw_index();
            } else {
                index->mutable_global_vector_kmeans_tree_index();
            }
            NKikimrSchemeOp::TIndexedTableCreationConfig converted;
            Ydb::StatusIds::StatusCode status;
            TString error;
            UNIT_ASSERT_C(FillIndexDescription(converted, backup, false, status, error), error);
            auto& description = *converted.MutableIndexDescription(0);
            description.AddIndexImplTableDescriptions()->SetName("indexImplLevelTable");
            description.AddIndexImplTableDescriptions()->SetName("indexImplPostingTable");
            NKikimrSchemeOp::TTableDescription table;
            table.SetName("Table");
            auto* key = table.AddColumns();
            key->SetName("key");
            key->SetType("Uint32");
            table.AddKeyColumnNames("key");
            UNIT_ASSERT(NTableIndex::CalcIndexImplTables(table, description).empty());
        }
    }
}
