#include <ydb/core/base/domain.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/core/tx/schemeshard/index/index_utils.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/library/aclib/aclib.h>

#include <util/string/printf.h>
#include <util/string/strip.h>

#include <optional>
#include <source_location>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

struct TVectorBuildRow {
    NKikimrSchemeOp::EIndexType IndexType;
    TString CreationConfig;

    template<class TColumn>
    typename TColumn::Type GetValue() const {
        if constexpr (std::is_same_v<TColumn, Schema::IndexBuild::IndexType>) {
            return IndexType;
        } else if constexpr (std::is_same_v<TColumn, Schema::IndexBuild::CreationConfig>) {
            return CreationConfig;
        } else if constexpr (std::is_same_v<TColumn, Schema::IndexBuild::State>) {
            return typename TColumn::Type(TIndexBuildInfo::EState::Done);
        } else {
            return {};
        }
    }

    template<class TColumn>
    bool HaveValue() const {
        return std::is_same_v<TColumn, Schema::IndexBuild::CreationConfig>;
    }

    template<class TColumn>
    typename TColumn::Type GetValueOrDefault(const typename TColumn::Type& fallback = {}) const {
        return HaveValue<TColumn>() ? GetValue<TColumn>() : fallback;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TSchemeShardInfoTypesTest) {

    NKikimrSchemeOp::TPartitionConfig MakeCompactionTierConfig() {
        NKikimrSchemeOp::TPartitionConfig config;
        auto* generation = config.MutableCompactionPolicy()->AddGeneration();
        generation->SetGenerationId(0);
        generation->SetSizeToCompact(1000);
        generation->SetCountToCompact(5);
        generation->SetForceSizeToCompact(20000);
        auto* family = config.AddColumnFamilies();
        family->SetId(0);
        family->MutableStorageConfig();
        return config;
    }

    void AddCompactionTierFamily(NKikimrSchemeOp::TPartitionConfig& config) {
        auto* family = config.AddColumnFamilies();
        family->SetId(42);
        family->SetName("archive");
    }

    void AppendCompactionTierGeneration(NKikimrSchemeOp::TPartitionConfig& config, ui32 tierId) {
        auto* policy = config.MutableCompactionPolicy();
        const ui32 generationId = policy->GenerationSize();
        auto* generation = policy->AddGeneration();
        generation->CopyFrom(policy->GetGeneration(0));
        generation->SetGenerationId(generationId);
        generation->SetTierId(tierId);
    }

    Y_UNIT_TEST(CreateCompactionGenerationTierReferences) {
        auto config = MakeCompactionTierConfig();
        config.ClearColumnFamilies();
        TString error;
        // Both an omitted TierId and an explicit zero accept an implicit default family.
        UNIT_ASSERT_C(TPartitionConfigMerger::VerifyCreateParams(config, nullptr, false, error), error);
        auto* generation = config.MutableCompactionPolicy()->MutableGeneration(0);
        generation->SetTierId(0);
        UNIT_ASSERT_C(TPartitionConfigMerger::VerifyCreateParams(config, nullptr, false, error), error);

        generation->SetTierId(42);
        UNIT_ASSERT(!TPartitionConfigMerger::VerifyCreateParams(config, nullptr, false, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Compaction generation 0 refers to tier 42");
        UNIT_ASSERT_STRING_CONTAINS(error, "not a column family");

        auto* family = config.AddColumnFamilies();
        family->SetId(0);
        family->MutableStorageConfig();
        AddCompactionTierFamily(config);
        error.clear();
        UNIT_ASSERT_C(TPartitionConfigMerger::VerifyCreateParams(config, nullptr, false, error), error);
    }

    Y_UNIT_TEST(IndexImplTableDropsTierIdsOfExcludedFamilies) {
        auto base = MakeCompactionTierConfig();
        AddCompactionTierFamily(base);
        base.MutableCompactionPolicy()->MutableGeneration(0)->SetTierId(0);
        AppendCompactionTierGeneration(base, 42);

        const auto index = NTableIndex::CalcVectorKmeansTreeLevelImplTableDesc(base, {});
        const auto& indexConfig = index.GetPartitionConfig();
        UNIT_ASSERT_VALUES_EQUAL(indexConfig.ColumnFamiliesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(indexConfig.GetColumnFamilies(0).GetId(), 0u);
        auto expected = base.GetCompactionPolicy();
        expected.MutableGeneration(1)->ClearTierId();
        UNIT_ASSERT_VALUES_EQUAL(indexConfig.GetCompactionPolicy().DebugString(), expected.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(base.GetCompactionPolicy().GetGeneration(1).GetTierId(), 42u);
        TString error;
        UNIT_ASSERT_C(TPartitionConfigMerger::VerifyCreateParams(indexConfig, nullptr, false, error), error);
    }

    Y_UNIT_TEST(AlterCompactionGenerationTierRequiresAMergedFamily) {
        const auto source = MakeCompactionTierConfig();
        NKikimrSchemeOp::TPartitionConfig changes;
        changes.MutableCompactionPolicy()->CopyFrom(source.GetCompactionPolicy());
        AppendCompactionTierGeneration(changes, 42);
        ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TColumnDescription> columns;
        NKikimrSchemeOp::TPartitionConfig result;
        TString error;
        UNIT_ASSERT_C(TPartitionConfigMerger::ApplyChanges(result, source, changes, columns, nullptr, false, error), error);
        UNIT_ASSERT(!TPartitionConfigMerger::VerifyAlterParams(source, result, nullptr, false, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Compaction generation 1 refers to tier 42");

        // A family and a policy referring to it may be introduced by the same ALTER.
        AddCompactionTierFamily(changes);
        error.clear();
        UNIT_ASSERT_C(TPartitionConfigMerger::ApplyChanges(result, source, changes, columns, nullptr, false, error), error);
        UNIT_ASSERT_C(TPartitionConfigMerger::VerifyAlterParams(source, result, nullptr, false, error), error);
    }

    Y_UNIT_TEST(AlterCompactionGenerationTierUsesInheritedFamilies) {
        auto source = MakeCompactionTierConfig();
        AddCompactionTierFamily(source);
        NKikimrSchemeOp::TPartitionConfig changes;
        changes.MutableCompactionPolicy()->CopyFrom(source.GetCompactionPolicy());
        AppendCompactionTierGeneration(changes, 42);
        ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TColumnDescription> columns;
        NKikimrSchemeOp::TPartitionConfig result;
        TString error;
        UNIT_ASSERT_C(TPartitionConfigMerger::ApplyChanges(result, source, changes, columns, nullptr, false, error), error);
        UNIT_ASSERT_C(TPartitionConfigMerger::VerifyAlterParams(source, result, nullptr, false, error), error);
    }

    Y_UNIT_TEST_FLAG(IndexImplTableInheritsOnlyDefaultFamilyTierIds, NamedPolicy) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().NStoragePools(1));

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->DefaultTaskPriority = 37;
        policy->Generations.front().TierId = 0;
        policy->Generations.back().TierId = 42;
        NKikimrCompaction::TCompactionPolicy expectedPolicy;
        policy->Serialize(expectedPolicy);
        runtime.GetAppData().DomainsInfo->AddCompactionPolicy(
            NamedPolicy ? "WithArchive" : TDomainsInfo::UserTableDefaultPoicyName(), policy);

        const ui64 txId = 100;
        TestCreateIndexedTable(runtime, txId, "/MyRoot", Sprintf(R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
                PartitionConfig {
                    %s
                    ColumnFamilies { Id: 42 Name: "archive" }
                }
            }
            IndexDescription {
                Name: "ByValue"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )", NamedPolicy ? R"(NamedCompactionPolicy: "WithArchive")" : ""));
        env.TestWaitNotification(runtime, txId);

        const auto table = DescribePath(runtime, "/MyRoot/Table", true);
        TestDescribeResult(table, {NLs::IsTable});
        const auto& tablePolicy = table.GetPathDescription().GetTable().GetPartitionConfig().GetCompactionPolicy();
        UNIT_ASSERT_VALUES_EQUAL(tablePolicy.DebugString(), expectedPolicy.DebugString());

        const auto index = DescribePrivatePath(runtime, "/MyRoot/Table/ByValue/indexImplTable", true);
        TestDescribeResult(index, {NLs::IsTable, NLs::ColumnFamiliesCount(1), NLs::ColumnFamiliesHas(0)});
        const auto& indexPolicy = index.GetPathDescription().GetTable().GetPartitionConfig().GetCompactionPolicy();
        expectedPolicy.MutableGeneration(expectedPolicy.GenerationSize() - 1)->ClearTierId();
        UNIT_ASSERT_VALUES_EQUAL(indexPolicy.DebugString(), expectedPolicy.DebugString());
    }

    Y_UNIT_TEST(DirectCreateOfIndexBuildTablePreservesGenerationTierIds) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().NStoragePools(1));
        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Vectors"
                Columns { Name: "id" Type: "Uint64" }
                Columns { Name: "embedding" Type: "String" }
                KeyColumnNames: ["id"]
            }
            IndexDescription {
                Name: "ByVector"
                KeyColumnNames: ["embedding"]
                Type: EIndexTypeGlobalVectorKmeansTree
                VectorIndexKmeansTreeDescription { Settings {
                    settings { metric: DISTANCE_COSINE vector_type: VECTOR_TYPE_FLOAT vector_dimension: 2 }
                    clusters: 4 levels: 2
                } }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->Generations.back().TierId = 42;
        runtime.GetAppData().DomainsInfo->AddCompactionPolicy("WithArchive", policy);
        auto* request = InternalTransaction(CreateTableRequest(++txId, "/MyRoot/Vectors/ByVector", R"(
            Name: "indexImplPostingTable0build"
            Columns { Name: "id" Type: "Uint64" }
            KeyColumnNames: ["id"]
            PartitionConfig {
                NamedCompactionPolicy: "WithArchive"
                ColumnFamilies { Id: 42 Name: "archive" }
            }
        )"));
        AsyncSend(runtime, TTestTxConfig::SchemeShard, request);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, txId);

        const auto table = DescribePrivatePath(runtime, "/MyRoot/Vectors/ByVector/indexImplPostingTable0build", true);
        TestDescribeResult(table, {NLs::IsTable, NLs::ColumnFamiliesHas(42, "archive")});
        NKikimrCompaction::TCompactionPolicy expected;
        policy->Serialize(expected);
        const auto& actual = table.GetPathDescription().GetTable().GetPartitionConfig().GetCompactionPolicy();
        UNIT_ASSERT_VALUES_EQUAL(actual.DebugString(), expected.DebugString());
    }

    Y_UNIT_TEST(CopyIndexedTablePreservesIndexImplGenerationTier) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().NStoragePools(1));
        runtime.GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");
        ui64 txId = 100;
        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ByValue"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const TString sourcePath = "/MyRoot/Table/ByValue/indexImplTable";
        const auto index = DescribePrivatePath(runtime, sourcePath, true);
        TestDescribeResult(index, {NLs::IsTable});
        const auto& policy = index.GetPathDescription().GetTable().GetPartitionConfig().GetCompactionPolicy();
        const int generationId = policy.GenerationSize();
        UNIT_ASSERT(generationId > 0);

        auto* request = AlterTableRequest(++txId, "/MyRoot/Table/ByValue", R"(Name: "indexImplTable")");
        auto& changes = *request->Record.MutableTransaction(0)->MutableAlterTable()->MutablePartitionConfig();
        changes.MutableCompactionPolicy()->CopyFrom(policy);
        AddCompactionTierFamily(changes);
        AppendCompactionTierGeneration(changes, 42);
        request->Record.SetUserToken(NACLib::TUserToken("root@builtin", {}).SerializeAsString());
        AsyncSend(runtime, TTestTxConfig::SchemeShard, request);
        TestModificationResult(runtime, txId);
        env.TestWaitNotification(runtime, txId);

        auto checkArchiveTier = [&](const TString& path) {
            const auto index = DescribePrivatePath(runtime, path, true);
            TestDescribeResult(index, {NLs::IsTable, NLs::ColumnFamiliesHas(42, "archive")});
            const auto& policy = index.GetPathDescription().GetTable().GetPartitionConfig().GetCompactionPolicy();
            UNIT_ASSERT_VALUES_EQUAL_C(policy.GenerationSize(), generationId + 1, path);
            UNIT_ASSERT_VALUES_EQUAL_C(policy.GetGeneration(generationId).GetTierId(), 42u, path);
        };
        checkArchiveTier(sourcePath);

        TestCopyTable(runtime, ++txId, "/MyRoot", "Copy", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);
        checkArchiveTier("/MyRoot/Copy/ByValue/indexImplTable");
    }

    NKikimrCompaction::TCompactionPolicy CompactionPolicyWithGenerationIds(
            const TVector<std::optional<ui32>>& ids) {
        NKikimrCompaction::TCompactionPolicy policy;
        for (const auto id : ids) {
            auto* generation = policy.AddGeneration();
            if (id) {
                generation->SetGenerationId(*id);
            }
        }
        return policy;
    }

    Y_UNIT_TEST(CompactionPolicyRejectsInvalidGenerationIds) {
        struct TCase {
            TVector<std::optional<ui32>> Ids;
            ui32 Index;
            ui32 ActualId;
        };
        const TCase cases[] = {
            {{1}, 0, 1},
            {{0, 0}, 1, 0},
            {{0, 2}, 1, 2},
            {{0, 2, 1}, 1, 2},
            {{0, std::nullopt}, 1, 0},
        };
        for (const auto& test : cases) {
            const auto policy = CompactionPolicyWithGenerationIds(test.Ids);
            TString error;
            UNIT_ASSERT(!TPartitionConfigMerger::VerifyCompactionPolicy(policy, error));
            UNIT_ASSERT_VALUES_EQUAL(error, Sprintf(
                "Compaction generation %u: expected GenerationId %u, got %u",
                test.Index, test.Index, test.ActualId));
        }
    }

    Y_UNIT_TEST(CompactionPolicyAcceptsSequentialGenerationIds) {
        for (const bool omitFirstId : {false, true}) {
            auto policy = CompactionPolicyWithGenerationIds({0, 1, 2});
            if (omitFirstId) {
                policy.MutableGeneration(0)->ClearGenerationId();
            }
            TString error;
            UNIT_ASSERT_C(TPartitionConfigMerger::VerifyCompactionPolicy(policy, error), error);
        }
    }

    Y_UNIT_TEST(CreateAndAlterRejectInvalidCompactionGenerationIds) {
        NKikimrSchemeOp::TPartitionConfig source;
        source.MutableCompactionPolicy()->CopyFrom(CompactionPolicyWithGenerationIds({0, 1, 2}));
        TString error;
        UNIT_ASSERT_C(TPartitionConfigMerger::VerifyCreateParams(source, nullptr, false, error), error);

        auto invalid = source;
        invalid.MutableCompactionPolicy()->MutableGeneration(1)->SetGenerationId(2);
        UNIT_ASSERT(!TPartitionConfigMerger::VerifyCreateParams(invalid, nullptr, false, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "Compaction generation 1: expected GenerationId 1, got 2");
        error.clear();
        UNIT_ASSERT(!TPartitionConfigMerger::VerifyAlterParams(source, invalid, nullptr, false, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "Compaction generation 1: expected GenerationId 1, got 2");
    }

    Y_UNIT_TEST_FLAG(VectorIndexMismatchedBuildDescriptionIsBroken, Hnsw) {
        NKikimrSchemeOp::TIndexCreationConfig config;
        if (Hnsw) {
            config.MutableVectorIndexKmeansTreeDescription();
        } else {
            config.MutableHnswIndexDescription();
        }
        const TVectorBuildRow row{Hnsw ? NKikimrSchemeOp::EIndexTypeGlobalHnsw
            : NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree, config.SerializeAsString()};
        TIndexBuildInfo restored;
        TIndexBuildInfo::FillFromRow(row, &restored);
        UNIT_ASSERT(restored.IsBroken);
        UNIT_ASSERT_STRING_CONTAINS(restored.GetIssue(), "type and description do not match");
        // Querying diagnostics must not throw a second bad_variant_access.
        UNIT_ASSERT_VALUES_EQUAL(restored.GetVectorSettings().ByteSizeLong(), 0u);
    }

    Y_UNIT_TEST_FLAG(VectorIndexRoutingSettings, Hnsw) {
        TIndexBuildInfo build;
        build.IndexType = Hnsw
            ? NKikimrSchemeOp::EIndexTypeGlobalHnsw
            : NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree;
        if (Hnsw) {
            build.SpecializedIndexDescription.emplace<NKikimrSchemeOp::THnswIndexDescription>();
        } else {
            build.SpecializedIndexDescription.emplace<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>();
        }
        auto& settings = build.MutableVectorSettings();
        settings.set_levels(1);
        settings.set_clusters(Hnsw ? 1 : 2);
        auto* vector = settings.mutable_settings();
        vector->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        vector->set_vector_dimension(2);
        if (Hnsw) {
            vector->set_m(8);
            vector->set_ef_construction(40);
            vector->set_min_rows(1);
            vector->set_delta_rows(256);
        }
        const auto original = settings.SerializeAsString();
        TString error;
        const auto routing = build.GetKMeansVectorSettings();
        UNIT_ASSERT_C(NKikimr::NKMeans::ValidateSettings(routing, error), error);
        UNIT_ASSERT_C(NKikimr::NKMeans::CreateClusters(routing, 2, error), error);
        UNIT_ASSERT_VALUES_EQUAL(build.GetVectorSettings().SerializeAsString(), original);
    }

    Y_UNIT_TEST_FLAG(VectorIndexDescriptionPersistence, Hnsw) {
        NKikimrSchemeOp::TIndexCreationConfig config;
        config.SetName("vidx");
        config.SetType(Hnsw
            ? NKikimrSchemeOp::EIndexTypeGlobalHnsw
            : NKikimrSchemeOp::EIndexTypeGlobalVectorKmeansTree);
        config.AddKeyColumnNames("scope");
        config.AddKeyColumnNames("custom_embedding");
        config.AddDataColumnNames("payload");
        auto* settings = Hnsw
            ? config.MutableHnswIndexDescription()->MutableSettings()
            : config.MutableVectorIndexKmeansTreeDescription()->MutableSettings();
        settings->set_clusters(Hnsw ? 1 : 2);
        settings->set_levels(1);
        settings->set_autoupdate(false);
        auto* vector = settings->mutable_settings();
        vector->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        vector->set_vector_dimension(128);
        if (Hnsw) {
            vector->set_min_rows(0);
            vector->set_m(24);
            vector->set_ef_construction(300);
            vector->set_delta_rows(1ULL << 40);
        }

        TString error;
        const auto index = TTableIndexInfo::Create(config, error);
        UNIT_ASSERT_C(index, error);
        const auto& altered = *index->AlterData;
        UNIT_ASSERT_VALUES_EQUAL(altered.IndexKeys.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(altered.IndexKeys.back(), "custom_embedding");
        UNIT_ASSERT_VALUES_EQUAL(altered.IndexDataColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(altered.IndexDataColumns.front(), "payload");

        // The description is stored separately from the index type and keys.
        const auto serialized = altered.SerializeDescription();
        const TTableIndexInfo restored(altered.AlterVersion, altered.Type, altered.State, serialized);
        UNIT_ASSERT_VALUES_EQUAL(restored.SerializeDescription(), serialized);
        if (Hnsw) {
            UNIT_ASSERT(std::holds_alternative<NKikimrSchemeOp::THnswIndexDescription>(restored.SpecializedIndexDescription));
            const auto& description = std::get<NKikimrSchemeOp::THnswIndexDescription>(restored.SpecializedIndexDescription);
            UNIT_ASSERT_VALUES_EQUAL(description.GetSettings().SerializeAsString(), settings->SerializeAsString());
        } else {
            UNIT_ASSERT(std::holds_alternative<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(restored.SpecializedIndexDescription));
            const auto& description = std::get<NKikimrSchemeOp::TVectorIndexKmeansTreeDescription>(restored.SpecializedIndexDescription);
            UNIT_ASSERT_VALUES_EQUAL(description.GetSettings().SerializeAsString(), settings->SerializeAsString());
        }
    }

    void VerifyDeduplicateColumnFamiliesById(
            const TString& srcProtoText,
            const TString& dstProtoText,
            const TString& posMapExpected)
    {
        NKikimrSchemeOp::TPartitionConfig config;
        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(srcProtoText, &config);
        UNIT_ASSERT_C(parseOk, "Failed to parse TPartitionConfig:\n" << srcProtoText);

        auto posById = TPartitionConfigMerger::DeduplicateColumnFamiliesById(config);

        TString result;
        ::google::protobuf::TextFormat::PrintToString(config, &result);
        TString expected = StripStringLeft(dstProtoText, [](const auto* it) { return *it == '\n'; });

        UNIT_ASSERT_VALUES_EQUAL(result, expected);

        TVector<ui32> ids;
        for (const auto& kv : posById) {
            ids.push_back(kv.first);
        }
        std::sort(ids.begin(), ids.end());

        TStringBuilder posMapResult;
        posMapResult << '{';
        bool first = true;
        for (ui32 id : ids) {
            if (first) {
                first = false;
            } else {
                posMapResult << ',';
            }
            posMapResult << ' ';
            posMapResult << id << " -> " << posById[id];
        }
        posMapResult << " }";
        UNIT_ASSERT_VALUES_EQUAL(TString(posMapResult), posMapExpected);
    }

    Y_UNIT_TEST(EmptyFamilies) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
}
)",
            R"(
ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0 }");
    }

    Y_UNIT_TEST(LostId) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    Y_UNIT_TEST(DeduplicationOrder) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    Y_UNIT_TEST(MultipleDeduplications) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    // Helper: call ApplyChanges with minimal args (no appData, no columns)
    bool ApplyBloomChanges(
        NKikimrSchemeOp::TPartitionConfig& result,
        const NKikimrSchemeOp::TPartitionConfig& src,
        const NKikimrSchemeOp::TPartitionConfig& changes,
        TString& errDescr)
    {
        ::google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TColumnDescription> columns;
        return TPartitionConfigMerger::ApplyChanges(result, src, changes, columns, nullptr, false, errDescr);
    }

    void AddBloomPrefix(NKikimrSchemeOp::TPartitionConfig& config, ui32 prefixLen, double fpp = NKikimr::NTable::DefaultBloomFilterFpp) {
        auto* entry = config.AddByKeyFilterPrefixes();
        entry->SetPrefixLength(prefixLen);
        entry->SetFalsePositiveProbability(fpp);
    }

    Y_UNIT_TEST(BloomFilterPrefixMerge) {
        // Existing [3, 1], delta adds [1, 2] -> result should be [1, 2, 3] (merged, sorted, deduped)
        NKikimrSchemeOp::TPartitionConfig src;
        AddBloomPrefix(src, 3);
        AddBloomPrefix(src, 1);

        NKikimrSchemeOp::TPartitionConfig changes;
        AddBloomPrefix(changes, 1);
        AddBloomPrefix(changes, 2);

        NKikimrSchemeOp::TPartitionConfig result;
        TString errDescr;
        UNIT_ASSERT(ApplyBloomChanges(result, src, changes, errDescr));
        UNIT_ASSERT_VALUES_EQUAL(result.ByKeyFilterPrefixesSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(result.GetByKeyFilterPrefixes(0).GetPrefixLength(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetByKeyFilterPrefixes(1).GetPrefixLength(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result.GetByKeyFilterPrefixes(2).GetPrefixLength(), 3);
    }

    Y_UNIT_TEST(BloomFilterPrefixMergeFpp) {
        // New FPP wins on conflict: existing prefix 1 with fpp=0.01, delta adds prefix 1 with fpp=0.05
        NKikimrSchemeOp::TPartitionConfig src;
        AddBloomPrefix(src, 1, 0.01);

        NKikimrSchemeOp::TPartitionConfig changes;
        AddBloomPrefix(changes, 1, 0.05);

        NKikimrSchemeOp::TPartitionConfig result;
        TString errDescr;
        UNIT_ASSERT(ApplyBloomChanges(result, src, changes, errDescr));
        UNIT_ASSERT_VALUES_EQUAL(result.ByKeyFilterPrefixesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetByKeyFilterPrefixes(0).GetPrefixLength(), 1);
        UNIT_ASSERT_DOUBLES_EQUAL(result.GetByKeyFilterPrefixes(0).GetFalsePositiveProbability(), 0.05, 1e-9);
    }

    Y_UNIT_TEST(BloomFilterDisableClearsPrefixes) {
        // Existing config has prefixes [1, 2], delta disables EnableFilterByKey -> prefixes cleared
        NKikimrSchemeOp::TPartitionConfig src;
        src.SetEnableFilterByKey(true);
        AddBloomPrefix(src, 1);
        AddBloomPrefix(src, 2);

        NKikimrSchemeOp::TPartitionConfig changes;
        changes.SetEnableFilterByKey(false);

        NKikimrSchemeOp::TPartitionConfig result;
        TString errDescr;
        UNIT_ASSERT(ApplyBloomChanges(result, src, changes, errDescr));
        UNIT_ASSERT_VALUES_EQUAL(result.GetEnableFilterByKey(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.ByKeyFilterPrefixesSize(), 0);
    }

    Y_UNIT_TEST(BloomFilterDisableAndAddPrefixesConflict) {
        // Delta has both EnableFilterByKey=false and ByKeyFilterPrefixes -> must be rejected
        NKikimrSchemeOp::TPartitionConfig src;

        NKikimrSchemeOp::TPartitionConfig changes;
        changes.SetEnableFilterByKey(false);
        AddBloomPrefix(changes, 1);

        NKikimrSchemeOp::TPartitionConfig result;
        TString errDescr;
        UNIT_ASSERT(!ApplyBloomChanges(result, src, changes, errDescr));
        UNIT_ASSERT_STRING_CONTAINS(errDescr, "Cannot disable KEY_BLOOM_FILTER and add bloom filter prefixes");
    }

    Y_UNIT_TEST(IndexBuildInfoAddParent) {
        TIndexBuildInfo info;

        info.KMeans.ParentBegin = 1;
        info.KMeans.Parent = 1;
        info.KMeans.ChildBegin = 201;
        info.KMeans.Child = 201;

        auto addShard = [&](ui64 fromCluster, ui64 toCluster, ui32 shard, bool partialFrom = false) {
            TVector<TCell> from = {TCell::Make(fromCluster), TCell::Make(123)};
            if (partialFrom) {
                from.pop_back();
            }
            TVector<TCell> to = {TCell::Make(toCluster), TCell::Make(123)};
            auto range = TSerializedTableRange(from, true, to, true);
            info.AddParent(range, TShardIdx(1, shard));
        };
        auto checkShards = [&](ui64 from, ui64 to, std::vector<ui32> shards) {
            auto rng = info.Cluster2Shards.at(to);
            if (rng.From != from || rng.Shards.size() != shards.size()) {
                return false;
            }
            for (size_t i = 0; i < shards.size(); i++) {
                if (rng.Shards[i] != TShardIdx(1, shards[i])) {
                    return false;
                }
            }
            return true;
        };

        // no intersection + empty list
        addShard(1, 3, 1);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 1);
        UNIT_ASSERT(checkShards(1, 3, {1}));

        // add an intersecting shard. it even crashed here until 02.07.2025 :)
        // intersection by start + both larger than 1
        addShard(3, 4, 2);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 3);
        UNIT_ASSERT(checkShards(1, 2, {1}));
        UNIT_ASSERT(checkShards(3, 3, {1, 2}));
        UNIT_ASSERT(checkShards(4, 4, {2}));

        // intersection by start + both 1 (duplicate range)
        // incomplete range is "from (3, +infinity)", thus equal to "from 4"
        addShard(3, 4, 3, true);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 3);
        UNIT_ASSERT(checkShards(4, 4, {2, 3}));

        // intersection by start + old 1 + new larger than 1
        addShard(4, 6, 4);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 4);
        UNIT_ASSERT(checkShards(4, 4, {2, 3, 4}));
        UNIT_ASSERT(checkShards(5, 6, {4}));

        // intersection by start + old larger than 1 + new 1
        addShard(6, 6, 5);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 5);
        UNIT_ASSERT(checkShards(5, 5, {4}));
        UNIT_ASSERT(checkShards(6, 6, {4, 5}));

        // no intersection + after non-empty list
        addShard(19, 20, 6);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 6);
        UNIT_ASSERT(checkShards(19, 20, {6}));

        // intersection by end + both larger than 1
        addShard(18, 19, 7);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 8);
        UNIT_ASSERT(checkShards(18, 18, {7}));
        UNIT_ASSERT(checkShards(19, 19, {6, 7}));
        UNIT_ASSERT(checkShards(20, 20, {6}));

        // intersection by end + both 1 (duplicate range)
        addShard(18, 18, 8);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 8);
        UNIT_ASSERT(checkShards(18, 18, {7, 8}));

        // intersection by end + old 1 + new larger than 1
        addShard(16, 18, 9);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 9);
        UNIT_ASSERT(checkShards(16, 17, {9}));
        UNIT_ASSERT(checkShards(18, 18, {7, 8, 9}));

        // intersection by end + old larger than 1 + new 1
        addShard(16, 16, 10);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 10);
        UNIT_ASSERT(checkShards(16, 16, {9, 10}));
        UNIT_ASSERT(checkShards(17, 17, {9}));

        // intersection by both
        addShard(7, 9, 11);
        addShard(13, 15, 12);
        addShard(9, 13, 13);
        UNIT_ASSERT_VALUES_EQUAL(info.Cluster2Shards.size(), 15);
        UNIT_ASSERT(checkShards(7, 8, {11}));
        UNIT_ASSERT(checkShards(9, 9, {11, 13}));
        UNIT_ASSERT(checkShards(10, 12, {13}));
        UNIT_ASSERT(checkShards(13, 13, {12, 13}));
        UNIT_ASSERT(checkShards(14, 15, {12}));

    }

    Y_UNIT_TEST(FillItemsFromSchemaMappingTest) {
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId& tablet, TTabletStorageInfo* info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);

        const TPathId domainPathId = TPath::Resolve("/MyRoot", schemeshard).Base()->PathId;

        auto getImportInfo = [&](const TString& settingsProto, const TString& schemaMapping, bool expectSuccess = true, const TString& expectedError = {}) -> TImportInfo::TPtr {
            Ydb::Import::ImportFromS3Settings settings;
            UNIT_ASSERT(NProtoBuf::TextFormat::ParseFromString(settingsProto, &settings));

            TImportInfo::TPtr importInfo = new TImportInfo(42, "uid_42", TImportInfo::EKind::S3, settings, domainPathId, "localhost");

            for (const auto& item : settings.items()) {
                auto& importInfoItem = importInfo->Items.emplace_back(item.destination_path());
                importInfoItem.SrcPath = item.source_path();
                importInfoItem.SrcPrefix = item.source_prefix();
            }

            importInfo->SchemaMapping.ConstructInPlace();
            TString error;
            UNIT_ASSERT_C(importInfo->SchemaMapping->Deserialize(schemaMapping, error), error);

            auto fillResult = runtime.RunCall(
                [&]() {
                    return importInfo->FillItemsFromSchemaMapping(schemeshard);
                }
            );
            if (expectSuccess) {
                UNIT_ASSERT_C(fillResult.Success, fillResult.ErrorMessage);
            } else {
                UNIT_ASSERT(!fillResult.Success);
                UNIT_ASSERT_STRING_CONTAINS(fillResult.ErrorMessage, expectedError);
            }

            std::sort(importInfo->Items.begin(), importInfo->Items.end(), [](const TImportInfo::TItem& item1, const TImportInfo::TItem& item2) { return item1.SrcPath < item2.SrcPath; });
            return importInfo;
        };

        auto validateImportItem = [](const TImportInfo::TItem& item, const TString& dstPath, const TString& srcPrefix, const TString& srcPath, TMaybe<NBackup::TEncryptionIV> iv = Nothing(), const std::source_location location = std::source_location::current()) {
            UNIT_ASSERT_VALUES_EQUAL_C(item.DstPathName, dstPath, location.file_name() << ':' << location.line());
            UNIT_ASSERT_VALUES_EQUAL_C(item.SrcPrefix, srcPrefix, location.file_name() << ':' << location.line());
            UNIT_ASSERT_VALUES_EQUAL_C(item.SrcPath, srcPath, location.file_name() << ':' << location.line());
            UNIT_ASSERT_EQUAL_C(item.ExportItemIV, iv, location.file_name() << ':' << location.line());
        };

        const TString settingsWithoutFilter = "";

        const TString settingsWithDestinationPath = R"proto(
            destination_path: "/MyRoot/restored//"
        )proto";

        const TString settingsWithFilterByPath = R"proto(
            items {
                source_path: "dir1/Table1"
            }
        )proto";

        const TString settingsWithRecursiveFilterByPath = R"proto(
            items {
                source_path: "dir1"
            }
        )proto";

        const TString settingsWithRecursiveFilterByPathWithDestinationPath = R"proto(
            destination_path: "/MyRoot/dst"
            items {
                source_path: "dir2"
            }
        )proto";

        const TString settingsWithRecursiveFilterByPathAndDestination = R"proto(
            items {
                source_path: "dir1"
                destination_path: "/MyRoot/dir1dst"
            }
        )proto";

        const TString settingsWithExplicitParams = R"proto(
            items {
                source_prefix: "prefix1"
                destination_path: "/MyRoot/d1"
            }
            items {
                source_prefix: "prefix/prefix3"
                destination_path: "/MyRoot/d3"
            }
        )proto";

        const TString settingsWithExplicitParamsTwoDestinations = R"proto(
            destination_path: "/MyRoot/common_dest"
            items {
                source_prefix: "prefix1"
                destination_path: "d1"
            }
            items {
                source_prefix: "prefix/prefix3"
                destination_path: "/MyRoot/d3"
            }
        )proto";

        const TString settingsWithInvalidFilterByPath = R"proto(
            items {
                source_path: "invalid"
            }
            items {
                source_path: "dir1"
            }
        )proto";

        const TString settingsWithInvalidFilterByPrefix = R"proto(
            items {
                source_prefix: "invalid"
            }
            items {
                source_path: "dir1"
            }
        )proto";

        const TString emptySchemaMapping = R"json(
        {
            "exportedObjects": {}
        }
        )json";

        const TString schemaMapping = R"json(
        {
            "exportedObjects": {
                "dir1/Table1": {
                    "exportPrefix": "prefix1"
                },
                "dir2/Table2": {
                    "exportPrefix": "prefix2"
                },
                "dir1/dir2/Table3": {
                    "exportPrefix": "prefix/prefix3"
                }
            }
        }
        )json";

        const TString schemaMappingWithIVs = R"json(
        {
            "exportedObjects": {
                "dir1/Table1": {
                    "exportPrefix": "prefix1",
                    "iv": "1234567890ABCDEF98765432"
                },
                "dir2/Table2": {
                    "exportPrefix": "prefix2",
                    "iv": "1234567890ABCDEF98765432"
                },
                "dir1/dir2/Table3": {
                    "exportPrefix": "prefix/prefix3",
                    "iv": "1234567890ABCDEF98765432"
                }
            }
        }
        )json";

        const NBackup::TEncryptionIV iv = NBackup::TEncryptionIV::FromHexString("1234567890ABCDEF98765432");

        {
            auto importInfo = getImportInfo(settingsWithoutFilter, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 3);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
            validateImportItem(importInfo->Items[2], "/MyRoot/dir2/Table2", "prefix2", "dir2/Table2");
        }

        {
            auto importInfo = getImportInfo(settingsWithDestinationPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 3);
            validateImportItem(importInfo->Items[0], "/MyRoot/restored/dir1/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/restored/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
            validateImportItem(importInfo->Items[2], "/MyRoot/restored/dir2/Table2", "prefix2", "dir2/Table2");
        }

        {
            auto importInfo = getImportInfo(settingsWithFilterByPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 1);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1");
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPathWithDestinationPath, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 1);
            validateImportItem(importInfo->Items[0], "/MyRoot/dst/dir2/Table2", "prefix2", "dir2/Table2");
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPathAndDestination, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1dst/Table1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1dst/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithExplicitParams, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/d1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/d3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithExplicitParamsTwoDestinations, schemaMapping);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/common_dest/d1", "prefix1", "dir1/Table1");
            validateImportItem(importInfo->Items[1], "/MyRoot/d3", "prefix/prefix3", "dir1/dir2/Table3");
        }

        {
            auto importInfo = getImportInfo(settingsWithoutFilter, schemaMappingWithIVs);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 3);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1", iv);
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3", iv);
            validateImportItem(importInfo->Items[2], "/MyRoot/dir2/Table2", "prefix2", "dir2/Table2", iv);
        }

        {
            auto importInfo = getImportInfo(settingsWithRecursiveFilterByPath, schemaMappingWithIVs);
            UNIT_ASSERT_VALUES_EQUAL(importInfo->Items.size(), 2);
            validateImportItem(importInfo->Items[0], "/MyRoot/dir1/Table1", "prefix1", "dir1/Table1", iv);
            validateImportItem(importInfo->Items[1], "/MyRoot/dir1/dir2/Table3", "prefix/prefix3", "dir1/dir2/Table3", iv);
        }

        {
            auto importInfo = getImportInfo(settingsWithInvalidFilterByPath, schemaMapping, false, "cannot find source path \"invalid\" in schema mapping");
        }

        {
            auto importInfo = getImportInfo(settingsWithInvalidFilterByPrefix, schemaMapping, false, "cannot find prefix \"invalid\" in schema mapping");
        }

        {
            auto importInfo = getImportInfo(settingsWithoutFilter, emptySchemaMapping, false, "no items to import");
        }
    }

}

Y_UNIT_TEST_SUITE(TExportSettingsTest) {
    template <typename TSettings>
    void CheckSettings(TExportInfo::EKind kind) {
        TSettings settings;
        settings.add_items()->set_source_path("/Root/first");
        settings.add_items()->set_source_path("/Root/second");
        settings.set_number_of_retries(7);
        TExportInfo info(1, "", kind, settings, TPathId(), "");
        const auto& cached = info.GetSettings<TSettings>();
        UNIT_ASSERT_VALUES_EQUAL(cached.items_size(), 2);
        UNIT_ASSERT(&cached == &info.GetSettings<TSettings>());
        UNIT_ASSERT(&cached.items(0) == &info.GetSettings<TSettings>().items(0));

        // Responses and metadata preparation must not mutate the cached settings.
        auto updated = cached;
        updated.mutable_items(0)->set_source_path("/Root/replaced");
        UNIT_ASSERT_VALUES_EQUAL(cached.items(0).source_path(), "/Root/first");
        UNIT_ASSERT(info.SetSettings(std::move(updated)));
        UNIT_ASSERT_VALUES_EQUAL(info.GetSettings<TSettings>().items(0).source_path(), "/Root/replaced");

        // Reconstruct through the same serialized-settings constructor used on reboot.
        TExportInfo restored(1, "", kind, info.GetSerializedSettings(), TPathId(), "");
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSettings<TSettings>().SerializeAsString(),
            info.GetSettings<TSettings>().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(restored.GetSettings<TSettings>().number_of_retries(), 7);
    }

    Y_UNIT_TEST(YtSettings) { CheckSettings<Ydb::Export::ExportToYtSettings>(TExportInfo::EKind::YT); }
    Y_UNIT_TEST(S3Settings) { CheckSettings<Ydb::Export::ExportToS3Settings>(TExportInfo::EKind::S3); }
    Y_UNIT_TEST(FsSettings) { CheckSettings<Ydb::Export::ExportToFsSettings>(TExportInfo::EKind::FS); }

    template <typename TSettings>
    void CheckEncryptionKeyRemoval(TExportInfo::EKind kind) {
        TSettings settings;
        settings.mutable_encryption_settings()->mutable_symmetric_key()->set_key("secret");
        TExportInfo info(1, "", kind, settings, TPathId(), "");
        auto updated = info.GetSettings<TSettings>();
        updated.mutable_encryption_settings()->clear_symmetric_key();
        UNIT_ASSERT(info.SetSettings(std::move(updated)));
        UNIT_ASSERT(!info.GetSettings<TSettings>().encryption_settings().has_symmetric_key());
        TExportInfo restored(1, "", kind, info.GetSerializedSettings(), TPathId(), "");
        UNIT_ASSERT(!restored.GetSettings<TSettings>().encryption_settings().has_symmetric_key());
    }

    Y_UNIT_TEST(S3EncryptionKeyRemoval) {
        CheckEncryptionKeyRemoval<Ydb::Export::ExportToS3Settings>(TExportInfo::EKind::S3);
    }
    Y_UNIT_TEST(FsEncryptionKeyRemoval) {
        CheckEncryptionKeyRemoval<Ydb::Export::ExportToFsSettings>(TExportInfo::EKind::FS);
    }
}
