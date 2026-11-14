#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

enum class EKind {
    Sync,
    Async,
    Unique,
    UniqueOffline,
    Vector,
    PrefixedVector,
    FulltextPlain,
    FulltextRelevance,
    FulltextCompact,
    FulltextCompactRelevance,
    Json,
    JsonCompact,
};

bool IsVector(EKind kind) {
    return kind == EKind::Vector || kind == EKind::PrefixedVector;
}

bool IsCompact(EKind kind) {
    return kind == EKind::FulltextCompact || kind == EKind::FulltextCompactRelevance || kind == EKind::JsonCompact;
}

Ydb::Table::TableIndex MakeIndex(EKind kind, bool rebuild) {
    Ydb::Table::TableIndex index;
    index.set_name("idx");
    switch (kind) {
    case EKind::Sync:
        index.mutable_global_index();
        index.add_index_columns("value");
        break;
    case EKind::Async:
        index.mutable_global_async_index();
        index.add_index_columns("value");
        break;
    case EKind::Unique:
    case EKind::UniqueOffline:
        index.mutable_global_unique_index();
        index.add_index_columns("value");
        break;
    case EKind::Vector:
    case EKind::PrefixedVector: {
        if (kind == EKind::PrefixedVector) {
            index.add_index_columns("prefix");
        }
        index.add_index_columns("embedding");
        auto* vector = index.mutable_global_vector_kmeans_tree_index();
        if (!rebuild) {
            auto* settings = vector->mutable_vector_settings();
            settings->set_clusters(2);
            settings->set_levels(1);
            settings->mutable_settings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
            settings->mutable_settings()->set_vector_dimension(4);
            settings->mutable_settings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        }
        break;
    }
    case EKind::FulltextPlain:
    case EKind::FulltextRelevance:
    case EKind::FulltextCompact:
    case EKind::FulltextCompactRelevance: {
        index.add_index_columns("text");
        const bool relevance = kind == EKind::FulltextRelevance || kind == EKind::FulltextCompactRelevance;
        auto* settings = relevance
            ? index.mutable_global_fulltext_relevance_index()->mutable_fulltext_settings()
            : index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings();
        if (!rebuild) {
            auto* column = settings->add_columns();
            column->set_column("text");
            column->mutable_analyzers()->set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        }
        break;
    }
    case EKind::Json:
    case EKind::JsonCompact:
        index.mutable_global_json_index();
        index.add_index_columns("json");
        break;
    }
    if (!rebuild && kind != EKind::Json && kind != EKind::JsonCompact) {
        index.add_data_columns("payload");
    }
    // Exercise independent inheritance of keys, covered columns and specialized settings.
    // Vector requests also have a separate omitted-keys regression below.
    if (rebuild && !IsVector(kind)) {
        index.clear_index_columns();
    }
    return index;
}

using TSchemeBlocker = TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction>;

auto IsOperation(NKikimrSchemeOp::EOperationType type) {
    return [type](const TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
        return ev->Get()->Record.TransactionSize() && ev->Get()->Record.GetTransaction(0).GetOperationType() == type;
    };
}

// Every implementation reaches one of these scan requests in Filling. Blocking requests rather
// than elapsed time also works for small tables, empty event queues and SchemeShard restarts.
struct TFillingBlocker {
    TBlockEvents<TEvDataShard::TEvBuildIndexCreateRequest> Secondary;
    TBlockEvents<TEvDataShard::TEvBuildFulltextIndexRequest> Fulltext;
    TBlockEvents<TEvDataShard::TEvLocalKMeansRequest> Vector;
    TBlockEvents<TEvDataShard::TEvPrefixKMeansRequest> Prefix;

    explicit TFillingBlocker(TTestActorRuntime& runtime)
        : Secondary(runtime)
        , Fulltext(runtime)
        , Vector(runtime)
        , Prefix(runtime)
    {}

    bool Empty() const {
        return Secondary.empty() && Fulltext.empty() && Vector.empty() && Prefix.empty();
    }

    void Release() {
        Secondary.Stop().Unblock();
        Fulltext.Stop().Unblock();
        Vector.Stop().Unblock();
        Prefix.Stop().Unblock();
    }
};

struct TRebuildTest {
    static constexpr ui64 SchemeShard = TTestTxConfig::SchemeShard;
    static constexpr ui64 RowCount = 32;
    const TString Table = "/MyRoot/Table";
    const TString IndexPath = Table + "/idx";
    const EKind Kind;
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId = 100;
    ui64 LivePathId = 0;
    NKikimrSchemeOp::TIndexDescription LiveDescription;
    TMap<TString, ui64> LiveChildren;
    TMap<TString, ui32> LiveRows;
    TVector<TString> SecondaryRows;

    explicit TRebuildTest(EKind kind, Ydb::Table::TableIndex initialIndex = {})
        : Kind(kind)
        , Env(Runtime, TTestEnvOptions().EnableCompactFulltextIndex(IsCompact(kind)))
    {
        if (kind == EKind::UniqueOffline) {
            Runtime.GetAppData().FeatureFlags.SetEnableOnlineAddUniqueIndex(false);
            Reboot(); // This feature flag is cached by SchemeShard at activation.
        }
        TestCreateTable(Runtime, ++TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "id" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            Columns { Name: "text" Type: "String" }
            Columns { Name: "json" Type: "Json" }
            Columns { Name: "embedding" Type: "String" }
            Columns { Name: "prefix" Type: "Uint32" }
            Columns { Name: "payload" Type: "String" }
            KeyColumnNames: ["id"]
        )");
        Env.TestWaitNotification(Runtime, TxId);
        for (ui64 id = 1; id <= RowCount; ++id) {
            const ui64 value = id * 10;
            const ui32 prefix = id % 2;
            const TString text = id % 2 ? "green apple" : "red apple";
            const TString json = id % 2 ? R"({"color":"green","n":1})" : R"({"color":"red","n":2})";
            const TString payload = TStringBuilder() << "payload-" << id;
            const ui8 vector[] = {ui8(id * 7), ui8(id * 13), ui8(id * 19), ui8(id * 23),
                ui8(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8)};
            UploadRow(Runtime, Table, 0, {1}, {2, 3, 4, 5, 6, 7},
                {TCell::Make(id)},
                {TCell::Make(value), TCell(text.data(), text.size()), TCell(json.data(), json.size()),
                    TCell(reinterpret_cast<const char*>(vector), sizeof(vector)), TCell::Make(prefix),
                    TCell(payload.data(), payload.size())});
        }
        if (initialIndex.type_case() == Ydb::Table::TableIndex::TYPE_NOT_SET) {
            initialIndex = MakeIndex(kind, false);
        }
        const ui64 buildId = Start(initialIndex, false);
        Wait(buildId, Ydb::Table::IndexBuildState::STATE_DONE);
        auto description = DescribePrivatePath(Runtime, IndexPath);
        TestDescribeResult(description, {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady),
            NLs::IndexType(NTableIndex::ConvertIndexType(initialIndex.type_case(), IsCompact(kind)))});
        LivePathId = description.GetPathDescription().GetSelf().GetPathId();
        LiveDescription = description.GetPathDescription().GetTableIndex();
        for (const auto& child : description.GetPathDescription().GetChildren()) {
            LiveChildren.emplace(child.GetName(), child.GetPathId());
            if (child.GetPathType() == NKikimrSchemeOp::EPathTypeTable) {
                LiveRows.emplace(child.GetName(), CountRows(Runtime, SchemeShard, IndexPath + "/" + child.GetName()));
            }
        }
        UNIT_ASSERT(!LiveRows.empty());
        for (const auto& [name, rows] : LiveRows) {
            UNIT_ASSERT_C(rows > 0, name);
        }
        if (IsSecondary()) {
            SecondaryRows = ReadShards(Runtime, SchemeShard, IndexPath + "/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(LiveRows.at("indexImplTable"), RowCount);
        } else if (IsVector(kind)) {
            UNIT_ASSERT_VALUES_EQUAL(LiveRows.at("indexImplPostingTable"), RowCount);
        }
        TestForgetBuildIndex(Runtime, ++TxId, SchemeShard, "/MyRoot", buildId);
    }

    bool IsSecondary() const {
        return Kind == EKind::Sync || Kind == EKind::Async || Kind == EKind::Unique || Kind == EKind::UniqueOffline;
    }

    ui64 Start(const Ydb::Table::TableIndex& index, bool rebuild = true,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
    {
        const ui64 id = ++TxId;
        auto* request = CreateBuildIndexRequest(id, "/MyRoot", Table, index);
        request->Record.MutableSettings()->set_is_rebuild(rebuild);
        const auto sender = Runtime.AllocateEdgeActor();
        Runtime.SendToPipe(SchemeShard, sender, request);
        const auto response = Runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), expected, response->Get()->Record.DebugString());
        return id;
    }

    ui64 Start() {
        return Start(MakeIndex(Kind, true));
    }

    TString TemporaryPath(ui64 id) const {
        return TStringBuilder() << Table << "/__ydb_rebuild_" << id;
    }

    void State(ui64 id, Ydb::Table::IndexBuildState::State expected) {
        const auto result = TestGetBuildIndex(Runtime, SchemeShard, "/MyRoot", id);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetIndexBuild().GetState(), expected, result.DebugString());
    }

    void Wait(ui64 id, Ydb::Table::IndexBuildState::State expected) {
        Env.TestWaitNotification(Runtime, id);
        State(id, expected);
    }

    void Reboot() {
        RebootTablet(Runtime, SchemeShard, Runtime.AllocateEdgeActor());
    }

    void CheckMetadata(const NKikimrSchemeOp::TIndexDescription& index) {
        UNIT_ASSERT_VALUES_EQUAL(index.GetType(), LiveDescription.GetType());
        UNIT_ASSERT_VALUES_EQUAL(index.KeyColumnNamesSize(), LiveDescription.KeyColumnNamesSize());
        for (size_t i = 0; i < index.KeyColumnNamesSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(index.GetKeyColumnNames(i), LiveDescription.GetKeyColumnNames(i));
        }
        UNIT_ASSERT_VALUES_EQUAL(index.DataColumnNamesSize(), LiveDescription.DataColumnNamesSize());
        for (size_t i = 0; i < index.DataColumnNamesSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(index.GetDataColumnNames(i), LiveDescription.GetDataColumnNames(i));
        }
        UNIT_ASSERT_VALUES_EQUAL(index.GetVectorIndexKmeansTreeDescription().SerializeAsString(),
            LiveDescription.GetVectorIndexKmeansTreeDescription().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(index.GetFulltextIndexDescription().SerializeAsString(),
            LiveDescription.GetFulltextIndexDescription().SerializeAsString());
    }

    void CheckLive() {
        const auto description = DescribePrivatePath(Runtime, IndexPath);
        TestDescribeResult(description, {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
        UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().GetSelf().GetPathId(), LivePathId);
        CheckMetadata(description.GetPathDescription().GetTableIndex());
        UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().ChildrenSize(), LiveChildren.size());
        for (const auto& child : description.GetPathDescription().GetChildren()) {
            UNIT_ASSERT_VALUES_EQUAL_C(child.GetPathId(), LiveChildren.at(child.GetName()), child.GetName());
        }
    }

    void CheckFinished(ui64 id, bool replaced) {
        TestDescribeResult(DescribePrivatePath(Runtime, TemporaryPath(id)), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(Runtime, Table), {NLs::IndexesCount(1)});
        const auto description = DescribePrivatePath(Runtime, IndexPath);
        TestDescribeResult(description, {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
        CheckMetadata(description.GetPathDescription().GetTableIndex());
        if (replaced) {
            UNIT_ASSERT(description.GetPathDescription().GetSelf().GetPathId() != LivePathId);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPathDescription().ChildrenSize(), LiveChildren.size());
            for (const auto& child : description.GetPathDescription().GetChildren()) {
                UNIT_ASSERT_C(child.GetPathId() != LiveChildren.at(child.GetName()), child.GetName());
            }
        } else {
            CheckLive();
        }
        for (const auto& [name, rows] : LiveRows) {
            const auto actual = CountRows(Runtime, SchemeShard, IndexPath + "/" + name);
            if (replaced && IsVector(Kind) && name == "indexImplLevelTable") {
                // KMeans may produce a different number of non-empty clusters on each build.
                UNIT_ASSERT_C(actual > 0 && actual <= RowCount, actual);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(actual, rows, name);
            }
        }
        if (IsSecondary()) {
            UNIT_ASSERT_VALUES_EQUAL(ReadShards(Runtime, SchemeShard, IndexPath + "/indexImplTable"), SecondaryRows);
        }
        const auto snapshotResult = ReadSystemTable(Runtime, SchemeShard,
            "SnapshotTables", {"Id", "TableOwnerId", "TableLocalId"}, {"Id"});
        const auto snapshots = NClient::TValue::Create(snapshotResult);
        UNIT_ASSERT_VALUES_EQUAL_C(snapshots["Result"]["List"].Size(), 0, "Rebuild leaked a snapshot");
    }

    void Cancel(ui64 id, Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS) {
        TestCancelBuildIndex(Runtime, ++TxId, SchemeShard, "/MyRoot", id, {expected});
    }
};

void CheckPartitioning(TRebuildTest& test, const TString& name, ui32 partitions, ui32 maxPartitions,
    ui32 sizeMb, ui32 replicas)
{
    const auto description = DescribePrivatePath(test.Runtime, test.IndexPath + "/" + name, true, true);
    TestDescribeResult(description, {NLs::IsTable, NLs::PartitionCount(partitions),
        NLs::MinPartitionsCountEqual(partitions), NLs::MaxPartitionsCountEqual(maxPartitions)});
    const auto& config = description.GetPathDescription().GetTable().GetPartitionConfig();
    UNIT_ASSERT_VALUES_EQUAL(config.GetPartitioningPolicy().GetSizeToSplit(), ui64(sizeMb) << 20);
    ui32 followers = 0;
    for (const auto& group : config.GetFollowerGroups()) {
        followers += group.GetFollowerCount();
        UNIT_ASSERT(!group.GetRequireAllDataCenters());
    }
    UNIT_ASSERT_VALUES_EQUAL(followers, replicas);
}

void PreservesPartitioning(EKind kind) {
    auto index = MakeIndex(kind, false);
    auto* settings = kind == EKind::Sync ? index.mutable_global_index()->mutable_settings()
        : kind == EKind::Async ? index.mutable_global_async_index()->mutable_settings()
        : index.mutable_global_unique_index()->mutable_settings();
    settings->mutable_partitioning_settings()->set_min_partitions_count(3);
    settings->mutable_partitioning_settings()->set_max_partitions_count(3);
    settings->mutable_partitioning_settings()->set_partition_size_mb(64);
    settings->mutable_read_replicas_settings()->set_any_az_read_replicas_count(1);
    for (ui64 boundary : {160u, 250u}) {
        auto* point = settings->mutable_partition_at_keys()->add_split_points();
        point->mutable_type()->mutable_tuple_type()->add_elements()->mutable_optional_type()
            ->mutable_item()->set_type_id(Ydb::Type::UINT64);
        point->mutable_value()->add_items()->set_uint64_value(boundary);
    }
    TRebuildTest test(kind, index);
    CheckPartitioning(test, "indexImplTable", 3, 3, 64, 1);
    const auto before = DescribePrivatePath(test.Runtime, test.IndexPath + "/indexImplTable", true, true);
    const ui64 id = test.Start(); // Omit all implementation settings in the rebuild request.
    test.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
    test.CheckFinished(id, true);
    test.Reboot();
    CheckPartitioning(test, "indexImplTable", 3, 3, 64, 1);
    const auto after = DescribePrivatePath(test.Runtime, test.IndexPath + "/indexImplTable", true, true);
    const auto& oldPartitions = before.GetPathDescription().GetTablePartitions();
    const auto& newPartitions = after.GetPathDescription().GetTablePartitions();
    UNIT_ASSERT_VALUES_EQUAL(oldPartitions.size(), newPartitions.size());
    for (int i = 0; i < oldPartitions.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(oldPartitions.Get(i).GetEndOfRangeKeyPrefix(),
            newPartitions.Get(i).GetEndOfRangeKeyPrefix());
    }

    // Explicit settings must replace inherited boundaries and may remove read replicas.
    auto override = MakeIndex(kind, true);
    settings = kind == EKind::Sync ? override.mutable_global_index()->mutable_settings()
        : kind == EKind::Async ? override.mutable_global_async_index()->mutable_settings()
        : override.mutable_global_unique_index()->mutable_settings();
    settings->set_uniform_partitions(2);
    settings->mutable_partitioning_settings()->set_min_partitions_count(2);
    settings->mutable_partitioning_settings()->set_max_partitions_count(4);
    settings->mutable_partitioning_settings()->set_partition_size_mb(32);
    settings->mutable_read_replicas_settings()->set_any_az_read_replicas_count(0);
    const ui64 next = test.Start(override);
    test.Wait(next, Ydb::Table::IndexBuildState::STATE_DONE);
    test.Reboot();
    CheckPartitioning(test, "indexImplTable", 2, 4, 32, 0);
    test.CheckMetadata(DescribePrivatePath(test.Runtime, test.IndexPath).GetPathDescription().GetTableIndex());
    UNIT_ASSERT_VALUES_EQUAL(CountRows(test.Runtime, test.SchemeShard, test.IndexPath + "/indexImplTable"), test.RowCount);
    TestDescribeResult(DescribePrivatePath(test.Runtime, test.TemporaryPath(next)), {NLs::PathNotExist});

    // An explicitly empty split list requests one partition; it is not an omitted list.
    settings->mutable_partition_at_keys();
    settings->mutable_partitioning_settings()->set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
    settings->mutable_partitioning_settings()->clear_partition_size_mb();
    settings->mutable_partitioning_settings()->set_min_partitions_count(1);
    settings->mutable_partitioning_settings()->set_max_partitions_count(1);
    const ui64 single = test.Start(override);
    test.Wait(single, Ydb::Table::IndexBuildState::STATE_DONE);
    test.Reboot();
    CheckPartitioning(test, "indexImplTable", 1, 1, 0, 0);
    UNIT_ASSERT_VALUES_EQUAL(CountRows(test.Runtime, test.SchemeShard, test.IndexPath + "/indexImplTable"), test.RowCount);
    TestDescribeResult(DescribePrivatePath(test.Runtime, test.TemporaryPath(single)), {NLs::PathNotExist});
}

void Lifecycle(EKind kind) {
    TRebuildTest test(kind);
    auto& runtime = test.Runtime;
    TSchemeBlocker initialization(runtime, IsOperation(NKikimrSchemeOp::ESchemeOpCreateIndexBuild));
    TFillingBlocker filling(runtime);
    TSchemeBlocker applying(runtime, IsOperation(NKikimrSchemeOp::ESchemeOpApplyIndexBuild));
    TSchemeBlocker replacement(runtime, IsOperation(NKikimrSchemeOp::ESchemeOpMoveIndex));
    const ui64 id = test.Start();

    runtime.WaitFor("rebuild initialization", [&] { return !initialization.empty(); });
    test.State(id, Ydb::Table::IndexBuildState::STATE_PREPARING);
    test.CheckLive();
    TestDescribeResult(DescribePrivatePath(runtime, test.TemporaryPath(id)), {NLs::PathNotExist});
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_PREPARING);
    test.CheckLive();
    initialization.Stop().Unblock();

    runtime.WaitFor("rebuild filling", [&] { return !filling.Empty(); });
    test.State(id, Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);
    test.CheckLive();
    const auto temporary = DescribePrivatePath(runtime, test.TemporaryPath(id));
    TestDescribeResult(temporary,
        {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateWriteOnly)});
    UNIT_ASSERT(temporary.GetPathDescription().GetSelf().GetPathId() != test.LivePathId);
    test.CheckMetadata(temporary.GetPathDescription().GetTableIndex());
    // The source table lock must protect both the public index and another rebuild.
    const ui64 competing = test.Start(MakeIndex(kind, true), true, Ydb::StatusIds::OVERLOADED);
    TestDescribeResult(DescribePrivatePath(runtime, test.TemporaryPath(competing)), {NLs::PathNotExist});
    TestDropTableIndex(runtime, ++test.TxId, "/MyRoot", R"(TableName: "Table" IndexName: "idx")",
        {NKikimrScheme::StatusMultipleModifications});
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);
    test.CheckLive();
    filling.Release();

    runtime.WaitFor("rebuild applying", [&] { return !applying.empty(); });
    test.State(id, Ydb::Table::IndexBuildState::STATE_APPLYING);
    test.CheckLive();
    test.Cancel(id, Ydb::StatusIds::PRECONDITION_FAILED);
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_APPLYING);
    test.CheckLive();
    applying.Stop().Unblock();

    runtime.WaitFor("rebuild replacement", [&] { return !replacement.empty(); });
    test.State(id, Ydb::Table::IndexBuildState::STATE_APPLYING);
    test.CheckLive();
    const auto readyTemporary = DescribePrivatePath(runtime, test.TemporaryPath(id));
    TestDescribeResult(readyTemporary, {NLs::PathExist, NLs::IndexState(NKikimrSchemeOp::EIndexStateReady)});
    test.CheckMetadata(readyTemporary.GetPathDescription().GetTableIndex());
    test.Cancel(id, Ydb::StatusIds::PRECONDITION_FAILED);
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_APPLYING);
    test.CheckLive();
    replacement.Stop().Unblock();

    test.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
    test.CheckFinished(id, true);
    test.Cancel(id, Ydb::StatusIds::PRECONDITION_FAILED);
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_DONE);
    test.CheckFinished(id, true);
    TestForgetBuildIndex(runtime, ++test.TxId, test.SchemeShard, "/MyRoot", id);
    UNIT_ASSERT_VALUES_EQUAL(TestListBuildIndex(runtime, test.SchemeShard, "/MyRoot").EntriesSize(), 0);
}

void Cancellation(EKind kind, bool preparing = false) {
    TRebuildTest test(kind);
    auto& runtime = test.Runtime;
    TSchemeBlocker initialization(runtime, IsOperation(NKikimrSchemeOp::ESchemeOpCreateIndexBuild));
    TFillingBlocker filling(runtime);
    TSchemeBlocker cleanup(runtime, IsOperation(NKikimrSchemeOp::ESchemeOpCancelIndexBuild));
    const ui64 id = test.Start();
    runtime.WaitFor("rebuild initialization", [&] { return !initialization.empty(); });
    if (preparing) {
        test.State(id, Ydb::Table::IndexBuildState::STATE_PREPARING);
        test.Cancel(id);
        initialization.Stop().Unblock();
    } else {
        initialization.Stop().Unblock();
        runtime.WaitFor("rebuild filling", [&] { return !filling.Empty(); });
        test.State(id, Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);
        test.CheckLive();
        test.Cancel(id);
    }
    filling.Release();
    runtime.WaitFor("rebuild cancellation cleanup", [&] { return !cleanup.empty(); });
    test.State(id, Ydb::Table::IndexBuildState::STATE_CANCELLATION);
    test.CheckLive();
    test.Cancel(id, Ydb::StatusIds::PRECONDITION_FAILED);
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_CANCELLATION);
    test.CheckLive();
    cleanup.Stop().Unblock();
    test.Wait(id, Ydb::Table::IndexBuildState::STATE_CANCELLED);
    test.CheckFinished(id, false);
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_CANCELLED);
    test.CheckFinished(id, false);
    // A fresh rebuild proves that cancellation released the source table lock.
    const ui64 retry = test.Start();
    test.Wait(retry, Ydb::Table::IndexBuildState::STATE_DONE);
    test.CheckFinished(retry, true);
}

void Rejection(EKind kind, bool finalized = true) {
    TRebuildTest test(kind);
    auto& runtime = test.Runtime;
    const auto operation = finalized ? NKikimrSchemeOp::ESchemeOpMoveIndex : NKikimrSchemeOp::ESchemeOpApplyIndexBuild;
    TSchemeBlocker application(runtime, IsOperation(operation));
    TSchemeBlocker cleanup(runtime, IsOperation(finalized
        ? NKikimrSchemeOp::ESchemeOpDropIndex : NKikimrSchemeOp::ESchemeOpCancelIndexBuild));
    const ui64 id = test.Start();
    runtime.WaitFor("rebuild application", [&] { return !application.empty(); });
    test.CheckLive();
    test.State(id, Ydb::Table::IndexBuildState::STATE_APPLYING);
    TestDescribeResult(DescribePrivatePath(runtime, test.TemporaryPath(id)), {NLs::PathExist,
        NLs::IndexState(finalized ? NKikimrSchemeOp::EIndexStateReady : NKikimrSchemeOp::EIndexStateWriteOnly)});
    // Exercise real proposal validation on both sides of snapshot finalization. No injected
    // success/failure responses: the rejected operation must leave cleanup to the state machine.
    auto reject = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([&](auto& ev) {
        if (IsOperation(operation)(ev)) {
            auto* tx = ev->Get()->Record.MutableTransaction(0);
            if (finalized) {
                tx->MutableMoveIndex()->SetAllowOverwrite(false);
            } else {
                tx->MutableApplyIndexBuild()->SetForReplication(true);
            }
        }
    });
    application.Stop().Unblock();
    runtime.WaitFor("rejected rebuild cleanup", [&] { return !cleanup.empty(); });
    test.State(id, Ydb::Table::IndexBuildState::STATE_REJECTION);
    test.CheckLive();
    const auto rejected = TestGetBuildIndex(runtime, test.SchemeShard, "/MyRoot", id);
    UNIT_ASSERT_STRING_CONTAINS(rejected.DebugString(), finalized
        ? "overwrite flag has not been set"
        : "Replication index finalization requires an internal operation on a replica");
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_REJECTION);
    test.CheckLive();
    cleanup.Stop().Unblock();
    test.Wait(id, Ydb::Table::IndexBuildState::STATE_REJECTED);
    test.CheckFinished(id, false);
    test.Reboot();
    test.State(id, Ydb::Table::IndexBuildState::STATE_REJECTED);
    test.CheckFinished(id, false);
    reject.Remove();
    const ui64 retry = test.Start();
    test.Wait(retry, Ydb::Table::IndexBuildState::STATE_DONE);
    test.CheckFinished(retry, true);
}

} // namespace

Y_UNIT_TEST_SUITE(RebuildIndexLifecycle) {

// Each kind runs the same public invariants, with real scans and all its implementation tables.
// There is one success, one early cancellation and one post-finalization rejection per kind.
#define REBUILD_INDEX_TESTS(Name) \
    Y_UNIT_TEST(Name##StagesAndReboots) { Lifecycle(EKind::Name); } \
    Y_UNIT_TEST(Name##CancelFilling) { Cancellation(EKind::Name); } \
    Y_UNIT_TEST(Name##RejectReplacement) { Rejection(EKind::Name); }

    REBUILD_INDEX_TESTS(Sync)
    REBUILD_INDEX_TESTS(Async)
    REBUILD_INDEX_TESTS(Unique)
    REBUILD_INDEX_TESTS(UniqueOffline)
    REBUILD_INDEX_TESTS(Vector)
    REBUILD_INDEX_TESTS(PrefixedVector)
    REBUILD_INDEX_TESTS(FulltextPlain)
    REBUILD_INDEX_TESTS(FulltextRelevance)
    REBUILD_INDEX_TESTS(FulltextCompact)
    REBUILD_INDEX_TESTS(FulltextCompactRelevance)
    REBUILD_INDEX_TESTS(Json)
    REBUILD_INDEX_TESTS(JsonCompact)

#undef REBUILD_INDEX_TESTS

    Y_UNIT_TEST(CancelPreparingSecondary) {
        Cancellation(EKind::Sync, true);
    }

    Y_UNIT_TEST(CancelPreparingVector) {
        Cancellation(EKind::Vector, true);
    }

    Y_UNIT_TEST(CancelPreparingCompactJson) {
        Cancellation(EKind::JsonCompact, true);
    }

    Y_UNIT_TEST(RejectApplyingSecondary) {
        Rejection(EKind::Sync, false);
    }

    Y_UNIT_TEST(RejectApplyingVector) {
        Rejection(EKind::Vector, false);
    }

    Y_UNIT_TEST(RejectApplyingCompactJson) {
        Rejection(EKind::JsonCompact, false);
    }

    Y_UNIT_TEST(VectorInheritsOmittedIndexColumns) {
        TRebuildTest test(EKind::Vector);
        auto index = MakeIndex(EKind::Vector, true);
        index.clear_index_columns();
        const ui64 id = test.Start(index);
        test.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        test.CheckFinished(id, true);
    }

    Y_UNIT_TEST(SyncPreservesPartitioning) {
        PreservesPartitioning(EKind::Sync);
    }

    Y_UNIT_TEST(AsyncPreservesPartitioning) {
        PreservesPartitioning(EKind::Async);
    }

    Y_UNIT_TEST(UniquePreservesPartitioning) {
        PreservesPartitioning(EKind::Unique);
    }

    Y_UNIT_TEST_FLAG(FulltextPreservesPartitioning, Compact) {
        const auto kind = Compact ? EKind::FulltextCompactRelevance : EKind::FulltextRelevance;
        auto index = MakeIndex(kind, false);
        auto* relevance = index.mutable_global_fulltext_relevance_index();
        auto configure = [](Ydb::Table::GlobalIndexSettings* settings, ui32 sizeMb) {
            settings->mutable_partitioning_settings()->set_min_partitions_count(1);
            settings->mutable_partitioning_settings()->set_max_partitions_count(4);
            settings->mutable_partitioning_settings()->set_partition_size_mb(sizeMb);
            settings->mutable_read_replicas_settings()->set_any_az_read_replicas_count(1);
        };
        configure(relevance->mutable_dict_table_settings(), 64);
        configure(relevance->mutable_docs_table_settings(), 96);
        configure(relevance->mutable_stats_table_settings(), 128);
        configure(relevance->mutable_posting_table_settings(), 160);
        TRebuildTest test(kind, index);
        auto check = [&] {
            if (!Compact) {
                CheckPartitioning(test, "indexImplDictTable", 1, 4, 64, 1);
            }
            CheckPartitioning(test, "indexImplDocsTable", 1, 4, 96, 1);
            CheckPartitioning(test, "indexImplStatsTable", 1, 4, 128, 1);
            CheckPartitioning(test, "indexImplTable", 1, 4, 160, 1);
        };
        check();
        const ui64 id = test.Start();
        test.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        test.CheckFinished(id, true);
        test.Reboot();
        check();
    }

    Y_UNIT_TEST_FLAG(JsonPreservesPartitioning, Compact) {
        const auto kind = Compact ? EKind::JsonCompact : EKind::Json;
        auto index = MakeIndex(kind, false);
        auto* settings = index.mutable_global_json_index()->mutable_settings();
        settings->mutable_partitioning_settings()->set_min_partitions_count(1);
        settings->mutable_partitioning_settings()->set_max_partitions_count(4);
        settings->mutable_partitioning_settings()->set_partition_size_mb(96);
        settings->mutable_read_replicas_settings()->set_any_az_read_replicas_count(1);
        TRebuildTest test(kind, index);
        CheckPartitioning(test, "indexImplTable", 1, 4, 96, 1);
        const ui64 id = test.Start();
        test.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        test.CheckFinished(id, true);
        test.Reboot();
        CheckPartitioning(test, "indexImplTable", 1, 4, 96, 1);
    }

    Y_UNIT_TEST(PrefixedVectorOmittedColumnsWithPartitioning) {
        TRebuildTest test(EKind::PrefixedVector);
        auto index = MakeIndex(EKind::PrefixedVector, true);
        index.clear_index_columns();
        auto* settings = index.mutable_global_vector_kmeans_tree_index()->mutable_prefix_table_settings();
        settings->set_uniform_partitions(2);
        settings->mutable_partitioning_settings()->set_min_partitions_count(2);
        settings->mutable_partitioning_settings()->set_max_partitions_count(3);
        settings->mutable_partitioning_settings()->set_partition_size_mb(64);
        settings->mutable_read_replicas_settings()->set_any_az_read_replicas_count(1);
        const ui64 id = test.Start(index);
        test.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        CheckPartitioning(test, "indexImplPrefixTable", 2, 3, 64, 1);
        test.CheckFinished(id, true);
        test.Reboot();
        CheckPartitioning(test, "indexImplPrefixTable", 2, 3, 64, 1);
    }

    Y_UNIT_TEST_FLAG(UniqueValidationFailurePreservesLiveIndex, Online) {
        TRebuildTest test(Online ? EKind::Unique : EKind::UniqueOffline);
        auto index = MakeIndex(test.Kind, true);
        index.add_index_columns("text"); // Existing rows contain repeated green/red values.
        const ui64 id = test.Start(index);
        test.Wait(id, Ydb::Table::IndexBuildState::STATE_REJECTED);
        test.CheckFinished(id, false);
        test.Reboot();
        test.CheckFinished(id, false);
        const ui64 retry = test.Start();
        test.Wait(retry, Ydb::Table::IndexBuildState::STATE_DONE);
        test.CheckFinished(retry, true);
    }

    Y_UNIT_TEST(FulltextExplicitAnalyzerSettings) {
        TRebuildTest test(EKind::FulltextPlain);
        auto index = MakeIndex(test.Kind, true);
        auto* settings = index.mutable_global_fulltext_plain_index()->mutable_fulltext_settings();
        auto* column = settings->add_columns();
        column->set_column("text");
        column->mutable_analyzers()->set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
        const ui64 id = test.Start(index);
        test.Wait(id, Ydb::Table::IndexBuildState::STATE_DONE);
        *test.LiveDescription.MutableFulltextIndexDescription()->MutableSettings() = *settings;
        // Each "green apple"/"red apple" value now produces one whole token, not two.
        test.LiveRows["indexImplTable"] = test.RowCount;
        test.CheckFinished(id, true);
        test.Reboot();
        test.CheckFinished(id, true);
        const auto description = DescribePrivatePath(test.Runtime, test.IndexPath);
        const auto pathId = description.GetPathDescription().GetSelf().GetPathId();

        column->mutable_analyzers()->clear_tokenizer();
        test.Start(index, true, Ydb::StatusIds::BAD_REQUEST);
        const auto unchanged = DescribePrivatePath(test.Runtime, test.IndexPath);
        UNIT_ASSERT_VALUES_EQUAL(unchanged.GetPathDescription().GetSelf().GetPathId(), pathId);
        test.CheckFinished(id, true);
    }
}
