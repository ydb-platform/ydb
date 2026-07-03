// Full Backup Op tests.
//
// PR 1 (M1) covers structural scaffolding: schema, types, persist helper
// signatures. Subsequent milestones (M2-M7) exercise the live behavior added
// in Blocks A-D of the single-PR plan via runtime fixtures.

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_schema.h>
#include <ydb/core/tx/schemeshard/schemeshard_subop_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_backup.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/grpc_services/rpc_backup_base.h>

#include <library/cpp/testing/unittest/registar.h>

#include <type_traits>

#define DEFAULT_NAME_1 "FullBackupCol1"
#define DEFAULT_NAME_2 "FullBackupCol2"

using namespace NSchemeShardUT_Private;

namespace NKikimr::NSchemeShard {

Y_UNIT_TEST_SUITE(TFullBackupSchemaTest) {
    Y_UNIT_TEST(TablesPresent) {
        // Verify schema table ids are reserved at the locked-in values.
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::TableId, 136u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackupItems::TableId, 137u);

        // Verify column ids on FullBackups match the locked-in layout.
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::Id::ColumnId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::State::ColumnId, 2u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::DomainPathOwnerId::ColumnId, 3u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::DomainPathId::ColumnId, 4u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::UserSID::ColumnId, 5u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::StartTime::ColumnId, 6u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::EndTime::ColumnId, 7u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::FinalIssues::ColumnId, 8u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::BackupCollectionPathOwnerId::ColumnId, 9u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackups::BackupCollectionLocalPathId::ColumnId, 10u);

        // Verify column ids on FullBackupItems match the locked-in layout.
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackupItems::Id::ColumnId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackupItems::PathOwnerId::ColumnId, 2u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackupItems::PathId::ColumnId, 3u);
        UNIT_ASSERT_VALUES_EQUAL(Schema::FullBackupItems::State::ColumnId, 4u);
    }
}

Y_UNIT_TEST_SUITE(TFullBackupInfoTest) {
    Y_UNIT_TEST(RoundTripSerialize) {
        // Build a fully populated TFullBackupInfo and verify the in-memory
        // structure matches the spec (header + two items, including the new
        // FinalIssues + BackupCollectionPathId fields and the Failed state).
        const TPathId domainPathId{1, 100};
        const TPathId bcPathId{1, 200};

        TFullBackupInfo::TPtr info = new TFullBackupInfo(42, domainPathId);
        info->State = TFullBackupInfo::EState::Transferring;
        info->BackupCollectionPathId = bcPathId;
        info->ExpectedItemCount = 2;
        info->UserSID = "root@builtin";
        info->StartTime = TInstant::Seconds(1700000000);
        info->EndTime = TInstant::Seconds(0);
        info->FinalIssues.clear();

        const TPathId itemA{1, 300};
        const TPathId itemB{1, 301};
        {
            auto& it = info->Items[itemA];
            it.PathId = itemA;
            it.State = TFullBackupInfo::TItem::EState::Transferring;
        }
        {
            auto& it = info->Items[itemB];
            it.PathId = itemB;
            it.State = TFullBackupInfo::TItem::EState::Done;
        }

        UNIT_ASSERT_VALUES_EQUAL(info->Id, 42u);
        UNIT_ASSERT_EQUAL(info->DomainPathId, domainPathId);
        UNIT_ASSERT_EQUAL(info->BackupCollectionPathId, bcPathId);
        UNIT_ASSERT_VALUES_EQUAL(info->Items.size(), 2u);
        UNIT_ASSERT(info->Items.contains(itemA));
        UNIT_ASSERT(info->Items.contains(itemB));
        UNIT_ASSERT_EQUAL(info->Items.at(itemA).State,
            TFullBackupInfo::TItem::EState::Transferring);
        UNIT_ASSERT_EQUAL(info->Items.at(itemB).State,
            TFullBackupInfo::TItem::EState::Done);

        // Helpers.
        UNIT_ASSERT(!info->IsDone());
        UNIT_ASSERT(!info->IsFailed());
        UNIT_ASSERT(!info->IsFinished());
        UNIT_ASSERT(!info->IsAllItemsDone());

        // Flip the still-Transferring item to Done -> header helpers update.
        info->Items.at(itemA).State = TFullBackupInfo::TItem::EState::Done;
        UNIT_ASSERT(info->IsAllItemsDone());
        UNIT_ASSERT(!info->HasAnyFailed());

        // Flip one item to Failed -> mixed terminal state.
        info->Items.at(itemA).State = TFullBackupInfo::TItem::EState::Failed;
        UNIT_ASSERT(!info->IsAllItemsDone());
        UNIT_ASSERT(info->HasAnyFailed());

        // Header transition to Failed.
        info->State = TFullBackupInfo::EState::Failed;
        info->FinalIssues = "shard rejected snapshot";
        info->EndTime = TInstant::Seconds(1700000123);
        UNIT_ASSERT(info->IsFailed());
        UNIT_ASSERT(info->IsFinished());

        // Header transition to Done.
        info->State = TFullBackupInfo::EState::Done;
        info->FinalIssues.clear();
        UNIT_ASSERT(info->IsDone());
        UNIT_ASSERT(info->IsFinished());

        // Verify the enum values stay where the schema persist code expects.
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::EState::Invalid), 0u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::EState::Transferring), 1u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::EState::Failed), 230u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::EState::Done), 240u);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::TItem::EState::Invalid), 0u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::TItem::EState::Transferring), 1u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::TItem::EState::Failed), 230u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(TFullBackupInfo::TItem::EState::Done), 240u);
    }

    Y_UNIT_TEST(SubOpTypeAndPersistSignatures) {
        // The TxCreateFullBackupOp enumerator must exist and be distinct from
        // TxCreateLongIncrementalBackupOp (used by the parallel incremental
        // long-op).
        static_assert(static_cast<int>(TxCreateFullBackupOp) !=
            static_cast<int>(TxCreateLongIncrementalBackupOp),
            "TxCreateFullBackupOp must be a fresh enumerator");
        UNIT_ASSERT_UNEQUAL(TxTypeName(TxCreateFullBackupOp), "");

        // Persist helpers must exist with the expected signatures.
        using PersistMember = void (TSchemeShard::*)(NIceDb::TNiceDb&, ui64);
        using PersistStateStatic = void (*)(NIceDb::TNiceDb&, const TFullBackupInfo&);
        using PersistItemStatic = void (*)(NIceDb::TNiceDb&, ui64, const TFullBackupInfo::TItem&);
        using PersistRemoveStatic = void (*)(NIceDb::TNiceDb&, const TFullBackupInfo&);

        PersistMember persist = &TSchemeShard::PersistFullBackup;
        PersistStateStatic persistState = &TSchemeShard::PersistFullBackupState;
        PersistItemStatic persistItem = &TSchemeShard::PersistFullBackupItem;
        PersistRemoveStatic persistRemove = &TSchemeShard::PersistRemoveFullBackup;

        UNIT_ASSERT(persist != nullptr);
        UNIT_ASSERT(persistState != nullptr);
        UNIT_ASSERT(persistItem != nullptr);
        UNIT_ASSERT(persistRemove != nullptr);
    }
}

// Direct unit tests for the shared GET/LIST proto-fill logic. These exercise
// FillFullBackupProto without a runtime fixture, so they can drive states
// (Failed, partial-progress) that the synchronous runtime fixtures cannot
// easily reach.
Y_UNIT_TEST_SUITE(TFullBackupFillProtoTest) {
    static TFullBackupInfo::TPtr MakeInfo(ui64 id, TFullBackupInfo::EState state, ui32 expected) {
        TFullBackupInfo::TPtr info = new TFullBackupInfo(id, TPathId{1, 100});
        info->State = state;
        info->BackupCollectionPathId = TPathId{1, 200};
        info->ExpectedItemCount = expected;
        return info;
    }

    static void AddItem(TFullBackupInfo& info, ui64 localPathId, TFullBackupInfo::TItem::EState state) {
        const TPathId pathId{1, localPathId};
        auto& it = info.Items[pathId];
        it.PathId = pathId;
        it.State = state;
    }

    Y_UNIT_TEST(FailedStateReportsGenericError) {
        // Regression: a Failed full backup must surface GENERIC_ERROR on the
        // inner proto (the only field rpc_get_operation reads), not SUCCESS.
        auto info = MakeInfo(7, TFullBackupInfo::EState::Failed, 1);
        info->FinalIssues = "shard rejected snapshot";
        AddItem(*info, 300, TFullBackupInfo::TItem::EState::Failed);

        NKikimrBackup::TFullBackup proto;
        TSchemeShard::FillFullBackupProto(proto, *info);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(proto.GetStatus()),
            static_cast<int>(Ydb::StatusIds::GENERIC_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(proto.GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE));
        UNIT_ASSERT_GE(proto.IssuesSize(), 1u);
    }

    Y_UNIT_TEST(DoneStateReportsSuccess) {
        auto info = MakeInfo(8, TFullBackupInfo::EState::Done, 2);
        AddItem(*info, 300, TFullBackupInfo::TItem::EState::Done);
        AddItem(*info, 301, TFullBackupInfo::TItem::EState::Done);

        NKikimrBackup::TFullBackup proto;
        TSchemeShard::FillFullBackupProto(proto, *info);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(proto.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(proto.GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE));
        UNIT_ASSERT_VALUES_EQUAL(proto.GetProgressPercent(), 100);
    }

    Y_UNIT_TEST(ProgressUsesExpectedItemCountDenominator) {
        // Regression: with 1 of 2 expected items registered+done, progress must
        // be 50% (ExpectedItemCount denominator), NOT 100% (Items.size()). The
        // old LIST handler divided by Items.size() and reported 100% here.
        auto info = MakeInfo(9, TFullBackupInfo::EState::Transferring, 2);
        AddItem(*info, 300, TFullBackupInfo::TItem::EState::Done);

        NKikimrBackup::TFullBackup proto;
        TSchemeShard::FillFullBackupProto(proto, *info);

        UNIT_ASSERT_VALUES_EQUAL(proto.GetProgressPercent(), 50);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(proto.GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA));
    }

    Y_UNIT_TEST(ToOperationFailedBackupIsIdentifiable) {
        // A Failed full backup (GENERIC_ERROR + PROGRESS_DONE, Id set) must still
        // carry an operation id, metadata and result, else it is unidentifiable
        // in ListOperations and its failure metadata is lost in GetOperation.
        auto info = MakeInfo(42, TFullBackupInfo::EState::Failed, 1);
        info->FinalIssues = "shard rejected snapshot";
        AddItem(*info, 300, TFullBackupInfo::TItem::EState::Failed);

        NKikimrBackup::TFullBackup proto;
        TSchemeShard::FillFullBackupProto(proto, *info);
        UNIT_ASSERT(proto.HasId());

        auto op = NKikimr::NGRpcService::TFullBackupConv::ToOperation(proto);

        UNIT_ASSERT_C(!op.id().empty(), "Failed backup must have an operation id");
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(op.status()),
            static_cast<int>(Ydb::StatusIds::GENERIC_ERROR));
        UNIT_ASSERT(op.ready());
        UNIT_ASSERT(op.has_metadata());
        Ydb::Backup::BackupMetadata md;
        UNIT_ASSERT(op.metadata().UnpackTo(&md));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(md.progress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE));
        UNIT_ASSERT(op.has_result());
    }

    Y_UNIT_TEST(ToOperationInProgressNotReady) {
        // In-progress (SUCCESS + TRANSFER_DATA): id/metadata present, ready=false.
        auto info = MakeInfo(43, TFullBackupInfo::EState::Transferring, 2);
        AddItem(*info, 300, TFullBackupInfo::TItem::EState::Done);

        NKikimrBackup::TFullBackup proto;
        TSchemeShard::FillFullBackupProto(proto, *info);

        auto op = NKikimr::NGRpcService::TFullBackupConv::ToOperation(proto);
        UNIT_ASSERT(!op.id().empty());
        UNIT_ASSERT(!op.ready());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(op.status()),
            static_cast<int>(Ydb::StatusIds::SUCCESS));
    }

    Y_UNIT_TEST(ToOperationDoneReady) {
        auto info = MakeInfo(44, TFullBackupInfo::EState::Done, 1);
        AddItem(*info, 300, TFullBackupInfo::TItem::EState::Done);

        NKikimrBackup::TFullBackup proto;
        TSchemeShard::FillFullBackupProto(proto, *info);

        auto op = NKikimr::NGRpcService::TFullBackupConv::ToOperation(proto);
        UNIT_ASSERT(!op.id().empty());
        UNIT_ASSERT(op.ready());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(op.status()),
            static_cast<int>(Ydb::StatusIds::SUCCESS));
    }

    Y_UNIT_TEST(ToOperationErrorEnvelopeIsBareStatus) {
        // Error envelope (mirrors the GET NOT_FOUND path): inner proto has a
        // Status but no Id, so the operation carries only that status.
        NKikimrBackup::TFullBackup proto;
        proto.SetStatus(Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT(!proto.HasId());

        auto op = NKikimr::NGRpcService::TFullBackupConv::ToOperation(proto);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(op.status()),
            static_cast<int>(Ydb::StatusIds::NOT_FOUND));
        UNIT_ASSERT_C(op.id().empty(), "error envelope must not carry an operation id");
        UNIT_ASSERT(!op.has_metadata());
        UNIT_ASSERT(!op.has_result());
    }
}

}  // namespace NKikimr::NSchemeShard

// =====================================================================
// Runtime-level tests for Blocks A-D (M2 - M7).
//
// These tests share a small set of fixtures:
//   PrepareDirs    - create /MyRoot/.backups/collections.
//   PrepareTables  - create the source tables that belong to the collection.
//   IssueBackup    - issue ESchemeOpBackupBackupCollection and wait for the
//                    aggregator to settle.
//
// Most tests target the SchemeShard internal Tx layer
// (TEvGetFullBackupRequest/...) and the aggregator behavior; full RPC E2E
// (Block D paths over grpc) is covered by the smoke test at the bottom.
// =====================================================================

namespace {

TString CollectionWithTwoTables(const TString& name) {
    return Sprintf(R"(
        Name: "%s"

        ExplicitEntryList {
            Entries {
                Type: ETypeTable
                Path: "/MyRoot/Table1"
            }
            Entries {
                Type: ETypeTable
                Path: "/MyRoot/Table2"
            }
        }
        Cluster: {}
    )", name.c_str());
}

TString CollectionWithOneTable(const TString& name) {
    return Sprintf(R"(
        Name: "%s"

        ExplicitEntryList {
            Entries {
                Type: ETypeTable
                Path: "/MyRoot/Table1"
            }
        }
        Cluster: {}
    )", name.c_str());
}

TString EmptyCollection(const TString& name) {
    return Sprintf(R"(
        Name: "%s"

        ExplicitEntryList {}
        Cluster: {}
    )", name.c_str());
}

void PrepareDirs(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId) {
    TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
    env.TestWaitNotification(runtime, txId);
    TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
    env.TestWaitNotification(runtime, txId);
}

void PrepareTable(TTestBasicRuntime& runtime, TTestEnv& env, ui64& txId, const TString& name) {
    TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
        Name: "%s"
        Columns { Name: "key" Type: "Uint32" }
        Columns { Name: "value" Type: "Utf8" }
        KeyColumnNames: ["key"]
    )", name.c_str()));
    env.TestWaitNotification(runtime, txId);
}

void AsyncBackupBackupCollection(TTestBasicRuntime& runtime, ui64 txId, const TString& workingDir, const TString& request) {
    auto modifyTx = std::make_unique<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TTestTxConfig::SchemeShard);
    auto transaction = modifyTx->Record.AddTransaction();
    transaction->SetWorkingDir(workingDir);
    transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpBackupBackupCollection);

    bool parseOk = ::google::protobuf::TextFormat::ParseFromString(request, transaction->MutableBackupBackupCollection());
    UNIT_ASSERT(parseOk);

    AsyncSend(runtime, TTestTxConfig::SchemeShard, modifyTx.release(), 0);
}

ui64 TestBackupBackupCollection(TTestBasicRuntime& runtime, ui64 txId, const TString& workingDir, const TString& request, const TExpectedResult& expectedResult = {NKikimrScheme::StatusAccepted}) {
    AsyncBackupBackupCollection(runtime, txId, workingDir, request);
    return TestModificationResults(runtime, txId, {expectedResult});
}

// Helper: send TEvGetFullBackupRequest, wait for response, return record.
NKikimrBackup::TEvGetFullBackupResponse InternalGetFullBackup(
    TTestBasicRuntime& runtime,
    ui64 backupId,
    const TString& db = "/MyRoot")
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<NKikimr::NSchemeShard::TEvBackup::TEvGetFullBackupRequest>(db, backupId);
    runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, req.Release(), 0, GetPipeConfigWithRetries());
    TAutoPtr<IEventHandle> handle;
    auto* response = runtime.GrabEdgeEventRethrow<NKikimr::NSchemeShard::TEvBackup::TEvGetFullBackupResponse>(handle);
    UNIT_ASSERT(response);
    return response->Record;
}

NKikimrBackup::TEvListFullBackupsResponse InternalListFullBackups(
    TTestBasicRuntime& runtime,
    ui64 pageSize = 10,
    const TString& pageToken = TString(),
    const TString& db = "/MyRoot")
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<NKikimr::NSchemeShard::TEvBackup::TEvListFullBackupsRequest>(db, pageSize, pageToken);
    runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, req.Release(), 0, GetPipeConfigWithRetries());
    TAutoPtr<IEventHandle> handle;
    auto* response = runtime.GrabEdgeEventRethrow<NKikimr::NSchemeShard::TEvBackup::TEvListFullBackupsResponse>(handle);
    UNIT_ASSERT(response);
    return response->Record;
}

NKikimrBackup::TEvForgetFullBackupResponse InternalForgetFullBackup(
    TTestBasicRuntime& runtime,
    ui64 backupId,
    ui64 txId = 1,
    const TString& db = "/MyRoot")
{
    auto sender = runtime.AllocateEdgeActor();
    auto req = MakeHolder<NKikimr::NSchemeShard::TEvBackup::TEvForgetFullBackupRequest>(txId, db, backupId);
    runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, req.Release(), 0, GetPipeConfigWithRetries());
    TAutoPtr<IEventHandle> handle;
    auto* response = runtime.GrabEdgeEventRethrow<NKikimr::NSchemeShard::TEvBackup::TEvForgetFullBackupResponse>(handle);
    UNIT_ASSERT(response);
    return response->Record;
}

}  // namespace

// ---------------------------------------------------------------------
// M2 + M3 - Aggregator + decomposition behavior.
// ---------------------------------------------------------------------
Y_UNIT_TEST_SUITE(TFullBackupAggregatorTest) {

    Y_UNIT_TEST(ProposePersistsRow) {
        // Issue a BACKUP against a collection with one table; verify the
        // aggregator row exists via the internal GET path and that the row's
        // BackupCollectionPathId matches the collection.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(resp.GetFullBackup().GetId(), backupTxId);
    }

    Y_UNIT_TEST(EmptyCollectionRejectedAtDecomposition) {
        // Empty ExplicitEntryList -> StatusInvalidParameter before any
        // aggregator row is written.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", EmptyCollection(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")",
            {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::NOT_FOUND));
    }

    Y_UNIT_TEST(NonExistentCollectionReject) {
        // BACKUP against absent collection -> StatusPathDoesNotExist; no row.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/missing")",
            {NKikimrScheme::EStatus::StatusPathDoesNotExist});
        env.TestWaitNotification(runtime, backupTxId);
    }

    Y_UNIT_TEST(CreatesFullBackupRowWithCorrectExpectedItemCount) {
        // For a collection with two tables, the aggregator records
        // ExpectedItemCount=2 (one per copied base table; no indexes). After
        // the sub-ops finish, the row's Items map has been lazily populated
        // and the row is Done.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithTwoTables(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");
        PrepareTable(runtime, env, txId, "Table2");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        // Progress should be done after the sub-ops finish: this implicitly
        // verifies that ExpectedItemCount was set correctly, because the
        // header only flips to Done once Items.size() reaches the expected
        // count AND every observed item is terminal.
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(resp.GetFullBackup().GetProgressPercent(), 100);
    }

    Y_UNIT_TEST(IncrementalConfigBackupTracksCdc) {
        // Exploratory: when the collection has IncrementalBackupConfig, the
        // BACKUP also arms CDC streams. The full-backup row must not report
        // Done until those CDC-stream sub-ops have also completed (i.e. the
        // whole operation finished), and a continuous-backup CDC stream must
        // exist on the source table once the backup is Done.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/Table1"
                }
            }
            Cluster: {}
            IncrementalBackupConfig: {}
        )");
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());

        // A continuous-backup CDC stream must exist on the source table.
        auto desc = DescribePrivatePath(runtime, "/MyRoot/Table1", true, true);
        const auto& tableDesc = desc.GetPathDescription().GetTable();
        bool foundCdc = false;
        for (size_t i = 0; i < tableDesc.CdcStreamsSize(); ++i) {
            if (tableDesc.GetCdcStreams(i).GetName().EndsWith("_continuousBackupImpl")) {
                foundCdc = true;
                break;
            }
        }
        UNIT_ASSERT_C(foundCdc, "expected a _continuousBackupImpl CDC stream on the source table once backup is Done");
    }

    Y_UNIT_TEST(IndexedTableBackupReachesDone) {
        // Coverage for ExpectedItemCount on a collection containing a table
        // with a secondary index. The aggregator must count the base table AND
        // its index impl table so the planned count matches the CopyTable
        // sub-ops the CCT actually emits. A mismatch would either hang the
        // header in Transferring (overcount) or finalize it early (undercount),
        // so reaching PROGRESS_DONE proves the count agrees with the emitted
        // copies for the indexed case.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            Cluster: {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(resp.GetFullBackup().GetProgressPercent(), 100);
    }

    Y_UNIT_TEST(DroppedIndexBackupReachesDone) {
        // End-to-end smoke test for backing up a collection whose table had an
        // index dropped beforehand: the backup must still reach PROGRESS_DONE
        // with a consistent count. This exercises the index-LEVEL skip (a
        // dropped index node is excluded from both the count and the CCT
        // emission), so ExpectedItemCount stays equal to the emitted CopyTable
        // sub-ops and the aggregator does not wedge.
        //
        // NOTE: dropping an index marks the index NODE itself Dropped(), which
        // is filtered before the per-impl-table loop, so this test does not
        // reach the per-impl IsDeleted() check; that check is a defensive
        // mirror of CCT for the (not reachable via DropIndex) case of a live
        // index with an individually-deleted impl table.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList {
                Entries {
                    Type: ETypeTable
                    Path: "/MyRoot/TableWithIndex"
                }
            }
            Cluster: {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "TableWithIndex"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ValueIndex"
                KeyColumnNames: ["value"]
                Type: EIndexTypeGlobal
            }
        )");
        env.TestWaitNotification(runtime, txId);

        // Drop the index before backing up: its impl table becomes deleted.
        TestDropTableIndex(runtime, ++txId, "/MyRoot", R"(
            TableName: "TableWithIndex"
            IndexName: "ValueIndex"
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(resp.GetFullBackup().GetProgressPercent(), 100);
    }

    Y_UNIT_TEST(LazyItemRegistrationOnHookFire) {
        // Items must NOT appear in TFullBackupInfo::Items at aggregator
        // Propose time; they are registered lazily by the progress driver
        // when each CopyTable sub-op's DoDoneParts hook fires. After the
        // CCT settles, both items must be present and Done.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithTwoTables(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");
        PrepareTable(runtime, env, txId, "Table2");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        // After hooks fire and the aggregator settles, the row is Done. The
        // observable contract (via GET) is that the row reports SUCCESS and
        // PROGRESS_DONE, which proves that items were registered (state
        // would otherwise stay Transferring forever).
        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(resp.GetFullBackup().GetProgressPercent(), 100);
    }

    Y_UNIT_TEST(SecondBackupRejectedWhileFirstInFlight) {
        // The BCPathToFullBackup concurrency guard: while a first BACKUP's
        // header is still non-terminal, a second BACKUP on the same collection
        // must be rejected with StatusMultipleModifications. We hold the first
        // header in Transferring by dropping the self-notification that drives
        // FinalizeFullBackupOnOpComplete, so the guard entry is never cleared.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        const ui64 backupTxId1 = ++txId;
        runtime.SetObserverFunc([backupTxId1](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKikimr::NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::EventType) {
                auto* m = ev->Get<NKikimr::NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>();
                if (m && m->Record.GetTxId() == backupTxId1) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Issue the first BACKUP; do NOT TestWaitNotification it. Its finalize
        // notification is dropped, so the header stays Transferring.
        TestBackupBackupCollection(runtime, backupTxId1, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");

        auto inflight = InternalGetFullBackup(runtime, backupTxId1);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(inflight.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA),
            inflight.ShortDebugString());

        // Advance the clock so the second BACKUP's target dir differs (a same
        // timestamp would be rejected at decomposition, masking the guard).
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));

        const ui64 backupTxId2 = ++txId;
        TestBackupBackupCollection(runtime, backupTxId2, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")",
            {NKikimrScheme::StatusMultipleModifications});
    }

    Y_UNIT_TEST(ForgetInFlightReturnsPreconditionFailed) {
        // FORGET on a non-terminal full backup must be rejected
        // (PRECONDITION_FAILED), protecting AbortUnsafe's assumption that the
        // row still exists. Same in-flight hold as above.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        const ui64 backupTxId = ++txId;
        runtime.SetObserverFunc([backupTxId](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKikimr::NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::EventType) {
                auto* m = ev->Get<NKikimr::NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>();
                if (m && m->Record.GetTxId() == backupTxId) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");

        auto inflight = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(inflight.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_TRANSFER_DATA),
            inflight.ShortDebugString());

        auto forget = InternalForgetFullBackup(runtime, backupTxId, /*txId=*/++txId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(forget.GetStatus()),
            static_cast<int>(Ydb::StatusIds::PRECONDITION_FAILED),
            forget.ShortDebugString());
    }

    Y_UNIT_TEST(ForceDropCollectionMidFlightFailsBackup) {
        // Force-dropping the collection while a BACKUP is in flight must drive
        // the aggregator's AbortUnsafe so the header reaches a TERMINAL Failed
        // state (GENERIC_ERROR + "force-dropped" on GET), NOT hang forever in
        // Transferring -- a stuck header would wedge FORGET and the
        // BCPathToFullBackup guard. Also pins the GET status-masking contract:
        // the GET envelope is SUCCESS while the inner FullBackup.Status carries
        // the GENERIC_ERROR.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        auto desc = DescribePath(runtime, "/MyRoot/.backups/collections/" DEFAULT_NAME_1);
        const ui64 collectionPathId = desc.GetPathDescription().GetSelf().GetPathId();

        // Issue BACKUP and force-drop the collection without letting the backup
        // settle first, so the force-drop aborts the in-flight aggregator.
        const ui64 backupTxId = ++txId;
        AsyncBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        const ui64 dropTxId = ++txId;
        AsyncForceDropUnsafe(runtime, dropTxId, collectionPathId);
        env.TestWaitNotification(runtime, {backupTxId, dropTxId});

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        // The GET envelope succeeds (outer SUCCESS); the failure is carried on
        // the INNER FullBackup.Status (GENERIC_ERROR) -- the status-masking
        // contract the GET handler must preserve.
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetFullBackup().GetStatus()),
            static_cast<int>(Ydb::StatusIds::GENERIC_ERROR),
            resp.ShortDebugString());
        UNIT_ASSERT_C(resp.GetFullBackup().IssuesSize() >= 1, resp.ShortDebugString());
    }
}

// ---------------------------------------------------------------------
// M4 - Hook + progress driver behavior.
// ---------------------------------------------------------------------
Y_UNIT_TEST_SUITE(TFullBackupProgressTest) {

    Y_UNIT_TEST(AllItemsDoneFlipsHeader) {
        // After issuing BACKUP and waiting for sub-ops to settle, the
        // aggregator's header should be Done and progress 100%.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithTwoTables(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");
        PrepareTable(runtime, env, txId, "Table2");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(resp.GetFullBackup().GetProgressPercent(), 100);
    }

    Y_UNIT_TEST(LostItemDoneEventStillFinalizes) {
        // Lost-event resilience (issue #1): the per-item TEvFullBackupItemDone
        // events are sent self->self post-commit and are NOT durable. If they
        // are lost (here: dropped via observer to simulate a reboot landing in
        // the window after a CopyTable commits Done but before the progress Tx
        // applies it), the items never register in the row. The backup
        // OPERATION still completes, and the full-backup header must still
        // reach terminal Done -- driven by operation completion -- instead of
        // hanging forever in Transferring (which also wedges FORGET and blocks
        // future backups of the collection).
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithTwoTables(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");
        PrepareTable(runtime, env, txId, "Table2");

        // Drop every full-backup item-done event.
        runtime.SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NKikimr::NSchemeShard::TEvPrivate::TEvFullBackupItemDone::EventType) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS), resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            resp.ShortDebugString());
    }

    Y_UNIT_TEST(TerminalStateIdempotent) {
        // Re-asking for GET after the aggregator finishes returns the same
        // terminal state without crashing.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        // Multiple GET calls all return SUCCESS+PROGRESS_DONE.
        for (int i = 0; i < 3; ++i) {
            auto resp = InternalGetFullBackup(runtime, backupTxId);
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
                static_cast<int>(Ydb::StatusIds::SUCCESS),
                resp.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<int>(resp.GetFullBackup().GetProgress()),
                static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE));
        }
    }
}

// ---------------------------------------------------------------------
// M6 - GET / LIST / FORGET handlers.
// ---------------------------------------------------------------------
Y_UNIT_TEST_SUITE(TFullBackupGetForgetListTest) {

    Y_UNIT_TEST(GetNotFound) {
        // GET on a non-existent id returns NOT_FOUND.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        PrepareDirs(runtime, env, txId);

        auto resp = InternalGetFullBackup(runtime, /*backupId=*/9999);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::NOT_FOUND));
    }

    Y_UNIT_TEST(GetDone) {
        // GET on a settled backup returns SUCCESS+PROGRESS_DONE.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(resp.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE));
    }

    Y_UNIT_TEST(ListPagination) {
        // One backup against the collection produces one entry in LIST.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        ui64 backupTxId1 = ++txId;
        TestBackupBackupCollection(runtime, backupTxId1, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId1);

        auto resp = InternalListFullBackups(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            resp.ShortDebugString());
        UNIT_ASSERT_GE(resp.EntriesSize(), 1u);
    }

    Y_UNIT_TEST(ForgetTerminal) {
        // After completion, FORGET removes the row; subsequent GET returns
        // NOT_FOUND.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;
        PrepareDirs(runtime, env, txId);

        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithOneTable(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");

        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        auto forget = InternalForgetFullBackup(runtime, backupTxId, /*txId=*/++txId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(forget.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            forget.ShortDebugString());

        auto resp = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(resp.GetStatus()),
            static_cast<int>(Ydb::StatusIds::NOT_FOUND));
    }
}

// ---------------------------------------------------------------------
// M7 - End-to-end smoke. BACKUP -> poll -> Done -> Forget.
// ---------------------------------------------------------------------
Y_UNIT_TEST_SUITE(TFullBackupE2ESmokeTest) {

    Y_UNIT_TEST(BackupPollForget) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", CollectionWithTwoTables(DEFAULT_NAME_1));
        env.TestWaitNotification(runtime, txId);
        PrepareTable(runtime, env, txId, "Table1");
        PrepareTable(runtime, env, txId, "Table2");

        // 1) Issue BACKUP.
        ui64 backupTxId = ++txId;
        TestBackupBackupCollection(runtime, backupTxId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, backupTxId);

        // 2) Poll via internal GET; sub-ops settle synchronously in this
        // fixture, so the aggregator should already be Done.
        auto get = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(get.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            get.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(
            static_cast<int>(get.GetFullBackup().GetProgress()),
            static_cast<int>(Ydb::Backup::BackupProgress::PROGRESS_DONE),
            get.ShortDebugString());

        // 3) FORGET succeeds; row is gone.
        auto forget = InternalForgetFullBackup(runtime, backupTxId, /*txId=*/++txId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(forget.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            forget.ShortDebugString());

        auto gone = InternalGetFullBackup(runtime, backupTxId);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(gone.GetStatus()),
            static_cast<int>(Ydb::StatusIds::NOT_FOUND));
    }
}

// ---------------------------------------------------------------------
// Full lifecycle, in-process (no networking): exercise EVERY backup-collection
// operation through the id it returns, polling each to a terminal state via
// its Get-by-id API. This is the C++ analog of the functional python test
// TestFullCycleOperationIdPolling:
//   * full BACKUP        -> poll InternalGetFullBackup(id) until PROGRESS_DONE
//   * incremental BACKUP -> poll TestGetIncrementalBackup(id) until PROGRESS_DONE
//   * RESTORE            -> poll the restore list/get until PROGRESS_DONE
// plus LIST + FORGET on the full backup. The sync test fixture settles
// sub-ops eagerly, so the poll loops converge immediately; they are written
// as real loops to mirror the external control loop and to stay correct if a
// phase ever needs extra ticks (e.g. CDC drain).
// ---------------------------------------------------------------------
Y_UNIT_TEST_SUITE(TFullBackupLifecycleTest) {

    // In-process analog of `ydb operation get ydb://fullbackup?id=N`.
    void WaitFullBackupDone(TTestBasicRuntime& runtime, TTestEnv& env, ui64 id,
            TDuration timeout = TDuration::Seconds(60)) {
        const TInstant deadline = runtime.GetCurrentTime() + timeout;
        for (;;) {
            auto resp = InternalGetFullBackup(runtime, id);
            if (resp.GetFullBackup().GetProgress() == Ydb::Backup::BackupProgress::PROGRESS_DONE) {
                UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(resp.GetStatus()),
                    static_cast<int>(Ydb::StatusIds::SUCCESS), resp.ShortDebugString());
                return;
            }
            UNIT_ASSERT_C(runtime.GetCurrentTime() < deadline,
                "full backup " << id << " did not reach PROGRESS_DONE: " << resp.ShortDebugString());
            env.SimulateSleep(runtime, TDuration::Seconds(1));
        }
    }

    // In-process analog of `ydb operation get ydb://incbackup?id=N`.
    void WaitIncrementalBackupDone(TTestBasicRuntime& runtime, TTestEnv& env, ui64 id,
            TDuration timeout = TDuration::Seconds(60)) {
        const TInstant deadline = runtime.GetCurrentTime() + timeout;
        for (;;) {
            auto resp = TestGetIncrementalBackup(runtime, id, "/MyRoot");
            if (resp.GetIncrementalBackup().GetProgress() == Ydb::Backup::BackupProgress::PROGRESS_DONE) {
                return;
            }
            UNIT_ASSERT_C(runtime.GetCurrentTime() < deadline,
                "incremental backup " << id << " did not reach PROGRESS_DONE: " << resp.ShortDebugString());
            env.SimulateSleep(runtime, TDuration::Seconds(1));
        }
    }

    // In-process analog of `ydb operation get ydb://restore?id=N`: poll the
    // latest restore entry to PROGRESS_DONE and return its terminal status.
    Ydb::StatusIds::StatusCode WaitRestoreDone(TTestBasicRuntime& runtime, TTestEnv& env,
            TDuration timeout = TDuration::Seconds(60)) {
        const TInstant deadline = runtime.GetCurrentTime() + timeout;
        for (;;) {
            auto list = TestListBackupCollectionRestores(runtime, "/MyRoot");
            if (!list.GetEntries().empty()) {
                const auto& entry = *list.GetEntries().rbegin();
                if (entry.GetProgress() == Ydb::Backup::RestoreProgress::PROGRESS_DONE) {
                    return static_cast<Ydb::StatusIds::StatusCode>(entry.GetStatus());
                }
            }
            UNIT_ASSERT_C(runtime.GetCurrentTime() < deadline,
                "restore did not reach PROGRESS_DONE within timeout");
            env.SimulateSleep(runtime, TDuration::Seconds(1));
        }
    }

    Y_UNIT_TEST(FullCycleAllOperationIdsPolling) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        PrepareDirs(runtime, env, txId);

        // Incremental-enabled collection over a single table.
        TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
            Name: ")" DEFAULT_NAME_1 R"("
            ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table1" } }
            Cluster: {}
            IncrementalBackupConfig: {}
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table1"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);
        UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2},
            {NKikimr::TCell::Make(1u)}, {NKikimr::TCell::Make(100u)});

        // ---- 1) FULL backup: poll its id to Done; it must be listable. ----
        const ui64 fullId = ++txId;
        TestBackupBackupCollection(runtime, fullId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, fullId);
        WaitFullBackupDone(runtime, env, fullId);

        {
            auto list = InternalListFullBackups(runtime);
            bool found = false;
            for (const auto& e : list.GetEntries()) {
                if (e.GetId() == fullId) {
                    found = true;
                    break;
                }
            }
            UNIT_ASSERT_C(found, "full backup id " << fullId << " not in ListFullBackups");
        }

        // ---- 2) modify data, INCREMENTAL backup: poll its id to Done. ----
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        UploadRow(runtime, "/MyRoot/Table1", 0, {1}, {2},
            {NKikimr::TCell::Make(2u)}, {NKikimr::TCell::Make(200u)});

        const ui64 incrId = ++txId;
        TestBackupIncrementalBackupCollection(runtime, incrId, "/MyRoot",
            R"(Name: ".backups/collections/)" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, incrId);
        WaitIncrementalBackupDone(runtime, env, incrId);

        // ---- 3) drop the source table, RESTORE: poll the restore op to Done. ----
        TestDropTable(runtime, ++txId, "/MyRoot", "Table1");
        env.TestWaitNotification(runtime, txId);

        const ui64 restoreId = ++txId;
        TestRestoreBackupCollection(runtime, restoreId, "/MyRoot/.backups/collections/",
            R"(Name: ")" DEFAULT_NAME_1 R"(")");
        env.TestWaitNotification(runtime, restoreId);

        const auto restoreStatus = WaitRestoreDone(runtime, env);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(restoreStatus),
            static_cast<int>(Ydb::StatusIds::SUCCESS),
            "restore terminal status not SUCCESS");

        // The restored table must exist again.
        {
            auto desc = DescribePath(runtime, "/MyRoot/Table1");
            UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(desc.GetStatus()),
                static_cast<int>(NKikimrScheme::StatusSuccess),
                "restored table /MyRoot/Table1 not found: " << desc.ShortDebugString());
        }

        // ---- 4) FORGET the full backup; its id must then be gone. ----
        auto forget = InternalForgetFullBackup(runtime, fullId, /*txId=*/++txId);
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<int>(forget.GetStatus()),
            static_cast<int>(Ydb::StatusIds::SUCCESS), forget.ShortDebugString());
        auto gone = InternalGetFullBackup(runtime, fullId);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(gone.GetStatus()),
            static_cast<int>(Ydb::StatusIds::NOT_FOUND));
    }
}
