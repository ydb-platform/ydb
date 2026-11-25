#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/backup/common/table_checksum_events.h>
#include <ydb/core/base/request_types.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/mvcc_version.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/long_tx_service.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/mon_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/table_checksum_helpers.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/lib/value/value.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr;
using namespace NKikimr::Tests;

Y_UNIT_TEST_SUITE(TableChecksumSnapshotRecovery) {
    template <bool Importing>
    void RunRealServicesChecksum(bool disableOnlineRo = false) {
        // Keep the registered UDFs alive until all runtime actors are destroyed
        auto functions = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone();
        NMiniKQL::FillStaticModules(*functions);
        UNIT_ASSERT(functions->IsLoadedUdfModule("Digest"));
        TTestBasicRuntime runtime;
        runtime.AddAppDataInit([functions](ui32, TAppData& app) {
            app.TenantName = "/MyRoot";
            app.FunctionRegistry = functions.Get();
        });
        TTestEnv env(runtime, TTestEnvOptions().SetupKqpProxy(true));
        runtime.UpdateCurrentTime(TInstant::MilliSeconds(env.GetCoordinatorStep()));
        auto& app = runtime.GetAppData();
        app.FeatureFlags.SetEnableBackupTableChecksum(true);
        app.FeatureFlags.SetEnableChecksumTable(true);
        app.FeatureFlags.SetDisableOnlineRO(disableOnlineRo);
        app.FeatureFlags.SetEnableFsBackups(true);
        app.FeatureFlags.SetEnableSnapshotsLocking(true);
        app.FeatureFlags.SetEnableDataShardSnapshotLocking(true);
        app.FeatureFlags.SetEnableDataShardDirectPartImport(false);
        app.FeatureFlags.SetEnableExportFiltering(false);
        app.FeatureFlags.SetEnableEncryptedExport(false);
        app.SnapshotRegistryHolder = CreateImmutableSnapshotRegistryHolder();
        const auto service = runtime.Register(NLongTxService::CreateLongTxService());
        runtime.RegisterService(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId()), service);
        runtime.EnableScheduleForActor(service);
        THashSet<TActorId> dataShards;
        auto boots = runtime.AddObserver<TEvTablet::TEvBoot>([&](auto& ev) {
            // Allow the first cleanup timer scheduled during DataShard initialization
            if (ev->Get()->TabletStorageInfo->TabletType == TTabletTypes::DataShard) {
                dataShards.insert(ev->GetRecipientRewrite());
                runtime.EnableScheduleForActor(ev->GetRecipientRewrite());
            }
        });

        // The fake coordinator plans versions using the same clock as the real registry
        auto plans = runtime.AddObserver<TEvTxProxy::TEvProposeTransaction>([&](auto& ev) {
            auto* tx = ev->Get()->Record.MutableTransaction();
            tx->SetMinStep(Max(tx->GetMinStep(), runtime.GetCurrentTime().MilliSeconds()));
        });
        ui32 registrations = 0;
        auto acquireRequests =
            runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>([&](auto& ev) {
                registrations += ev->Get()->Snapshot.Defined();
            });
        bool recovering = false;
        TTableId checksumTable;
        auto backupRestoreProposals = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (!recovering || ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            if constexpr (Importing) {
                if (tx.HasRestore()) {
                    checksumTable = TTableId(TTestTxConfig::SchemeShard, tx.GetRestore().GetTableId());
                }
            } else if (tx.HasBackup() && tx.GetBackup().HasTableChecksum()) {
                checksumTable = TTableId(TTestTxConfig::SchemeShard, tx.GetBackup().GetTableId());
            }
        });
        ui32 confirmations = 0;
        auto confirmationRequests = runtime.AddObserver<TEvDataShard::TEvRefreshVolatileSnapshotRequest>([&](auto&) {
            ++confirmations;
        });
        TVector<TString> queryVersions;
        bool checkingRejectedReads = false;
        // The same request is delivered to both the proxy and its session
        THashSet<TString> querySessions;
        auto queries = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (recovering && !checkingRejectedReads && ev->Get()->GetQuery().Contains("CHECKSUM TABLE") &&
                querySessions.insert(ev->Get()->GetSessionId()).second) {
                if constexpr (Importing) {
                    UNIT_ASSERT(ev->Get()->GetMvccVersion());
                    queryVersions.push_back(*ev->Get()->GetMvccVersion());
                } else {
                    UNIT_ASSERT(!ev->Get()->GetMvccVersion());
                    UNIT_ASSERT(ev->Get()->GetTxControl().begin_tx().online_read_only().allow_inconsistent_reads());
                }
            }
        });
        TRowVersion checkpoint = TRowVersion::Min();
        TActorId originalReader;
        TBlockEvents<TEvDataShard::TEvRead> reads(runtime, [&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (!recovering || (originalReader && ev->Sender != originalReader) ||
                TPathId(record.GetTableId().GetOwnerId(), record.GetTableId().GetTableId()) != checksumTable.PathId) {
                return false;
            }
            if constexpr (Importing) {
                UNIT_ASSERT(!(record.GetHints() & NKikimrTxDataShard::TEvRead::HINT_READ_BACKUP_COPY));
                UNIT_ASSERT(record.HasSnapshot() && record.GetSnapshot().GetTxId());
                const auto version = TRowVersion::FromProto(record.GetSnapshot());
                if (!checkpoint.Step) {
                    checkpoint = version;
                }
                UNIT_ASSERT_VALUES_EQUAL(version, checkpoint);
            } else {
                UNIT_ASSERT(record.GetHints() & NKikimrTxDataShard::TEvRead::HINT_READ_BACKUP_COPY);
                if (!disableOnlineRo) {
                    UNIT_ASSERT(!record.HasSnapshot());
                }
            }
            originalReader = ev->Sender;
            return ev->Sender == originalReader;
        });
        TestCreateTable(runtime, 100, "/MyRoot", R"(
            Name: "Source"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, 100);
        const auto sourceShards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Source");
        UNIT_ASSERT_VALUES_EQUAL(sourceShards.size(), 1u);
        UpdateRow(runtime, "Source", 1, "first", sourceShards.front());
        UpdateRow(runtime, "Source", 2, "second", sourceShards.front());
        TTempDir directory;
        recovering = !Importing;
        TestExport(runtime, 200, "/MyRoot", Sprintf(R"(
            ExportToFsSettings {
                base_path: "%s"
                items { source_path: "/MyRoot/Source" destination_path: "table" }
            }
        )", directory.Path().GetPath().c_str()));
        ui64 operationId = 200;
        TString target = "Source";
        if (Importing) {
            env.TestWaitNotification(runtime, 200);
            TestGetExport(runtime, 200, "/MyRoot", Ydb::StatusIds::SUCCESS);
            recovering = true;
            operationId = 201;
            target = "Restored";
            AsyncImport(runtime, operationId, "/MyRoot", Sprintf(R"(
                ImportFromFsSettings {
                    base_path: "%s"
                    items { source_path: "table" destination_path: "/MyRoot/Restored" }
                }
            )", directory.Path().GetPath().c_str()));
        }
        runtime.WaitFor("the native checksum query reaches DataShard", [&] {
            return !reads.empty();
        });
        if constexpr (Importing) {
            UNIT_ASSERT_VALUES_EQUAL(queryVersions.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(queryVersions.front(), NKqp::FormatMvccVersion(checkpoint));
        }

        UNIT_ASSERT_VALUES_EQUAL(registrations, Importing ? 1u : 0u);
        UNIT_ASSERT_VALUES_EQUAL(confirmations, 0u);
        if constexpr (!Importing) {
            checkingRejectedReads = true;
            TestMoveTable(
                runtime, 202, "/MyRoot/export-200/0", "/MyRoot/Replacement", { NKikimrScheme::StatusSchemeError });
            TestDropTable(runtime, 203, "/MyRoot/export-200", "0", { NKikimrScheme::StatusMultipleModifications });
            const auto copyShards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/export-200/0");
            UNIT_ASSERT_VALUES_EQUAL(copyShards.size(), 1u);
            const auto expectUnmarkedReadRejected = [&] {
                const auto edge = runtime.AllocateEdgeActor();
                auto request = MakeHolder<TEvDataShard::TEvRead>();
                request->Record.SetReadId(1);
                request->Record.MutableTableId()->SetOwnerId(checksumTable.PathId.OwnerId);
                request->Record.MutableTableId()->SetTableId(checksumTable.PathId.LocalPathId);
                request->Record.AddColumns(1);
                request->Ranges.emplace_back(TOwnedCellVec(), true, TOwnedCellVec(), true);
                runtime.SendToPipe(copyShards.front(), edge, request.Release());
                const auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(edge);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
            };
            expectUnmarkedReadRejected();
            const auto queryBackupCopy =
                [&](const NACLib::TUserToken& token, bool internal, bool checksumRequest, bool expectSuccess)
            {
                const auto edge = runtime.AllocateEdgeActor();
                auto query = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
                query->Record.SetUserToken(token.SerializeAsString());
                if (checksumRequest) {
                    query->Record.SetRequestType(TString(NRequestTypes::ChecksumTable));
                }
                auto& request = *query->Record.MutableRequest();
                request.SetDatabase("/MyRoot");
                request.SetIsInternalCall(internal);
                request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
                request.SetQuery("SELECT COUNT(*) FROM `/MyRoot/export-200/0`;");
                request.MutableTxControl()
                    ->mutable_begin_tx()
                    ->mutable_online_read_only()
                    ->set_allow_inconsistent_reads(true);
                request.MutableTxControl()->set_commit_tx(true);
                runtime.Send(
                    new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), edge, query.Release()), 0, true);
                const auto result = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(edge);
                UNIT_ASSERT_C((result->Get()->Record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) == expectSuccess,
                    result->Get()->Record.ShortDebugString());
            };
            queryBackupCopy(NACLib::TSystemUsers::Tmp(), true, true, false);
            queryBackupCopy(NACLib::TSystemUsers::Metadata(), false, true, false);
            queryBackupCopy(NACLib::TSystemUsers::Metadata(), true, false, false);
            queryBackupCopy(NACLib::TSystemUsers::Metadata(), true, true, true);
            checkingRejectedReads = false;
            for (ui64 shard : { sourceShards.front(), copyShards.front() }) {
                const auto rows =
                    ReadSystemTable(runtime, shard, "Snapshots", { "Oid", "Tid", "Step", "TxId" }, { "Step", "TxId" });
                UNIT_ASSERT_VALUES_EQUAL(NKikimr::NClient::TValue::Create(rows)["Result"]["List"].Size(), 0u);
            }
            UpdateRow(runtime, "Source", 3, "outside_copy", sourceShards.front());
            reads.Stop().Unblock();
            env.TestWaitNotification(runtime, operationId);
            TestGetExport(runtime, operationId, "/MyRoot", Ydb::StatusIds::SUCCESS);
            const TString dir = directory.Path().GetPath() + "/table";
            const auto metadata = NBackup::TMetadata::Deserialize(TFileInput(dir + "/metadata.json").ReadAll(), true);
            UNIT_ASSERT(metadata.GetTableChecksum() && !metadata.GetTableChecksum()->Error);
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Count, 2u);
            UNIT_ASSERT(!TFileInput(dir + "/data_00.csv").ReadAll().Contains("outside_copy"));
            return;
        }

        const auto targetShards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/" + target);
        UpdateRow(runtime, target, 1, "changed", targetShards.front());
        UpdateRow(runtime, target, 3, "extra", targetShards.front());
        ui32 subscriptions = 0;
        auto ownerChecks = runtime.AddObserver<TEvSchemeShard::TEvNotifyTxCompletion>([&](auto& ev) {
            if (ev->Get()->Record.GetTxId() == operationId && dataShards.contains(ev->Sender)) {
                ++subscriptions;
            }
        });
        runtime.AdvanceCurrentTime(TDuration::Minutes(6));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(subscriptions, 0u);
        UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, checksumTable, targetShards, checkpoint), 2u);
        UNIT_ASSERT_VALUES_EQUAL(queryVersions.size(), 1u);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        reads.clear();
        reads.Stop();
        env.TestWaitNotification(runtime, operationId);
        const auto result = TestGetImport(runtime, operationId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "validation interrupted by SchemeShard restart");
        UNIT_ASSERT_VALUES_EQUAL(queryVersions.size(), 1u);
    }

    Y_UNIT_TEST(RealServicesInterruptImportChecksumAtAgedCheckpoint) {
        RunRealServicesChecksum<true>();
    }

    Y_UNIT_TEST_TWIN(RealServicesChecksumOnPrivateExportCopyWithoutPins, DisableOnlineRO) {
        RunRealServicesChecksum<false>(DisableOnlineRO);
    }
}
