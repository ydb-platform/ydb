#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/backup/common/table_checksum_snapshot.h>
#include <ydb/core/backup/query/table_checksum.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/mvcc_version.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/long_tx_service.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/table_checksum_helpers.h>
#include <ydb/library/testlib/helpers.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr;
using namespace NKikimr::Tests;

Y_UNIT_TEST_SUITE(TableChecksumSnapshotRecovery) {
    Y_UNIT_TEST_TWIN(RealServicesRecoverAgedCheckpoint, Importing) {
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
        ui32 retained = 0;
        ui32 rejected = 0;
        auto registrations =
            runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>([&](auto& ev) {
                retained += ev->Get()->Status == Ydb::StatusIds::SUCCESS;
                if (ev->Get()->Status == Ydb::StatusIds::PRECONDITION_FAILED) {
                    UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Issues.ToOneLineString(), "safe registration window");
                    ++rejected;
                }
            });
        bool recovering = false;
        TTableId checksumTable;
        auto acquireRequests =
            runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>([&](auto& ev) {
                if (recovering && !ev->Get()->TableIds.empty()) {
                    checksumTable = ev->Get()->TableIds.front();
                }
            });
        TVector<TString> queryVersions;
        // The same request is delivered to both the proxy and its session
        THashSet<TString> querySessions;
        auto queries = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (recovering && ev->Get()->GetQuery().Contains("CHECKSUM TABLE") &&
                querySessions.insert(ev->Get()->GetSessionId()).second) {
                UNIT_ASSERT(ev->Get()->GetMvccVersion());
                queryVersions.push_back(*ev->Get()->GetMvccVersion());
            }
        });
        TRowVersion checkpoint = TRowVersion::Min();
        TActorId originalReader;
        TBlockEvents<TEvDataShard::TEvRead> reads(runtime, [&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (!recovering || !record.HasSnapshot() || !record.GetSnapshot().GetTxId() ||
                TPathId(record.GetTableId().GetOwnerId(), record.GetTableId().GetTableId()) != checksumTable.PathId) {
                return false;
            }
            const auto version = TRowVersion::FromProto(ev->Get()->Record.GetSnapshot());
            if (!checkpoint.Step) {
                checkpoint = version;
                originalReader = ev->Sender;
            }
            UNIT_ASSERT_VALUES_EQUAL(version, checkpoint);
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
        runtime.WaitFor("the native checksum query has retained its checkpoint and reached DataShard", [&] {
            return !reads.empty() && retained >= (Importing ? 2u : 1u);
        });
        UNIT_ASSERT(checkpoint.Step && checkpoint.TxId);
        UNIT_ASSERT_VALUES_EQUAL(queryVersions.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(queryVersions.front(), NKqp::FormatMvccVersion(checkpoint));

        const auto targetShards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/" + target);
        UpdateRow(runtime, target, 1, "changed", targetShards.front());
        UpdateRow(runtime, target, 3, "extra", targetShards.front());
        ui32 subscriptions = 0;
        THashSet<TActorId> subscribers;
        auto ownerChecks = runtime.AddObserver<TEvSchemeShard::TEvNotifyTxCompletion>([&](auto& ev) {
            if (ev->Get()->Record.GetTxId() == operationId && dataShards.contains(ev->Sender)) {
                ++subscriptions;
            }
        });
        auto ownerReplies = runtime.AddObserver<TEvSchemeShard::TEvNotifyTxCompletionRegistered>([&](auto& ev) {
            if (ev->Get()->Record.GetTxId() == operationId && dataShards.contains(ev->GetRecipientRewrite())) {
                subscribers.insert(ev->GetRecipientRewrite());
            }
        });
        runtime.AdvanceCurrentTime(NBackup::TableChecksumSnapshotTimeout + TDuration::Seconds(1));
        // Export has one source pin and one copy pin; import has just the restored table pin
        runtime.WaitFor("expired checksum pins subscribe to operation completion", [&] {
            return subscribers.size() == (Importing ? 1u : 2u);
        }, TDuration::Seconds(30));
        const auto confirmed =
            ConfirmTableChecksumSnapshot(runtime, checksumTable, TTestTxConfig::SchemeShard, checkpoint);
        UNIT_ASSERT_VALUES_EQUAL_C(confirmed->Get()->Status, Ydb::StatusIds::SUCCESS, confirmed->Get()->Error);
        runtime.AdvanceCurrentTime(NBackup::TableChecksumSnapshotTimeout);
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(subscriptions, subscribers.size());
        ownerChecks.Remove();
        ownerReplies.Remove();
        UNIT_ASSERT_VALUES_EQUAL(queryVersions.size(), 1u);
        UNIT_ASSERT_LT(
            TInstant::MilliSeconds(checkpoint.Step) + NBackup::TableChecksumSnapshotTimeout, runtime.GetCurrentTime());
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        if (!Importing) {
            const auto copies = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/export-200/0");
            UNIT_ASSERT_VALUES_EQUAL(copies.size(), 1u);
            // Export's checksum actor belongs to the uploader, so restart its owner too
            RebootTablet(runtime, copies.front(), runtime.AllocateEdgeActor());
        }
        reads.clear();
        reads.Stop();
        env.TestWaitNotification(runtime, operationId);
        if (Importing) {
            TestGetImport(runtime, operationId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        } else {
            TestGetExport(runtime, operationId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        }
        UNIT_ASSERT_GE(queryVersions.size(), 2u);
        for (const auto& version : queryVersions) {
            UNIT_ASSERT_VALUES_EQUAL(version, queryVersions.front());
        }
        runtime.WaitFor("the real registry rejects the aged checkpoint on recovery", [&] {
            return rejected > 0;
        }, TDuration::Seconds(30));
        const auto metadata = NBackup::TMetadata::Deserialize(
            TFileInput(directory.Path().GetPath() + "/table/metadata.json").ReadAll(), true);
        UNIT_ASSERT(metadata.GetTableChecksum().has_value());
        UNIT_ASSERT(!metadata.GetTableChecksum()->Error.has_value());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Count, 2u);
    }
}
