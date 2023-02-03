#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/metering/metering.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/env.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

namespace {

    void Run(TTestBasicRuntime& runtime, TTestEnv& env, const TVector<TString>& tables, const TString& request,
            Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
            const TString& dbName = "/MyRoot", bool serverless = false, const TString& userSID = "") {

        ui64 txId = 100;

        ui64 schemeshardId = TTestTxConfig::SchemeShard;
        if (dbName != "/MyRoot") {
            TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "%s"
            )", TStringBuf(serverless ? "/MyRoot/Shared" : dbName).RNextTok('/').data()));
            env.TestWaitNotification(runtime, txId);

            TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", Sprintf(R"(
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                Name: "%s"
                StoragePools {
                  Name: "name_User_kind_hdd-1"
                  Kind: "common"
                }
                StoragePools {
                  Name: "name_User_kind_hdd-2"
                  Kind: "external"
                }
            )", TStringBuf(serverless ? "/MyRoot/Shared" : dbName).RNextTok('/').data()));
            env.TestWaitNotification(runtime, txId);

            if (serverless) {
                const auto attrs = AlterUserAttrs({
                    {"cloud_id", "CLOUD_ID_VAL"},
                    {"folder_id", "FOLDER_ID_VAL"},
                    {"database_id", "DATABASE_ID_VAL"}
                });

                TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "%s"
                    ResourcesDomainKey {
                        SchemeShard: %lu
                        PathId: 2
                    }
                )", TStringBuf(dbName).RNextTok('/').data(), TTestTxConfig::SchemeShard), attrs);
                env.TestWaitNotification(runtime, txId);

                TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    ExternalSchemeShard: true
                    ExternalHive: false
                    Name: "%s"
                    StoragePools {
                      Name: "name_User_kind_hdd-1"
                      Kind: "common"
                    }
                    StoragePools {
                      Name: "name_User_kind_hdd-2"
                      Kind: "external"
                    }
                )", TStringBuf(dbName).RNextTok('/').data()));
                env.TestWaitNotification(runtime, txId);
            }

            TestDescribeResult(DescribePath(runtime, dbName), {
                NLs::PathExist,
                NLs::ExtractTenantSchemeshard(&schemeshardId)
            });
        }

        for (const auto& table : tables) {
            TestCreateTable(runtime, schemeshardId, ++txId, dbName, table);
            env.TestWaitNotification(runtime, txId, schemeshardId);
        }

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        TestExport(runtime, schemeshardId, ++txId, dbName, request, userSID);
        env.TestWaitNotification(runtime, txId, schemeshardId);

        const ui64 exportId = txId;
        TestGetExport(runtime, schemeshardId, exportId, dbName, expectedStatus);

        TestForgetExport(runtime, schemeshardId, ++txId, dbName, exportId);
        env.TestWaitNotification(runtime, exportId, schemeshardId);

        TestGetExport(runtime, schemeshardId, exportId, dbName, Ydb::StatusIds::NOT_FOUND);
    }

    using TDelayFunc = std::function<bool(TAutoPtr<IEventHandle>&)>;

    void Cancel(const TVector<TString>& tables, const TString& request, TDelayFunc delayFunc) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        for (const auto& table : tables) {
            TestCreateTable(runtime, ++txId, "/MyRoot", table);
            env.TestWaitNotification(runtime, txId);
        }

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        THolder<IEventHandle> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (delayFunc(ev)) {
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestExport(runtime, ++txId, "/MyRoot", request);
        const ui64 exportId = txId;

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);

        TestCancelExport(runtime, ++txId, "/MyRoot", exportId);
        runtime.Send(delayed.Release(), 0, true);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);

        TestForgetExport(runtime, ++txId, "/MyRoot", exportId);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

} // anonymous

Y_UNIT_TEST_SUITE(TExportToS3Tests) {
    void RunS3(TTestBasicRuntime& runtime, const TVector<TString>& tables, const TString& request) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestEnv env(runtime);
        Run(runtime, env, tables, Sprintf(request.c_str(), port), Ydb::StatusIds::SUCCESS, "/MyRoot", false);
    }

    Y_UNIT_TEST(ShouldSucceedOnSingleShardTable) {
        TTestBasicRuntime runtime;

        RunS3(runtime, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnMultiShardTable) {
        TTestBasicRuntime runtime;

        RunS3(runtime, {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnManyTables) {
        TTestBasicRuntime runtime;

        RunS3(runtime, {
            R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
            R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table1"
                destination_prefix: "table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2"
              }
            }
        )");
    }

    Y_UNIT_TEST(ShouldOmitNonStrictStorageSettings) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        TString scheme;
        runtime.SetObserverFunc([&scheme](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (!scheme && ev->GetTypeRewrite() == NWrappers::NExternalStorage::EvPutObjectRequest) {
                const auto* msg = ev->Get<NWrappers::NExternalStorage::TEvPutObjectRequest>();
                scheme = msg->Body;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const TVector<TString> tables = {R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            PartitionConfig {
              ColumnFamilies {
                Id: 0
                StorageConfig {
                  SysLog {
                    PreferredPoolKind: "hdd-1"
                    AllowOtherKinds: true
                  }
                  Log {
                    PreferredPoolKind: "hdd-1"
                    AllowOtherKinds: true
                  }
                  Data {
                    PreferredPoolKind: "hdd-1"
                    AllowOtherKinds: true
                  }
                }
              }
            }
        )"};

        Run(runtime, env, tables, Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port));

        UNIT_ASSERT_NO_DIFF(scheme, R"(columns {
  name: "key"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
columns {
  name: "value"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
primary_key: "key"
storage_settings {
  store_external_blobs: DISABLED
}
column_families {
  name: "default"
  compression: COMPRESSION_NONE
}
partitioning_settings {
  partitioning_by_size: DISABLED
  partitioning_by_load: DISABLED
  min_partitions_count: 1
}
)");
    }

    void CancelShouldSucceed(TDelayFunc delayFunc) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        Cancel({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port), delayFunc);
    }

    Y_UNIT_TEST(CancelUponCreatingExportDirShouldSucceed) {
        CancelShouldSucceed([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMkDir;
        });
    }

    Y_UNIT_TEST(CancelUponCopyingTablesShouldSucceed) {
        CancelShouldSucceed([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables;
        });
    }

    void CancelUponTransferringShouldSucceed(const TVector<TString>& tables, const TString& request) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        Cancel(tables, Sprintf(request.c_str(), port), [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpBackup;
        });
    }

    Y_UNIT_TEST(CancelUponTransferringSingleShardTableShouldSucceed) {
        CancelUponTransferringShouldSucceed({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(CancelUponTransferringMultiShardTableShouldSucceed) {
        CancelUponTransferringShouldSucceed({
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )");
    }

    Y_UNIT_TEST(CancelUponTransferringSingleTableShouldSucceed) {
        // same as CancelUponTransferringSingleShardTableShouldSucceed
    }

    Y_UNIT_TEST(CancelUponTransferringManyTablesShouldSucceed) {
        CancelUponTransferringShouldSucceed({
            R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
            R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table1"
                destination_prefix: "table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2"
              }
            }
        )");
    }

    Y_UNIT_TEST(DropSourceTableBeforeTransferring) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        bool dropNotification = false;
        THolder<IEventHandle> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvSchemeShard::EvModifySchemeTransaction:
                break;
            case TEvSchemeShard::EvNotifyTxCompletionResult:
                if (dropNotification) {
                    delayed.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            const auto* msg = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>();
            if (msg->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                dropNotification = true;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port));
        const ui64 exportId = txId;

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        runtime.Send(delayed.Release(), 0, true);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);

        TestForgetExport(runtime, ++txId, "/MyRoot", exportId);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    void DropCopiesBeforeTransferring(ui32 tablesCount) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        for (ui32 i = 1; i <= tablesCount; ++i) {
            TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "Table%d"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", i));
            env.TestWaitNotification(runtime, txId);
        }

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        bool dropNotification = false;
        THolder<IEventHandle> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvSchemeShard::EvModifySchemeTransaction:
                break;
            case TEvSchemeShard::EvNotifyTxCompletionResult:
                if (dropNotification) {
                    delayed.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            const auto* msg = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>();
            if (msg->Record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                dropNotification = true;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TStringBuilder items;
        for (ui32 i = 1; i <= tablesCount; ++i) {
            items << "items {"
                << " source_path: \"/MyRoot/Table" << i << "\""
                << " destination_prefix: \"\""
            << " }";
        }

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              %s
            }
        )", port, items.c_str()));
        const ui64 exportId = txId;

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);

        for (ui32 i = 0; i < tablesCount; ++i) {
            TestDropTable(runtime, ++txId, Sprintf("/MyRoot/export-%" PRIu64, exportId), ToString(i));
            env.TestWaitNotification(runtime, txId);
        }

        runtime.Send(delayed.Release(), 0, true);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);

        TestForgetExport(runtime, ++txId, "/MyRoot", exportId);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(DropCopiesBeforeTransferring1) {
        DropCopiesBeforeTransferring(1);
    }

    Y_UNIT_TEST(DropCopiesBeforeTransferring2) {
        DropCopiesBeforeTransferring(2);
    }

    void RebootDuringFinish(bool rejectUploadParts, Ydb::StatusIds::StatusCode expectedStatus) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto writeRow = [&](ui64 tabletId, const char* key, const char* value) {
            NKikimrMiniKQL::TResult result;
            TString error;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
                (
                    (let key '( '('key (Utf8 '%s) ) ) )
                    (let row '( '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__Table key row) ))
                )
            )", key, value), result, error);

            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "");
        };

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        writeRow(TTestTxConfig::FakeHiveTablets, "a", "valueA");
        writeRow(TTestTxConfig::FakeHiveTablets, "b", "valueB");

        runtime.SetLogPriority(NKikimrServices::S3_WRAPPER, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        TMaybe<ui64> backupTxId;
        TMaybe<ui64> tabletId;
        bool delayed = false;

        auto prevObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::EvProposeTransaction: {
                    auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                    if (record.GetTxKind() != NKikimrTxDataShard::ETransactionKind::TX_KIND_SCHEME) {
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }

                    NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
                    UNIT_ASSERT(schemeTx.ParseFromString(record.GetTxBody()));

                    if (schemeTx.HasBackup()) {
                        backupTxId = record.GetTxId();
                        // hijack
                        schemeTx.MutableBackup()->MutableScanSettings()->SetRowsBatchSize(1);
                        record.SetTxBody(schemeTx.SerializeAsString());
                    }

                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                case TEvDataShard::EvProposeTransactionResult: {
                    if (!backupTxId) {
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }

                    const auto& record = ev->Get<TEvDataShard::TEvProposeTransactionResult>()->Record;
                    if (record.GetTxId() != *backupTxId) {
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }

                    tabletId = record.GetOrigin();
                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                case NWrappers::NExternalStorage::EvCompleteMultipartUploadRequest:
                case NWrappers::NExternalStorage::EvAbortMultipartUploadRequest:
                    delayed = true;
                    return TTestActorRuntime::EEventAction::DROP;

                default:
                    return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port).WithRejectUploadParts(rejectUploadParts));
        UNIT_ASSERT(s3Mock.Start());

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port));
        const ui64 exportId = txId;

        if (!delayed || !tabletId) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed, &tabletId](IEventHandle&) -> bool {
                return delayed && tabletId;
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);

        RebootTablet(runtime, *tabletId, runtime.AllocateEdgeActor());
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", expectedStatus);

        TestForgetExport(runtime, ++txId, "/MyRoot", exportId);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(RebootDuringCompletion) {
        RebootDuringFinish(false, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(RebootDuringAbortion) {
        RebootDuringFinish(true, Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldExcludeBackupTableFromStats) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);

        TTestEnv env(runtime, opts);

        ui64 txId = 100;

        auto writeRow = [&](ui64 tabletId, const TString& key, const TString& value) {
            NKikimrMiniKQL::TResult result;
            TString error;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, Sprintf(R"(
                (
                    (let key '( '('key (Utf8 '%s) ) ) )
                    (let row '( '('value (Utf8 '%s) ) ) )
                    (return (AsList (UpdateRow '__user__Table key row) ))
                )
            )", key.c_str(), value.c_str()), result, error);

            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "");
        };

        THashSet<ui64> statsCollected;
        runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvPeriodicTableStats) {
                statsCollected.insert(ev->Get<TEvDataShard::TEvPeriodicTableStats>()->Record.GetDatashardId());
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto waitForStats = [&](ui32 count) {
            statsCollected.clear();

            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) -> bool {
                return statsCollected.size() == count;
            });
            runtime.DispatchEvents(opts);

            return DescribePath(runtime, "/MyRoot")
                .GetPathDescription()
                .GetDomainDescription()
                .GetDiskSpaceUsage();
        };

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        for (int i = 1; i < 500; ++i) {
            writeRow(TTestTxConfig::FakeHiveTablets, Sprintf("a%i", i), "value");
        }

        // trigger memtable's compaction
        TestCopyTable(runtime, ++txId, "/MyRoot", "CopyTable", "/MyRoot/Table");
        env.TestWaitNotification(runtime, txId);
        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        env.TestWaitNotification(runtime, txId);

        const auto expected = waitForStats(1);

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/CopyTable"
                destination_prefix: ""
              }
            }
        )", port));
        const ui64 exportId = txId;
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto afterExport = waitForStats(2);
        UNIT_ASSERT_STRINGS_EQUAL(expected.DebugString(), afterExport.DebugString());

        TestForgetExport(runtime, ++txId, "/MyRoot", exportId);
        env.TestWaitNotification(runtime, exportId);

        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        const auto afterForget = waitForStats(1);
        UNIT_ASSERT_STRINGS_EQUAL(expected.DebugString(), afterForget.DebugString());
    }

    Y_UNIT_TEST(CheckItemProgress) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);

        const auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.ItemsProgressSize(), 1);

        const auto& item = entry.GetItemsProgress(0);
        UNIT_ASSERT_VALUES_EQUAL(item.parts_total(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.parts_completed(), 1);
        UNIT_ASSERT(item.has_start_time());
        UNIT_ASSERT(item.has_end_time());
    }
}
