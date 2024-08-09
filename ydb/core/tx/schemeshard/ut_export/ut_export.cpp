#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/metering/metering.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/env.h>

#include <library/cpp/testing/hook/hook.h>

#include <aws/core/Aws.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

using TTablesWithAttrs = TVector<std::pair<TString, TMap<TString, TString>>>;

namespace {

    Aws::SDKOptions Options;

    Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
        Aws::InitAPI(Options);
    }

    Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
        Aws::ShutdownAPI(Options);
    }

    void Run(TTestBasicRuntime& runtime, TTestEnv& env, const std::variant<TVector<TString>, TTablesWithAttrs>& tablesVar, const TString& request,
            Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
            const TString& dbName = "/MyRoot", bool serverless = false, const TString& userSID = "") {

        TTablesWithAttrs tables;

        if (std::holds_alternative<TVector<TString>>(tablesVar)) {
            for (const auto& table : std::get<TVector<TString>>(tablesVar)) {
                tables.emplace_back(table, TMap<TString, TString>{});
            }
        } else {
            tables = std::get<TTablesWithAttrs>(tablesVar);
        }

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

        for (const auto& [table, attrs] : tables) {
            TVector<std::pair<TString, TString>> attrsVec;
            attrsVec.assign(attrs.begin(), attrs.end());
            const auto userAttrs = AlterUserAttrs(attrsVec);
            TestCreateTable(runtime, schemeshardId, ++txId, dbName, table, {
                NKikimrScheme::StatusAccepted,
                NKikimrScheme::StatusAlreadyExists,
            }, userAttrs);
            env.TestWaitNotification(runtime, txId, schemeshardId);
        }

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        const auto initialStatus = expectedStatus == Ydb::StatusIds::PRECONDITION_FAILED
            ? expectedStatus
            : Ydb::StatusIds::SUCCESS;
        TestExport(runtime, schemeshardId, ++txId, dbName, request, userSID, initialStatus);
        env.TestWaitNotification(runtime, txId, schemeshardId);

        if (initialStatus != Ydb::StatusIds::SUCCESS) {
            return;
        }

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
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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

    const Ydb::Table::PartitioningSettings& GetPartitioningSettings(
        const Ydb::Table::CreateTableRequest& tableDescription
    ) {
        UNIT_ASSERT_C(tableDescription.has_partitioning_settings(), tableDescription.DebugString());
        return tableDescription.partitioning_settings();
    }

    const Ydb::Table::PartitioningSettings& GetIndexTablePartitioningSettings(
        const Ydb::Table::CreateTableRequest& tableDescription
    ) {
        UNIT_ASSERT_C(tableDescription.indexes_size(), tableDescription.DebugString());

        const auto& index = tableDescription.indexes(0);
        UNIT_ASSERT_C(index.has_global_index(), index.DebugString());
        UNIT_ASSERT_C(index.global_index().has_settings(), index.DebugString());

        const auto& settings = index.global_index().settings();
        UNIT_ASSERT_C(settings.has_partitioning_settings(), settings.DebugString());
        return settings.partitioning_settings();
    }

    // It might be an overkill to convert expectedString to expectedProto and back to .DebugString(),
    // but it allows us to ignore whitespace differences when comparing the protobufs.
    auto CreateProtoComparator(TString&& expectedString) {
        return [expectedString = std::move(expectedString)](const auto& proto) {
            std::decay_t<decltype(proto)> expectedProto;
            UNIT_ASSERT_C(
                google::protobuf::TextFormat::ParseFromString(expectedString, &expectedProto),
                expectedString
            );
            UNIT_ASSERT_STRINGS_EQUAL(proto.DebugString(), expectedProto.DebugString());
        };
    }

    void CheckTableScheme(const TString& scheme, auto&& fieldGetter, auto&& fieldChecker) {
        Ydb::Table::CreateTableRequest proto;
        UNIT_ASSERT_C(
            google::protobuf::TextFormat::ParseFromString(scheme, &proto),
            scheme
        );
        fieldChecker(fieldGetter(proto));
    }

    void CheckPermissions(const TString& permissions, auto&& fieldChecker) {
        Ydb::Scheme::ModifyPermissionsRequest proto;
        UNIT_ASSERT_C(
            google::protobuf::TextFormat::ParseFromString(permissions, &proto),
            permissions
        );
        fieldChecker(proto);
    }

} // anonymous

Y_UNIT_TEST_SUITE(TExportToS3Tests) {
    void RunS3(TTestBasicRuntime& runtime, const TVector<TString>& tables, const TString& requestTpl) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        auto request = Sprintf(requestTpl.c_str(), port);

        TTestEnv env(runtime);
        Run(runtime, env, tables, request, Ydb::StatusIds::SUCCESS, "/MyRoot", false);

        for (auto &path : GetExportTargetPaths(request)) {
            auto canonPath = (path.StartsWith("/") || path.empty()) ? path : TString("/") + path;
            auto it = s3Mock.GetData().find(canonPath + "/metadata.json");
            UNIT_ASSERT(it != s3Mock.GetData().end());
            it = s3Mock.GetData().find(canonPath + "/scheme.pb");
            UNIT_ASSERT(it != s3Mock.GetData().end());
            it = s3Mock.GetData().find(canonPath + "/permissions.pb");
            UNIT_ASSERT(it != s3Mock.GetData().end());
        }
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

        const TVector<TString> tables = {R"(
            Name: "Table"
            Columns {
                Name: "key"
                Type: "Utf8"
                DefaultFromLiteral {
                    type {
                        optional_type {
                            item {
                                type_id: UTF8
                            }
                        }
                    }
                    value {
                        items {
                            text_value: "b"
                        }
                    }
                }
            }
            Columns {
                Name: "value"
                Type: "Utf8"
                DefaultFromLiteral {
                    type {
                        optional_type {
                            item {
                                type_id: UTF8
                            }
                        }
                    }
                    value {
                        items {
                            text_value: "a"
                        }
                    }
                }
            }
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

        auto schemeIt = s3Mock.GetData().find("/scheme.pb");
        UNIT_ASSERT(schemeIt != s3Mock.GetData().end());

        TString scheme = schemeIt->second;

        UNIT_ASSERT_NO_DIFF(scheme, R"(columns {
  name: "key"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
  from_literal {
    type {
      optional_type {
        item {
          type_id: UTF8
        }
      }
    }
    value {
      items {
        text_value: "b"
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
  from_literal {
    type {
      optional_type {
        item {
          type_id: UTF8
        }
      }
    }
    value {
      items {
        text_value: "a"
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

    Y_UNIT_TEST(ShouldPreserveIncrBackupFlag) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const TTablesWithAttrs tables{
            {
                R"(
                Name: "Table"
                Columns {
                    Name: "key"
                    Type: "Utf8"
                    DefaultFromLiteral {
                        type {
                            optional_type {
                                item {
                                    type_id: UTF8
                                }
                            }
                        }
                        value {
                            items {
                                text_value: "b"
                            }
                        }
                    }
                }
                Columns {
                    Name: "value"
                    Type: "Utf8"
                    DefaultFromLiteral {
                        type {
                            optional_type {
                                item {
                                    type_id: UTF8
                                }
                            }
                        }
                        value {
                            items {
                                text_value: "a"
                            }
                        }
                    }
                }
                KeyColumnNames: ["key"]
                )",
                {{"__incremental_backup", "{}"}},
            },
        };

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

        auto schemeIt = s3Mock.GetData().find("/scheme.pb");
        UNIT_ASSERT(schemeIt != s3Mock.GetData().end());

        TString scheme = schemeIt->second;

        UNIT_ASSERT_NO_DIFF(scheme, R"(columns {
  name: "key"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
  from_literal {
    type {
      optional_type {
        item {
          type_id: UTF8
        }
      }
    }
    value {
      items {
        text_value: "b"
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
  from_literal {
    type {
      optional_type {
        item {
          type_id: UTF8
        }
      }
    }
    value {
      items {
        text_value: "a"
      }
    }
  }
}
primary_key: "key"
attributes {
  key: "__incremental_backup"
  value: "{}"
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
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UpdateRow(runtime, "Table", 1, "valueA");
        UpdateRow(runtime, "Table", 2, "valueB");

        runtime.SetLogPriority(NKikimrServices::S3_WRAPPER, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        TMaybe<ui64> backupTxId;
        TMaybe<ui64> tabletId;
        bool delayed = false;

        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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
        TTestEnv env(runtime, TTestEnvOptions().DisableStatsBatching(true));
        ui64 txId = 100;

        THashSet<ui64> statsCollected;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        for (int i = 1; i < 500; ++i) {
            UpdateRow(runtime, "Table", i, "value");
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

    Y_UNIT_TEST(ShouldRestartOnScanErrors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UpdateRow(runtime, "Table", 1, "valueA");

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        THolder<IEventHandle> injectResult;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NSharedCache::EvResult) {
                const auto* msg = ev->Get<NSharedCache::TEvResult>();
                UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);

                auto result = MakeHolder<NSharedCache::TEvResult>(msg->Origin, msg->Cookie, NKikimrProto::ERROR);
                std::move(msg->Loaded.begin(), msg->Loaded.end(), std::back_inserter(result->Loaded));

                injectResult = MakeHolder<IEventHandle>(ev->Recipient, ev->Sender, result.Release(), ev->Flags, ev->Cookie);
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

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

        if (!injectResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&injectResult](IEventHandle&) -> bool {
                return bool(injectResult);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(injectResult.Release(), 0, true);

        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldSucceedOnConcurrentTxs) {
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

        THolder<IEventHandle> copyTables;
        auto origObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvSchemeShard::EvModifySchemeTransaction) {
                const auto& record = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record;
                if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                    copyTables.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto exportId = ++txId;
        TestExport(runtime, exportId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port));

        if (!copyTables) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&copyTables](IEventHandle&) -> bool {
                return bool(copyTables);
            });
            runtime.DispatchEvents(opts);
        }

        THolder<IEventHandle> proposeTxResult;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvProposeTransactionResult) {
                proposeTxResult.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "extra"  Type: "Utf8"}
        )");

        if (!proposeTxResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&proposeTxResult](IEventHandle&) -> bool {
                return bool(proposeTxResult);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(origObserver);
        runtime.Send(copyTables.Release(), 0, true);
        runtime.Send(proposeTxResult.Release(), 0, true);
        env.TestWaitNotification(runtime, txId);

        env.TestWaitNotification(runtime, exportId);
        TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldSucceedOnConcurrentExport) {
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

        TVector<THolder<IEventHandle>> copyTables;
        auto origObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvSchemeShard::EvModifySchemeTransaction) {
                const auto& record = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record;
                if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                    copyTables.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto waitCopyTables = [&runtime, &copyTables](ui32 size) {
            if (copyTables.size() != size) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&copyTables, size](IEventHandle&) -> bool {
                    return copyTables.size() == size;
                });
                runtime.DispatchEvents(opts);
            }
        };

        TVector<ui64> exportIds;
        for (ui32 i = 1; i <= 3; ++i) {
            exportIds.push_back(++txId);
            TestExport(runtime, exportIds[i - 1], "/MyRoot", Sprintf(R"(
                ExportToS3Settings {
                  endpoint: "localhost:%d"
                  scheme: HTTP
                  items {
                    source_path: "/MyRoot/Table"
                    destination_prefix: "Table%u"
                  }
                }
            )", port, i));
            waitCopyTables(i);
        }

        runtime.SetObserverFunc(origObserver);
        for (auto& ev : copyTables) {
            runtime.Send(ev.Release(), 0, true);
        }

        for (ui64 exportId : exportIds) {
            env.TestWaitNotification(runtime, exportId);
            TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(ShouldSucceedOnConcurrentImport) {
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

        // prepare backup data
        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: "Backup1"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TVector<THolder<IEventHandle>> delayed;
        auto origObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvSchemeShard::EvModifySchemeTransaction) {
                const auto& record = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record;
                const auto opType = record.GetTransaction(0).GetOperationType();
                switch (opType) {
                case NKikimrSchemeOp::ESchemeOpRestore:
                case NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables:
                    delayed.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                default:
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto waitForDelayed = [&runtime, &delayed](ui32 size) {
            if (delayed.size() != size) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&delayed, size](IEventHandle&) -> bool {
                    return delayed.size() == size;
                });
                runtime.DispatchEvents(opts);
            }
        };

        const auto importId = ++txId;
        TestImport(runtime, importId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "Backup1"
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        // wait for restore op
        waitForDelayed(1);

        const auto exportId = ++txId;
        TestExport(runtime, exportId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Restored"
                destination_prefix: "Backup2"
              }
            }
        )", port));
        // wait for copy table op
        waitForDelayed(2);

        runtime.SetObserverFunc(origObserver);
        for (auto& ev : delayed) {
            runtime.Send(ev.Release(), 0, true);
        }

        env.TestWaitNotification(runtime, importId);
        TestGetImport(runtime, importId, "/MyRoot");
        env.TestWaitNotification(runtime, exportId);
        TestGetExport(runtime, exportId, "/MyRoot");
    }

    void ShouldCheckQuotas(const TSchemeLimits& limits, Ydb::StatusIds::StatusCode expectedFailStatus) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        const TString userSID = "user@builtin";
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().SystemBackupSIDs({userSID}));

        SetSchemeshardSchemaLimits(runtime, limits);

        const TVector<TString> tables = {
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        };
        const TString request = Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port);

        Run(runtime, env, tables, request, expectedFailStatus);
        Run(runtime, env, tables, request, Ydb::StatusIds::SUCCESS, "/MyRoot", false, userSID);
    }

    Y_UNIT_TEST(ShouldCheckQuotas) {
        ShouldCheckQuotas(TSchemeLimits{.MaxExports = 0}, Ydb::StatusIds::PRECONDITION_FAILED);
        ShouldCheckQuotas(TSchemeLimits{.MaxChildrenInDir = 1}, Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldRetryAtFinalStage) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UpdateRow(runtime, "Table", 1, "valueA");
        UpdateRow(runtime, "Table", 2, "valueB");
        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        THolder<IEventHandle> injectResult;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::EvProposeTransaction: {
                    auto& record = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                    if (record.GetTxKind() != NKikimrTxDataShard::ETransactionKind::TX_KIND_SCHEME) {
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }

                    NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
                    UNIT_ASSERT(schemeTx.ParseFromString(record.GetTxBody()));

                    if (schemeTx.HasBackup()) {
                        schemeTx.MutableBackup()->MutableScanSettings()->SetRowsBatchSize(1);
                        record.SetTxBody(schemeTx.SerializeAsString());
                    }

                    return TTestActorRuntime::EEventAction::PROCESS;
                }

                case NWrappers::NExternalStorage::EvCompleteMultipartUploadResponse: {
                    auto response = MakeHolder<NWrappers::NExternalStorage::TEvCompleteMultipartUploadResponse>(
                        std::nullopt,
                        Aws::Utils::Outcome<Aws::S3::Model::CompleteMultipartUploadResult, Aws::S3::S3Error>(
                            Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::SLOW_DOWN, true)
                        )
                    );
                    injectResult = MakeHolder<IEventHandle>(ev->Recipient, ev->Sender, response.Release(), ev->Flags, ev->Cookie);
                    return TTestActorRuntime::EEventAction::DROP;
                }

                default: {
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
            }
        });

        const auto exportId = ++txId;
        TestExport(runtime, txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              number_of_retries: 10
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port));

        if (!injectResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&injectResult](IEventHandle&) -> bool {
                return bool(injectResult);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
        runtime.Send(injectResult.Release(), 0, true);

        env.TestWaitNotification(runtime, exportId);
        TestGetExport(runtime, exportId, "/MyRoot");
    }

    Y_UNIT_TEST(CorruptedDyNumber) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().DisableStatsBatching(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "DyNumber" }
                KeyColumnNames: ["key"]
            )");
        env.TestWaitNotification(runtime, txId);

        // Write bad DyNumber
        UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});

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

        TestGetExport(runtime, txId, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(UidAsIdempotencyKey) {
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

        const auto request = Sprintf(R"(
            OperationParams {
              labels {
                key: "uid"
                value: "foo"
              }
            }
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port);

        // create operation
        TestExport(runtime, ++txId, "/MyRoot", request);
        const ui64 exportId = txId;
        // create operation again with same uid
        TestExport(runtime, ++txId, "/MyRoot", request);
        // new operation was not created
        TestGetExport(runtime, txId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        // check previous operation
        TestGetExport(runtime, exportId, "/MyRoot");
        env.TestWaitNotification(runtime, exportId);
    }

    Y_UNIT_TEST(ExportStartTime) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.UpdateCurrentTime(TInstant::Now());
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

        const auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_PREPARING);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(!entry.HasEndTime());
    }

    Y_UNIT_TEST(CompletedExportEndTime) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.UpdateCurrentTime(TInstant::Now());
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

        runtime.AdvanceCurrentTime(TDuration::Seconds(30)); // doing export

        env.TestWaitNotification(runtime, txId);

        const auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());
    }

    Y_UNIT_TEST(CancelledExportEndTime) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto delayFunc = [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpBackup;
        };

        THolder<IEventHandle> delayed;
        auto prevObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (delayFunc(ev)) {
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
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

        runtime.AdvanceCurrentTime(TDuration::Seconds(30)); // doing export

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }
        runtime.SetObserverFunc(prevObserver);

        TestCancelExport(runtime, ++txId, "/MyRoot", exportId);

        auto desc = TestGetExport(runtime, exportId, "/MyRoot");
        auto entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(!entry.HasEndTime());

        runtime.Send(delayed.Release(), 0, true);
        env.TestWaitNotification(runtime, exportId);

        desc = TestGetExport(runtime, exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLED);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());
    }

    Y_UNIT_TEST(ExportPartitioningSettings) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        Run(runtime, env, TVector<TString>{
                R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                    PartitionConfig {
                        PartitioningPolicy {
                            MinPartitionsCount: 10
                            SplitByLoadSettings: {
                                Enabled: true
                            }
                        }
                    }
                )"
            },
            Sprintf(
                R"(
                    ExportToS3Settings {
                        endpoint: "localhost:%d"
                        scheme: HTTP
                        items {
                            source_path: "/MyRoot/Table"
                            destination_prefix: ""
                        }
                    }
                )",
                port
            )
        );

        auto* scheme = s3Mock.GetData().FindPtr("/scheme.pb");
        UNIT_ASSERT(scheme);
        CheckTableScheme(*scheme, GetPartitioningSettings, CreateProtoComparator(R"(
            partitioning_by_size: DISABLED
            partitioning_by_load: ENABLED
            min_partitions_count: 10
        )"));
    }

    Y_UNIT_TEST(ExportIndexTablePartitioningSettings) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription {
                Name: "ByValue"
                KeyColumnNames: ["value"]
                IndexImplTableDescriptions: [ {
                    PartitionConfig {
                        PartitioningPolicy {
                            MinPartitionsCount: 10
                            SplitByLoadSettings: {
                                Enabled: true
                            }
                        }
                    }
                } ]
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(
                R"(
                    ExportToS3Settings {
                        endpoint: "localhost:%d"
                        scheme: HTTP
                        items {
                            source_path: "/MyRoot/Table"
                            destination_prefix: ""
                        }
                    }
                )",
                port
            )
        );
        env.TestWaitNotification(runtime, txId);

        auto* scheme = s3Mock.GetData().FindPtr("/scheme.pb");
        UNIT_ASSERT(scheme);
        CheckTableScheme(*scheme, GetIndexTablePartitioningSettings, CreateProtoComparator(R"(
            partitioning_by_size: DISABLED
            partitioning_by_load: ENABLED
            min_partitions_count: 10
        )"));
    }

    Y_UNIT_TEST(UserSID) {
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

        const TString request = Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", port);
        const TString userSID = "user@builtin";
        TestExport(runtime, ++txId, "/MyRoot", request, userSID);

        const auto desc = TestGetExport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_PREPARING);
        UNIT_ASSERT_VALUES_EQUAL(entry.GetUserSID(), userSID);
    }

    Y_UNIT_TEST(TablePermissions) {
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

        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user@builtin", NACLib::InheritNone);
        TestModifyACL(runtime, ++txId, "/MyRoot", "Table", diffACL.SerializeAsString(), "user@builtin");
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

        auto* permissions = s3Mock.GetData().FindPtr("/permissions.pb");
        UNIT_ASSERT(permissions);
        CheckPermissions(*permissions, CreateProtoComparator(R"(
            actions {
                change_owner: "user@builtin"
            }
            actions {
                grant {
                    subject: "user@builtin"
                    permission_names: "ydb.generic.use"
                }
            }
        )"));
    }
}
