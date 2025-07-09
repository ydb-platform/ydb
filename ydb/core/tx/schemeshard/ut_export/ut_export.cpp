#include <ydb/public/api/protos/ydb_export.pb.h>

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <library/cpp/testing/hook/hook.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/env.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

using TTablesWithAttrs = TVector<std::pair<TString, TMap<TString, TString>>>;

using namespace NKikimr::Tests;

namespace {

    Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
        NKikimr::InitAwsAPI();
    }

    Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
        NKikimr::ShutdownAwsAPI();
    }

    void Run(TTestBasicRuntime& runtime, TTestEnv& env, const std::variant<TVector<TString>, TTablesWithAttrs>& tablesVar, const TString& request,
            Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
            const TString& dbName = "/MyRoot", bool serverless = false, const TString& userSID = "", const TString& peerName = "",
            const TVector<TString>& cdcStreams = {}, bool checkAutoDropping = false) {

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

        for (const auto& cdcStream : cdcStreams) {
            TestCreateCdcStream(runtime, schemeshardId, ++txId, dbName, cdcStream);
            env.TestWaitNotification(runtime, txId, schemeshardId);
        }

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        const auto initialStatus = expectedStatus == Ydb::StatusIds::PRECONDITION_FAILED
            ? expectedStatus
            : Ydb::StatusIds::SUCCESS;
        TestExport(runtime, schemeshardId, ++txId, dbName, request, userSID, peerName, initialStatus);
        env.TestWaitNotification(runtime, txId, schemeshardId);

        if (initialStatus != Ydb::StatusIds::SUCCESS) {
            return;
        }

        const ui64 exportId = txId;
        TestGetExport(runtime, schemeshardId, exportId, dbName, expectedStatus);

        if (!runtime.GetAppData().FeatureFlags.GetEnableExportAutoDropping() && checkAutoDropping) {
          auto desc = DescribePath(runtime, "/MyRoot");
          Cerr << "desc: " << desc.GetPathDescription().ChildrenSize()<< Endl;
          UNIT_ASSERT(desc.GetPathDescription().ChildrenSize() > 1);

          bool foundExportDir = false;
          bool foundOriginalTable = false;

          for (size_t i = 0; i < desc.GetPathDescription().ChildrenSize(); ++i) {
              const auto& child = desc.GetPathDescription().GetChildren(i);
              const auto& name = child.GetName();

              if (name.StartsWith("Table")) {
                  foundOriginalTable = true;
              } else if (name.StartsWith("export-")) {
                  foundExportDir = true;
                  auto exportDirDesc = DescribePath(runtime, "/MyRoot/" + name);
                  UNIT_ASSERT(exportDirDesc.GetPathDescription().ChildrenSize() >= 1);
                  UNIT_ASSERT_EQUAL(exportDirDesc.GetPathDescription().GetChildren(0).GetName(), "0");
              }
          }

          UNIT_ASSERT(foundExportDir);
          UNIT_ASSERT(foundOriginalTable);
        } else if (checkAutoDropping) {
          auto desc = DescribePath(runtime, "/MyRoot");
          Cerr << "desc: " << desc.GetPathDescription().ChildrenSize()<< Endl;
          for (size_t i = 0; i < desc.GetPathDescription().ChildrenSize(); ++i) {
              const auto& child = desc.GetPathDescription().GetChildren(i);
              const auto& name = child.GetName();
              UNIT_ASSERT(!name.StartsWith("export-"));
          }
        }

        TestForgetExport(runtime, schemeshardId, ++txId, dbName, exportId);
        env.TestWaitNotification(runtime, exportId, schemeshardId);

        TestGetExport(runtime, schemeshardId, exportId, dbName, Ydb::StatusIds::NOT_FOUND);
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

    class TExportFixture : public NUnitTest::TBaseFixture {
    public:
        void RunS3(const TVector<TString>& tables, const TString& requestTpl, Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS, bool checkS3FilesExistence = true) {
            auto requestStr = Sprintf(requestTpl.c_str(), S3Port());
            NKikimrExport::TCreateExportRequest request;
            UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(requestStr, &request));

            Env(); // Init test env
            Runtime().GetAppData().FeatureFlags.SetEnableEncryptedExport(true);

            Run(Runtime(), Env(), tables, requestStr, expectedStatus, "/MyRoot", false);

            auto calcPath = [&](const TString& targetPath, const TString& file) {
                TString canonPath = (targetPath.StartsWith("/") || targetPath.empty()) ? targetPath : TString("/") + targetPath;
                TString result = canonPath;
                result += '/';
                result += file;
                if (request.GetExportToS3Settings().has_encryption_settings()) {
                    result += ".enc";
                }
                return result;
            };

            if (expectedStatus == Ydb::StatusIds::SUCCESS && checkS3FilesExistence) {
                for (auto& path : GetExportTargetPaths(requestStr)) {
                    UNIT_ASSERT_C(HasS3File(calcPath(path, "metadata.json")), calcPath(path, "metadata.json"));
                    UNIT_ASSERT_C(HasS3File(calcPath(path, "scheme.pb")), calcPath(path, "scheme.pb"));
                }
            }
        }

        bool HasS3File(const TString& path) {
            auto it = S3Mock().GetData().find(path);
            return it != S3Mock().GetData().end();
        }

        template <class T>
        void CheckHasAllS3Files(std::initializer_list<T> paths) {
            for (const T& path : paths) {
                UNIT_ASSERT_C(HasS3File(path), "Path \"" << path << "\" is expected to exist in S3");
            }
        }

        template <class T>
        void CheckNoSuchS3Files(std::initializer_list<T> paths) {
            for (const T& path : paths) {
                UNIT_ASSERT_C(!HasS3File(path), "Path \"" << path << "\" is expected not to exist in S3");
            }
        }

        template <class T>
        void CheckNoS3Prefix(std::initializer_list<T> prefixes) {
            for (const T& prefix : prefixes) {
                for (auto&& [fileName, _] : S3Mock().GetData()) {
                    UNIT_ASSERT_C(!fileName.StartsWith(prefix), "S3 path \"" << fileName << "\" has prefix \"" << prefix << "\", which is not expected prefix");
                }
            }
        }

        TString GetS3FileContent(const TString& path) {
            auto it = S3Mock().GetData().find(path);
            if (it != S3Mock().GetData().end()) {
                return it->second;
            }
            return {};
        }

        void TearDown(NUnitTest::TTestContext&) override {
            if (S3ServerMock) {
                S3ServerMock = Nothing();
                S3ServerPort = 0;
            }
        }

        using TDelayFunc = std::function<bool(TAutoPtr<IEventHandle>&)>;

        void Cancel(const TVector<TString>& tables, const TString& request, TDelayFunc delayFunc) {
            std::vector<std::string> auditLines;
            Runtime().AuditLogBackends = std::move(CreateTestAuditLogBackends(auditLines));

            Env(); // Init test env
            ui64 txId = 100;

            for (const auto& table : tables) {
                TestCreateTable(Runtime(), ++txId, "/MyRoot", table);
                Env().TestWaitNotification(Runtime(), txId);
            }

            Runtime().SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
            Runtime().SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

            THolder<IEventHandle> delayed;
            auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
                if (delayFunc(ev)) {
                    delayed.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            });

            TestExport(Runtime(), ++txId, "/MyRoot", request);
            const ui64 exportId = txId;

            // Check audit record for export start
            {
                auto line = FindAuditLine(auditLines, "operation=EXPORT START");
                UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
                UNIT_ASSERT_STRING_CONTAINS(line, "operation=EXPORT START");
                UNIT_ASSERT_STRING_CONTAINS(line, Sprintf("id=%lu", exportId));
                UNIT_ASSERT_STRING_CONTAINS(line, "remote_address=");  // can't check the value
                UNIT_ASSERT_STRING_CONTAINS(line, "subject={none}");
                UNIT_ASSERT_STRING_CONTAINS(line, "database=/MyRoot");
                UNIT_ASSERT_STRING_CONTAINS(line, "status=SUCCESS");
                UNIT_ASSERT_STRING_CONTAINS(line, "detailed_status=SUCCESS");
                UNIT_ASSERT(!line.contains("reason"));
                UNIT_ASSERT(!line.contains("start_time"));
                UNIT_ASSERT(!line.contains("end_time"));
            }

            if (!delayed) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                    return bool(delayed);
                });
                Runtime().DispatchEvents(opts);
            }

            Runtime().SetObserverFunc(prevObserver);

            TestCancelExport(Runtime(), ++txId, "/MyRoot", exportId);
            Runtime().Send(delayed.Release(), 0, true);
            Env().TestWaitNotification(Runtime(), exportId);

            // Check audit record for export end
            //
            {
                auto line = FindAuditLine(auditLines, "operation=EXPORT END");
                UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
                UNIT_ASSERT_STRING_CONTAINS(line, "operation=EXPORT END");
                UNIT_ASSERT_STRING_CONTAINS(line, Sprintf("id=%lu", exportId));
                UNIT_ASSERT_STRING_CONTAINS(line, "remote_address=");  // can't check the value
                UNIT_ASSERT_STRING_CONTAINS(line, "subject={none}");
                UNIT_ASSERT_STRING_CONTAINS(line, "database=/MyRoot");
                UNIT_ASSERT_STRING_CONTAINS(line, "status=ERROR");
                UNIT_ASSERT_STRING_CONTAINS(line, "detailed_status=CANCELLED");
                UNIT_ASSERT_STRING_CONTAINS(line, "reason=Cancelled");
                UNIT_ASSERT_STRING_CONTAINS(line, "start_time=");
                UNIT_ASSERT_STRING_CONTAINS(line, "end_time=");
            }

            TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);

            TestForgetExport(Runtime(), ++txId, "/MyRoot", exportId);
            Env().TestWaitNotification(Runtime(), exportId);

            TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        }

        void CancelUponTransferringShouldSucceed(const TVector<TString>& tables, const TString& request) {
            Cancel(tables, Sprintf(request.c_str(), S3Port()), [](TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                    return false;
                }

                return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                    .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpBackup;
            });
        }

        void CancelShouldSucceed(TDelayFunc delayFunc) {
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
            )", S3Port()), delayFunc);
        }

        void DropCopiesBeforeTransferring(ui32 tablesCount) {
            Env(); // Init test env
            ui64 txId = 100;

            for (ui32 i = 1; i <= tablesCount; ++i) {
                TestCreateTable(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
                    Name: "Table%d"
                    Columns { Name: "key" Type: "Utf8" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )", i));
                Env().TestWaitNotification(Runtime(), txId);
            }

            Runtime().SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
            Runtime().SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

            bool dropNotification = false;
            THolder<IEventHandle> delayed;
            auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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

            TStringBuilder items;
            for (ui32 i = 1; i <= tablesCount; ++i) {
                items << "items {"
                    << " source_path: \"/MyRoot/Table" << i << "\""
                    << " destination_prefix: \"\""
                << " }";
            }

            TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
                ExportToS3Settings {
                  endpoint: "localhost:%d"
                  scheme: HTTP
                  %s
                }
            )", S3Port(), items.c_str()));
            const ui64 exportId = txId;

            if (!delayed) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                    return bool(delayed);
                });
                Runtime().DispatchEvents(opts);
            }

            Runtime().SetObserverFunc(prevObserver);

            for (ui32 i = 0; i < tablesCount; ++i) {
                TestDropTable(Runtime(), ++txId, Sprintf("/MyRoot/export-%" PRIu64, exportId), ToString(i));
                Env().TestWaitNotification(Runtime(), txId);
            }

            Runtime().Send(delayed.Release(), 0, true);
            Env().TestWaitNotification(Runtime(), exportId);

            TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);

            TestForgetExport(Runtime(), ++txId, "/MyRoot", exportId);
            Env().TestWaitNotification(Runtime(), exportId);

            TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        }

        void RebootDuringFinish(bool rejectUploadParts, Ydb::StatusIds::StatusCode expectedStatus) {
            S3Settings().WithRejectUploadParts(rejectUploadParts);

            Env(); // Init test env
            ui64 txId = 100;

            TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )");
            Env().TestWaitNotification(Runtime(), txId);

            UpdateRow(Runtime(), "Table", 1, "valueA");
            UpdateRow(Runtime(), "Table", 2, "valueB");

            Runtime().SetLogPriority(NKikimrServices::S3_WRAPPER, NActors::NLog::PRI_TRACE);
            Runtime().SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
            Runtime().SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

            TMaybe<ui64> backupTxId;
            TMaybe<ui64> tabletId;
            bool delayed = false;

            auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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
                            schemeTx.MutableBackup()->MutableS3Settings()->MutableLimits()->SetMinWriteBatchSize(1);
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

            TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
                ExportToS3Settings {
                  endpoint: "localhost:%d"
                  scheme: HTTP
                  items {
                    source_path: "/MyRoot/Table"
                    destination_prefix: ""
                  }
                }
            )", S3Port()));
            const ui64 exportId = txId;

            if (!delayed || !tabletId) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&delayed, &tabletId](IEventHandle&) -> bool {
                    return delayed && tabletId;
                });
                Runtime().DispatchEvents(opts);
            }

            Runtime().SetObserverFunc(prevObserver);

            RebootTablet(Runtime(), *tabletId, Runtime().AllocateEdgeActor());
            Env().TestWaitNotification(Runtime(), exportId);

            TestGetExport(Runtime(), exportId, "/MyRoot", expectedStatus);

            TestForgetExport(Runtime(), ++txId, "/MyRoot", exportId);
            Env().TestWaitNotification(Runtime(), exportId);

            TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        }

        void ShouldCheckQuotas(const TSchemeLimits& limits, Ydb::StatusIds::StatusCode expectedFailStatus) {
            const TString userSID = "user@builtin";
            EnvOptions().SystemBackupSIDs({userSID}).EnableRealSystemViewPaths(false);
            Env(); // Init test env

            SetSchemeshardSchemaLimits(Runtime(), limits);

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
            )", S3Port());

            Run(Runtime(), Env(), tables, request, expectedFailStatus);
            Run(Runtime(), Env(), tables, request, Ydb::StatusIds::SUCCESS, "/MyRoot", false, userSID);
        }

        void TestTopic(bool enablePermissions = false) {
          EnvOptions().EnablePermissionsExport(enablePermissions);
          Env();
          ui64 txId = 100;

          TestCreatePQGroup(Runtime(), ++txId, "/MyRoot", R"(
              Name: "Topic"
              TotalGroupCount: 2
              PartitionPerTablet: 1
              PQTabletConfig {
                  PartitionConfig {
                      LifetimeSeconds: 10
                  }
              }
          )");
          Env().TestWaitNotification(Runtime(), txId);

          auto request = Sprintf(R"(
              ExportToS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                  source_path: "/MyRoot/Topic"
                  destination_prefix: ""
                }
              }
          )", S3Port());

          auto schemeshardId = TTestTxConfig::SchemeShard;
          TestExport(Runtime(), schemeshardId, ++txId, "/MyRoot", request, "", "", Ydb::StatusIds::SUCCESS);
          Env().TestWaitNotification(Runtime(), txId, schemeshardId);

          TestGetExport(Runtime(), schemeshardId, txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
          UNIT_ASSERT(HasS3File("/create_topic.pb"));
          UNIT_ASSERT_STRINGS_EQUAL(GetS3FileContent("/create_topic.pb"), R"(partitioning_settings {
  min_active_partitions: 2
  max_active_partitions: 1
  auto_partitioning_settings {
    strategy: AUTO_PARTITIONING_STRATEGY_DISABLED
    partition_write_speed {
      stabilization_window {
        seconds: 300
      }
      up_utilization_percent: 80
      down_utilization_percent: 20
    }
  }
}
retention_period {
  seconds: 10
}
supported_codecs {
}
partition_write_speed_bytes_per_second: 50000000
partition_write_burst_bytes: 50000000
)");

          if (enablePermissions) {
            UNIT_ASSERT(HasS3File("/permissions.pb"));
            UNIT_ASSERT_STRINGS_EQUAL(GetS3FileContent("/permissions.pb"), R"(actions {
  change_owner: "root@builtin"
}
)");
          }

        };

    protected:
        TS3Mock::TSettings& S3Settings() {
            if (!S3ServerSettings) {
                S3ServerPort = PortManager.GetPort();
                S3ServerSettings.ConstructInPlace(S3ServerPort);
            }
            return *S3ServerSettings;
        }

        TS3Mock& S3Mock() {
            if (!S3ServerMock) {
                S3ServerMock.ConstructInPlace(S3Settings());
                UNIT_ASSERT(S3ServerMock->Start());
            }
            return *S3ServerMock;
        }

        ui16 S3Port() {
            S3Mock();
            return S3ServerPort;
        }

        TTestBasicRuntime& Runtime() {
            if (!TestRuntime) {
                TestRuntime.ConstructInPlace();
            }
            return *TestRuntime;
        }

        TTestEnvOptions& EnvOptions() {
            if (!TestEnvOptions) {
                TestEnvOptions.ConstructInPlace();
            }
            return *TestEnvOptions;
        }

        TTestEnv& Env() {
            if (!TestEnv) {
                TestEnv.ConstructInPlace(Runtime(), EnvOptions());
            }
            return *TestEnv;
        }

    private:
        TPortManager PortManager;
        ui16 S3ServerPort = 0;
        TMaybe<TTestBasicRuntime> TestRuntime;
        TMaybe<TS3Mock::TSettings> S3ServerSettings;
        TMaybe<TS3Mock> S3ServerMock;
        TMaybe<TTestEnvOptions> TestEnvOptions;
        TMaybe<TTestEnv> TestEnv;
    };

} // anonymous

Y_UNIT_TEST_SUITE_F(TExportToS3Tests, TExportFixture) {
    Y_UNIT_TEST(ShouldSucceedOnSingleShardTable) {
        RunS3({
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
        RunS3({
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
        RunS3({
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

        Run(Runtime(), Env(), tables, Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        auto schemeIt = S3Mock().GetData().find("/scheme.pb");
        UNIT_ASSERT(schemeIt != S3Mock().GetData().end());

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

        Run(Runtime(), Env(), tables, Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        auto schemeIt = S3Mock().GetData().find("/scheme.pb");
        UNIT_ASSERT(schemeIt != S3Mock().GetData().end());

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
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        Runtime().SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        Runtime().SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);

        bool dropNotification = false;
        THolder<IEventHandle> delayed;
        auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));
        const ui64 exportId = txId;

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            Runtime().DispatchEvents(opts);
        }

        Runtime().SetObserverFunc(prevObserver);

        TestDropTable(Runtime(), ++txId, "/MyRoot", "Table");
        Env().TestWaitNotification(Runtime(), txId);

        Runtime().Send(delayed.Release(), 0, true);
        Env().TestWaitNotification(Runtime(), exportId);

        TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);

        TestForgetExport(Runtime(), ++txId, "/MyRoot", exportId);
        Env().TestWaitNotification(Runtime(), exportId);

        TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(DropCopiesBeforeTransferring1) {
        DropCopiesBeforeTransferring(1);
    }

    Y_UNIT_TEST(DropCopiesBeforeTransferring2) {
        DropCopiesBeforeTransferring(2);
    }

    Y_UNIT_TEST(RebootDuringCompletion) {
        RebootDuringFinish(false, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(RebootDuringAbortion) {
        RebootDuringFinish(true, Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldExcludeBackupTableFromStats) {
        EnvOptions().DisableStatsBatching(true);
        Env(); // Init test env
        ui64 txId = 100;

        THashSet<ui64> statsCollected;
        Runtime().GetAppData().FeatureFlags.SetEnableExportAutoDropping(true);
        Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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
            Runtime().DispatchEvents(opts);

            return DescribePath(Runtime(), "/MyRoot")
                .GetPathDescription()
                .GetDomainDescription()
                .GetDiskSpaceUsage();
        };

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        for (int i = 1; i < 500; ++i) {
            UpdateRow(Runtime(), "Table", i, "value");
        }

        // trigger memtable's compaction
        TestCopyTable(Runtime(), ++txId, "/MyRoot", "CopyTable", "/MyRoot/Table");
        Env().TestWaitNotification(Runtime(), txId);
        TestDropTable(Runtime(), ++txId, "/MyRoot", "Table");
        Env().TestWaitNotification(Runtime(), txId);

        const auto expected = waitForStats(1);

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/CopyTable"
                destination_prefix: ""
              }
            }
        )", S3Port()));
        const ui64 exportId = txId;
        ::NKikimrSubDomains::TDiskSpaceUsage afterExport;

        TTestActorRuntime::TEventObserver prevObserverFunc;
        prevObserverFunc = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (auto* p = event->CastAsLocal<TEvSchemeShard::TEvModifySchemeTransaction>()) {
                auto& record = p->Record;
                if (record.TransactionSize() >= 1 &&
                    record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpDropTable) {
                    afterExport = waitForStats(2);
                }
            }
            return prevObserverFunc(event);
        });

        Env().TestWaitNotification(Runtime(), exportId);

        TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT_STRINGS_EQUAL(expected.DebugString(), afterExport.DebugString());

        TestForgetExport(Runtime(), ++txId, "/MyRoot", exportId);
        Env().TestWaitNotification(Runtime(), exportId);

        TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        const auto afterForget = waitForStats(1);
        UNIT_ASSERT_STRINGS_EQUAL(expected.DebugString(), afterForget.DebugString());
    }

    Y_UNIT_TEST(CheckItemProgress) {
        Env(); // Init test env
        ui64 txId = 100;
        Runtime().GetAppData().FeatureFlags.SetEnableExportAutoDropping(true);
        TBlockEvents<NKikimr::NWrappers::NExternalStorage::TEvPutObjectRequest> blockPartition01(Runtime(), [](auto&& ev) {
            return ev->Get()->Request.GetKey() == "/data_01.csv";
        });

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 10 } }}}
        )");
        Env().TestWaitNotification(Runtime(), txId);

        WriteRow(Runtime(), ++txId, "/MyRoot/Table", 0, 1, "v1");
        Env().TestWaitNotification(Runtime(), txId);
        WriteRow(Runtime(), ++txId, "/MyRoot/Table", 1, 100, "v100");
        Env().TestWaitNotification(Runtime(), txId);

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        Runtime().WaitFor("put object request from 01 partition", [&]{ return blockPartition01.size() >= 1; });
        bool isCompleted = false;

        while (!isCompleted) {
            const auto desc = TestGetExport(Runtime(), txId, "/MyRoot");
            const auto entry = desc.GetResponse().GetEntry();
            const auto& item = entry.GetItemsProgress(0);

            if (item.parts_completed() > 0) {
                isCompleted = true;
                UNIT_ASSERT_VALUES_EQUAL(item.parts_total(), 2);
                UNIT_ASSERT_VALUES_EQUAL(item.parts_completed(), 1);
                UNIT_ASSERT(item.has_start_time());
            } else {
                Runtime().SimulateSleep(TDuration::Seconds(1));
            }
        }

        blockPartition01.Stop();
        blockPartition01.Unblock();

        Env().TestWaitNotification(Runtime(), txId);

        const auto desc = TestGetExport(Runtime(), txId, "/MyRoot");
        const auto entry = desc.GetResponse().GetEntry();

        UNIT_ASSERT_VALUES_EQUAL(entry.ItemsProgressSize(), 1);
    }

    Y_UNIT_TEST(ShouldRestartOnScanErrors) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        UpdateRow(Runtime(), "Table", 1, "valueA");

        THolder<IEventHandle> injectResult;
        auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NSharedCache::EvResult) {
                const auto* msg = ev->Get<NSharedCache::TEvResult>();
                UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);

                auto result = MakeHolder<NSharedCache::TEvResult>(msg->PageCollection, msg->Cookie, NKikimrProto::ERROR);
                std::move(msg->Pages.begin(), msg->Pages.end(), std::back_inserter(result->Pages));

                injectResult = MakeHolder<IEventHandle>(ev->Recipient, ev->Sender, result.Release(), ev->Flags, ev->Cookie);
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        if (!injectResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&injectResult](IEventHandle&) -> bool {
                return bool(injectResult);
            });
            Runtime().DispatchEvents(opts);
        }

        Runtime().SetObserverFunc(prevObserver);
        Runtime().Send(injectResult.Release(), 0, true);

        Env().TestWaitNotification(Runtime(), txId);
        TestGetExport(Runtime(), txId, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldSucceedOnConcurrentTxs) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        THolder<IEventHandle> copyTables;
        auto origObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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
        TestExport(Runtime(), exportId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        if (!copyTables) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&copyTables](IEventHandle&) -> bool {
                return bool(copyTables);
            });
            Runtime().DispatchEvents(opts);
        }

        THolder<IEventHandle> proposeTxResult;
        Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvProposeTransactionResult) {
                proposeTxResult.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestAlterTable(Runtime(), ++txId, "/MyRoot", R"(
              Name: "Table"
              Columns { Name: "extra"  Type: "Utf8"}
        )");

        if (!proposeTxResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&proposeTxResult](IEventHandle&) -> bool {
                return bool(proposeTxResult);
            });
            Runtime().DispatchEvents(opts);
        }

        Runtime().SetObserverFunc(origObserver);
        Runtime().Send(copyTables.Release(), 0, true);
        Runtime().Send(proposeTxResult.Release(), 0, true);
        Env().TestWaitNotification(Runtime(), txId);

        Env().TestWaitNotification(Runtime(), exportId);
        TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ShouldSucceedOnConcurrentExport) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        TVector<THolder<IEventHandle>> copyTables;
        auto origObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvSchemeShard::EvModifySchemeTransaction) {
                const auto& record = ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record;
                if (record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables) {
                    copyTables.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto waitCopyTables = [this, &copyTables](ui32 size) {
            if (copyTables.size() != size) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&copyTables, size](IEventHandle&) -> bool {
                    return copyTables.size() == size;
                });
                Runtime().DispatchEvents(opts);
            }
        };

        TVector<ui64> exportIds;
        for (ui32 i = 1; i <= 3; ++i) {
            exportIds.push_back(++txId);
            TestExport(Runtime(), exportIds[i - 1], "/MyRoot", Sprintf(R"(
                ExportToS3Settings {
                  endpoint: "localhost:%d"
                  scheme: HTTP
                  items {
                    source_path: "/MyRoot/Table"
                    destination_prefix: "Table%u"
                  }
                }
            )", S3Port(), i));
            waitCopyTables(i);
        }

        Runtime().SetObserverFunc(origObserver);
        for (auto& ev : copyTables) {
            Runtime().Send(ev.Release(), 0, true);
        }

        for (ui64 exportId : exportIds) {
            Env().TestWaitNotification(Runtime(), exportId);
            TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(ShouldSucceedOnConcurrentImport) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        // prepare backup data
        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: "Backup1"
              }
            }
        )", S3Port()));
        Env().TestWaitNotification(Runtime(), txId);
        TestGetExport(Runtime(), txId, "/MyRoot");

        TVector<THolder<IEventHandle>> delayed;
        auto origObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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

        auto waitForDelayed = [this, &delayed](ui32 size) {
            if (delayed.size() != size) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&delayed, size](IEventHandle&) -> bool {
                    return delayed.size() == size;
                });
                Runtime().DispatchEvents(opts);
            }
        };

        const auto importId = ++txId;
        TestImport(Runtime(), importId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "Backup1"
                destination_path: "/MyRoot/Restored"
              }
            }
        )", S3Port()));
        // wait for restore op
        waitForDelayed(1);

        const auto exportId = ++txId;
        TestExport(Runtime(), exportId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Restored"
                destination_prefix: "Backup2"
              }
            }
        )", S3Port()));
        // wait for copy table op
        waitForDelayed(2);

        Runtime().SetObserverFunc(origObserver);
        for (auto& ev : delayed) {
            Runtime().Send(ev.Release(), 0, true);
        }

        Env().TestWaitNotification(Runtime(), importId);
        TestGetImport(Runtime(), importId, "/MyRoot");
        Env().TestWaitNotification(Runtime(), exportId);
        TestGetExport(Runtime(), exportId, "/MyRoot");
    }

    Y_UNIT_TEST(ShouldCheckQuotasExportsLimited) {
        ShouldCheckQuotas(TSchemeLimits{.MaxExports = 0}, Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(ShouldCheckQuotasChildrenLimited) {
        ShouldCheckQuotas(TSchemeLimits{.MaxChildrenInDir = 1}, Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldRetryAtFinalStage) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        UpdateRow(Runtime(), "Table", 1, "valueA");
        UpdateRow(Runtime(), "Table", 2, "valueB");
        Runtime().SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);

        THolder<IEventHandle> injectResult;
        auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
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
                        schemeTx.MutableBackup()->MutableS3Settings()->MutableLimits()->SetMinWriteBatchSize(1);
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
        TestExport(Runtime(), txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              number_of_retries: 10
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        if (!injectResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&injectResult](IEventHandle&) -> bool {
                return bool(injectResult);
            });
            Runtime().DispatchEvents(opts);
        }

        Runtime().SetObserverFunc(prevObserver);
        Runtime().Send(injectResult.Release(), 0, true);

        Env().TestWaitNotification(Runtime(), exportId);
        TestGetExport(Runtime(), exportId, "/MyRoot");
    }

    Y_UNIT_TEST(CorruptedDyNumber) {
        EnvOptions().DisableStatsBatching(true);
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "DyNumber" }
                KeyColumnNames: ["key"]
            )");
        Env().TestWaitNotification(Runtime(), txId);

        // Write bad DyNumber
        UploadRow(Runtime(), "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));
        Env().TestWaitNotification(Runtime(), txId);

        TestGetExport(Runtime(), txId, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(UidAsIdempotencyKey) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

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
        )", S3Port());

        // create operation
        TestExport(Runtime(), ++txId, "/MyRoot", request);
        const ui64 exportId = txId;
        // create operation again with same uid
        TestExport(Runtime(), ++txId, "/MyRoot", request);
        // new operation was not created
        TestGetExport(Runtime(), txId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        // check previous operation
        TestGetExport(Runtime(), exportId, "/MyRoot");
        Env().TestWaitNotification(Runtime(), exportId);
    }

    Y_UNIT_TEST(ExportStartTime) {
        Env(); // Init test env
        Runtime().UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        const auto desc = TestGetExport(Runtime(), txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_PREPARING);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(!entry.HasEndTime());
    }

    Y_UNIT_TEST(CompletedExportEndTime) {
        Env(); // Init test env
        Runtime().UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        Runtime().AdvanceCurrentTime(TDuration::Seconds(30)); // doing export

        Env().TestWaitNotification(Runtime(), txId);

        const auto desc = TestGetExport(Runtime(), txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());
    }

    Y_UNIT_TEST(CancelledExportEndTime) {
        Env(); // Init test env
        Runtime().UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        auto delayFunc = [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpBackup;
        };

        THolder<IEventHandle> delayed;
        auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (delayFunc(ev)) {
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));
        const ui64 exportId = txId;

        Runtime().AdvanceCurrentTime(TDuration::Seconds(30)); // doing export

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            Runtime().DispatchEvents(opts);
        }
        Runtime().SetObserverFunc(prevObserver);

        TestCancelExport(Runtime(), ++txId, "/MyRoot", exportId);

        auto desc = TestGetExport(Runtime(), exportId, "/MyRoot");
        auto entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(!entry.HasEndTime());

        Runtime().Send(delayed.Release(), 0, true);
        Env().TestWaitNotification(Runtime(), exportId);

        desc = TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLED);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());
    }

    // Based on CompletedExportEndTime
    Y_UNIT_TEST(AuditCompletedExport) {
        std::vector<std::string> auditLines;
        Runtime().AuditLogBackends = std::move(CreateTestAuditLogBackends(auditLines));
        Env(); // Init test env
        Runtime().UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        // Prepare table to export
        //
        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        // Start export
        //
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
        )", S3Port());
        TestExport(Runtime(), ++txId, "/MyRoot", request, /*userSID*/ "user@builtin", /*peerName*/ "127.0.0.1:9876");

        // Check audit record for export start
        {
            auto line = FindAuditLine(auditLines, "operation=EXPORT START");
            UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
            UNIT_ASSERT_STRING_CONTAINS(line, "operation=EXPORT START");
            UNIT_ASSERT_STRING_CONTAINS(line, Sprintf("id=%lu", txId));
            UNIT_ASSERT_STRING_CONTAINS(line, "uid=foo");
            UNIT_ASSERT_STRING_CONTAINS(line, "remote_address=127.0.0.1");
            UNIT_ASSERT_STRING_CONTAINS(line, "subject=user@builtin");
            UNIT_ASSERT_STRING_CONTAINS(line, "database=/MyRoot");
            UNIT_ASSERT_STRING_CONTAINS(line, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(line, "detailed_status=SUCCESS");
            UNIT_ASSERT(!line.contains("reason"));
            UNIT_ASSERT(!line.contains("start_time"));
            UNIT_ASSERT(!line.contains("end_time"));
        }

        // Do export
        //
        Runtime().AdvanceCurrentTime(TDuration::Seconds(30));

        Env().TestWaitNotification(Runtime(), txId);

        const auto desc = TestGetExport(Runtime(), txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_DONE);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());

        // Check audit record for export end
        //
        {
            auto line = FindAuditLine(auditLines, "operation=EXPORT END");
            UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
            UNIT_ASSERT_STRING_CONTAINS(line, "operation=EXPORT END");
            UNIT_ASSERT_STRING_CONTAINS(line, Sprintf("id=%lu", txId));
            UNIT_ASSERT_STRING_CONTAINS(line, "remote_address=127.0.0.1");
            UNIT_ASSERT_STRING_CONTAINS(line, "subject=user@builtin");
            UNIT_ASSERT_STRING_CONTAINS(line, "database=/MyRoot");
            UNIT_ASSERT_STRING_CONTAINS(line, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(line, "detailed_status=SUCCESS");
            UNIT_ASSERT(!line.contains("reason"));
            UNIT_ASSERT_STRING_CONTAINS(line, "start_time=");
            UNIT_ASSERT_STRING_CONTAINS(line, "end_time=");
        }
    }

    Y_UNIT_TEST(AuditCancelledExport) {
        std::vector<std::string> auditLines;
        Runtime().AuditLogBackends = std::move(CreateTestAuditLogBackends(auditLines));
        Env(); // Init test env
        Runtime().UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        // Prepare table to export
        //
        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        auto delayFunc = [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpBackup;
        };

        THolder<IEventHandle> delayed;
        auto prevObserver = Runtime().SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (delayFunc(ev)) {
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Start export
        //
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
        )", S3Port());
        TestExport(Runtime(), ++txId, "/MyRoot", request, /*userSID*/ "user@builtin", /*peerName*/ "127.0.0.1:9876");
        const ui64 exportId = txId;

        // Check audit record for export start
        {
            auto line = FindAuditLine(auditLines, "operation=EXPORT START");
            UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
            UNIT_ASSERT_STRING_CONTAINS(line, "operation=EXPORT START");
            UNIT_ASSERT_STRING_CONTAINS(line, Sprintf("id=%lu", exportId));
            UNIT_ASSERT_STRING_CONTAINS(line, "uid=foo");
            UNIT_ASSERT_STRING_CONTAINS(line, "remote_address=127.0.0.1");
            UNIT_ASSERT_STRING_CONTAINS(line, "subject=user@builtin");
            UNIT_ASSERT_STRING_CONTAINS(line, "database=/MyRoot");
            UNIT_ASSERT_STRING_CONTAINS(line, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(line, "detailed_status=SUCCESS");
            UNIT_ASSERT(!line.contains("reason"));
            UNIT_ASSERT(!line.contains("start_time"));
            UNIT_ASSERT(!line.contains("end_time"));
        }

        // Do export (unsuccessfully)
        //
        Runtime().AdvanceCurrentTime(TDuration::Seconds(30));

        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            Runtime().DispatchEvents(opts);
        }
        Runtime().SetObserverFunc(prevObserver);

        // Cancel export mid-air
        //
        TestCancelExport(Runtime(), ++txId, "/MyRoot", exportId);

        auto desc = TestGetExport(Runtime(), exportId, "/MyRoot");
        auto entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(!entry.HasEndTime());

        Runtime().Send(delayed.Release(), 0, true);
        Env().TestWaitNotification(Runtime(), exportId);

        desc = TestGetExport(Runtime(), exportId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_CANCELLED);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());

        // Check audit record for export end
        //
        {
            auto line = FindAuditLine(auditLines, "operation=EXPORT END");
            UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
            UNIT_ASSERT_STRING_CONTAINS(line, "operation=EXPORT END");
            UNIT_ASSERT_STRING_CONTAINS(line, Sprintf("id=%lu", exportId));
            UNIT_ASSERT_STRING_CONTAINS(line, "uid=foo");
            UNIT_ASSERT_STRING_CONTAINS(line, "remote_address=127.0.0.1");  // can't check the value
            UNIT_ASSERT_STRING_CONTAINS(line, "subject=user@builtin");
            UNIT_ASSERT_STRING_CONTAINS(line, "database=/MyRoot");
            UNIT_ASSERT_STRING_CONTAINS(line, "status=ERROR");
            UNIT_ASSERT_STRING_CONTAINS(line, "detailed_status=CANCELLED");
            UNIT_ASSERT_STRING_CONTAINS(line, "reason=Cancelled");
            UNIT_ASSERT_STRING_CONTAINS(line, "start_time=");
            UNIT_ASSERT_STRING_CONTAINS(line, "end_time=");
        }
    }

    Y_UNIT_TEST(ExportPartitioningSettings) {
        Run(Runtime(), Env(), TVector<TString>{
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
            Sprintf(R"(
                ExportToS3Settings {
                  endpoint: "localhost:%d"
                  scheme: HTTP
                  items {
                    source_path: "/MyRoot/Table"
                    destination_prefix: ""
                  }
                }
            )", S3Port())
        );

        auto* scheme = S3Mock().GetData().FindPtr("/scheme.pb");
        UNIT_ASSERT(scheme);
        CheckTableScheme(*scheme, GetPartitioningSettings, CreateProtoComparator(R"(
            partitioning_by_size: DISABLED
            partitioning_by_load: ENABLED
            min_partitions_count: 10
        )"));
    }

    Y_UNIT_TEST(ExportIndexTablePartitioningSettings) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateIndexedTable(Runtime(), ++txId, "/MyRoot", R"(
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
        Env().TestWaitNotification(Runtime(), txId);

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));
        Env().TestWaitNotification(Runtime(), txId);

        auto* scheme = S3Mock().GetData().FindPtr("/scheme.pb");
        UNIT_ASSERT(scheme);
        CheckTableScheme(*scheme, GetIndexTablePartitioningSettings, CreateProtoComparator(R"(
            partitioning_by_size: DISABLED
            partitioning_by_load: ENABLED
            min_partitions_count: 10
        )"));
    }

    Y_UNIT_TEST(UserSID) {
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        const TString request = Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port());
        const TString userSID = "user@builtin";
        TestExport(Runtime(), ++txId, "/MyRoot", request, userSID);

        const auto desc = TestGetExport(Runtime(), txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Export::ExportProgress::PROGRESS_PREPARING);
        UNIT_ASSERT_VALUES_EQUAL(entry.GetUserSID(), userSID);
    }

    Y_UNIT_TEST(TablePermissions) {
        EnvOptions().EnablePermissionsExport(true);
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        NACLib::TDiffACL diffACL;
        diffACL.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user@builtin", NACLib::InheritNone);
        TestModifyACL(Runtime(), ++txId, "/MyRoot", "Table", diffACL.SerializeAsString(), "user@builtin");
        Env().TestWaitNotification(Runtime(), txId);

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));
        Env().TestWaitNotification(Runtime(), txId);

        auto* permissions = S3Mock().GetData().FindPtr("/permissions.pb");
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

    Y_UNIT_TEST(Checksums) {
        EnvOptions().EnablePermissionsExport(true).EnableChecksumsExport(true);
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        UploadRow(Runtime(), "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));
        Env().TestWaitNotification(Runtime(), txId);

        UNIT_ASSERT_VALUES_EQUAL(S3Mock().GetData().size(), 8);
        const auto* dataChecksum = S3Mock().GetData().FindPtr("/data_00.csv.sha256");
        UNIT_ASSERT(dataChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*dataChecksum, "19dcd641390a61063ee45f3e6e06b8f0d3acfc33f934b9bf1ba204668a98f21d data_00.csv");

        const auto* metadataChecksum = S3Mock().GetData().FindPtr("/metadata.json.sha256");
        UNIT_ASSERT(metadataChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*metadataChecksum, "29c79eb8109b4142731fc894869185d6c0e99c4b2f605ea3fc726b0328b8e316 metadata.json");

        const auto* schemeChecksum = S3Mock().GetData().FindPtr("/scheme.pb.sha256");
        UNIT_ASSERT(schemeChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*schemeChecksum, "cb1fb80965ae92e6369acda2b3b5921fd5518c97d6437f467ce00492907f9eb6 scheme.pb");

        const auto* permissionsChecksum = S3Mock().GetData().FindPtr("/permissions.pb.sha256");
        UNIT_ASSERT(permissionsChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*permissionsChecksum, "b41fd8921ff3a7314d9c702dc0e71aace6af8443e0102add0432895c5e50a326 permissions.pb");
    }

    Y_UNIT_TEST(EnableChecksumsPersistance) {
        EnvOptions().EnableChecksumsExport(true);
        Env(); // Init test env
        ui64 txId = 100;

        // Create test table
        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        // Add some test data
        UploadRow(Runtime(), "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});

        // Block sending backup task to datashards
        TBlockEvents<TEvDataShard::TEvProposeTransaction> block(Runtime(), [](auto& ev) {
            NKikimrTxDataShard::TFlatSchemeTransaction schemeTx;
            UNIT_ASSERT(schemeTx.ParseFromString(ev.Get()->Get()->GetTxBody()));
            return schemeTx.HasBackup();
        });

        // Start export and expect it to be blocked
        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port()));

        Runtime().WaitFor("backup task is sent to datashards", [&]{ return block.size() >= 1; });

        // Stop blocking new events
        block.Stop();

        // Reboot SchemeShard to resend backup task
        RebootTablet(Runtime(), TTestTxConfig::SchemeShard, Runtime().AllocateEdgeActor());

        // Wait for export to complete
        Env().TestWaitNotification(Runtime(), txId);

        // Verify checksums are created
        UNIT_ASSERT_VALUES_EQUAL(S3Mock().GetData().size(), 6);

        const auto* dataChecksum = S3Mock().GetData().FindPtr("/data_00.csv.sha256");
        UNIT_ASSERT(dataChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*dataChecksum, "19dcd641390a61063ee45f3e6e06b8f0d3acfc33f934b9bf1ba204668a98f21d data_00.csv");

        const auto* metadataChecksum = S3Mock().GetData().FindPtr("/metadata.json.sha256");
        UNIT_ASSERT(metadataChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*metadataChecksum, "fbb85825fb12c5f38661864db884ba3fd1512fc4b0a2a41960d7d62d19318ab6 metadata.json");

        const auto* schemeChecksum = S3Mock().GetData().FindPtr("/scheme.pb.sha256");
        UNIT_ASSERT(schemeChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*schemeChecksum, "cb1fb80965ae92e6369acda2b3b5921fd5518c97d6437f467ce00492907f9eb6 scheme.pb");
    }

    Y_UNIT_TEST(ChecksumsWithCompression) {
        EnvOptions().EnableChecksumsExport(true);
        Env(); // Init test env
        ui64 txId = 100;

        TestCreateTable(Runtime(), ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(Runtime(), txId);

        UploadRow(Runtime(), "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(1u)}, {TCell::Make(1u)});

        TestExport(Runtime(), ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
              compression: "zstd"
            }
        )", S3Port()));
        Env().TestWaitNotification(Runtime(), txId);

        const auto* dataChecksum = S3Mock().GetData().FindPtr("/data_00.csv.sha256");
        UNIT_ASSERT(dataChecksum);
        UNIT_ASSERT_VALUES_EQUAL(*dataChecksum, "19dcd641390a61063ee45f3e6e06b8f0d3acfc33f934b9bf1ba204668a98f21d data_00.csv");
    }

    class ChangefeedGenerator {
    public:
        ChangefeedGenerator(const ui64 count, const TS3Mock& s3Mock)
            : Count(count)
            , S3Mock(s3Mock)
            , Changefeeds(GenChangefeeds())
        {}

        const TVector<TString>& GetChangefeeds() const {
            return Changefeeds;
        }

        void Check() {
            for (ui64 i = 1; i <= Count; ++i) {
                auto changefeedDir = "/" + GenChangefeedName(i);
                auto* changefeed = S3Mock.GetData().FindPtr(changefeedDir + "/changefeed_description.pb");
                UNIT_ASSERT_VALUES_EQUAL(*changefeed, Sprintf(R"(name: "update_feed%d"
mode: MODE_UPDATES
format: FORMAT_JSON
state: STATE_ENABLED
)", i));

                auto* topic = S3Mock.GetData().FindPtr(changefeedDir + "/topic_description.pb");
                UNIT_ASSERT(topic);
                UNIT_ASSERT_VALUES_EQUAL(*topic, Sprintf(R"(partitioning_settings {
  min_active_partitions: 1
  max_active_partitions: 1
  auto_partitioning_settings {
    strategy: AUTO_PARTITIONING_STRATEGY_DISABLED
    partition_write_speed {
      stabilization_window {
        seconds: 300
      }
      up_utilization_percent: 80
      down_utilization_percent: 20
    }
  }
}
partitions {
  active: true
}
retention_period {
  seconds: 86400
}
partition_write_speed_bytes_per_second: 1048576
partition_write_burst_bytes: 1048576
attributes {
  key: "__max_partition_message_groups_seqno_stored"
  value: "6000000"
}
attributes {
  key: "_allow_unauthenticated_read"
  value: "true"
}
attributes {
  key: "_allow_unauthenticated_write"
  value: "true"
}
attributes {
  key: "_message_group_seqno_retention_period_ms"
  value: "1382400000"
}
)", i));

                const auto* changefeedChecksum = S3Mock.GetData().FindPtr(changefeedDir + "/changefeed_description.pb.sha256");
                UNIT_ASSERT(changefeedChecksum);

                const auto* topicChecksum = S3Mock.GetData().FindPtr(changefeedDir + "/topic_description.pb.sha256");
                UNIT_ASSERT(topicChecksum);
            }
        }

    private:
        static TString GenChangefeedName(const ui64 num) {
            return TStringBuilder() << "update_feed" << num;
        }

        TVector<TString> GenChangefeeds() {
            TVector<TString> result(Count);
            std::generate(result.begin(), result.end(), [n = 1]() mutable {
                    return Sprintf(
                        R"(
                            TableName: "Table"
                            StreamDescription {
                                Name: "%s"
                                Mode: ECdcStreamModeUpdate
                                Format: ECdcStreamFormatJson
                                State: ECdcStreamStateReady
                            }
                        )", GenChangefeedName(n++).data()
                    );
                }
            );
            return result;
        }

        const ui64 Count;
        const TS3Mock& S3Mock;
        const TVector<TString> Changefeeds;
    };

    Y_UNIT_TEST(Changefeeds) {
        ChangefeedGenerator gen(3, S3Mock());

        auto request = Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port());

        EnvOptions().EnableChecksumsExport(true);
        Env(); // Init test env
        Runtime().GetAppData().FeatureFlags.SetEnableChangefeedsExport(true);

        Run(Runtime(), Env(), TVector<TString>{
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, request, Ydb::StatusIds::SUCCESS, "/MyRoot", false, "", "", gen.GetChangefeeds());

        gen.Check();
    }

    Y_UNIT_TEST(SchemaMapping) {
        RunS3({
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
              destination_prefix: "my_export"
              items {
                source_path: "/MyRoot/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2_prefix"
              }
            }
        )");

        UNIT_ASSERT(HasS3File("/my_export/metadata.json"));
        UNIT_ASSERT(HasS3File("/my_export/SchemaMapping/metadata.json"));
        UNIT_ASSERT(HasS3File("/my_export/SchemaMapping/mapping.json"));
        UNIT_ASSERT(HasS3File("/my_export/Table1/scheme.pb"));
        UNIT_ASSERT(HasS3File("/my_export/table2_prefix/scheme.pb"));
        UNIT_ASSERT_STRINGS_EQUAL(GetS3FileContent("/my_export/metadata.json"), "{\"kind\":\"SimpleExportV0\"}");
    }

    Y_UNIT_TEST(SchemaMappingEncryption) {
        RunS3({
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
              destination_prefix: "my_export"
              items {
                source_path: "/MyRoot/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2_prefix"
              }
              encryption_settings {
                encryption_algorithm: "AES-128-GCM"
                symmetric_key {
                    key: "0123456789012345"
                }
              }
            }
        )");

        UNIT_ASSERT(HasS3File("/my_export/metadata.json"));
        UNIT_ASSERT(HasS3File("/my_export/SchemaMapping/metadata.json.enc"));
        UNIT_ASSERT(HasS3File("/my_export/SchemaMapping/mapping.json.enc"));
        UNIT_ASSERT(HasS3File("/my_export/001/scheme.pb.enc"));
        UNIT_ASSERT(HasS3File("/my_export/table2_prefix/scheme.pb.enc"));
    }

    Y_UNIT_TEST(SchemaMappingEncryptionIncorrectKey) {
        RunS3({
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
              destination_prefix: "my_export"
              items {
                source_path: "/MyRoot/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
                destination_prefix: "table2_prefix"
              }
              encryption_settings {
                encryption_algorithm: "AES-128-GCM"
                symmetric_key {
                    key: "123"
                }
              }
            }
        )", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(EncryptedExport) {
        RunS3({
            R"(
                Name: "Table1"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
            R"(
                Name: "Table2"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )",
        }, R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              destination_prefix: "my_export"
              items {
                source_path: "/MyRoot/Table1"
              }
              items {
                source_path: "/MyRoot/Table2"
              }
              encryption_settings {
                encryption_algorithm: "AES-128-GCM"
                symmetric_key {
                    key: "0123456789012345"
                }
              }
            }
        )");

        CheckHasAllS3Files({
            "/my_export/metadata.json",
            "/my_export/SchemaMapping/metadata.json.enc",
            "/my_export/SchemaMapping/mapping.json.enc",
            "/my_export/001/scheme.pb.enc",
            "/my_export/001/data_00.csv.enc",
            "/my_export/001/data_01.csv.enc",
            "/my_export/002/scheme.pb.enc",
            "/my_export/002/data_00.csv.enc",
            "/my_export/002/data_01.csv.enc",
        });

        THashSet<TString> ivs;
        for (auto [key, content] : S3Mock().GetData()) {
            if (key == "/my_export/metadata.json") {
                continue;
            }

            // All files except backup metadata must be encrypted
            UNIT_ASSERT_C(key.EndsWith(".enc"), key);

            // Check that we can decrypt content with our key (== it is really encrypted with our key)
            TBuffer decryptedData;
            NBackup::TEncryptionIV iv;
            UNIT_ASSERT_NO_EXCEPTION_C(std::tie(decryptedData, iv) = NBackup::TEncryptedFileDeserializer::DecryptFullFile(
                NBackup::TEncryptionKey("0123456789012345"),
                TBuffer(content.data(), content.size())
            ), key);

            // All ivs are unique
            UNIT_ASSERT_C(ivs.insert(iv.GetBinaryString()).second, key);
        }
    }

    Y_UNIT_TEST(AutoDropping) {
        auto request = Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port());

        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableExportAutoDropping(true);

        Run(Runtime(), Env(), TVector<TString>{
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, request, Ydb::StatusIds::SUCCESS, "/MyRoot", false, "", "", {}, true);
    }

    Y_UNIT_TEST(DisableAutoDropping) {
        auto request = Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: ""
              }
            }
        )", S3Port());

        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableExportAutoDropping(false);

        Run(Runtime(), Env(), TVector<TString>{
            R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )",
        }, request, Ydb::StatusIds::SUCCESS, "/MyRoot", false, "", "", {}, true);
    }

    Y_UNIT_TEST(Topics) {
      TestTopic();
    }

    Y_UNIT_TEST(TopicsWithPermissions) {
      TestTopic(true);
    }
}
