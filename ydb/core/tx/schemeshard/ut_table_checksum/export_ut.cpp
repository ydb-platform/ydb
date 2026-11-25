#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/backup/common/table_checksum_events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/export_scan.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/ut_helpers/backup_test_fixture.h>
#include <ydb/core/tx/schemeshard/ut_helpers/mon_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/table_checksum_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/library/testlib/helpers.h>
#include <yql/essentials/ast/yql_ast_escaping.h>
#include <util/stream/str.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

#include <atomic>
#include <memory>

#include <ydb/core/tx/tx_allocator_client/actor_client.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/http/http_proxy.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

class TTableChecksumExportFixture : public TBackupTestFixture {
public:
    TTestEnv& Env() {
        auto& env = TBackupTestFixture::Env();
        if (!ChecksumRequests) {
            ChecksumRequests.ConstructInPlace(
                Runtime().AddObserver<TEvDataShard::TEvStoreBackupChecksumRequest>([this](auto& ev) {
                    if (!ev->Get()->Outcome) {
                        ChecksumUploader = ev->Sender;
                        ChecksumTxId = ev->Get()->TxId;
                        Runtime().EnableScheduleForActor(ChecksumUploader);
                    }
                }));
        }
        return env;
    }

    void SendChecksumResult(
        Ydb::StatusIds::StatusCode status, NBackup::TTableChecksum checksum, const TString& error = {}) {
        UNIT_ASSERT(ChecksumUploader);
        Runtime().Send(
            new IEventHandle(ChecksumUploader, ChecksumUploader,
                new NBackup::TEvTableChecksum::TEvResult(ChecksumTxId, 0, status, checksum, error)), 0, true);
    }

    void RunTableChecksumMetadataRecovery(
        bool dataShard, bool withChangefeed = false, const NBackup::TTableChecksum& checksum = MakeTableChecksum()) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        ui32 changefeedUploads = 0;
        auto observer = Runtime().AddObserver<TEvPutObjectRequest>([&](auto& ev) {
            changefeedUploads += TString(ev->Get()->Request.GetKey()).EndsWith("stream/changefeed_description.pb");
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TBlockEvents<TEvPutObjectRequest> metadata(Runtime(), [](auto& ev) {
            return TString(ev->Get()->Request.GetKey()).EndsWith("backup/table/metadata.json");
        });
        const ui64 id = StartCompletionMarkerExport(true, false, withChangefeed);
        Runtime().WaitFor("checksum start", [&] {
            return !starts.empty();
        });

        SendChecksumResult(Ydb::StatusIds::SUCCESS, checksum, "");
        Runtime().WaitFor("final metadata after data", [&] {
            return !metadata.empty() && HasS3File("/backup/table/data_00.csv") &&
                   HasS3File("/backup/table/data_01.csv");
        });
        const TString expected = metadata.front()->Get()->Body;
        const auto saved = NBackup::TMetadata::Deserialize(expected, true);
        UNIT_ASSERT(saved.GetTableChecksum().has_value());
        UNIT_ASSERT(*saved.GetTableChecksum() == checksum);
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(false);
        if (withChangefeed) {
            UNIT_ASSERT_VALUES_EQUAL(changefeedUploads, 1);
        }
        if (dataShard) {
            const auto shards =
                GetTableShards(Runtime(), TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
            RebootTablet(Runtime(), shards.front(), Runtime().AllocateEdgeActor());
            Runtime().WaitFor("recovered metadata retains C without starting another checksum query", [&] {
                return metadata.size() == 2;
            });
            metadata.Stop().Unblock();
        } else {
            metadata.Stop();
            RebootTablet(Runtime(), TTestTxConfig::SchemeShard, Runtime().AllocateEdgeActor());
            metadata.Unblock();
        }
        Env().TestWaitNotification(Runtime(), id);
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(starts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetS3FileContent("/backup/table/metadata.json"), expected);
        if (withChangefeed) {
            UNIT_ASSERT_VALUES_EQUAL(changefeedUploads, 2);
        }
        metadata.Unblock();
    }

    void RunTableChecksumMetadataRetry(bool checksumFile) {
        EnvOptions().EnableChecksumsExport(true);
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TVector<TString> payloads;
        bool injected = false;
        auto observer = Runtime().AddObserver<TEvPutObjectRequest>([&](auto& ev) {
            const TString key(ev->Get()->Request.GetKey());
            const bool metadata = key.EndsWith("backup/table/metadata.json");
            if (metadata) {
                payloads.push_back(ev->Get()->Body);
            }
            if (injected || !(checksumFile ? key.EndsWith("backup/table/metadata.json.sha256") : metadata)) {
                return;
            }
            injected = true;
            Aws::Client::AWSError<Aws::S3::S3Errors> error(
                Aws::S3::S3Errors::SLOW_DOWN, "SlowDown", "Injected metadata failure", true);
            error.SetResponseCode(Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE);
            Runtime().Send(
                new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvPutObjectResponse(key, TEvPutObjectResponse::TOutcome(std::move(error)))), 0, true);
            ev.Reset();
        });
        const ui64 id = StartCompletionMarkerExport();
        Runtime().WaitFor("checksum start", [&] {
            return !starts.empty();
        });

        SendChecksumResult(Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "");
        Env().TestWaitNotification(Runtime(), id);
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(payloads.size(), 2);
        UNIT_ASSERT(!payloads.front().empty());
        UNIT_ASSERT_VALUES_EQUAL(payloads.front(), payloads.back());
        UNIT_ASSERT_VALUES_EQUAL(GetS3FileContent("/backup/table/metadata.json"), payloads.front());
        UNIT_ASSERT_VALUES_EQUAL(GetS3FileContent("/backup/table/metadata.json.sha256"),
            NBackup::ComputeChecksum(payloads.front()) + " metadata.json");
    }

    auto ForceMultipartUpload() {
        return Runtime().AddObserver<TEvDataShard::TEvProposeTransaction>([](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                NKikimrTxDataShard::TFlatSchemeTransaction tx;
                UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
                if (tx.HasBackup()) {
                    tx.MutableBackup()->MutableScanSettings()->SetRowsBatchSize(1);
                    tx.MutableBackup()->MutableS3Settings()->MutableLimits()->SetMinWriteBatchSize(1);
                    record.SetTxBody(tx.SerializeAsString());
                }
            }
        });
    }

    bool HasUploadedBackupObject(TStringBuf suffix) {
        // The S3 mock keeps the absolute request URI when it acts as an HTTP proxy
        return AnyOf(S3Mock().GetData(), [suffix](const auto& object) {
            return object.first.EndsWith(suffix);
        });
    }

    void RunTableChecksumUnsupportedStoredVersion(bool reboot, bool multipart, bool withResolver = false) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        auto proposals = ForceMultipartUpload();
        if (!multipart) {
            proposals.Remove();
        }
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TBlockEvents<TEvDataShard::TEvStoreBackupChecksumRequest> stores(Runtime(), [](auto& ev) {
            return ev->Get()->Outcome.has_value();
        });
        TBlockEvents<TEvUploadPartRequest> parts(Runtime());
        TBlockEvents<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> resolves(Runtime());
        bool checksumFailed = false;
        auto replies = Runtime().AddObserver<TEvDataShard::TEvStoreBackupChecksumResponse>([&](auto& ev) {
            const auto& outcome = ev->Get()->Outcome;
            if (outcome && outcome->Error.Contains("unsupported stored table checksum version")) {
                checksumFailed = true;
            }
        });
        const ui64 id = StartCompletionMarkerExport(true, multipart);
        Runtime().WaitFor("checksum start before storing a result", [&] {
            return !starts.empty() && (!multipart || !parts.empty());
        });

        SendChecksumResult(Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "");
        Runtime().WaitFor("checksum persistence request", [&] {
            return !stores.empty();
        });
        const ui64 backupTxId = stores.front()->Get()->TxId;
        const auto shards =
            GetTableShards(Runtime(), TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
        const ui64 shard = shards.front();
        const auto stored =
            ReadSystemTable(Runtime(), shard, "TxDetails", { "TxId", "Origin" }, { "TxId", "Origin", "Body" });
        const auto value = NKikimr::NClient::TValue::Create(stored);
        const auto rows = value["Result"]["List"];
        TString body;
        for (size_t idx = 0; idx < rows.Size(); ++idx) {
            const auto row = rows[static_cast<int>(idx)];
            const ui64 txId = row["TxId"];
            const ui64 origin = row["Origin"];
            if (txId == backupTxId && origin == shard) {
                body = static_cast<TString>(row["Body"]);
                break;
            }
        }
        UNIT_ASSERT(!body.empty());
        NKikimrTxDataShard::TFlatSchemeTransaction tx;
        UNIT_ASSERT(tx.ParseFromString(body));
        UNIT_ASSERT(tx.HasBackup() && tx.GetBackup().HasTableChecksum());
        auto* checksum = tx.MutableBackup()->MutableTableChecksum();
        checksum->SetVersion(999);
        checksum->SetAlgorithm("CityHash64");
        checksum->SetCount(0);
        checksum->SetHash("0000000000000000");
        TStringStream escaped;
        NYql::EscapeArbitraryAtom(tx.SerializeAsString(), '"', &escaped);
        NKikimrMiniKQL::TResult result;
        TString error;
        const auto writeStatus = LocalMiniKQL(Runtime(), shard, Sprintf(R"((
                (let key '('('TxId (Uint64 '%lu)) '('Origin (Uint64 '%lu))))
                (let row '('('Body (String '%s))))
                (return (AsList (UpdateRow 'TxDetails key row)))
            ))", backupTxId, shard, escaped.Str().c_str()), result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(writeStatus, NKikimrProto::EReplyStatus::OK, error);
        if (withResolver) {
            auto* endpoint = Runtime().GetAppData().S3ProxyResolverConfig.AddEndpoints();
            endpoint->SetEndpoint(Sprintf("localhost:%d", S3Port()));
            auto* resolver = endpoint->MutableHttpResolver();
            resolver->SetResolveUrl("checksum-resolver.local/resolve");
            resolver->SetHttpPort(S3Port());
        }
        if (reboot) {
            parts.clear();
            RebootTablet(Runtime(), shard, Runtime().AllocateEdgeActor());
        } else {
            stores.Stop().Unblock();
        }
        if (withResolver) {
            Runtime().WaitFor("resolve proxy before resuming the upload", [&] {
                return !resolves.empty();
            });
            const auto& resolve = resolves.front();
            NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(resolve->Get()->Request);
            response->Status = "200";
            response->Body = "localhost";
            Runtime().Send(new IEventHandle(resolve->Sender, resolve->GetRecipientRewrite(),
                new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(resolve->Get()->Request, std::move(response))));
        }
        Runtime().WaitFor("unsupported stored checksum detected", [&] {
            return checksumFailed;
        });
        if (multipart) {
            const auto status = TestGetExport(Runtime(), id, "/MyRoot");
            UNIT_ASSERT_VALUES_EQUAL(
                status.GetResponse().GetEntry().GetProgress(), Ydb::Export::ExportProgress::PROGRESS_TRANSFER_DATA);
        }
        parts.Stop().Unblock();
        Env().TestWaitNotification(Runtime(), id);
        const auto status = TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "unsupported stored table checksum version 999");
        UNIT_ASSERT_VALUES_EQUAL(starts.size(), 1);
        UNIT_ASSERT(HasUploadedBackupObject("/backup/table/data_00.csv"));
        UNIT_ASSERT(!HasUploadedBackupObject("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasUploadedBackupObject("/backup/table/data_complete.json"));
        UNIT_ASSERT(!HasUploadedBackupObject("/backup/table/export_complete.json"));
    }

    void RunTableChecksumProxyFailureBeforeReady(bool queryFailure) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        TestCreateTable(Runtime(), 100, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                KeyColumnNames: ["key"]
            )");
        Env().TestWaitNotification(Runtime(), 100);
        auto* endpoint = Runtime().GetAppData().S3ProxyResolverConfig.AddEndpoints();
        endpoint->SetEndpoint(Sprintf("localhost:%d", S3Port()));
        endpoint->MutableHttpResolver()->SetResolveUrl("checksum-resolver.local/resolve");
        endpoint->MutableHttpResolver()->SetHttpPort(S3Port());
        auto proposals = Runtime().AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
            if (tx.HasBackup()) {
                tx.MutableBackup()->SetNumberOfRetries(0);
                if (!queryFailure) {
                    auto* checksum = tx.MutableBackup()->MutableTableChecksum();
                    checksum->SetVersion(999);
                    checksum->SetAlgorithm("CityHash64");
                    checksum->SetCount(0);
                    checksum->SetHash("0000000000000000");
                }
                record.SetTxBody(tx.SerializeAsString());
            }
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TBlockEvents<NDataShard::TEvExportScan::TEvReady> ready(Runtime());
        TBlockEvents<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> resolves(Runtime());
        const ui64 id = 101;
        TestExport(Runtime(), id, "/MyRoot", Sprintf(R"(
                ExportToS3Settings {
                    endpoint: "localhost:%d"
                    scheme: HTTP
                    items { source_path: "/MyRoot/Table" destination_prefix: "backup/table" }
                }
            )", S3Port()));
        Runtime().WaitFor("proxy resolution before scanner readiness", [&] {
            return !ready.empty() && !resolves.empty() && (!queryFailure || !starts.empty());
        });
        if (queryFailure) {
            SendChecksumResult(Ydb::StatusIds::GENERIC_ERROR, {}, "Injected checksum failure");
            Runtime().SimulateSleep(TDuration::MilliSeconds(1));
        }
        const auto& resolve = resolves.front();
        NHttp::THttpIncomingResponsePtr response;
        if (queryFailure) {
            response = new NHttp::THttpIncomingResponse(resolve->Get()->Request);
            response->Status = "200";
            response->Body = "localhost";
        }
        Runtime().Send(new IEventHandle(resolve->Sender, resolve->GetRecipientRewrite(),
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(
                resolve->Get()->Request, std::move(response), queryFailure ? "" : "Injected resolver failure")));
        Runtime().SimulateSleep(TDuration::MilliSeconds(1));
        ready.Stop().Unblock();
        Env().TestWaitNotification(Runtime(), id);
        const auto status = TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        if (queryFailure) {
            UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "Injected checksum failure");
            UNIT_ASSERT(HasUploadedBackupObject("/backup/table/data_00.csv"));
        } else {
            UNIT_ASSERT(starts.empty());
            UNIT_ASSERT_C(!status.DebugString().Contains("unsupported stored table checksum"), status.DebugString());
            UNIT_ASSERT(!HasUploadedBackupObject("/backup/table/data_00.csv"));
        }
        UNIT_ASSERT(!HasUploadedBackupObject("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasUploadedBackupObject("/backup/table/data_complete.json"));
    }

    void RunTableChecksumFailureDuringMultipart(bool atCompletion) {
        Env();
        auto& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        auto multipart = ForceMultipartUpload();
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        TBlockEvents<TEvUploadPartRequest> parts(runtime, [&](auto&) {
            return !atCompletion;
        });
        TBlockEvents<TEvCompleteMultipartUploadRequest> complete(runtime, [&](auto&) {
            return atCompletion;
        });
        bool checksumFailed = false;
        auto replies = runtime.AddObserver<TEvDataShard::TEvStoreBackupChecksumResponse>([&](auto& ev) {
            const auto& outcome = ev->Get()->Outcome;
            checksumFailed |= outcome && outcome->Error.Contains("Injected checksum failure");
        });
        const auto id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("checksum and multipart request are pending", [&] {
            return !starts.empty() && (atCompletion ? !complete.empty() : !parts.empty());
        });

        SendChecksumResult(Ydb::StatusIds::GENERIC_ERROR, {}, "Injected checksum failure");
        runtime.WaitFor("fatal checksum outcome committed", [&] {
            return checksumFailed;
        });
        const auto pending = TestGetExport(runtime, id, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(
            pending.GetResponse().GetEntry().GetProgress(), Ydb::Export::ExportProgress::PROGRESS_TRANSFER_DATA);
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));

        parts.Stop().Unblock();
        complete.Stop().Unblock();
        Env().TestWaitNotification(runtime, id);
        const auto status = TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "Injected checksum failure");
        UNIT_ASSERT(HasS3File("/backup/table/data_00.csv"));
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/export_complete.json"));
    }

private:
    TMaybe<TTestBasicRuntime::TEventObserverHolder> ChecksumRequests;
    TActorId ChecksumUploader;
    ui64 ChecksumTxId = 0;
};

} // namespace

Y_UNIT_TEST_SUITE_F(TExportToS3Tests, TTableChecksumExportFixture) {
    Y_UNIT_TEST_QUAD(TableChecksumSourceSchemaChangedBeforeTask, Reboot, Drop) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto sessions = MockChecksumSession(runtime);
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            UNIT_ASSERT_STRING_CONTAINS(ev->Get()->GetQuery(), "/export-101/0");
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(
                *response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(2, "0000000000000011"));
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        ui32 allocations = 0;
        TBlockEvents<TEvTxAllocatorClient::TEvAllocateResult> replies(runtime, [&](auto& ev) {
            // The export allocates mkdir, copy, then backup transaction IDs
            return ev->Cookie == 101 && ++allocations == 3;
        });
        TBlockEvents<TEvDataShard::TEvProposeTransaction> backups(runtime, [](auto& ev) {
            if (!Reboot || ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return false;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            return tx.HasBackup();
        });
        const auto id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("copy completed before constructing the backup task", [&] {
            return !replies.empty();
        });
        ui64 txId = id;
        if (Drop) {
            TestDropTable(runtime, ++txId, "/MyRoot", "Table");
            Env().TestWaitNotification(runtime, txId);
        } else {
            TestAlterTable(runtime, ++txId, "/MyRoot", R"(Name: "Table" DropColumns { Name: "value" })");
            Env().TestWaitNotification(runtime, txId);
            TestAlterTable(runtime, ++txId, "/MyRoot", R"(Name: "Table" Columns { Name: "value" Type: "Utf8" })");
            Env().TestWaitNotification(runtime, txId);
        }
        if (Reboot) {
            flags.SetEnableBackupTableChecksum(false);
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        }
        replies.Stop().Unblock();
        if (Reboot) {
            runtime.WaitFor("backup requests wait before reaching DataShard", [&] {
                return backups.size() == 2;
            });
            for (const auto& request : backups) {
                NKikimrTxDataShard::TFlatSchemeTransaction tx;
                UNIT_ASSERT(tx.ParseFromString(request->Get()->Record.GetTxBody()));
                UNIT_ASSERT(!tx.GetBackup().HasTableChecksum());
            }
            backups.clear();
            flags.SetEnableBackupTableChecksum(true);
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            runtime.WaitFor("recovered checksum requests wait before reaching DataShard", [&] {
                return backups.size() == 2;
            });
            for (const auto& request : backups) {
                NKikimrTxDataShard::TFlatSchemeTransaction tx;
                UNIT_ASSERT(tx.ParseFromString(request->Get()->Record.GetTxBody()));
                UNIT_ASSERT(tx.GetBackup().HasTableChecksum());
            }
            backups.Stop().Unblock();
        }
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto checksum =
            NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true).GetTableChecksum();
        UNIT_ASSERT(checksum.has_value() && !checksum->Error.has_value());
        UNIT_ASSERT_VALUES_EQUAL(checksum->Count, 2u);
        CheckCompletionMarkerContents(id);
    }

    Y_UNIT_TEST_QUAD(TableChecksumMigratedTableExport, IsFs, Reboot) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableFsBackups(true);
        flags.SetEnableExportFiltering(false);
        ui64 txId = 100;
        TestCreateSubDomain(runtime, ++txId, "/MyRoot", R"(Name: "Tenant")");
        TestAlterSubDomain(runtime, ++txId, "/MyRoot", R"(
            Name: "Tenant"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
        )");
        Env().TestWaitNotification(runtime, txId);
        TestCreateTable(runtime, ++txId, "/MyRoot/Tenant", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(runtime, txId);
        TestUpgradeSubDomain(runtime, ++txId, "/MyRoot", "Tenant");
        Env().TestWaitNotification(runtime, txId);
        TestUpgradeSubDomainDecision(runtime, ++txId, "/MyRoot", "Tenant", NKikimrSchemeOp::TUpgradeSubDomain::Commit);
        Env().TestWaitNotification(runtime, txId);
        ui64 schemeShardId = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Tenant"),
            {
                NLs::IsExternalSubDomain("Tenant"), NLs::ExtractTenantSchemeshard(&schemeShardId),
            });
        UNIT_ASSERT_VALUES_UNEQUAL(schemeShardId, TTestTxConfig::SchemeShard);
        const auto table = DescribePath(runtime, schemeShardId, "/MyRoot/Tenant/Table").GetPathDescription();
        UNIT_ASSERT_VALUES_EQUAL(table.GetSelf().GetSchemeshardId(), TTestTxConfig::SchemeShard);
        UNIT_ASSERT_VALUES_EQUAL(table.GetDomainDescription().GetProcessingParams().GetSchemeShard(), schemeShardId);
        const TTableId tableId(table.GetSelf().GetSchemeshardId(), table.GetSelf().GetPathId());
        auto proposals = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            if (tx.HasBackup() && tx.GetBackup().HasTableChecksum() && tx.GetBackup().GetShardNum() == 0) {
                const auto& task = tx.GetBackup();
                UNIT_ASSERT_VALUES_EQUAL(task.GetTable().GetSelf().GetSchemeshardId(), tableId.PathId.OwnerId);
                UNIT_ASSERT_VALUES_EQUAL(
                    task.GetTable().GetDomainDescription().GetProcessingParams().GetSchemeShard(), schemeShardId);
            }
        });
        auto sessions = MockChecksumSession(runtime, "/MyRoot/Tenant");
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            UNIT_ASSERT(!ev->Get()->GetMvccVersion());
            UNIT_ASSERT(ev->Get()->GetTxControl().begin_tx().online_read_only().allow_inconsistent_reads());
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(*response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum());
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        TTempDir directory;
        const auto exportId = ++txId;
        NKikimrExport::TCreateExportRequest request;
        if (IsFs) {
            auto* settings = request.MutableExportToFsSettings();
            settings->set_base_path(directory.Path());
            auto* item = settings->add_items();
            item->set_source_path("/MyRoot/Tenant/Table");
            item->set_destination_path("backup/migrated");
        } else {
            auto* settings = request.MutableExportToS3Settings();
            settings->set_endpoint(Sprintf("localhost:%d", S3Port()));
            settings->set_scheme(Ydb::Export::ExportToS3Settings::HTTP);
            auto* item = settings->add_items();
            item->set_source_path("/MyRoot/Tenant/Table");
            item->set_destination_prefix("backup/migrated");
        }
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        TestExport(runtime, schemeShardId, exportId, "/MyRoot/Tenant", request.DebugString());
        runtime.WaitFor("migrated table checksum starts", [&] {
            return starts.size() == 1;
        });
        if (Reboot) {
            RebootTablet(runtime, schemeShardId, runtime.AllocateEdgeActor());
        }
        starts.Stop().Unblock();
        Env().TestWaitNotification(runtime, exportId, schemeShardId);
        TestGetExport(runtime, schemeShardId, exportId, "/MyRoot/Tenant", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(queries, 1);
        const auto metadata = IsFs ? TFileInput(directory.Path().GetPath() + "/backup/migrated/metadata.json").ReadAll()
                                   : GetS3FileContent("/backup/migrated/metadata.json");
        const auto checksum = NBackup::TMetadata::Deserialize(metadata, true).GetTableChecksum();
        UNIT_ASSERT(checksum.has_value());
        UNIT_ASSERT(!checksum->Error.has_value());
        UNIT_ASSERT_VALUES_EQUAL(checksum->Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(checksum->Hash, "0000000000000000");
    }

    Y_UNIT_TEST_TWIN(TableChecksumLegacyCopyReadFailure, Legacy) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        auto sessions = MockChecksumSession(runtime);
        auto rejected = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Legacy ? Ydb::StatusIds::ABORTED : Ydb::StatusIds::SCHEME_ERROR);
            response->Record.MutableResponse()->AddQueryIssues()->set_message(
                Legacy ? "Can't read from a backup table (shard# 1 node# 1 state# Ready)"
                       : "Unrelated schema error mentioning Can't read from a backup table (shard# 1 node# 1 state# "
                         "Ready)");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        TBlockEvents<TEvDataShard::TEvRefreshVolatileSnapshotRequest> confirmations(runtime);
        const auto id = StartCompletionMarkerExport();
        Env().TestWaitNotification(runtime, id);
        const auto status = TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(confirmations.empty());
        UNIT_ASSERT_STRING_CONTAINS(
            status.DebugString(), Legacy ? "read from a backup table" : "Unrelated schema error");
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
    }

    // Query execution is covered by Python functional tests; hold its start here to
    // deterministically exercise the uploader's independent completion branches
    Y_UNIT_TEST(TableChecksumDoesNotBlockData) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NSchemeShard;
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        const ui64 id = StartCompletionMarkerExport();
        Runtime().WaitFor("data export while checksum is paused", [&] {
            return !starts.empty() && HasS3File("/backup/table/data_00.csv") && HasS3File("/backup/table/data_01.csv");
        });
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));

        SendChecksumResult(Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "");
        Env().TestWaitNotification(Runtime(), id);
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto metadata = NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true);
        UNIT_ASSERT(metadata.GetTableChecksum());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Count, 0);
        CheckCompletionMarkerContents(id);
    }

    Y_UNIT_TEST(TableChecksumBeforeData) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TBlockEvents<NKikimr::TEvDataShard::TEvStoreBackupChecksumRequest> checksum(Runtime(), [](auto& ev) {
            return ev->Get()->Outcome.has_value();
        });
        TBlockEvents<TEvPutObjectRequest> data(Runtime(), [](auto& ev) {
            return TString(ev->Get()->Request.GetKey()).EndsWith(".csv");
        });
        const ui64 id = StartCompletionMarkerExport();
        Runtime().WaitFor("paused checksum and data", [&] {
            return !starts.empty() && !data.empty();
        });

        SendChecksumResult(Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "");
        Runtime().WaitFor("checksum complete while data is paused", [&] {
            return !checksum.empty();
        });
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
        checksum.Stop().Unblock();
        data.Stop().Unblock();
        Env().TestWaitNotification(Runtime(), id);
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        CheckCompletionMarkerContents(id);
    }

    Y_UNIT_TEST_TWIN(TableChecksumFailureWaitsForData, DataFirst) {
        EnvOptions().EnableChecksumsExport(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TBlockEvents<TEvPutObjectRequest> data(Runtime(), [](auto& ev) {
            return !DataFirst && TString(ev->Get()->Request.GetKey()).EndsWith(".csv");
        });
        ui32 savedChecksums = 0;
        auto saved = runtime.AddObserver<TEvDataShard::TEvStoreBackupChecksumRequest>([&](auto& ev) {
            if (ev->Get()->Outcome) {
                UNIT_ASSERT(!ev->Get()->Outcome->Checksum);
                UNIT_ASSERT_STRING_CONTAINS(ev->Get()->Outcome->Error, "backup checksum unavailable");
                ++savedChecksums;
            }
        });
        bool dataUploaded = false;
        auto replies = runtime.AddObserver<TEvPutObjectResponse>([&](auto& ev) {
            if (ev->Get()->Key && ev->Get()->Key->EndsWith("backup/table/data_00.csv.sha256")) {
                UNIT_ASSERT(ev->Get()->Result.IsSuccess());
                dataUploaded = true;
            }
        });
        const auto id = StartCompletionMarkerExport();
        Runtime().WaitFor("checksum and data export ready", [&] {
            return !starts.empty() && (DataFirst ? dataUploaded : !data.empty());
        });
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        SendChecksumResult(Ydb::StatusIds::PRECONDITION_FAILED, {}, "backup checksum unavailable");
        data.Stop().Unblock();
        Env().TestWaitNotification(Runtime(), id);
        const auto status = TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "backup checksum unavailable");
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT_VALUES_EQUAL(savedChecksums, 1);
        UNIT_ASSERT(HasS3File("/backup/table/data_00.csv"));
        UNIT_ASSERT(HasS3File("/backup/table/data_01.csv"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
    }

    Y_UNIT_TEST_TWIN(TableChecksumReportsInterruptionAfterRestart, RetentionEnabled) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        auto sessions = MockChecksumSession(runtime);
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(
                *response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(2, "0000000000000011"));
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        const auto id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("data uploaded before checksum calculation", [&] {
            return !starts.empty() && HasS3File("/backup/table/data_00.csv") && HasS3File("/backup/table/data_01.csv");
        });
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));

        flags.SetEnableBackupTableChecksum(false);
        flags.SetEnableSnapshotsLocking(RetentionEnabled);
        flags.SetEnableDataShardSnapshotLocking(RetentionEnabled);
        const auto copyShards =
            GetTableShards(runtime, TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
        RebootTablet(runtime, copyShards.front(), runtime.AllocateEdgeActor());
        Env().TestWaitNotification(runtime, id);
        const auto status = TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "CHECKSUM_INTERRUPTED");

        UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
    }

    Y_UNIT_TEST(TableChecksumResultInterruptedBeforeCommit) {
        auto& env = Env();
        auto& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        TBlockEvents<TEvDataShard::TEvStoreBackupChecksumRequest> stores(runtime, [](auto& ev) {
            return ev->Get()->Outcome.has_value();
        });
        const auto id = StartCompletionMarkerExport();
        runtime.WaitFor("checksum launch", [&] {
            return starts.size() == 1;
        });
        const auto checksum = MakeTableChecksum(2, "0000000000000011");
        SendChecksumResult(Ydb::StatusIds::SUCCESS, checksum, "");
        runtime.WaitFor("result reaches the uploader before its local commit", [&] {
            return stores.size() == 1;
        });
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        const auto copyShards =
            GetTableShards(runtime, TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
        RebootTablet(runtime, copyShards.front(), runtime.AllocateEdgeActor());
        env.TestWaitNotification(runtime, id);
        const auto status = TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "CHECKSUM_INTERRUPTED");
        UNIT_ASSERT_VALUES_EQUAL(starts.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(stores.size(), 1u);
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
    }

    Y_UNIT_TEST(TableChecksumMetadataRecovery) {
        RunTableChecksumMetadataRecovery(false);
    }

    Y_UNIT_TEST(TableChecksumDataShardRecovery) {
        RunTableChecksumMetadataRecovery(true);
    }

    Y_UNIT_TEST_TWIN(TableChecksumWideHashRecovery, Sha256) {
        const auto checksum = MakeTableChecksum(2, TString(Sha256 ? 64 : 32, 'F'), Sha256 ? "Sha256" : "CityHash128");
        RunTableChecksumMetadataRecovery(true, false, checksum);
    }

    Y_UNIT_TEST(TableChecksumChangefeedRecovery) {
        RunTableChecksumMetadataRecovery(true, true);
    }

    Y_UNIT_TEST(TableChecksumUnsupportedStoredVersion) {
        RunTableChecksumUnsupportedStoredVersion(false, false);
    }

    Y_UNIT_TEST(TableChecksumUnsupportedStoredVersionMultipart) {
        RunTableChecksumUnsupportedStoredVersion(false, true);
    }

    Y_UNIT_TEST(TableChecksumUnsupportedStoredVersionRecovery) {
        RunTableChecksumUnsupportedStoredVersion(true, false);
    }

    Y_UNIT_TEST(TableChecksumUnsupportedStoredVersionMultipartRecovery) {
        RunTableChecksumUnsupportedStoredVersion(true, true);
    }

    Y_UNIT_TEST(TableChecksumUnsupportedStoredVersionProxyRecovery) {
        RunTableChecksumUnsupportedStoredVersion(true, true, true);
    }

    Y_UNIT_TEST(TableChecksumQueryFailureBeforeProxyReady) {
        RunTableChecksumProxyFailureBeforeReady(true);
    }

    Y_UNIT_TEST(TableChecksumResolverFailureBeforeReady) {
        RunTableChecksumProxyFailureBeforeReady(false);
    }

    Y_UNIT_TEST(TableChecksumMetadataRetry) {
        RunTableChecksumMetadataRetry(false);
    }

    Y_UNIT_TEST(TableChecksumMetadataChecksumRetry) {
        RunTableChecksumMetadataRetry(true);
    }

    Y_UNIT_TEST(TableChecksumFailureDuringUploadPart) {
        RunTableChecksumFailureDuringMultipart(false);
    }

    Y_UNIT_TEST(TableChecksumFailureDuringMultipartCompletion) {
        RunTableChecksumFailureDuringMultipart(true);
    }

    Y_UNIT_TEST(TableChecksumClosesLateSessionAfterCancellation) {
        EnvOptions().SetupKqpProxy(true);
        auto& env = Env();
        auto& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        TBlockEvents<NKqp::TEvKqp::TEvCreateSessionRequest> admissions(runtime, [](auto& ev) {
            return ev->Get()->Record.GetRequest().GetDatabase() == "/MyRoot";
        });
        TBlockEvents<NKqp::TEvKqp::TEvCloseSessionRequest> closes(runtime);
        TBlockEvents<NKqp::TEvKqp::TEvQueryRequest> queries(runtime, [](auto& ev) {
            return ev->Get()->GetQuery().Contains("CHECKSUM TABLE");
        });
        const auto id = StartCompletionMarkerExport();
        runtime.WaitFor("checksum waits for session admission", [&] {
            return admissions.size() == 1;
        });
        TestCancelExport(runtime, id + 100, "/MyRoot", id);
        env.TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);

        const TString session = TStringBuilder()
                                << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D";
        auto admitted = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
        admitted->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
        admitted->Record.MutableResponse()->SetSessionId(session);
        const auto& request = admissions.front();
        runtime.Send(
            new IEventHandle(request->Sender, request->Recipient, admitted.Release(), 0, request->Cookie), 0, true);
        runtime.WaitFor("late session is closed", [&] {
            return !closes.empty();
        });
        UNIT_ASSERT_VALUES_EQUAL(closes.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(closes.front()->Get()->Record.GetRequest().GetSessionId(), session);
        UNIT_ASSERT(queries.empty());
    }

    Y_UNIT_TEST(TableChecksumCancellation) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NSchemeShard;
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        const ui64 id = StartCompletionMarkerExport();
        Runtime().WaitFor("paused checksum", [&] {
            return !starts.empty();
        });
        TestCancelExport(Runtime(), id + 100, "/MyRoot", id);
        Env().TestWaitNotification(Runtime(), id);
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);

        SendChecksumResult(Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "");
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
    }
}
