#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/backup/query/table_checksum.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/mvcc_version.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/export_scan.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/tx_processing.h>
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
    enum class ESourceChange {
        Schema,
        Identity,
    };

    void CheckTableChecksumSourceChange(ESourceChange change, bool reboot) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        auto& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 starts = 0;
        auto queries = runtime.AddObserver<NBackup::TEvTableChecksum::TEvStart>([&](auto&) {
            ++starts;
        });
        THolder<IEventHandle> pending;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                UNIT_ASSERT(!pending);
                pending.Reset(ev.Release());
            }
        });
        using namespace NKikimr::NWrappers::NExternalStorage;
        TBlockEvents<TEvPutObjectRequest> metadata(runtime, [&](auto& ev) {
            return reboot && TString(ev->Get()->Request.GetKey()).EndsWith("backup/table/metadata.json");
        });
        const auto id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("checksum query paused after source validation", [&] {
            return pending && HasS3File("/backup/table/data_00.csv") && HasS3File("/backup/table/data_01.csv");
        });
        ui64 txId = id;
        if (change == ESourceChange::Schema) {
            TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "extra" Type: "Utf8" }
                )");
            Env().TestWaitNotification(runtime, txId);
        } else {
            TestDropTable(runtime, ++txId, "/MyRoot", "Table");
            Env().TestWaitNotification(runtime, txId);
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Uint32" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
            Env().TestWaitNotification(runtime, txId);
        }
        auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
        response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
        FillTableChecksumResult(
            *response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(2, "0000000000000011"));
        runtime.Send(
            new IEventHandle(pending->Sender, pending->Recipient, response.Release(), 0, pending->Cookie), 0, true);
        pending.Reset();

        TString expected;
        if (reboot) {
            runtime.WaitFor("checksum error persisted before metadata upload", [&] {
                return !metadata.empty();
            });
            expected = metadata.front()->Get()->Body;
            metadata.Stop();
            // Recovery must reuse the saved error even if the source changes again
            TestDropTable(runtime, ++txId, "/MyRoot", "Table");
            Env().TestWaitNotification(runtime, txId);
            const auto shards =
                GetTableShards(runtime, TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
            RebootTablet(runtime, shards.front(), runtime.AllocateEdgeActor());
        }
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto payload = GetS3FileContent("/backup/table/metadata.json");
        if (reboot) {
            UNIT_ASSERT_VALUES_EQUAL(payload, expected);
        }
        const auto checksum = NBackup::TMetadata::Deserialize(payload, true).GetTableChecksum();
        UNIT_ASSERT(checksum.has_value() && checksum->Error.has_value());
        UNIT_ASSERT_VALUES_EQUAL(
            checksum->Error->Code, change == ESourceChange::Schema ? "SOURCE_SCHEMA_CHANGED" : "SOURCE_TABLE_CHANGED");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(payload, &json));
        UNIT_ASSERT(!json["table_checksum"].Has("count"));
        UNIT_ASSERT(!json["table_checksum"].Has("hash"));
        UNIT_ASSERT_VALUES_EQUAL(starts, 1);
        CheckCompletionMarkerContents(id);
    }

    void CheckTableChecksumSourceUnavailable(bool timeout) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        auto& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        TActorId actor;
        auto starts = runtime.AddObserver<NBackup::TEvTableChecksum::TEvStart>([&](auto& ev) {
            actor = ev->GetRecipientRewrite();
            runtime.EnableScheduleForActor(actor);
        });
        ui32 describes = 0;
        TInstant firstFailure;
        auto sources = runtime.AddObserver<TEvSchemeShard::TEvDescribeScheme>([&](auto& ev) {
            if (ev->Sender != actor || ev->Get()->Record.GetPath() != "/MyRoot/Table") {
                return;
            }
            ++describes;
            if (!timeout && describes > 2) {
                return;
            }
            if (describes == 1) {
                firstFailure = runtime.GetCurrentTime();
            }
            auto response = MakeHolder<TEvSchemeShard::TEvDescribeSchemeResult>();
            response->MutableRecord()->SetStatus(NKikimrScheme::StatusNotAvailable);
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
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
        const auto id = StartCompletionMarkerExport();
        Env().TestWaitNotification(runtime, id);
        const auto status =
            TestGetExport(runtime, id, "/MyRoot", timeout ? Ydb::StatusIds::CANCELLED : Ydb::StatusIds::SUCCESS);
        if (timeout) {
            UNIT_ASSERT_GT(describes, 3u);
            UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
            UNIT_ASSERT_GE(runtime.GetCurrentTime() - firstFailure, TDuration::Minutes(5));
            UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "Cannot check table checksum source");
            UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        } else {
            UNIT_ASSERT_VALUES_EQUAL(describes, 4u);
            UNIT_ASSERT_VALUES_EQUAL(queries, 1u);
            const auto metadata =
                NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true);
            UNIT_ASSERT(metadata.GetTableChecksum().has_value());
            UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Hash, "0000000000000011");
            CheckCompletionMarkerContents(id);
        }
    }

    void RunTableChecksumMetadataRecovery(
        bool dataShard, bool withChangefeed = false, const NBackup::TTableChecksum& checksum = MakeTableChecksum()) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        const auto service = Runtime().AllocateEdgeActor();
        Runtime().RegisterService(NLongTxService::MakeLongTxServiceID(Runtime().GetNodeId()), service);
        TVector<std::shared_ptr<std::atomic<bool>>> leases;
        ui32 registered = 0;
        auto acquire = Runtime().AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Snapshot.Defined());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.size(), 2u);
            auto alive = std::make_shared<std::atomic<bool>>(true);
            leases.push_back(alive);
            auto response = MakeHolder<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>(
                "/MyRoot", *ev->Get()->Snapshot, NKqp::TSnapshotHandle(alive), std::move(ev->Get()->Orbit));
            Runtime().Send(new IEventHandle(ev->Sender, service, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        auto handles =
            Runtime().AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>([&](auto&) {
                ++registered;
            });
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
            return !starts.empty() && leases.size() == 1 && registered == 1;
        });
        const auto& start = starts.front();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS, checksum, "")));
        Runtime().WaitFor("final metadata after data", [&] {
            return !metadata.empty() && HasS3File("/backup/table/data_00.csv") &&
                   HasS3File("/backup/table/data_01.csv");
        });
        UNIT_ASSERT(leases.front()->load());
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
                return metadata.size() == 2 && leases.size() == 2 && registered == 2;
            });
            UNIT_ASSERT(!leases.front()->load());
            UNIT_ASSERT(leases.back()->load());
            metadata.Stop().Unblock();
        } else {
            metadata.Stop();
            RebootTablet(Runtime(), TTestTxConfig::SchemeShard, Runtime().AllocateEdgeActor());
            metadata.Unblock();
        }
        Env().TestWaitNotification(Runtime(), id);
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        Runtime().WaitFor("metadata completion releases all retention handles", [&] {
            return AllOf(leases, [](const auto& alive) {
                return !alive->load();
            });
        });
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
        const auto& start = starts.front();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "")));
        Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
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

    void RunTableChecksumUnsupportedStoredVersion(bool reboot, bool multipart, bool withResolver = false) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        auto proposals = ForceMultipartUpload();
        if (!multipart) {
            proposals.Remove();
        }
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TBlockEvents<TEvDataShard::TEvStoreBackupChecksumRequest> stores(Runtime());
        TBlockEvents<TEvUploadPartRequest> parts(Runtime());
        THolder<IEventHandle> abort;
        auto abortReplies = Runtime().AddObserver<TEvAbortMultipartUploadResponse>([&](auto& ev) {
            abort.Reset(ev.Release());
        });
        TBlockEvents<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest> resolves(Runtime());
        ui32 creations = 0;
        auto createRequests = Runtime().AddObserver<TEvCreateMultipartUploadRequest>([&](auto&) {
            ++creations;
        });
        TVector<TString> abortedIds;
        auto abortRequests = Runtime().AddObserver<TEvAbortMultipartUploadRequest>([&](auto& ev) {
            abortedIds.push_back(TString(ev->Get()->Request.GetUploadId()));
        });
        const ui64 id = StartCompletionMarkerExport(true, multipart);
        Runtime().WaitFor("checksum start before storing a result", [&] {
            return !starts.empty() && (!multipart || !parts.empty());
        });
        const TString uploadId = multipart ? TString(parts.front()->Get()->Request.GetUploadId()) : TString();
        const auto& start = starts.front();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "")));
        Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
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
            RebootTablet(Runtime(), shard, Runtime().AllocateEdgeActor());
        } else {
            stores.Stop().Unblock();
        }
        if (withResolver) {
            Runtime().WaitFor("resolve proxy before aborting the recovered upload", [&] {
                return !resolves.empty();
            });
            UNIT_ASSERT(abortedIds.empty());
            const auto& resolve = resolves.front();
            NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(resolve->Get()->Request);
            response->Status = "200";
            response->Body = "localhost";
            Runtime().Send(new IEventHandle(resolve->Sender, resolve->GetRecipientRewrite(),
                new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(resolve->Get()->Request, std::move(response))));
        }
        if (multipart) {
            Runtime().WaitFor("unsupported checksum aborts the saved multipart upload", [&] {
                return bool(abort);
            });
            const auto status = TestGetExport(Runtime(), id, "/MyRoot");
            UNIT_ASSERT_VALUES_EQUAL(
                status.GetResponse().GetEntry().GetProgress(), Ydb::Export::ExportProgress::PROGRESS_TRANSFER_DATA);
            UNIT_ASSERT_VALUES_EQUAL(abortedIds.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(abortedIds.front(), uploadId);
            abortReplies.Remove();
            Runtime().Send(abort.Release(), 0, true);
        }
        Env().TestWaitNotification(Runtime(), id);
        const auto status = TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "unsupported stored table checksum version 999");
        UNIT_ASSERT_VALUES_EQUAL(starts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(creations, multipart ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(abortedIds.size(), multipart ? 1 : 0);
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/export_complete.json"));
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
            const auto& start = starts.front();
            Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
                new NBackup::TEvTableChecksum::TEvResult(
                    id, 0, Ydb::StatusIds::GENERIC_ERROR, {}, "Injected checksum failure")));
            Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
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
        } else {
            UNIT_ASSERT(starts.empty());
        }
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
    }

    void RunTableChecksumFailureDuringMultipart(bool atCompletion) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        auto observer = ForceMultipartUpload();
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        THolder<IEventHandle> lateResponse;
        auto partReplies = Runtime().AddObserver<TEvUploadPartResponse>([&](auto& ev) {
            if (!atCompletion && !lateResponse) {
                lateResponse.Reset(ev.Release());
            }
        });
        auto completeReplies = Runtime().AddObserver<TEvCompleteMultipartUploadResponse>([&](auto& ev) {
            if (atCompletion && !lateResponse) {
                lateResponse.Reset(ev.Release());
            }
        });
        TBlockEvents<TEvDataShard::TEvS3Upload> aborts(Runtime(), [](auto& ev) {
            return ev->Get()->Upload && ev->Get()->Upload->Status == NDataShard::TS3Upload::EStatus::Abort;
        });
        const ui64 id = StartCompletionMarkerExport(true, true);
        Runtime().WaitFor("checksum and in-flight multipart request", [&] {
            return !starts.empty() && bool(lateResponse);
        });
        const auto& start = starts.front();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(
                id, 0, Ydb::StatusIds::GENERIC_ERROR, {}, "Injected checksum failure")));
        Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
        Runtime().WaitFor("durable multipart abort", [&] {
            return !aborts.empty();
        });
        partReplies.Remove();
        completeReplies.Remove();
        Runtime().Send(lateResponse.Release(), 0, true);
        aborts.Stop().Unblock();
        Env().TestWaitNotification(Runtime(), id);
        const auto status = TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "Injected checksum failure");
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
    }

    enum class EUploadStartStage {
        Lookup,
        Creation,
        CreationFailure,
        Persistence,
        RecoveredScheme,
        RecoveredLookup,
        RestartAbort,
    };

    void RunTableChecksumFailureDuringUploadStart(EUploadStartStage stage, bool delayReady = false) {
        Env();
        Runtime().GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NWrappers::NExternalStorage;
        const bool reboot = stage == EUploadStartStage::RecoveredScheme ||
                            stage == EUploadStartStage::RecoveredLookup || stage == EUploadStartStage::RestartAbort;
        auto proposals = ForceMultipartUpload();
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(Runtime());
        TBlockEvents<TEvDataShard::TEvGetS3Upload> lookups(Runtime());
        THolder<IEventHandle> lateResponse;
        THolder<IEventHandle> firstPart;
        bool restarted = false;
        bool captureEnabled = true;
        bool checksumFailed = false;
        bool finished = false;
        ui32 creations = 0;
        TVector<TString> abortedIds;
        TString uploadId;
        TBlockEvents<NDataShard::TEvExportScan::TEvReady> ready(Runtime(), [&](auto&) {
            return reboot && restarted && delayReady;
        });
        auto observer = Runtime().AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (starts.empty()) {
                return;
            }
            const auto owner = starts.back()->Sender;
            const bool toOwner = ev->GetRecipientRewrite() == owner;
            const bool fromOwner = ev->Sender == owner;
            bool delay = false;
            switch (ev->GetTypeRewrite()) {
                case TEvPutObjectRequest::EventType:
                    delay = fromOwner && restarted && stage == EUploadStartStage::RecoveredScheme &&
                            TString(ev->Get<TEvPutObjectRequest>()->Request.GetKey()).EndsWith("scheme.pb");
                    break;
                case TEvCreateMultipartUploadRequest::EventType:
                    if (fromOwner) {
                        ++creations;
                        delay = stage == EUploadStartStage::CreationFailure;
                    }
                    break;
                case TEvCreateMultipartUploadResponse::EventType:
                    if (toOwner && ev->Get<TEvCreateMultipartUploadResponse>()->Result.IsSuccess()) {
                        uploadId =
                            ev->Get<TEvCreateMultipartUploadResponse>()->Result.GetResult().GetUploadId().c_str();
                        delay = stage == EUploadStartStage::Creation;
                    }
                    break;
                case TEvDataShard::TEvS3Upload::EventType:
                    if (toOwner) {
                        const auto& upload = ev->Get<TEvDataShard::TEvS3Upload>()->Upload;
                        delay = (stage == EUploadStartStage::Lookup && !upload) ||
                                (upload && upload->Status == NDataShard::TS3Upload::EStatus::UploadParts &&
                                    (stage == EUploadStartStage::Persistence ||
                                        (stage == EUploadStartStage::RecoveredLookup && restarted)));
                    }
                    break;
                case TEvUploadPartResponse::EventType:
                    if (toOwner && reboot && !restarted && !firstPart) {
                        firstPart.Reset(ev.Release());
                        return;
                    }
                    break;
                case TEvAbortMultipartUploadRequest::EventType:
                    if (fromOwner) {
                        abortedIds.push_back(
                            ev->Get<TEvAbortMultipartUploadRequest>()->GetRequest().GetUploadId().c_str());
                    }
                    break;
                case TEvAbortMultipartUploadResponse::EventType:
                    delay = toOwner && stage == EUploadStartStage::RestartAbort && restarted;
                    break;
                case NBackup::TEvTableChecksum::TEvResult::EventType:
                    checksumFailed |=
                        toOwner && ev->Get<NBackup::TEvTableChecksum::TEvResult>()->Status != Ydb::StatusIds::SUCCESS;
                    break;
                case NDataShard::TEvExportScan::TEvFinish::EventType:
                    finished |= fromOwner;
                    break;
            }
            if (delay && captureEnabled) {
                captureEnabled = false;
                lateResponse.Reset(ev.Release());
            }
        });
        TBlockEvents<TEvDataShard::TEvS3Upload> aborts(Runtime(), [&](auto& ev) {
            return !starts.empty() && ev->GetRecipientRewrite() == starts.back()->Sender && ev->Get()->Upload &&
                   ev->Get()->Upload->Status == NDataShard::TS3Upload::EStatus::Abort;
        });
        const ui64 id = StartCompletionMarkerExport(true, true);
        Runtime().WaitFor("checksum owner before upload lookup", [&] {
            return !starts.empty() && !lookups.empty();
        });
        UNIT_ASSERT_VALUES_EQUAL(lookups.front()->Get()->ReplyTo, starts.back()->Sender);
        lookups.Unblock();
        if (reboot) {
            Runtime().WaitFor("persisted multipart session", [&] {
                return !starts.empty() && bool(firstPart);
            });
            restarted = true;
            const auto shards =
                GetTableShards(Runtime(), TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
            RebootTablet(Runtime(), shards.front(), Runtime().AllocateEdgeActor());
            if (stage == EUploadStartStage::RecoveredScheme) {
                Runtime().WaitFor("recovered scheme upload before multipart lookup", [&] {
                    return starts.size() == 2 && lateResponse && (!delayReady || !ready.empty());
                });
                UNIT_ASSERT(lookups.empty());
            } else {
                Runtime().WaitFor("recovered checksum owner before upload lookup", [&] {
                    return starts.size() == 2 && !lookups.empty();
                });
                UNIT_ASSERT_VALUES_EQUAL(lookups.front()->Get()->ReplyTo, starts.back()->Sender);
                lookups.Unblock();
            }
        }
        lookups.Stop();
        Runtime().WaitFor("pending multipart initialization", [&] {
            return bool(lateResponse) && starts.size() == (reboot ? 2 : 1);
        });
        checksumFailed = false;
        finished = false;
        const auto& start = starts.back();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
                           new NBackup::TEvTableChecksum::TEvResult(
                               id, 0, Ydb::StatusIds::GENERIC_ERROR, {}, "Injected checksum failure")), 0, true);
        Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
        Runtime().WaitFor("checksum failure processed", [&] {
            return checksumFailed;
        });
        Runtime().Send(new IEventHandle(start->Sender, {}, new TEvents::TEvWakeup()), 0, true);
        Runtime().SimulateSleep(TDuration::MilliSeconds(10));
        UNIT_ASSERT_C(!finished, "Uploader must wait for pending multipart initialization");

        if (stage == EUploadStartStage::CreationFailure) {
            Aws::Client::AWSError<Aws::S3::S3Errors> error(Aws::S3::S3Errors::ACCESS_DENIED, false);
            Runtime().Send(new IEventHandle(lateResponse->Sender, lateResponse->GetRecipientRewrite(),
                               new TEvCreateMultipartUploadResponse(std::nullopt,
                                   TEvCreateMultipartUploadResponse::TOutcome(std::move(error)))), 0, true);
            lateResponse.Reset();
        } else {
            Runtime().Send(lateResponse.Release(), 0, true);
        }
        ready.Stop().Unblock();
        const bool hasUpload = stage != EUploadStartStage::Lookup && stage != EUploadStartStage::CreationFailure;
        if (hasUpload) {
            Runtime().WaitFor("committed multipart abort", [&] {
                return !aborts.empty() || finished;
            });
            UNIT_ASSERT_C(!finished, "Uploader must recover and abort the persisted multipart upload");
            Runtime().Send(new IEventHandle(start->Sender, {}, new TEvents::TEvWakeup()), 0, true);
            Runtime().SimulateSleep(TDuration::MilliSeconds(10));
            UNIT_ASSERT_VALUES_EQUAL(abortedIds.size(), stage == EUploadStartStage::RestartAbort ? 1 : 0);
            aborts.Stop().Unblock();
        }
        Env().TestWaitNotification(Runtime(), id);
        const auto status = TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "Injected checksum failure");
        UNIT_ASSERT_VALUES_EQUAL(creations, stage == EUploadStartStage::Lookup ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(abortedIds.size(), !hasUpload ? 0 : stage == EUploadStartStage::RestartAbort ? 2 : 1);
        for (const auto& abortedId : abortedIds) {
            UNIT_ASSERT_VALUES_EQUAL(abortedId, uploadId);
        }
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
    }
};

} // namespace

Y_UNIT_TEST_SUITE_F(TExportToS3Tests, TTableChecksumExportFixture) {
    Y_UNIT_TEST_QUAD(TableChecksumSourceSchemaChangedBeforeTask, Reboot, Drop) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto queries = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([](auto&) {
            UNIT_FAIL("The changed source schema must be rejected before starting a checksum query");
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
        UNIT_ASSERT(checksum.has_value() && checksum->Error.has_value());
        UNIT_ASSERT_VALUES_EQUAL(checksum->Error->Code, Drop ? "SOURCE_TABLE_CHANGED" : "SOURCE_SCHEMA_CHANGED");
        CheckCompletionMarkerContents(id);
    }

    Y_UNIT_TEST_TWIN(TableChecksumSourceSchemaChanged, Reboot) {
        CheckTableChecksumSourceChange(ESourceChange::Schema, Reboot);
    }

    Y_UNIT_TEST_TWIN(TableChecksumSourceIdentityChanged, Reboot) {
        CheckTableChecksumSourceChange(ESourceChange::Identity, Reboot);
    }

    Y_UNIT_TEST_TWIN(TableChecksumSourceChangedDuringSnapshotConfirmation, Schema) {
        Env();
        auto& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        TBlockEvents<TEvDataShard::TEvRefreshVolatileSnapshotResponse> snapshots(runtime, [](auto& ev) {
            return !ev->Get()->Record.HasTableChecksumSnapshot();
        });
        const auto id = StartCompletionMarkerExport();
        runtime.WaitFor("source precheck finished before snapshot confirmation", [&] {
            return !snapshots.empty();
        });
        if (Schema) {
            TestAlterTable(runtime, id + 1, "/MyRoot", R"(Name: "Table" Columns { Name: "extra" Type: "Utf8" })");
        } else {
            TestDropTable(runtime, id + 1, "/MyRoot", "Table");
        }
        Env().TestWaitNotification(runtime, id + 1);
        for (auto& response : snapshots) {
            response->Get()->Record.SetStatus(
                NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse::SNAPSHOT_NOT_FOUND);
        }
        snapshots.Stop().Unblock();
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto checksum =
            NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true).GetTableChecksum();
        UNIT_ASSERT(checksum.has_value() && checksum->Error.has_value());
        UNIT_ASSERT_VALUES_EQUAL(checksum->Error->Code, Schema ? "SOURCE_SCHEMA_CHANGED" : "SOURCE_TABLE_CHANGED");
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
        TRowVersion snapshot = TRowVersion::Min();
        TTableId copyTableId;
        const auto service = runtime.AllocateEdgeActor();
        runtime.RegisterService(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId()), service);
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
                snapshot = TRowVersion(task.GetSnapshotStep(), task.GetSnapshotTxId());
                copyTableId = TTableId(schemeShardId, task.GetTableId());
            }
        });
        ui32 retentions = 0;
        auto acquire = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>([&](auto& ev) {
            if (ev->Get()->DatabaseName != "/MyRoot/Tenant") {
                return;
            }
            ++retentions;
            UNIT_ASSERT(ev->Get()->Snapshot.Defined());
            UNIT_ASSERT_VALUES_EQUAL(*ev->Get()->Snapshot, snapshot);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.size(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.front(), tableId);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.back(), copyTableId);
            auto alive = std::make_shared<std::atomic<bool>>(true);
            auto response = MakeHolder<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>(
                "/MyRoot/Tenant", snapshot, NKqp::TSnapshotHandle(alive), std::move(ev->Get()->Orbit));
            runtime.Send(new IEventHandle(ev->Sender, service, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            if (ev->Get()->Record.GetRequest().GetDatabase() != "/MyRoot/Tenant") {
                return;
            }
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            UNIT_ASSERT_VALUES_EQUAL(*ev->Get()->GetMvccVersion(), NKqp::FormatMvccVersion(snapshot));
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
            return starts.size() == 1 && retentions >= 1;
        });
        if (Reboot) {
            starts.clear();
            const auto shards =
                GetTableShards(runtime, schemeShardId, Sprintf("/MyRoot/Tenant/export-%" PRIu64 "/0", exportId));
            RebootTablet(runtime, shards.front(), runtime.AllocateEdgeActor());
            runtime.WaitFor("migrated table checksum restarts", [&] {
                return starts.size() == 1 && retentions >= 2;
            });
        }
        const auto confirmed = ConfirmTableChecksumSnapshot(runtime, tableId, schemeShardId, snapshot);
        UNIT_ASSERT_VALUES_EQUAL_C(confirmed->Get()->Status, Ydb::StatusIds::SUCCESS, confirmed->Get()->Error);
        starts.Stop().Unblock();
        Env().TestWaitNotification(runtime, exportId, schemeShardId);
        TestGetExport(runtime, schemeShardId, exportId, "/MyRoot/Tenant", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(queries, 1);
        UNIT_ASSERT_GE(retentions, Reboot ? 2u : 1u);
        const auto metadata = IsFs ? TFileInput(directory.Path().GetPath() + "/backup/migrated/metadata.json").ReadAll()
                                   : GetS3FileContent("/backup/migrated/metadata.json");
        const auto checksum = NBackup::TMetadata::Deserialize(metadata, true).GetTableChecksum();
        UNIT_ASSERT(checksum.has_value());
        UNIT_ASSERT(!checksum->Error.has_value());
        UNIT_ASSERT_VALUES_EQUAL(checksum->Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(checksum->Hash, "0000000000000000");
    }

    Y_UNIT_TEST(TableChecksumYtRecoveryDoesNotRetainSnapshots) {
        Env();
        auto& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        TestCreateTable(runtime, 100, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(runtime, 100);
        TBlockEvents<TEvDataShard::TEvProposeTransaction> snapshots(runtime, [](const auto& ev) {
            if (ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return false;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            return tx.HasSendSnapshot();
        });
        TestExport(runtime, 101, "/MyRoot", R"(
            ExportToYtSettings {
                host: "localhost"
                token: "token"
                items { source_path: "/MyRoot/Table" destination_path: "//backup/table" }
            }
        )");
        runtime.WaitFor("YT copy prepared before source snapshot", [&] {
            return snapshots.size() == 1;
        });
        NKikimrTxDataShard::TFlatSchemeTransaction first;
        UNIT_ASSERT(first.ParseFromString(snapshots.front()->Get()->Record.GetTxBody()));
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        runtime.WaitFor("YT copy reconfigured after restart", [&] {
            for (const auto& request : snapshots) {
                NKikimrTxDataShard::TFlatSchemeTransaction tx;
                UNIT_ASSERT(tx.ParseFromString(request->Get()->Record.GetTxBody()));
                if (tx.GetSeqNo().GetGeneration() > first.GetSeqNo().GetGeneration()) {
                    return true;
                }
            }
            return false;
        });
        for (const auto& request : snapshots) {
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(request->Get()->Record.GetTxBody()));
            UNIT_ASSERT(!tx.GetSendSnapshot().HasTableChecksumExportId());
            UNIT_ASSERT(tx.GetReadOnly());
        }
    }

    Y_UNIT_TEST(TableChecksumStaleSourceTimeoutDuringCopyConfirmation) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        auto& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        TActorId actor;
        auto starts = runtime.AddObserver<NBackup::TEvTableChecksum::TEvStart>([&](auto& ev) {
            actor = ev->GetRecipientRewrite();
        });
        ui64 sourceRequestId = 0;
        auto requests = runtime.AddObserver<TEvSchemeShard::TEvDescribeScheme>([&](auto& ev) {
            if (ev->Sender == actor && ev->Get()->Record.GetPath() == "/MyRoot/Table") {
                sourceRequestId = ev->Cookie;
            }
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvSnapshotConfirmed> confirmations(runtime, [&](auto& ev) {
            return ev->GetRecipientRewrite() == actor;
        });
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(*response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum());
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        const auto id = StartCompletionMarkerExport();
        runtime.WaitFor("copy capture confirmation waits", [&] {
            return confirmations.size() == 1;
        });
        UNIT_ASSERT(sourceRequestId);
        const auto copyReader = confirmations.front()->Sender;
        runtime.Send(
            new IEventHandle(actor, runtime.AllocateEdgeActor(), new TEvents::TEvWakeup(sourceRequestId)), 0, true);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        confirmations.Unblock(1);
        runtime.WaitFor("source confirmation still precedes the query", [&] {
            return !confirmations.empty() || queries;
        });
        UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
        UNIT_ASSERT_VALUES_UNEQUAL(confirmations.front()->Sender, copyReader);
        UNIT_ASSERT_VALUES_EQUAL(confirmations.front()->Get()->Status, Ydb::StatusIds::SUCCESS);
        confirmations.Stop().Unblock();
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(queries, 1u);
    }

    Y_UNIT_TEST(TableChecksumSourceLookupRetries) {
        CheckTableChecksumSourceUnavailable(false);
    }

    Y_UNIT_TEST(TableChecksumSourceLookupTimeout) {
        CheckTableChecksumSourceUnavailable(true);
    }

    Y_UNIT_TEST_TWIN(TableChecksumMissingDataShardPin, Unsupported) {
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        ui32 responses = 0;
        auto snapshots = runtime.AddObserver<TEvDataShard::TEvRefreshVolatileSnapshotResponse>([&](auto& ev) {
            ++responses;
            if (Unsupported) {
                ev->Get()->Record.ClearSupportsTableChecksumSnapshots();
            } else if (!ev->Get()->Record.HasTableChecksumSnapshot()) {
                // Copy capture evidence must not hide a lost source checkpoint
                ev->Get()->Record.SetStatus(NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse::SNAPSHOT_NOT_FOUND);
            }
        });
        const auto id = StartCompletionMarkerExport();
        Env().TestWaitNotification(runtime, id);
        const auto status =
            TestGetExport(runtime, id, "/MyRoot", Unsupported ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_GT(responses, 0);
        if (Unsupported) {
            UNIT_ASSERT(!NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true)
                             .GetTableChecksum()
                             .has_value());
            CheckCompletionMarkerContents(id);
        } else {
            UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "SNAPSHOT_NOT_FOUND");
            UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
            UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
        }
    }

    Y_UNIT_TEST_QUAD(TableChecksumLegacyCopyCapture, LegacySource, RebootSchemeShard) {
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        TSet<ui64> copyTargets;
        ui64 legacyTarget = 0;
        auto sources = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (!LegacySource || ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            if (!tx.HasSendSnapshot()) {
                return;
            }
            auto* snapshot = tx.MutableSendSnapshot();
            const auto target = snapshot->GetSendTo(0).GetShard();
            if (copyTargets.insert(target).second && copyTargets.size() == 2) {
                legacyTarget = target;
            }
            if (target == legacyTarget) {
                // An old source ignores the checksum-capture request
                snapshot->ClearTableChecksumExportId();
                ev->Get()->Record.SetTxBody(tx.SerializeAsString());
            }
        });
        TBlockEvents<TEvDataShard::TEvProposeTransaction> backups(runtime, [](auto& ev) {
            if (ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return false;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            return tx.HasBackup();
        });
        ui32 starts = 0;
        auto checksums = runtime.AddObserver<NBackup::TEvTableChecksum::TEvStart>([&](auto&) {
            ++starts;
        });
        ui32 queries = 0;
        auto queriesStarted = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto&) {
            ++queries;
        });
        const auto id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("both copy shards wait before the backup scan", [&] {
            return backups.size() == 2;
        });
        const auto copyPath = Sprintf("/MyRoot/export-%" PRIu64 "/0", id);
        const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, copyPath);
        NKikimrTxDataShard::TFlatSchemeTransaction task;
        UNIT_ASSERT(task.ParseFromString(backups.front()->Get()->Record.GetTxBody()));
        const auto& backup = task.GetBackup();
        const auto tableId = backup.GetTableId();
        const TRowVersion version(backup.GetSnapshotStep(), backup.GetSnapshotTxId());
        for (ui64 shard : shards) {
            if (!LegacySource) {
                // Simulate a copy completed by an old destination and subsequently upgraded
                const auto stored = ReadSystemTable(runtime, shard, "UserTables", { "Tid" }, { "Schema" });
                const auto rows = NClient::TValue::Create(stored)["Result"]["List"];
                UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 1u);
                NKikimrSchemeOp::TTableDescription schema;
                UNIT_ASSERT(schema.ParseFromString(static_cast<TString>(rows[0]["Schema"])));
                UNIT_ASSERT(schema.HasTableChecksumSnapshot());
                schema.ClearTableChecksumSnapshot();
                TStringStream escaped;
                NYql::EscapeArbitraryAtom(schema.SerializeAsString(), '"', &escaped);
                NKikimrMiniKQL::TResult result;
                TString error;
                UNIT_ASSERT_VALUES_EQUAL_C(LocalMiniKQL(runtime, shard, Sprintf(R"((
                    (let key '('('Tid (Uint64 '%lu))))
                    (let row '('('Schema (String '%s))))
                    (return (AsList (UpdateRow 'UserTables key row)))
                ))", tableId, escaped.Str().c_str()), result, error), NKikimrProto::OK, error);
            }
            if (!LegacySource || RebootSchemeShard) {
                RebootTablet(runtime, shard, runtime.AllocateEdgeActor());
            }
            if (!LegacySource) {
                const auto edge = runtime.AllocateEdgeActor();
                auto discard = MakeHolder<TEvDataShard::TEvDiscardVolatileSnapshotRequest>();
                discard->Record.SetOwnerId(TTestTxConfig::SchemeShard);
                discard->Record.SetPathId(tableId);
                discard->Record.SetStep(version.Step);
                discard->Record.SetTxId(version.TxId);
                runtime.SendToPipe(shard, edge, discard.Release());
                const auto response =
                    runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDiscardVolatileSnapshotResponse>(edge);
                UNIT_ASSERT_C(response->Get()->Record.GetStatus() ==
                                  NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse::DISCARDED,
                    response->Get()->Record.DebugString());
            }
        }
        if (RebootSchemeShard) {
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        }
        backups.Stop().Unblock();
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(starts, 1u);
        UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
        const auto metadata = NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true);
        UNIT_ASSERT(!metadata.GetTableChecksum().has_value());
        UNIT_ASSERT(HasS3File("/backup/table/data_00.csv"));
        UNIT_ASSERT(HasS3File("/backup/table/data_01.csv"));
        CheckCompletionMarkerContents(id);
    }

    Y_UNIT_TEST(TableChecksumCopyCaptureVersionMismatch) {
        Env();
        auto& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        auto responses = runtime.AddObserver<TEvDataShard::TEvRefreshVolatileSnapshotResponse>([&](auto& ev) {
            if (ev->Get()->Record.HasTableChecksumSnapshot()) {
                auto* snapshot = ev->Get()->Record.MutableTableChecksumSnapshot();
                snapshot->SetTxId(snapshot->GetTxId() + 1);
            }
        });
        const auto id = StartCompletionMarkerExport();
        Env().TestWaitNotification(runtime, id);
        const auto status = TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "capture does not match");
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
    }

    Y_UNIT_TEST_QUAD(TableChecksumRetentionApiOwnsSourceAndCopy, IsFs, Cancel) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableFsBackups(true);
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        const auto service = runtime.AllocateEdgeActor();
        runtime.RegisterService(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId()), service);

        struct TLease {
            TActorId Actor;
            TVector<TTableId> Tables;
            TRowVersion Version;
            std::shared_ptr<std::atomic<bool>> Alive;
        };

        TVector<TLease> leases;
        ui32 registered = 0;
        auto acquire =
            runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>([&](auto& ev) {
                UNIT_ASSERT(ev->Get()->Snapshot.Defined());
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->DatabaseName, "/MyRoot");
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.size(), 2u);
                auto alive = std::make_shared<std::atomic<bool>>(true);
                leases.push_back({ ev->Sender, ev->Get()->TableIds, *ev->Get()->Snapshot, alive });
                auto response = MakeHolder<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>(
                    "/MyRoot", leases.back().Version, NKqp::TSnapshotHandle(alive), std::move(ev->Get()->Orbit));
                runtime.Send(new IEventHandle(ev->Sender, service, response.Release(), 0, ev->Cookie), 0, true);
                ev.Reset();
            });
        auto handles = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>([&](auto&) {
            ++registered;
        });
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            UNIT_ASSERT(ev->Get()->GetMvccVersion());
            UNIT_ASSERT_VALUES_EQUAL(*ev->Get()->GetMvccVersion(), NKqp::FormatMvccVersion(leases.front().Version));
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(*response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum());
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        for (ui32 idx = 0; idx < 2; ++idx) {
            const ui64 txId = 100 + idx;
            TestCreateTable(runtime, txId, "/MyRoot", Sprintf(R"(
                Name: "Table%u"
                Columns { Name: "key" Type: "Uint32" }
                KeyColumnNames: ["key"]
            )", idx));
            Env().TestWaitNotification(runtime, txId);
        }
        TTempDir directory;
        NKikimrExport::TCreateExportRequest request;
        for (ui32 idx = 0; idx < 2; ++idx) {
            const auto source = Sprintf("/MyRoot/Table%u", idx);
            const auto destination = Sprintf("backup/table%u", idx);
            if (IsFs) {
                auto* settings = request.MutableExportToFsSettings();
                settings->set_base_path(directory.Path());
                auto* item = settings->add_items();
                item->set_source_path(source);
                item->set_destination_path(destination);
            } else {
                auto* settings = request.MutableExportToS3Settings();
                settings->set_endpoint(Sprintf("localhost:%d", S3Port()));
                settings->set_scheme(Ydb::Export::ExportToS3Settings::HTTP);
                auto* item = settings->add_items();
                item->set_source_path(source);
                item->set_destination_prefix(destination);
            }
        }
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        using namespace NKikimr::NWrappers::NExternalStorage;
        TBlockEvents<TEvPutObjectRequest> metadata(runtime, [](auto& ev) {
            const TString key(ev->Get()->Request.GetKey());
            return key.EndsWith("backup/table0/metadata.json") || key.EndsWith("backup/table1/metadata.json");
        });
        TestExport(runtime, 102, "/MyRoot", request.DebugString());
        runtime.WaitFor("each checksum actor retains its source and copy before the query", [&] {
            return leases.size() == 2 && registered == 2 && starts.size() == 2;
        });
        UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
        UNIT_ASSERT_VALUES_EQUAL(leases.front().Version, leases.back().Version);
        THashMap<TActorId, TActorId> uploaders;
        for (const auto& start : starts) {
            uploaders.emplace(start->GetRecipientRewrite(), start->Sender);
        }
        for (ui32 idx = 0; idx < 2; ++idx) {
            const auto source = DescribePath(runtime, Sprintf("/MyRoot/Table%u", idx)).GetPathDescription().GetSelf();
            const auto copy =
                DescribePath(runtime, Sprintf("/MyRoot/export-102/%u", idx), true, true).GetPathDescription().GetSelf();
            const TPathId sourceId(source.GetSchemeshardId(), source.GetPathId());
            const TPathId copyId(copy.GetSchemeshardId(), copy.GetPathId());
            const auto lease = FindIf(leases, [&](const auto& lease) {
                return lease.Tables.front().PathId == sourceId;
            });
            UNIT_ASSERT(lease != leases.end());
            UNIT_ASSERT_VALUES_EQUAL(lease->Tables.back().PathId, copyId);
            UNIT_ASSERT_VALUES_EQUAL(copy.GetCreateStep(), lease->Version.Step);
            UNIT_ASSERT(lease->Alive->load());
            UNIT_ASSERT(uploaders.contains(lease->Actor));
            const auto confirmed =
                ConfirmTableChecksumSnapshot(runtime, TTableId(copyId), TTestTxConfig::SchemeShard, lease->Version);
            UNIT_ASSERT_VALUES_EQUAL_C(confirmed->Get()->Status, Ydb::StatusIds::SUCCESS, confirmed->Get()->Error);
        }
        if (Cancel) {
            TestCancelExport(runtime, 103, "/MyRoot", 102);
            Env().TestWaitNotification(runtime, 102);
            runtime.WaitFor("cancellation releases both checksum actor handles", [&] {
                return AllOf(leases, [](const auto& lease) {
                    return !lease.Alive->load();
                });
            });
            UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
            TestGetExport(runtime, 102, "/MyRoot", Ydb::StatusIds::CANCELLED);
            return;
        }
        starts.Stop().Unblock();
        runtime.WaitFor("both checksum results are ready while metadata upload waits", [&] {
            return queries == 2 && metadata.size() == 2;
        });
        UNIT_ASSERT(AllOf(leases, [](const auto& lease) {
            return lease.Alive->load();
        }));
        const auto firstUploader = metadata.front()->Sender;
        const auto first = FindIf(leases, [&](const auto& lease) {
            return uploaders.at(lease.Actor) == firstUploader;
        });
        UNIT_ASSERT(first != leases.end());
        metadata.Stop().Unblock(1);
        runtime.WaitFor("finishing one upload releases only its handle", [&] {
            return !first->Alive->load();
        });
        for (const auto& lease : leases) {
            if (lease.Actor != first->Actor) {
                UNIT_ASSERT(lease.Alive->load());
            }
        }
        metadata.Stop().Unblock();
        Env().TestWaitNotification(runtime, 102);
        runtime.WaitFor("all export retention handles are released", [&] {
            return AllOf(leases, [](const auto& lease) {
                return !lease.Alive->load();
            });
        });
        TestGetExport(runtime, 102, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TableChecksumSourceVersionSurvivesImmediateWrite) {
        Env();
        auto& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        flags.SetEnableExportAutoDropping(true);
        TestCreateTable(runtime, 100, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        Env().TestWaitNotification(runtime, 100);
        const auto description = DescribePath(runtime, "/MyRoot/Table", true, true);
        const TTableId tableId(TTestTxConfig::SchemeShard, description.GetPathId());
        const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
        const ui64 shard = shards.front();
        UpdateRow(runtime, "Table", 1, "first", shard);
        UpdateRow(runtime, "Table", 2, "second", shard);

        TActorId sourceExecutor;
        THashSet<ui64> sourceTasks;
        auto submissions = runtime.AddObserver<NResourceBroker::TEvResourceBroker::TEvSubmitTask>([&](auto& ev) {
            if (ev->Get()->Task.TabletId == shard) {
                sourceExecutor = ev->Sender;
                sourceTasks.insert(ev->Get()->Task.TaskId);
            }
        });
        TBlockEvents<NResourceBroker::TEvResourceBroker::TEvResourceAllocated> allocations(
            runtime, [&](const auto& ev) {
                return ev->GetRecipientRewrite() == sourceExecutor && sourceTasks.contains(ev->Get()->TaskId);
            });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        TestExport(runtime, 101, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items { source_path: "/MyRoot/Table" destination_prefix: "backup/table" }
            }
        )", S3Port()));
        runtime.WaitFor("source copy snapshot compaction is paused", [&] {
            return !allocations.empty();
        }, TDuration::Seconds(5));
        const auto pinState =
            ReadSystemTable(runtime, shard, "Snapshots", { "Oid", "Tid", "Step", "TxId" }, { "Step", "TxId" });
        const auto pins = NKikimr::NClient::TValue::Create(pinState)["Result"]["List"];
        UNIT_ASSERT_VALUES_EQUAL(pins.Size(), 1);
        const TRowVersion version(static_cast<ui64>(pins[0]["Step"]), static_cast<ui64>(pins[0]["TxId"]));

        const auto writer = runtime.AllocateEdgeActor();
        bool completedWhilePaused = false;
        bool replied = false;
        bool paused = true;
        auto replies = runtime.AddObserver<TEvDataShard::TEvUploadRowsResponse>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == writer) {
                replied = true;
                completedWhilePaused = paused;
            }
        });
        auto upload = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        upload->Record.SetTableId(tableId.PathId.LocalPathId);
        upload->Record.MutableRowScheme()->AddKeyColumnIds(1);
        upload->Record.MutableRowScheme()->AddValueColumnIds(2);
        const TVector<TCell> keys{ TCell::Make(ui32(3)) };
        const TVector<TCell> values{ TCell("later", 5) };
        auto& row = *upload->Record.AddRows();
        row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
        row.SetValueColumns(TSerializedCellVec::Serialize(values));
        ForwardToTablet(runtime, shard, writer, upload.Release());
        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        paused = false;
        allocations.Stop().Unblock();
        runtime.WaitFor("ordinary bulk upsert completed", [&] {
            return replied;
        }, TDuration::Seconds(5));
        const auto written = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvUploadRowsResponse>(writer);
        UNIT_ASSERT_C(written->Get()->Record.GetStatus() == NKikimrTxDataShard::TError::OK,
            written->Get()->Record.GetErrorDescription());
        Cerr << "Ordinary write completed while copy paused: " << completedWhilePaused << Endl;
        UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, tableId, shards, TRowVersion::Max()), 3);
        // The first user-data read at C must happen after the write, so it cannot protect C itself
        UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, tableId, shards, version), 2);

        runtime.WaitFor("checksum waits after the copy", [&] {
            return !starts.empty();
        });
        const auto& start = starts.front();
        runtime.Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(
                101, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(2, "0000000000000011"), "")));
        runtime.Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
        Env().TestWaitNotification(runtime, 101);
        TestGetExport(runtime, 101, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_TWIN(TableChecksumDurableRetentionAfterRestart, RebootDataShards) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        TMaybe<TRowVersion> version;
        TTableId tableId;
        TTableId copyTableId;
        auto proposals = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            if (tx.HasReceiveSnapshot()) {
                const auto& receive = tx.GetReceiveSnapshot();
                UNIT_ASSERT(receive.GetTableChecksumExportId());
                copyTableId = TTableId(receive.GetTableId().GetOwnerId(), receive.GetTableId().GetTableId());
            }
            if (tx.HasBackup() && tx.GetBackup().HasTableChecksum() && tx.GetBackup().GetShardNum() == 0) {
                const auto& task = tx.GetBackup();
                version = TRowVersion(task.GetSnapshotStep(), task.GetSnapshotTxId());
                tableId = TTableId(task.GetTable().GetSelf().GetSchemeshardId(), task.GetTable().GetSelf().GetPathId());
            }
        });
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            UNIT_ASSERT(version);
            UNIT_ASSERT_VALUES_EQUAL(*ev->Get()->GetMvccVersion(), NKqp::FormatMvccVersion(*version));
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(
                *response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(2, "0000000000000011"));
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        TBlockEvents<NKikimr::NWrappers::NExternalStorage::TEvPutObjectRequest> data(runtime, [](const auto& ev) {
            return TString(ev->Get()->Request.GetKey()).EndsWith(".csv");
        });
        const auto id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("checksum is waiting to start", [&] {
            return version && starts.size() == 1;
        });
        const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, tableId, shards, *version), 2);
        UNIT_ASSERT(copyTableId);
        const auto copyConfirmed =
            ConfirmTableChecksumSnapshot(runtime, copyTableId, TTestTxConfig::SchemeShard, *version);
        UNIT_ASSERT_VALUES_EQUAL_C(copyConfirmed->Get()->Status, Ydb::StatusIds::SUCCESS, copyConfirmed->Get()->Error);
        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletion> ownerChecks(runtime, [&](const auto& ev) {
            return ev->Get()->Record.GetTxId() == id;
        });
        runtime.AdvanceCurrentTime(NBackup::TableChecksumSnapshotTimeout + TDuration::Minutes(1));
        if (RebootDataShards) {
            for (ui64 shard : shards) {
                RebootTablet(runtime, shard, runtime.AllocateEdgeActor());
            }
            const auto copyShards =
                GetTableShards(runtime, TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
            for (ui64 shard : copyShards) {
                RebootTablet(runtime, shard, runtime.AllocateEdgeActor());
            }
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        const auto confirmed = ConfirmTableChecksumSnapshot(runtime, tableId, TTestTxConfig::SchemeShard, *version);
        UNIT_ASSERT_VALUES_EQUAL_C(confirmed->Get()->Status, Ydb::StatusIds::SUCCESS, confirmed->Get()->Error);
        UNIT_ASSERT_VALUES_EQUAL(confirmed->Get()->Snapshot, *version);
        UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, tableId, shards, *version), 2);
        const auto recoveredCopy =
            ConfirmTableChecksumSnapshot(runtime, copyTableId, TTestTxConfig::SchemeShard, *version);
        UNIT_ASSERT_VALUES_EQUAL_C(recoveredCopy->Get()->Status, Ydb::StatusIds::SUCCESS, recoveredCopy->Get()->Error);
        ownerChecks.Stop().Unblock();
        data.Stop().Unblock();
        starts.Stop().Unblock();
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(queries, 1);
        CheckCompletionMarkerContents(id);
        runtime.SimulateSleep(NBackup::TableChecksumSnapshotTimeout + TDuration::Minutes(1));
        const auto released = ConfirmTableChecksumSnapshot(runtime, tableId, TTestTxConfig::SchemeShard, *version);
        UNIT_ASSERT_VALUES_EQUAL(released->Get()->Status, Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT_STRING_CONTAINS(released->Get()->Error, "SNAPSHOT_NOT_FOUND");
    }

    Y_UNIT_TEST_TWIN(TableChecksumReleasesIndexSourceAfterCopy, OwnsPin) {
        EnvOptions().EnableIndexMaterialization(true);
        auto& env = Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableExportAutoDropping(false);
        auto boots = runtime.AddObserver<TEvTablet::TEvBoot>([&](auto& ev) {
            if constexpr (!OwnsPin) {
                if (ev->Get()->TabletStorageInfo->TabletType == TTabletTypes::DataShard) {
                    runtime.EnableScheduleForActor(ev->GetRecipientRewrite());
                }
            }
        });
        TestCreateIndexedTable(runtime, 100, "/MyRoot", R"(
            TableDescription {
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            }
            IndexDescription { Name: "index" KeyColumnNames: ["value"] Type: EIndexTypeGlobal }
        )");
        env.TestWaitNotification(runtime, 100);
        const TString source = "/MyRoot/Table";
        const TString index = source + "/index/indexImplTable";
        const ui32 key = 1;
        const TString value = "row1";
        WriteRow(runtime, 101, source, 0, key, value);
        // Direct test writes bypass synchronous index maintenance
        const auto indexDesc = DescribePrivatePath(runtime, index, true);
        const auto& keyIds = indexDesc.GetPathDescription().GetTable().GetKeyColumnIds();
        UploadRow(runtime, indexDesc, 0, TVector<ui32>(keyIds.begin(), keyIds.end()), {},
            { TCell(value.data(), value.size()), TCell::Make(key) }, {});
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> indexBackups(runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() == 1 && record.GetTransaction(0).HasBackup() &&
                   record.GetTransaction(0).GetBackup().GetTableName() == "indexImplTable";
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> checksums(runtime);
        constexpr ui64 id = 102;
        auto backupTasks = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(record.GetTxBody()));
            if (tx.HasBackup() && tx.GetBackup().GetTableName() == "indexImplTable") {
                UNIT_ASSERT_VALUES_EQUAL(tx.GetTableChecksumExportId(), id);
                if constexpr (!OwnsPin) {
                    tx.SetTableChecksumExportId(id + 1);
                    record.SetTxBody(tx.SerializeAsString());
                }
            }
        });
        TestExport(runtime, id, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                include_index_data: true
                items { source_path: "/MyRoot/Table" destination_prefix: "table" }
            }
        )", S3Port()));
        runtime.WaitFor("index backup is held after copy completion", [&] {
            return !indexBackups.empty() && !checksums.empty();
        });
        const TString copy = TStringBuilder() << "/MyRoot/export-" << id << "/0/index/indexImplTable";
        const auto copyDescription = DescribePrivatePath(runtime, copy);
        const auto& copyPath = copyDescription.GetPathDescription().GetSelf();
        const TRowVersion version(copyPath.GetCreateStep(), copyPath.GetCreateTxId());
        UNIT_ASSERT(version.Step && version.TxId);
        const auto confirmPin = [&](const TString& path, bool retained, bool reboot = false) {
            using TResponse = NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse;
            const auto table = DescribePrivatePath(runtime, path, true);
            const auto& partitions = table.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT(!partitions.empty());
            const auto edge = runtime.AllocateEdgeActor();
            for (const auto& part : partitions) {
                if (reboot) {
                    RebootTablet(runtime, part.GetDatashardId(), edge);
                }
                auto refresh = MakeHolder<TEvDataShard::TEvRefreshVolatileSnapshotRequest>();
                refresh->Record.SetOwnerId(TTestTxConfig::SchemeShard);
                refresh->Record.SetPathId(table.GetPathId());
                refresh->Record.SetStep(version.Step);
                refresh->Record.SetTxId(version.TxId);
                runtime.SendToPipe(part.GetDatashardId(), edge, refresh.Release());
                const auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvRefreshVolatileSnapshotResponse>(edge);
                const auto expected = retained ? TResponse::REFRESHED : TResponse::SNAPSHOT_NOT_FOUND;
                UNIT_ASSERT_C(reply->Get()->Record.GetStatus() == expected,
                    path << ": " << reply->Get()->Record.ShortDebugString());
            }
        };
        confirmPin(source, true);
        confirmPin(index, false);
        confirmPin(copy, true);
        confirmPin(index, false, true);
        confirmPin(copy, true, true);

        const auto& start = checksums.front();
        runtime.Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(
                id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(1, "0000000000000011"), "")));
        runtime.Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
        indexBackups.Stop().Unblock();
        env.TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(GetS3FileContent("/table/index/indexImplTable/data_00.csv"), value);
        confirmPin(copy, !OwnsPin, true);
        if constexpr (!OwnsPin) {
            const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, copy);
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
            const auto shardActor = ResolveTablet(runtime, shards.front(), 0, false);
            TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> completion(runtime, [&](const auto& ev) {
                return ev->Get()->Record.GetTxId() == id && ev->GetRecipientRewrite() == shardActor;
            });
            const auto waitForCompletion = [&](const char* description) {
                runtime.AdvanceCurrentTime(NBackup::TableChecksumSnapshotTimeout + TDuration::Seconds(1));
                runtime.WaitFor(description, [&] {
                    return !completion.empty();
                }, TDuration::Seconds(30));
            };
            waitForCompletion("orphan pin receives completion");
            completion.clear();
            waitForCompletion("orphan pin retries lost completion");
            completion.Stop().Unblock();
            confirmPin(copy, false);
        }
    }

    Y_UNIT_TEST(TableChecksumRejectsRemovedCopySnapshot) {
        Env();
        auto& runtime = Runtime();
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        ui64 copyTxId = 0;
        auto proposals = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            if (tx.HasSendSnapshot() && tx.GetSendSnapshot().GetTableChecksumExportId()) {
                copyTxId = ev->Get()->Record.GetTxId();
            }
        });
        auto readsets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetTxId() != copyTxId || !record.GetReadSet().StartsWith(TStringBuf("\x01SRS", 4))) {
                return;
            }
            NKikimrTxDataShard::TSnapshotTransferReadSet readset;
            UNIT_ASSERT(readset.ParseFromString(record.GetReadSet().substr(4)));
            const TRowVersion version(record.GetStep(), copyTxId);
            auto* removed = readset.MutableRemovedRowVersions()->AddRanges();
            version.ToProto(removed->MutableLowerInclusive());
            version.Next().ToProto(removed->MutableUpperExclusive());
            record.SetReadSet(TString("\x01SRS", 4) + readset.SerializeAsString());
        });
        const ui64 id = StartCompletionMarkerExport(true, true);
        Env().TestWaitNotification(runtime, id);
        const auto result = TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(
            result.DebugString(), "Retained export snapshot is no longer available on the table copy");
        UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
    }

    Y_UNIT_TEST_QUAD(TableChecksumCopySnapshotRecoveryWithoutLocalPin, InvalidVersion, WithoutChecksumTask) {
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        ui64 backupTxId = 0;
        TTableId copyTableId;
        TRowVersion version = TRowVersion::Min();
        auto proposals = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            if (tx.HasBackup() && tx.GetBackup().HasTableChecksum() && tx.GetBackup().GetShardNum() == 0) {
                backupTxId = ev->Get()->Record.GetTxId();
                const auto& task = tx.GetBackup();
                copyTableId = TTableId(TTestTxConfig::SchemeShard, task.GetTableId());
                version = TRowVersion(task.GetSnapshotStep(), task.GetSnapshotTxId());
            }
        });
        bool failed = false;
        auto failures = runtime.AddObserver<TEvDataShard::TEvSchemaChanged>([&](auto& ev) {
            if (ev->Get()->Record.GetTxId() == backupTxId && !ev->Get()->Record.GetOpResult().GetSuccess()) {
                failed = true;
            }
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        const ui64 id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("checksum waits before copy recovery", [&] {
            return version.Step && !starts.empty();
        });
        const auto shards =
            GetTableShards(runtime, TTestTxConfig::SchemeShard, Sprintf("/MyRoot/export-%" PRIu64 "/0", id));
        const ui64 shard = shards.front();
        const auto edge = runtime.AllocateEdgeActor();
        auto discard = MakeHolder<TEvDataShard::TEvDiscardVolatileSnapshotRequest>();
        discard->Record.SetOwnerId(copyTableId.PathId.OwnerId);
        discard->Record.SetPathId(copyTableId.PathId.LocalPathId);
        discard->Record.SetStep(version.Step);
        discard->Record.SetTxId(version.TxId);
        ForwardToTablet(runtime, shard, edge, discard.Release());
        const auto discarded = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDiscardVolatileSnapshotResponse>(edge);
        UNIT_ASSERT_C(
            discarded->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse::DISCARDED,
            discarded->Get()->Record.DebugString());
        if (InvalidVersion || WithoutChecksumTask) {
            const auto state =
                ReadSystemTable(runtime, shard, "TxDetails", { "TxId", "Origin" }, { "TxId", "Origin", "Body" });
            const auto rows = NKikimr::NClient::TValue::Create(state)["Result"]["List"];
            TString body;
            for (size_t idx = 0; idx < rows.Size(); ++idx) {
                const auto row = rows[static_cast<int>(idx)];
                if (static_cast<ui64>(row["TxId"]) == backupTxId && static_cast<ui64>(row["Origin"]) == shard) {
                    body = static_cast<TString>(row["Body"]);
                }
            }
            UNIT_ASSERT(!body.empty());
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(body));
            if (InvalidVersion) {
                tx.MutableBackup()->SetSnapshotStep(0);
            }
            if (WithoutChecksumTask) {
                // Index implementation backups have no checksum calculation task
                tx.MutableBackup()->ClearTableChecksum();
            }
            TStringStream escaped;
            NYql::EscapeArbitraryAtom(tx.SerializeAsString(), '"', &escaped);
            NKikimrMiniKQL::TResult result;
            TString error;
            const auto status = LocalMiniKQL(runtime, shard, Sprintf(R"((
                (let key '('('TxId (Uint64 '%lu)) '('Origin (Uint64 '%lu))))
                (let row '('('Body (String '%s))))
                (return (AsList (UpdateRow 'TxDetails key row)))
            ))", backupTxId, shard, escaped.Str().c_str()), result, error);
            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::OK, error);
        }
        starts.clear();
        RebootTablet(runtime, shard, runtime.AllocateEdgeActor());
        if (WithoutChecksumTask && !InvalidVersion) {
            Env().TestWaitNotification(runtime, id);
            TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(!failed);
            UNIT_ASSERT(starts.empty());
            return;
        }
        runtime.WaitFor("recovered copy starts a checksum or reports its missing version", [&] {
            return failed || !starts.empty();
        }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(failed, InvalidVersion);
        if (!InvalidVersion) {
            const auto& start = starts.front();
            runtime.Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
                new NBackup::TEvTableChecksum::TEvResult(
                    id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(2, "0000000000000011"), "")));
            runtime.Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
        }
        Env().TestWaitNotification(runtime, id);
        const auto result =
            TestGetExport(runtime, id, "/MyRoot", InvalidVersion ? Ydb::StatusIds::CANCELLED : Ydb::StatusIds::SUCCESS);
        if (InvalidVersion) {
            UNIT_ASSERT_STRING_CONTAINS(
                result.DebugString(), "Retained export snapshot is no longer available on the table copy");
        }
    }

    Y_UNIT_TEST_TWIN(TableChecksumCopyPinIsStoredAtCompletion, RebootCopy) {
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        ui64 copyTxId = 0;
        ui64 copyShard = 0;
        TTableId copyTableId;
        TRowVersion version = TRowVersion::Min();
        THashMap<ui64, TActorId> actors;
        auto boots = runtime.AddObserver<TEvTablet::TEvBoot>([&](auto& ev) {
            actors[ev->Get()->TabletID] = ev->GetRecipientRewrite();
        });
        auto proposals = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            if (ev->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(ev->Get()->Record.GetTxBody()));
            if (tx.HasReceiveSnapshot()) {
                copyTxId = ev->Get()->Record.GetTxId();
                const auto& table = tx.GetReceiveSnapshot().GetTableId();
                copyTableId = TTableId(table.GetOwnerId(), table.GetTableId());
            }
        });
        auto readsets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetTxId() != copyTxId || !record.GetReadSet().StartsWith(TStringBuf("\x01SRS", 4))) {
                return;
            }
            if (!copyShard) {
                copyShard = record.GetTabletDest();
                version = TRowVersion(record.GetStep(), copyTxId);
            }
            NKikimrTxDataShard::TSnapshotTransferReadSet readset;
            UNIT_ASSERT(readset.ParseFromString(record.GetReadSet().substr(4)));
            // Force the durable ReceiveSnapshot boundary even when the source has no open transaction
            readset.SetWithOpenTxs(true);
            record.SetReadSet(TString("\x01SRS", 4) + readset.SerializeAsString());
        });
        size_t allowProgress = 1;
        TVector<TAutoPtr<IEventHandle>> blocked;
        auto progress = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (copyShard && actors.contains(copyShard) && ev->GetRecipientRewrite() == actors.at(copyShard) &&
                ev->GetTypeRewrite() == EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0 /* EvProgressTransaction */) {
                if (allowProgress) {
                    --allowProgress;
                } else {
                    blocked.emplace_back(std::move(ev));
                }
            }
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        TBlockEvents<NKikimr::NWrappers::NExternalStorage::TEvPutObjectRequest> data(runtime, [](const auto& ev) {
            return TString(ev->Get()->Request.GetKey()).EndsWith(".csv");
        });
        const ui64 id = StartCompletionMarkerExport(true, true);
        runtime.WaitFor("copy paused after receiving its parts", [&] {
            return !blocked.empty();
        });
        const auto checkState = [&](bool pending) {
            const auto tableState = ReadSystemTable(runtime, copyShard, "UserTables", { "Tid" }, { "Tid" });
            const auto tables = NKikimr::NClient::TValue::Create(tableState)["Result"]["List"];
            bool created = false;
            for (size_t idx = 0; idx < tables.Size(); ++idx) {
                created |= static_cast<ui64>(tables[static_cast<int>(idx)]["Tid"]) == copyTableId.PathId.LocalPathId;
            }
            UNIT_ASSERT_C(created, "Copy must be paused after receiving its table, not before CreateTable");
            const auto transactionState = ReadSystemTable(runtime, copyShard, "TxMain", { "TxId" }, { "TxId" });
            const auto transactions = NKikimr::NClient::TValue::Create(transactionState);
            const auto rows = transactions["Result"]["List"];
            bool found = false;
            for (size_t idx = 0; idx < rows.Size(); ++idx) {
                found |= static_cast<ui64>(rows[static_cast<int>(idx)]["TxId"]) == copyTxId;
            }
            UNIT_ASSERT_VALUES_EQUAL(found, pending);
            const auto snapshotState = ReadSystemTable(runtime, copyShard, "Snapshots",
                { "Oid", "Tid", "Step", "TxId" }, { "Oid", "Tid", "Step", "TxId", "Name" });
            const auto snapshots = NKikimr::NClient::TValue::Create(snapshotState);
            const auto pins = snapshots["Result"]["List"];
            UNIT_ASSERT_VALUES_EQUAL(pins.Size(), pending ? 0 : 1);
            if (!pending) {
                const NBackup::TTableChecksumSnapshotOwner expectedOwner{ TTestTxConfig::SchemeShard, id, false };
                UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(pins[0]["Name"]), expectedOwner.ToString());
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(pins[0]["Oid"]), copyTableId.PathId.OwnerId);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(pins[0]["Tid"]), copyTableId.PathId.LocalPathId);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(pins[0]["Step"]), version.Step);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(pins[0]["TxId"]), version.TxId);
            }
        };
        checkState(true);
        if (RebootCopy) {
            RebootTablet(runtime, copyShard, runtime.AllocateEdgeActor());
            checkState(true);
        }
        progress.Remove();
        for (auto& event : blocked) {
            runtime.Send(event.Release(), 0, true);
        }
        runtime.WaitFor("checksum waits after copy completion", [&] {
            return !starts.empty();
        });
        checkState(false);
        const auto confirmed = ConfirmTableChecksumSnapshot(runtime, copyTableId, TTestTxConfig::SchemeShard, version);
        UNIT_ASSERT_VALUES_EQUAL_C(confirmed->Get()->Status, Ydb::StatusIds::SUCCESS, confirmed->Get()->Error);
        const auto& start = starts.front();
        runtime.Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(
                id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(2, "0000000000000011"), "")));
        runtime.Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
        data.Stop().Unblock();
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
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
        const auto& start = starts.front();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "")));
        Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
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
        TBlockEvents<NKikimr::TEvDataShard::TEvStoreBackupChecksumRequest> checksum(Runtime());
        TBlockEvents<TEvPutObjectRequest> data(Runtime(), [](auto& ev) {
            return TString(ev->Get()->Request.GetKey()).EndsWith(".csv");
        });
        const ui64 id = StartCompletionMarkerExport();
        Runtime().WaitFor("paused checksum and data", [&] {
            return !starts.empty() && !data.empty();
        });
        const auto& start = starts.front();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "")));
        Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()));
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

    Y_UNIT_TEST_TWIN(UnsupportedDataShardMetadataWithoutChecksum, DataFirst) {
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
        auto saved = runtime.AddObserver<TEvDataShard::TEvStoreBackupChecksumRequest>([&](auto&) {
            ++savedChecksums;
        });
        bool unsupportedSent = false;
        auto skipChecksum = [&] {
            UNIT_ASSERT(!starts.empty());
            UNIT_ASSERT(!unsupportedSent);
            UNIT_ASSERT(!HasS3File("/backup/table/metadata.json"));
            const auto& start = starts.front();
            Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
                               new NBackup::TEvTableChecksum::TEvRetentionUnsupported()), 0, true);
            Runtime().Send(new IEventHandle(start->GetRecipientRewrite(), {}, new TEvents::TEvPoisonPill()), 0, true);
            unsupportedSent = true;
        };
        auto replies = runtime.AddObserver<TEvPutObjectResponse>([&](auto& ev) {
            if constexpr (DataFirst) {
                if (ev->Get()->Key && ev->Get()->Key->EndsWith("backup/table/data_00.csv.sha256")) {
                    UNIT_ASSERT(ev->Get()->Result.IsSuccess());
                    UNIT_ASSERT(!starts.empty());
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), starts.front()->Sender);
                    // This final data reply is processed before the fallback queued for the same uploader
                    skipChecksum();
                }
            }
        });
        const ui64 id = StartCompletionMarkerExport();
        Runtime().WaitFor("checksum and data export ready", [&] {
            return DataFirst ? unsupportedSent : !starts.empty() && !data.empty();
        });
        if constexpr (!DataFirst) {
            skipChecksum();
        }
        data.Stop().Unblock();
        Env().TestWaitNotification(Runtime(), id);
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        const auto metadata = NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true);
        UNIT_ASSERT(!metadata.GetTableChecksum().has_value());
        UNIT_ASSERT_VALUES_EQUAL(GetS3FileContent("/backup/table/metadata.json.sha256"),
            NBackup::ComputeChecksum(GetS3FileContent("/backup/table/metadata.json")) + " metadata.json");
        UNIT_ASSERT_VALUES_EQUAL(savedChecksums, 0);
        UNIT_ASSERT(HasS3File("/backup/table/data_00.csv"));
        UNIT_ASSERT(HasS3File("/backup/table/data_01.csv"));
        CheckCompletionMarkerContents(id);
    }

    Y_UNIT_TEST_TWIN(TableChecksumContinuesAfterFlagDisabledOnRecovery, RetentionEnabled) {
        EnvOptions().SetupKqpProxy(true);
        Env();
        TTestBasicRuntime& runtime = Runtime();
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableSnapshotsLocking(true);
        flags.SetEnableDataShardSnapshotLocking(true);
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
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
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        starts.Stop().Unblock();
        Env().TestWaitNotification(runtime, id);
        TestGetExport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);

        UNIT_ASSERT_VALUES_EQUAL(queries, 1u);
        const auto metadata = NBackup::TMetadata::Deserialize(GetS3FileContent("/backup/table/metadata.json"), true);
        UNIT_ASSERT(metadata.GetTableChecksum().has_value());
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Count, 2u);
        UNIT_ASSERT_VALUES_EQUAL(metadata.GetTableChecksum()->Hash, "0000000000000011");
        CheckCompletionMarkerContents(id);
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

    Y_UNIT_TEST(TableChecksumFailureDuringUploadLookup) {
        RunTableChecksumFailureDuringUploadStart(EUploadStartStage::Lookup);
    }

    Y_UNIT_TEST(TableChecksumFailureDuringMultipartCreation) {
        RunTableChecksumFailureDuringUploadStart(EUploadStartStage::Creation);
    }

    Y_UNIT_TEST(TableChecksumFailureDuringFailedMultipartCreation) {
        RunTableChecksumFailureDuringUploadStart(EUploadStartStage::CreationFailure);
    }

    Y_UNIT_TEST(TableChecksumFailureDuringUploadPersistence) {
        RunTableChecksumFailureDuringUploadStart(EUploadStartStage::Persistence);
    }

    Y_UNIT_TEST_TWIN(TableChecksumFailureBeforeRecoveredUploadLookup, Ready) {
        RunTableChecksumFailureDuringUploadStart(EUploadStartStage::RecoveredScheme, !Ready);
    }

    Y_UNIT_TEST(TableChecksumFailureDuringRecoveredUploadLookup) {
        RunTableChecksumFailureDuringUploadStart(EUploadStartStage::RecoveredLookup);
    }

    Y_UNIT_TEST(TableChecksumFailureDuringRestartAbort) {
        RunTableChecksumFailureDuringUploadStart(EUploadStartStage::RestartAbort);
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
        const auto& start = starts.front();
        Runtime().Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
            new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(), "")));
        TestGetExport(Runtime(), id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT(!HasS3File("/backup/table/data_complete.json"));
    }
}
