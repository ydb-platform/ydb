#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/backup/query/table_checksum.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/ut_helpers/backup_test_fixture.h>
#include <ydb/core/tx/schemeshard/ut_helpers/mon_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/table_checksum_helpers.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/library/testlib/helpers.h>
#include <yql/essentials/ast/yql_ast_escaping.h>
#include <util/stream/str.h>

#include <atomic>
#include <memory>

#include <ydb/core/base/request_types.h>
#include <ydb/core/kqp/common/simple/mvcc_version.h>

using namespace NSchemeShardUT_Private;
using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

THashMap<TString, TString> MakeChecksumImportFiles() {
    THashMap<TString, TString> files = {
        { "/scheme.pb", R"(
            columns { name: "key" type { optional_type { item { type_id: UTF8 } } } }
            columns { name: "value" type { optional_type { item { type_id: UTF8 } } } }
            primary_key: "key"
        )" },
        { "/data_00.csv", "\"a1\",\"value1\"\n" },
        { "/metadata.json", R"({"version":1})" },
    };
    for (const TString name : { "scheme.pb", "data_00.csv", "metadata.json" }) {
        files[TStringBuilder() << "/" << name << ".sha256"] =
            NBackup::ComputeChecksum(files.at("/" + name)) + " " + name;
    }
    return files;
}

void CheckImportedData(TTestActorRuntime& runtime) {
    const auto result = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", { "key" }, { "key", "value" });
    const auto value = NClient::TValue::Create(result);
    const auto rows = value["Result"]["List"];
    UNIT_ASSERT_VALUES_EQUAL(rows.Size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(rows[0]["key"]), "a1");
    UNIT_ASSERT_VALUES_EQUAL(static_cast<TString>(rows[0]["value"]), "value1");
}

} // namespace

Y_UNIT_TEST_SUITE(TImportWithRebootsTests) {
    Y_UNIT_TEST_TWIN(ImportWithoutTableChecksum, IsFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        TImportEnv<IsFs> storage(MakeChecksumImportFiles(), DefaultImportItems());
        storage.SetupRuntime(runtime);
        AsyncImport(runtime, 100, "/MyRoot", storage.Request);
        env.TestWaitNotification(runtime, 100);
        TestGetImport(runtime, 100, "/MyRoot", Ydb::StatusIds::SUCCESS);
        CheckImportedData(runtime);
    }

    Y_UNIT_TEST_QUAD(TableChecksumActorRetentionLifetime, IsFs, Cancel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().SetupKqpProxy(true));
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableDataShardDirectPartImport(false);
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        auto source = MakeChecksumImportFiles();
        source["/metadata.json"] =
            NBackup::TMetadata::WithTableChecksum(source["/metadata.json"], MakeTableChecksum(1, "0000000000000011"));
        source["/metadata.json.sha256"] = NBackup::ComputeChecksum(source["/metadata.json"]) + " metadata.json";
        THashMap<TString, TString> files;
        const TVector<TImportItem> items = { { "left", "/MyRoot/Left" }, { "right", "/MyRoot/Right" } };
        for (const auto& item : items) {
            for (const auto& [name, content] : source) {
                files["/" + item.SourcePrefix + name] = content;
            }
        }
        TImportEnv<IsFs> storage(files, items);
        storage.SetupRuntime(runtime);
        const auto service = runtime.AllocateEdgeActor();
        runtime.RegisterService(NLongTxService::MakeLongTxServiceID(runtime.GetNodeId()), service);

        struct TLease {
            TActorId Actor;
            TTableId Table;
            TRowVersion Version;
            std::shared_ptr<std::atomic<bool>> Alive;
        };

        TVector<TLease> leases;
        ui32 registered = 0;
        auto acquire = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Snapshot.Defined());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TableIds.size(), 1u);
            auto alive = std::make_shared<std::atomic<bool>>(true);
            leases.push_back({ ev->Sender, ev->Get()->TableIds.front(), *ev->Get()->Snapshot, alive });
            auto reply = MakeHolder<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>(
                "/MyRoot", leases.back().Version, NKqp::TSnapshotHandle(alive), std::move(ev->Get()->Orbit));
            runtime.Send(new IEventHandle(ev->Sender, service, reply.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        auto handles = runtime.AddObserver<NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult>([&](auto&) {
            ++registered;
        });
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto reply = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            reply->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            reply->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId(0) << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, reply.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            UNIT_ASSERT(ev->Get()->GetMvccVersion());
            UNIT_ASSERT(AnyOf(leases, [&](const auto& lease) {
                return *ev->Get()->GetMvccVersion() == NKqp::FormatMvccVersion(lease.Version);
            }));
            auto reply = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            reply->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(
                *reply->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(1, "0000000000000011"));
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, reply.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        TActorId owner;
        auto starts = runtime.AddObserver<NBackup::TEvTableChecksum::TEvStart>([&](auto& ev) {
            owner = ev->Sender;
        });
        TBlockEvents<TEvDataShard::TEvS3UploadRowsResponse> batches(runtime);
        TBlockEvents<NBackup::TEvTableChecksum::TEvResult> results(runtime, [&](auto& ev) {
            return ev->GetRecipientRewrite() == owner;
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvFinish> finishes(runtime);
        THashSet<TPathId> discarded;
        auto releases = runtime.AddObserver<TEvDataShard::TEvDiscardVolatileSnapshotResponse>([&](auto& ev) {
            if (ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse::DISCARDED) {
                discarded.emplace(ev->Get()->Record.GetOwnerId(), ev->Get()->Record.GetPathId());
            }
        });
        constexpr ui64 id = 100;
        AsyncImport(runtime, id, "/MyRoot", storage.Request);
        runtime.WaitFor("both actors retain D while the data restore is paused", [&] {
            return leases.size() == 2 && registered == 2 && batches.size() == 2;
        });
        UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
        for (const auto& lease : leases) {
            UNIT_ASSERT(lease.Alive->load());
            UNIT_ASSERT(lease.Version.Step);
        }
        if (Cancel) {
            TestCancelImport(runtime, id + 1, "/MyRoot", id);
            runtime.WaitFor("committed cancellation waits for the actors to clean up", [&] {
                return finishes.size() == 2;
            });
            UNIT_ASSERT(discarded.empty());
            UNIT_ASSERT(AllOf(leases, [](const auto& lease) {
                return lease.Alive->load();
            }));
            finishes.Stop().Unblock();
            env.TestWaitNotification(runtime, id);
            runtime.WaitFor("cancellation releases both handles and pins", [&] {
                return discarded.size() == 2 && AllOf(leases, [](const auto& lease) {
                    return !lease.Alive->load();
                });
            });
            UNIT_ASSERT_VALUES_EQUAL(queries, 0u);
            TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
            return;
        }
        batches.Stop().Unblock();
        runtime.WaitFor("both checksum results wait for comparison", [&] {
            return results.size() == 2;
        });
        for (const auto& lease : leases) {
            UNIT_ASSERT(lease.Actor != owner);
            UNIT_ASSERT(lease.Alive->load());
        }
        const auto firstActor = results.front()->Sender;
        const auto first = FindIf(leases, [&](const auto& lease) {
            return lease.Actor == firstActor;
        });
        UNIT_ASSERT(first != leases.end());
        const auto firstTable = first->Table.PathId;
        results.Unblock(1);
        runtime.WaitFor("the committed comparison waits for actor cleanup", [&] {
            return finishes.size() == 1;
        });
        UNIT_ASSERT_VALUES_EQUAL(finishes.front()->GetRecipientRewrite(), firstActor);
        UNIT_ASSERT(first->Alive->load());
        UNIT_ASSERT(discarded.empty());
        finishes.Unblock(1);
        runtime.WaitFor("the compared table releases its handle and pin", [&] {
            return !first->Alive->load() && discarded.contains(firstTable);
        });
        for (const auto& lease : leases) {
            if (lease.Table.PathId != firstTable) {
                UNIT_ASSERT(lease.Alive->load());
                UNIT_ASSERT(!discarded.contains(lease.Table.PathId));
            }
        }
        finishes.Stop().Unblock();
        results.Stop().Unblock();
        env.TestWaitNotification(runtime, id);
        runtime.WaitFor("all comparison handles and pins are released", [&] {
            return discarded.size() == 2 && AllOf(leases, [](const auto& lease) {
                return !lease.Alive->load();
            });
        });
        UNIT_ASSERT_VALUES_EQUAL(queries, 2u);
        TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_QUAD(ImportRetriesAfterWriteFreeze, IsFs, TableChecksumEnabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(TableChecksumEnabled);
        flags.SetEnableDataShardDirectPartImport(false);
        TImportEnv<IsFs> storage(MakeChecksumImportFiles(), DefaultImportItems(), "number_of_retries: 2");
        storage.SetupRuntime(runtime);
        TBlockEvents<TEvDataShard::TEvS3UploadRowsRequest> batches(runtime);
        TBlockEvents<TEvDataShard::TEvS3UploadRowsResponse> responses(runtime);
        AsyncImport(runtime, 100, "/MyRoot", storage.Request);
        runtime.WaitFor("restore row batch before writing", [&] {
            return batches.size() == 1;
        });
        const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
        const auto edge = runtime.AllocateEdgeActor();
        const auto setFreezeStage = [&](ui64 epoch, NKikimrTxDataShard::TDatabaseFreezeState::EStage stage) {
            auto request = MakeHolder<TEvDataShard::TEvDatabaseFreeze>();
            request->Record.SetEpoch(epoch);
            request->Record.SetSchemeShardId(TTestTxConfig::SchemeShard);
            request->Record.SetDomainOwnerId(TTestTxConfig::SchemeShard);
            request->Record.SetDomainLocalPathId(1);
            request->Record.SetStage(stage);
            runtime.SendToPipe(shards.front(), edge, request.Release(), 0, GetPipeConfigWithRetries());
            const auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDatabaseFreezeResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), ui32(Ydb::StatusIds::SUCCESS),
                response->Get()->Record.ShortDebugString());
        };

        setFreezeStage(1, NKikimrTxDataShard::TDatabaseFreezeState::FENCE_WRITES);
        batches.Unblock(1);
        runtime.WaitFor("retryable response while database writes are frozen", [&] {
            return responses.size() == 1;
        });
        const auto& response = *responses.front()->Get();
        UNIT_ASSERT_VALUES_EQUAL(response.Record.GetStatus(), ui32(NKikimrTxDataShard::TError::WRONG_SHARD_STATE));
        UNIT_ASSERT(response.IsRetriableError());
        UNIT_ASSERT_STRING_CONTAINS(response.Record.GetErrorDescription(), "frozen for writes");

        setFreezeStage(2, NKikimrTxDataShard::TDatabaseFreezeState::OPEN);
        responses.Stop().Unblock();
        batches.Stop().Unblock();
        env.TestWaitNotification(runtime, 100);
        TestGetImport(runtime, 100, "/MyRoot", Ydb::StatusIds::SUCCESS);
        CheckImportedData(runtime);
    }

    enum class EChecksumRetryCase {
        Recovery,
        Expiry,
        PermanentError,
        Cancellation,
        LateSuccess,
    };

    auto MockChecksumSnapshot(TTestActorRuntime & runtime, TVector<TRowVersion>* requests = nullptr) {
        auto describe = runtime.AddObserver<TEvSchemeShard::TEvDescribeScheme>([&runtime](auto& ev) {
            if (ev->Get()->Record.GetPathId() != 42) {
                return;
            }
            auto response = MakeHolder<TEvSchemeShard::TEvDescribeSchemeResult>();
            auto& record = *response->MutableRecord();
            record.SetStatus(NKikimrScheme::StatusSuccess);
            auto& path = *record.MutablePathDescription();
            path.MutableSelf()->SetSchemeshardId(TTestTxConfig::SchemeShard);
            path.MutableSelf()->SetPathId(42);
            // Deliver mocked shard requests through a tablet that exists in this runtime
            path.AddTablePartitions()->SetDatashardId(TTestTxConfig::SchemeShard);
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        auto shards =
            runtime.AddObserver<TEvDataShard::TEvRefreshVolatileSnapshotRequest>([&runtime, requests](auto& ev) {
                const auto& request = ev->Get()->Record;
                if (request.GetPathId() != 42) {
                    return;
                }
                if (requests) {
                    requests->emplace_back(request.GetStep(), request.GetTxId());
                }
                auto response = MakeHolder<TEvDataShard::TEvRefreshVolatileSnapshotResponse>();
                response->Record.SetOrigin(TTestTxConfig::SchemeShard);
                response->Record.SetStep(request.GetStep() ? request.GetStep() : 100);
                response->Record.SetTxId(request.GetTxId());
                response->Record.SetStatus(NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse::REFRESHED);
                response->Record.SetSupportsTableChecksumSnapshots(true);
                runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie),
                    0, true);
                ev.Reset();
            });
        return std::pair(std::move(describe), std::move(shards));
    }

    void TableChecksumRetries(
        EChecksumRetryCase scenario, Ydb::StatusIds::StatusCode transient = Ydb::StatusIds::UNAVAILABLE) {
        TTestBasicRuntime runtime;
        TTestEnv testEnv(runtime, TTestEnvOptions().SetupKqpProxy(true));
        const auto edge = runtime.AllocateEdgeActor();
        ui32 attempts = 0;
        TVector<TRowVersion> snapshotRequests;
        auto snapshotService = MockChecksumSnapshot(runtime, &snapshotRequests);
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            if (ev->Get()->Record.GetRequest().GetDatabase() != "/MyRoot") {
                return;
            }
            ++attempts;
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(
                scenario == EChecksumRetryCase::PermanentError ? Ydb::StatusIds::BAD_REQUEST : transient);
            runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release()), 0, true);
            ev.Reset();
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvResult> results(runtime);
        const auto query = runtime.Register(NBackup::CreateTableChecksumQuery(edge, 100, 0, "/MyRoot", "/MyRoot/Table",
            true, TTableId(TTestTxConfig::SchemeShard, 42), TTestTxConfig::SchemeShard, TRowVersion(0, 123)));
        runtime.Send(new IEventHandle(query, edge, new NBackup::TEvTableChecksum::TEvStart()), 0, true);
        TMaybe<TInstant> firstFailure;
        while (true) {
            runtime.WaitFor("checksum attempt result", [&] {
                return !results.empty();
            });
            auto& event = results.front();
            auto& result = *event->Get();
            if (event->GetRecipientRewrite() == edge) {
                const auto elapsed = runtime.GetCurrentTime() - *firstFailure;
                if (scenario == EChecksumRetryCase::Recovery || scenario == EChecksumRetryCase::LateSuccess) {
                    UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
                    UNIT_ASSERT_VALUES_EQUAL(result.Checksum.Count, 1);
                    UNIT_ASSERT_VALUES_EQUAL(result.Checksum.Hash, "0000000000000011");
                    UNIT_ASSERT_GE(elapsed,
                        scenario == EChecksumRetryCase::Recovery ? TDuration::Minutes(1) : TDuration::Minutes(5));
                } else if (scenario == EChecksumRetryCase::PermanentError) {
                    UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::BAD_REQUEST);
                    UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(result.Status, transient);
                    UNIT_ASSERT_GE(elapsed, TDuration::Minutes(5));
                    UNIT_ASSERT_LT(elapsed, TDuration::Minutes(5) + TDuration::Seconds(20));
                }
                if (scenario != EChecksumRetryCase::PermanentError) {
                    UNIT_ASSERT_GT(attempts, 3);
                }
                const auto finalAttempts = attempts;
                UNIT_ASSERT_VALUES_EQUAL(snapshotRequests.size(), 1);
                results.clear();
                runtime.AdvanceCurrentTime(TDuration::Minutes(6));
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(attempts, finalAttempts);
                UNIT_ASSERT(results.empty());
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(event->GetRecipientRewrite(), query);
            UNIT_ASSERT_C(attempts < 100, "Checksum retry window must expire");
            if (!firstFailure) {
                firstFailure = runtime.GetCurrentTime();
            }
            const auto elapsed = runtime.GetCurrentTime() - *firstFailure;
            const bool recover = (scenario == EChecksumRetryCase::Recovery && elapsed >= TDuration::Minutes(1)) ||
                                 (scenario == EChecksumRetryCase::LateSuccess && elapsed >= TDuration::Minutes(5));
            if (recover) {
                result.Status = Ydb::StatusIds::SUCCESS;
                result.Checksum = MakeTableChecksum(1, "0000000000000011");
                result.Error.clear();
            }
            const bool firstAttempt = attempts == 1;
            results.Unblock(1);
            runtime.SimulateSleep(TDuration::MilliSeconds(500));
            if (firstAttempt && scenario != EChecksumRetryCase::PermanentError) {
                UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
                UNIT_ASSERT_C(results.empty(), "Transient checksum retry must have a pause");
            }
            if (scenario == EChecksumRetryCase::Cancellation) {
                runtime.Send(new IEventHandle(query, edge, new TEvents::TEvPoisonPill()), 0, true);
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
                runtime.AdvanceCurrentTime(TDuration::Minutes(6));
                runtime.SimulateSleep(TDuration::MilliSeconds(1));
                UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
                UNIT_ASSERT(results.empty());
                return;
            }
            if (results.empty()) {
                runtime.AdvanceCurrentTime(TDuration::Seconds(15));
            }
        }
    }

    Y_UNIT_TEST(TableChecksumRetryRecoversAfterTabletStartup) {
        TableChecksumRetries(EChecksumRetryCase::Recovery);
    }

    Y_UNIT_TEST(TableChecksumRetryWindow) {
        TableChecksumRetries(EChecksumRetryCase::Expiry);
    }

    Y_UNIT_TEST(TableChecksumRetryOverloaded) {
        TableChecksumRetries(EChecksumRetryCase::Expiry, Ydb::StatusIds::OVERLOADED);
    }

    Y_UNIT_TEST(TableChecksumRetryAborted) {
        TableChecksumRetries(EChecksumRetryCase::Expiry, Ydb::StatusIds::ABORTED);
    }

    Y_UNIT_TEST(TableChecksumRetryBadSession) {
        TableChecksumRetries(EChecksumRetryCase::Recovery, Ydb::StatusIds::BAD_SESSION);
    }

    Y_UNIT_TEST(TableChecksumRetryExpiredSession) {
        TableChecksumRetries(EChecksumRetryCase::Recovery, Ydb::StatusIds::SESSION_EXPIRED);
    }

    Y_UNIT_TEST(TableChecksumRetryBusySession) {
        TableChecksumRetries(EChecksumRetryCase::Recovery, Ydb::StatusIds::SESSION_BUSY);
    }

    Y_UNIT_TEST(TableChecksumRetryInternalError) {
        TableChecksumRetries(EChecksumRetryCase::Recovery, Ydb::StatusIds::INTERNAL_ERROR);
    }

    Y_UNIT_TEST(TableChecksumRetryUndetermined) {
        TableChecksumRetries(EChecksumRetryCase::Recovery, Ydb::StatusIds::UNDETERMINED);
    }

    Y_UNIT_TEST(TableChecksumRetryPermanentError) {
        TableChecksumRetries(EChecksumRetryCase::PermanentError);
    }

    Y_UNIT_TEST(TableChecksumRetryCancellation) {
        TableChecksumRetries(EChecksumRetryCase::Cancellation);
    }

    Y_UNIT_TEST(TableChecksumRetrySuccessAfterWindow) {
        TableChecksumRetries(EChecksumRetryCase::LateSuccess);
    }

    Y_UNIT_TEST_QUAD(TableChecksumRetriesAfterLongQuery, Importing, Recover) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().SetupKqpProxy(true));
        const auto edge = runtime.AllocateEdgeActor();
        auto snapshotService = MockChecksumSnapshot(runtime);
        ui32 snapshotAttempts = 0;
        auto snapshotFailure = runtime.AddObserver<TEvDataShard::TEvRefreshVolatileSnapshotResponse>([&](auto& ev) {
            if (++snapshotAttempts == 1) {
                ev->Get()->Record.SetStatus(NKikimrTxDataShard::TEvRefreshVolatileSnapshotResponse::WRONG_SHARD_STATE);
            }
        });
        NKikimrSchemeOp::TTableDescription schema;
        schema.SetTableSchemaVersion(1);
        auto& column = *schema.AddColumns();
        column.SetId(1);
        column.SetName("key");
        column.SetType("Uint64");
        auto source = runtime.AddObserver<TEvSchemeShard::TEvDescribeScheme>([&](auto& ev) {
            if (ev->Get()->Record.GetPath() != "/MyRoot/Table") {
                return;
            }
            auto response = MakeHolder<TEvSchemeShard::TEvDescribeSchemeResult>();
            auto& record = *response->MutableRecord();
            record.SetStatus(NKikimrScheme::StatusSuccess);
            auto& path = *record.MutablePathDescription();
            path.MutableSelf()->SetSchemeshardId(TTestTxConfig::SchemeShard);
            path.MutableSelf()->SetPathId(42);
            *path.MutableTable() = schema;
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
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
        TBlockEvents<NKqp::TEvKqp::TEvQueryRequest> queries(runtime, [](auto& ev) {
            return ev->Get()->GetQuery().Contains("CHECKSUM TABLE");
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvResult> results(runtime, [&](auto& ev) {
            return ev->GetRecipientRewrite() == edge;
        });
        const auto worker = runtime.Register(NBackup::CreateTableChecksumQuery(edge, 100, 0, "/MyRoot", "/MyRoot/Table",
            Importing, TTableId(TTestTxConfig::SchemeShard, 42), TTestTxConfig::SchemeShard, TRowVersion(100, 123), {},
            NBackup::GetTableChecksumSchemaHash(schema)));
        runtime.EnableScheduleForActor(worker);
        runtime.Send(new IEventHandle(worker, edge, new NBackup::TEvTableChecksum::TEvStart()), 0, true);
        runtime.WaitFor("query starts after retrying snapshot confirmation", [&] {
            return !queries.empty() || !results.empty();
        });
        UNIT_ASSERT(results.empty());
        UNIT_ASSERT_VALUES_EQUAL(snapshotAttempts, 2);

        // The successful precheck must not consume the retry budget of a later long scan
        runtime.AdvanceCurrentTime(TDuration::Minutes(6));
        const auto firstFailure = runtime.GetCurrentTime();
        const auto respond = [&](bool success) {
            auto request = std::move(queries.front());
            queries.pop_front();
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(success ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::UNAVAILABLE);
            if (success) {
                FillTableChecksumResult(
                    *response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(1, "0000000000000011"));
            }
            runtime.Send(new IEventHandle(request->Sender, request->GetRecipientRewrite(), response.Release(), 0,
                             request->Cookie), 0, true);
        };
        respond(false);
        ui32 attempts = 1;
        while (true) {
            runtime.WaitFor("retry or terminal result after the long query", [&] {
                return !queries.empty() || !results.empty();
            });
            if (!results.empty()) {
                break;
            }
            UNIT_ASSERT_C(++attempts < 100, "Consecutive quick failures must exhaust the new retry window");
            respond(Recover);
            if (!Recover) {
                runtime.AdvanceCurrentTime(TDuration::Seconds(10));
            }
        }
        const auto& result = *results.front()->Get();
        if (Recover) {
            UNIT_ASSERT_VALUES_EQUAL(attempts, 2);
            UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result.Checksum.Count, 1);
            UNIT_ASSERT_VALUES_EQUAL(result.Checksum.Hash, "0000000000000011");
        } else {
            UNIT_ASSERT_GT(attempts, 3);
            UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);
            const auto elapsed = runtime.GetCurrentTime() - firstFailure;
            UNIT_ASSERT_GE(elapsed, TDuration::Minutes(5));
            UNIT_ASSERT_LT(elapsed, TDuration::Minutes(5) + TDuration::Seconds(20));
        }
    }

    Y_UNIT_TEST_QUAD(TableChecksumSelectsRecordedAlgorithm, IsFs, Invalid) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().SetupKqpProxy(true));
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableDataShardDirectPartImport(false);
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        const TString algorithm = Invalid ? "CityHash64\"\\\n" : "CityHash64";
        auto checksum = MakeTableChecksum(1, "0000000000000011");
        checksum.Algorithm = algorithm;
        auto files = MakeChecksumImportFiles();
        files["/metadata.json"] = NBackup::TMetadata::WithTableChecksum(files["/metadata.json"], checksum);
        files["/metadata.json.sha256"] = NBackup::ComputeChecksum(files["/metadata.json"]) + " metadata.json";
        TImportEnv<IsFs> storage(files, DefaultImportItems());
        storage.SetupRuntime(runtime);
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 attempts = 0;
        auto queries = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++attempts;
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetQuery(),
                Invalid ? R"(CHECKSUM TABLE `/MyRoot/Table` WITH (ALGORITHM = "CityHash64\"\\\n");)"
                        : R"(CHECKSUM TABLE `/MyRoot/Table` WITH (ALGORITHM = "CityHash64");)");
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Invalid ? Ydb::StatusIds::BAD_REQUEST : Ydb::StatusIds::SUCCESS);
            if (!Invalid) {
                FillTableChecksumResult(
                    *response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(1, "0000000000000011"));
            }
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        AsyncImport(runtime, 100, "/MyRoot", storage.Request);
        env.TestWaitNotification(runtime, 100);
        const auto result =
            TestGetImport(runtime, 100, "/MyRoot", Invalid ? Ydb::StatusIds::CANCELLED : Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        if (Invalid) {
            UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "BAD_REQUEST");
        }
        CheckImportedData(runtime);
    }

    Y_UNIT_TEST_QUAD(TableChecksumImportsWideHash, IsFs, Sha256) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().SetupKqpProxy(true));
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableDataShardDirectPartImport(false);
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        const auto checksum = MakeTableChecksum(1, TString(Sha256 ? 64 : 32, 'F'), Sha256 ? "Sha256" : "CityHash128");
        auto files = MakeChecksumImportFiles();
        files["/metadata.json"] = NBackup::TMetadata::WithTableChecksum(files["/metadata.json"], checksum);
        files["/metadata.json.sha256"] = NBackup::ComputeChecksum(files["/metadata.json"]) + " metadata.json";
        TImportEnv<IsFs> storage(files, DefaultImportItems());
        storage.SetupRuntime(runtime);
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId() << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 attempts = 0;
        auto queries = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++attempts;
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetQuery(), TStringBuilder()
                                                                << "CHECKSUM TABLE `/MyRoot/Table` WITH (ALGORITHM = \""
                                                                << checksum.Algorithm << "\");");
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            FillTableChecksumResult(*response->Record.MutableResponse()->AddYdbResults(), checksum);
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        AsyncImport(runtime, 100, "/MyRoot", storage.Request);
        env.TestWaitNotification(runtime, 100);
        TestGetImport(runtime, 100, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        CheckImportedData(runtime);
    }

    enum class EMalformedChecksumResult {
        MissingResult,
        CountType,
        Version,
        EmptyAlgorithm,
        EmptyTableHash,
    };

    void CheckMalformedChecksumResult(EMalformedChecksumResult scenario) {
        TTestBasicRuntime runtime;
        TTestEnv testEnv(runtime, TTestEnvOptions().SetupKqpProxy(true));
        const auto edge = runtime.AllocateEdgeActor();
        auto snapshotService = MockChecksumSnapshot(runtime);
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId(0) << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 attempts = 0;
        auto queries = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++attempts;
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            if (scenario != EMalformedChecksumResult::MissingResult) {
                auto& result = *response->Record.MutableResponse()->AddYdbResults();
                const auto checksum =
                    MakeTableChecksum(scenario == EMalformedChecksumResult::EmptyTableHash ? 0 : 1, "0000000000000011");
                FillTableChecksumResult(result, checksum);
                switch (scenario) {
                    case EMalformedChecksumResult::CountType:
                        result.mutable_columns(2)->mutable_type()->set_type_id(Ydb::Type::UTF8);
                        result.mutable_rows(0)->mutable_items(2)->Clear();
                        result.mutable_rows(0)->mutable_items(2)->set_text_value("invalid");
                        break;
                    case EMalformedChecksumResult::Version:
                        result.mutable_rows(0)->mutable_items(0)->set_uint32_value(2);
                        break;
                    case EMalformedChecksumResult::EmptyAlgorithm:
                        result.mutable_rows(0)->mutable_items(1)->set_text_value("");
                        break;
                    default:
                        break;
                }
            }
            runtime.Send(
                new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvResult> results(runtime, [&](auto& ev) {
            return ev->GetRecipientRewrite() == edge;
        });
        const auto query = runtime.Register(NBackup::CreateTableChecksumQuery(edge, 100, 0, "/MyRoot", "/MyRoot/Table",
            true, TTableId(TTestTxConfig::SchemeShard, 42), TTestTxConfig::SchemeShard, TRowVersion(0, 123)));
        runtime.Send(new IEventHandle(query, edge, new NBackup::TEvTableChecksum::TEvStart()), 0, true);
        runtime.WaitFor("invalid checksum result rejected", [&] {
            return !results.empty();
        });
        const auto& result = *results.front()->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(result.Status, Ydb::StatusIds::GENERIC_ERROR, result.Error);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "invalid table checksum query result");
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        results.clear();
        runtime.AdvanceCurrentTime(TDuration::Minutes(6));
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        UNIT_ASSERT(results.empty());
    }

    Y_UNIT_TEST_TWIN(TableChecksumRetryMalformedResult, InvalidType) {
        CheckMalformedChecksumResult(
            InvalidType ? EMalformedChecksumResult::CountType : EMalformedChecksumResult::MissingResult);
    }

    Y_UNIT_TEST(TableChecksumRejectsUnsupportedResultVersion) {
        CheckMalformedChecksumResult(EMalformedChecksumResult::Version);
    }

    Y_UNIT_TEST(TableChecksumRejectsEmptyAlgorithm) {
        CheckMalformedChecksumResult(EMalformedChecksumResult::EmptyAlgorithm);
    }

    Y_UNIT_TEST(TableChecksumRejectsNonzeroEmptyTableHash) {
        CheckMalformedChecksumResult(EMalformedChecksumResult::EmptyTableHash);
    }

    template <bool IsFs>
    void TableChecksumValidation(bool cancel) {
        TTestBasicRuntime runtime;
        TTestEnv testEnv(runtime, TTestEnvOptions());
        const auto data = MakeChecksumImportFiles();
        auto files = data;
        files["/metadata.json"] =
            R"({"version":0,"table_checksum":{"version":1,"algorithm":"CityHash64","count":"1","hash":"0000000000000011"}})";
        TImportEnv<IsFs> storage(files, DefaultImportItems());
        storage.SetupRuntime(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableExportFiltering(false);
        runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(false);
        runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(false);
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        using namespace NKikimr::NSchemeShard;
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        const ui64 id = 100;
        AsyncImport(runtime, id, "/MyRoot", storage.Request);
        runtime.WaitFor("post-import checksum validation", [&] {
            return starts.size() == 1;
        });
        const auto firstRecipient = starts[0]->GetRecipientRewrite();
        const auto firstOwner = starts[0]->Sender;
        if (cancel) {
            TestCancelImport(runtime, id + 1, "/MyRoot", id);
            testEnv.TestWaitNotification(runtime, id);
            runtime.Send(new IEventHandle(
                firstOwner, firstRecipient, new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS,
                                                MakeTableChecksum(1, "0000000000000011"), "")));
            TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        } else {
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
            runtime.WaitFor("resumed checksum validation", [&] {
                return starts.size() == 2;
            });
            runtime.Send(new IEventHandle(
                firstOwner, firstRecipient, new NBackup::TEvTableChecksum::TEvResult(id, 0, Ydb::StatusIds::SUCCESS,
                                                MakeTableChecksum(1, "0000000000000011"), "")));
            const auto status = TestGetImport(runtime, id, "/MyRoot");
            UNIT_ASSERT_VALUES_EQUAL(
                status.GetResponse().GetEntry().GetProgress(), Ydb::Import::ImportProgress::PROGRESS_TRANSFER_DATA);
            const auto& resumed = starts[1];
            runtime.Send(new IEventHandle(resumed->Sender, resumed->GetRecipientRewrite(),
                new NBackup::TEvTableChecksum::TEvResult(
                    id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(1, "0000000000000011"), "")));
            testEnv.TestWaitNotification(runtime, id);
            TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST_TWIN(TableChecksumValidationRecovery, IsFs) {
        TableChecksumValidation<IsFs>(false);
    }

    Y_UNIT_TEST_TWIN(TableChecksumValidationCancellation, IsFs) {
        TableChecksumValidation<IsFs>(true);
    }

    template <bool IsFs>
    void TableChecksumCheckpointRecovery(bool lostHistory, bool expiredRegistration = false,
        bool rebootDataShard = false, bool directPart = false, bool removePin = false) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().SetupKqpProxy(true));
        auto files = MakeChecksumImportFiles();
        // External metadata cannot supply the private validation checkpoint
        files["/metadata.json"] =
            R"({"version":0,"table_checksum":{"version":1,"algorithm":"CityHash64","count":"1","hash":"0000000000000011"},"checksum_snapshot":"1:1"})";
        TImportEnv<IsFs> storage(files, DefaultImportItems());
        storage.SetupRuntime(runtime);
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableBackupTableChecksum(true);
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        flags.SetEnableDataShardDirectPartImport(directPart);

        TVector<TRowVersion> snapshotRequests;
        auto snapshots = runtime.AddObserver<TEvDataShard::TEvRefreshVolatileSnapshotRequest>([&](auto& ev) {
            snapshotRequests.emplace_back(ev->Get()->Record.GetStep(), ev->Get()->Record.GetTxId());
        });
        TMaybe<TRowVersion> checkpoint;
        auto sessions = runtime.AddObserver<NKqp::TEvKqp::TEvCreateSessionRequest>([&](auto& ev) {
            auto response = MakeHolder<NKqp::TEvKqp::TEvCreateSessionResponse>();
            response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
            response->Record.MutableResponse()->SetSessionId(
                TStringBuilder() << "ydb://session/3?node_id=" << runtime.GetNodeId(0) << "&id=MS0yLTMtNA%3D%3D");
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        ui32 queries = 0;
        auto execute = runtime.AddObserver<NKqp::TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!ev->Get()->GetQuery().Contains("CHECKSUM TABLE")) {
                return;
            }
            ++queries;
            UNIT_ASSERT(checkpoint);
            UNIT_ASSERT_VALUES_EQUAL(
                ev->Get()->GetQuery(), "CHECKSUM TABLE `/MyRoot/Table` WITH (ALGORITHM = \"CityHash64\");");
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetRequestType(), NRequestTypes::ChecksumTable);
            UNIT_ASSERT_GT(ev->Get()->GetOperationTimeout(), TDuration::Minutes(5));
            UNIT_ASSERT(ev->Get()->GetTxControl().begin_tx().has_snapshot_read_only());
            UNIT_ASSERT(ev->Get()->GetTxControl().commit_tx());
            UNIT_ASSERT(ev->Get()->GetMvccVersion());
            UNIT_ASSERT_VALUES_EQUAL(*ev->Get()->GetMvccVersion(), NKqp::FormatMvccVersion(*checkpoint));
            auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
            response->Record.SetYdbStatus(lostHistory ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::SUCCESS);
            if (!lostHistory) {
                FillTableChecksumResult(
                    *response->Record.MutableResponse()->AddYdbResults(), MakeTableChecksum(1, "0000000000000011"));
            }
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie), 0, true);
            ev.Reset();
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        TBlockEvents<NBackup::TEvTableChecksum::TEvSnapshotConfirmed> confirmations(runtime);
        const ui64 id = 100;
        AsyncImport(runtime, id, "/MyRoot", storage.Request);
        runtime.WaitFor("validation actor created", [&] {
            return starts.size() == 1;
        });
        starts.Unblock(1);
        runtime.WaitFor("restore version confirmed before checksum execution", [&] {
            return confirmations.size() == 1;
        });
        checkpoint = confirmations.front()->Get()->Snapshot;
        UNIT_ASSERT_VALUES_EQUAL(confirmations.front()->Get()->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(checkpoint->Step);
        UNIT_ASSERT_VALUES_EQUAL(queries, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshotRequests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshotRequests.front(), *checkpoint);
        const TTableId table(TTestTxConfig::SchemeShard, DescribePath(runtime, "/MyRoot/Table").GetPathId());
        const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Table");
        UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, table, shards, *checkpoint), 1);
        starts.clear();

        if (expiredRegistration) {
            // Exceed both the registration window and the old-binary pin lease
            runtime.AdvanceCurrentTime(NBackup::TableChecksumSnapshotTimeout + TDuration::Minutes(1));
        }
        if (rebootDataShard) {
            for (ui64 shard : shards) {
                RebootTablet(runtime, shard, runtime.AllocateEdgeActor());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, table, shards, *checkpoint), 1);
        if (removePin) {
            const auto edge = runtime.AllocateEdgeActor();
            auto request = MakeHolder<TEvDataShard::TEvDiscardVolatileSnapshotRequest>();
            request->Record.SetOwnerId(table.PathId.OwnerId);
            request->Record.SetPathId(table.PathId.LocalPathId);
            request->Record.SetStep(checkpoint->Step);
            request->Record.SetTxId(checkpoint->TxId);
            runtime.SendToPipe(shards.front(), edge, request.Release());
            auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDiscardVolatileSnapshotResponse>(edge);
            UNIT_ASSERT_C(response->Get()->Record.GetStatus() ==
                              NKikimrTxDataShard::TEvDiscardVolatileSnapshotResponse::DISCARDED,
                response->Get()->Record.DebugString());
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        runtime.WaitFor("validation resumes after restart", [&] {
            return starts.size() == 1;
        });
        if (!removePin) {
            // Stopping the old checksum actor must preserve the recovery checkpoint
            UNIT_ASSERT_VALUES_EQUAL(CountTableChecksumSnapshotRows(runtime, table, shards, *checkpoint), 1);
        }
        starts.Stop().Unblock();
        runtime.WaitFor("restored checkpoint confirmed after restart", [&] {
            return confirmations.size() == 2;
        });
        if (!removePin) {
            UNIT_ASSERT_VALUES_EQUAL(confirmations.back()->Get()->Snapshot, *checkpoint);
        }
        confirmations.Stop().Unblock();
        env.TestWaitNotification(runtime, id);
        const auto status = TestGetImport(
            runtime, id, "/MyRoot", lostHistory || removePin ? Ydb::StatusIds::CANCELLED : Ydb::StatusIds::SUCCESS);
        if (removePin) {
            UNIT_ASSERT_STRING_CONTAINS(status.DebugString(), "SNAPSHOT_NOT_FOUND");
        }
        UNIT_ASSERT_VALUES_EQUAL(snapshotRequests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshotRequests.back(), TRowVersion(0, checkpoint->TxId));
        UNIT_ASSERT_VALUES_EQUAL(queries, removePin ? 0 : 1);
    }

    Y_UNIT_TEST_TWIN(TableChecksumCheckpointRecovery, IsFs) {
        TableChecksumCheckpointRecovery<IsFs>(false);
    }

    Y_UNIT_TEST_TWIN(TableChecksumCheckpointUnavailableOnRecovery, IsFs) {
        TableChecksumCheckpointRecovery<IsFs>(true);
    }

    Y_UNIT_TEST_TWIN(TableChecksumCheckpointExpiredOnRecovery, IsFs) {
        TableChecksumCheckpointRecovery<IsFs>(false, true);
    }

    Y_UNIT_TEST_QUAD(TableChecksumCheckpointDataShardRecovery, IsFs, DirectPart) {
        TableChecksumCheckpointRecovery<IsFs>(false, true, true, DirectPart);
    }

    Y_UNIT_TEST_TWIN(TableChecksumCheckpointPinRemoved, IsFs) {
        TableChecksumCheckpointRecovery<IsFs>(false, true, false, false, true);
    }

    Y_UNIT_TEST_TWIN(TableChecksumOldDataShard, IsFs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        auto files = MakeChecksumImportFiles();
        files["/metadata.json"] =
            R"({"version":0,"table_checksum":{"version":1,"algorithm":"CityHash64","count":"1","hash":"0000000000000011"}})";
        TImportEnv<IsFs> storage(files, DefaultImportItems());
        storage.SetupRuntime(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableBackupTableChecksum(true);
        ui32 confirmed = 0;
        auto snapshots = runtime.AddObserver<TEvDataShard::TEvRefreshVolatileSnapshotResponse>([&](auto& ev) {
            ++confirmed;
            ev->Get()->Record.ClearSupportsTableChecksumSnapshots();
        });
        AsyncImport(runtime, 100, "/MyRoot", storage.Request);
        env.TestWaitNotification(runtime, 100);
        TestGetImport(runtime, 100, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(confirmed, 1);
        CheckImportedData(runtime);
    }

    template <bool IsFs>
    void TableChecksumDestinationRemoved(bool reboot, bool recreate) {
        TTestBasicRuntime runtime;
        TTestEnv testEnv(runtime, TTestEnvOptions());
        const auto data = MakeChecksumImportFiles();
        auto files = data;
        files["/metadata.json"] =
            R"({"version":0,"table_checksum":{"version":1,"algorithm":"CityHash64","count":"1","hash":"0000000000000011"}})";
        TImportEnv<IsFs> storage(files, DefaultImportItems());
        storage.SetupRuntime(runtime);
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        flags.SetEnableDataShardDirectPartImport(false);
        flags.SetEnableBackupTableChecksum(true);
        ui32 restores = 0;
        auto observer = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([&](auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransaction()) {
                if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpRestore) {
                    ++restores;
                }
            }
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        const ui64 id = 100;
        AsyncImport(runtime, id, "/MyRoot", storage.Request);
        runtime.WaitFor("post-import checksum validation", [&] {
            return starts.size() == 1;
        });
        const auto originalPathId = DescribePath(runtime, "/MyRoot/Table").GetPathId();
        TestDropTable(runtime, id + 1, "/MyRoot", "Table");
        testEnv.TestWaitNotification(runtime, id + 1);
        ui64 replacementPathId = 0;
        if (recreate) {
            TestCreateTable(runtime, id + 2, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "replacement" Type: "Uint64" }
                KeyColumnNames: ["replacement"]
            )");
            testEnv.TestWaitNotification(runtime, id + 2);
            replacementPathId = DescribePath(runtime, "/MyRoot/Table").GetPathId();
            UNIT_ASSERT_UNEQUAL(replacementPathId, originalPathId);
        }
        if (reboot) {
            RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        } else {
            const auto& start = starts.front();
            runtime.Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
                new NBackup::TEvTableChecksum::TEvResult(
                    id, 0, Ydb::StatusIds::SUCCESS, MakeTableChecksum(1, "0000000000000011"), "")));
        }
        testEnv.TestWaitNotification(runtime, id);
        const auto status = TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(NYql::IssuesFromMessageAsString(status.GetResponse().GetEntry().GetIssues()),
            "destination table is no longer available");
        UNIT_ASSERT_VALUES_EQUAL(restores, 1);
        UNIT_ASSERT_VALUES_EQUAL(starts.size(), 1);
        if (recreate) {
            const auto table = DescribePath(runtime, "/MyRoot/Table");
            TestDescribeResult(table, { NLs::Finished });
            UNIT_ASSERT_VALUES_EQUAL(table.GetPathId(), replacementPathId);
        } else {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), { NLs::PathNotExist });
        }
    }

    Y_UNIT_TEST_QUAD(TableChecksumDroppedDestination, IsFs, Reboot) {
        TableChecksumDestinationRemoved<IsFs>(Reboot, false);
    }

    Y_UNIT_TEST_QUAD(TableChecksumRecreatedDestination, IsFs, Reboot) {
        TableChecksumDestinationRemoved<IsFs>(Reboot, true);
    }

    template <bool IsFs>
    void TableChecksumMetadataRecovery(bool unknownVersion, bool skipChecksumValidation) {
        TTestBasicRuntime runtime;
        TTestEnv testEnv(runtime, TTestEnvOptions());
        const auto data = MakeChecksumImportFiles();
        TImportEnv<IsFs> storage(
            data, DefaultImportItems(), skipChecksumValidation ? "skip_checksum_validation: true" : "");
        storage.SetupRuntime(runtime);
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        flags.SetEnableDataShardDirectPartImport(false);
        flags.SetEnableBackupTableChecksum(true);
        ui64 restoreTxId = 0;
        ui32 restores = 0;
        auto observer = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([&](auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransaction()) {
                if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpRestore) {
                    restoreTxId = ev->Get()->Record.GetTxId();
                    ++restores;
                }
            }
        });
        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> completions(runtime, [&](auto& ev) {
            return restoreTxId != 0 && ev->Get()->Record.GetTxId() == restoreTxId;
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        const ui64 id = 100;
        AsyncImport(runtime, id, "/MyRoot", storage.Request);
        runtime.WaitFor("restore completed before import recovery", [&] {
            return !completions.empty();
        });

        // An older binary could persist the checksum even when validation was skipped
        const TString metadata =
            unknownVersion
                ? R"({"version":0,"table_checksum":{"version":999,"count":"1","hash":"17"}})"
                : R"({"version":0,"table_checksum":{"version":1,"algorithm":"CityHash64","count":"1","hash":"0000000000000011"}})";
        TStringStream escaped;
        NYql::EscapeArbitraryAtom(metadata, '"', &escaped);
        NKikimrMiniKQL::TResult result;
        TString error;
        const auto status = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"((
            (let key '('('ImportId (Uint64 '%lu)) '('Index (Uint32 '0))))
            (let row '('('Metadata (String '%s))))
            (return (AsList (UpdateRow 'ImportItems key row)))
        ))", id, escaped.Str().c_str()), result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
        completions.Stop();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        testEnv.TestWaitNotification(runtime, id);
        TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(starts.empty());
        UNIT_ASSERT_VALUES_EQUAL(restores, 1);
        CheckImportedData(runtime);
    }

    Y_UNIT_TEST_QUAD(TableChecksumSkipValidationRecovery, IsFs, UnknownVersion) {
        TableChecksumMetadataRecovery<IsFs>(UnknownVersion, true);
    }

    Y_UNIT_TEST_TWIN(TableChecksumUnsupportedVersionRecovery, IsFs) {
        TableChecksumMetadataRecovery<IsFs>(true, false);
    }

    template <bool IsFs>
    void TableChecksumParallel(bool cancel, bool disableChecksumOnRecovery = false) {
        TTestBasicRuntime runtime;
        TTestEnv testEnv(runtime, TTestEnvOptions());
        const auto data = MakeChecksumImportFiles();
        const auto tableFiles = data;
        constexpr ui32 tables = 6;
        THashMap<TString, TString> files;
        TVector<TImportItem> items;
        for (ui32 idx = 0; idx < tables; ++idx) {
            const TString prefix = TStringBuilder() << "table" << idx;
            for (const auto& [name, content] : tableFiles) {
                files[TStringBuilder() << "/" << prefix << name] = content;
            }
            files[TStringBuilder() << "/" << prefix << "/metadata.json"] =
                R"({"version":0,"table_checksum":{"version":1,"algorithm":"CityHash64","count":"1","hash":"0000000000000011"}})";
            items.push_back({ prefix, TStringBuilder() << "/MyRoot/Table" << idx });
        }
        TImportEnv<IsFs> storage(files, items);
        storage.SetupRuntime(runtime);
        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableExportFiltering(false);
        flags.SetEnableEncryptedExport(false);
        flags.SetEnableDataShardDirectPartImport(false);
        flags.SetEnableBackupTableChecksum(true);
        THashSet<ui64> restoreTxIds;
        auto restores = runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([&](auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransaction()) {
                if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpRestore) {
                    restoreTxIds.insert(ev->Get()->Record.GetTxId());
                }
            }
        });
        THashSet<ui64> completedRestores;
        THashSet<ui64> discardedSnapshots;
        auto discarded = runtime.AddObserver<TEvDataShard::TEvDiscardVolatileSnapshotRequest>([&](auto& ev) {
            if (!ev->Get()->Record.GetStep() && restoreTxIds.contains(ev->Get()->Record.GetTxId())) {
                discardedSnapshots.insert(ev->Get()->Record.GetTxId());
            }
        });
        auto completions = runtime.AddObserver<TEvSchemeShard::TEvNotifyTxCompletionResult>([&](auto& ev) {
            const auto txId = ev->Get()->Record.GetTxId();
            if (restoreTxIds.contains(txId)) {
                completedRestores.insert(txId);
            }
        });
        TBlockEvents<NBackup::TEvTableChecksum::TEvStart> starts(runtime);
        const ui64 id = 100;
        AsyncImport(runtime, id, "/MyRoot", storage.Request);
        runtime.WaitFor("all checksum checks started in parallel", [&] {
            return starts.size() == tables;
        });

        using EState = TImportInfo::EState;
        const auto readStates = [&] {
            const auto stored = ReadSystemTable(
                runtime, TTestTxConfig::SchemeShard, "ImportItems", { "ImportId", "Index" }, { "State", "WaitTxId" });
            const auto value = NClient::TValue::Create(stored);
            const auto rows = value["Result"]["List"];
            THashMap<EState, ui32> counts;
            for (size_t idx = 0; idx < rows.Size(); ++idx) {
                const ui8 state = rows[static_cast<int>(idx)]["State"];
                const ui64 waitTxId = rows[static_cast<int>(idx)]["WaitTxId"];
                if (static_cast<EState>(state) == EState::Transferring) {
                    // An older binary can subscribe to this completed restore and continue normally
                    UNIT_ASSERT(restoreTxIds.contains(waitTxId));
                } else if (static_cast<EState>(state) == EState::Done) {
                    UNIT_ASSERT_VALUES_EQUAL(waitTxId, 0);
                }
                ++counts[static_cast<EState>(state)];
            }
            return counts;
        };
        UNIT_ASSERT_VALUES_EQUAL(readStates().Value(EState::Transferring, 0u), tables);
        UNIT_ASSERT_VALUES_EQUAL(completedRestores.size(), tables);
        UNIT_ASSERT_VALUES_EQUAL(restoreTxIds.size(), tables);
        const auto complete = [&](size_t index) {
            const auto& start = starts[index];
            // Only the item owning this query actor accepts the synthetic result
            for (ui32 idx = 0; idx < tables; ++idx) {
                runtime.Send(new IEventHandle(start->Sender, start->GetRecipientRewrite(),
                    new NBackup::TEvTableChecksum::TEvResult(
                        id, idx, Ydb::StatusIds::SUCCESS, MakeTableChecksum(1, "0000000000000011"), "")));
            }
        };
        if (cancel) {
            TestCancelImport(runtime, id + 1, "/MyRoot", id);
            testEnv.TestWaitNotification(runtime, id);
            for (size_t idx = 0; idx < tables; ++idx) {
                complete(idx);
            }
            TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_VALUES_EQUAL(starts.size(), tables);
            return;
        }

        flags.SetEnableBackupTableChecksum(!disableChecksumOnRecovery);
        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> replayed(runtime, [&](auto& ev) {
            return disableChecksumOnRecovery && restoreTxIds.contains(ev->Get()->Record.GetTxId());
        });
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        if (disableChecksumOnRecovery) {
            runtime.WaitFor("completed restores wait to be replayed", [&] {
                return replayed.size() == tables;
            });
            replayed.Unblock(1);
            runtime.WaitFor("the first completed table discards its checkpoint", [&] {
                return discardedSnapshots.size() == 1;
            }, TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(readStates().Value(EState::Done, 0u), 1u);
            const auto pending = TestGetImport(runtime, id, "/MyRoot");
            UNIT_ASSERT_VALUES_EQUAL(
                pending.GetResponse().GetEntry().GetProgress(), Ydb::Import::ImportProgress::PROGRESS_TRANSFER_DATA);
            replayed.Stop().Unblock();
            testEnv.TestWaitNotification(runtime, id);
            TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
            runtime.WaitFor("disabled validation still discards the restored checkpoints", [&] {
                return discardedSnapshots.size() == tables;
            }, TDuration::Seconds(5));
            UNIT_ASSERT_VALUES_EQUAL(starts.size(), tables);
            UNIT_ASSERT_VALUES_EQUAL(readStates().Value(EState::Done, 0u), tables);
            UNIT_ASSERT_VALUES_EQUAL(restoreTxIds.size(), tables);
            return;
        }
        runtime.WaitFor("all checksum checks resumed in parallel", [&] {
            return starts.size() == 2 * tables;
        });
        const auto recovered = readStates();
        UNIT_ASSERT_VALUES_EQUAL(recovered.Value(EState::Transferring, 0u), tables);
        const auto status = TestGetImport(runtime, id, "/MyRoot");
        UNIT_ASSERT_VALUES_EQUAL(
            status.GetResponse().GetEntry().GetProgress(), Ydb::Import::ImportProgress::PROGRESS_TRANSFER_DATA);

        for (size_t idx = tables; idx < 2 * tables; ++idx) {
            complete(idx);
        }
        testEnv.TestWaitNotification(runtime, id);
        TestGetImport(runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(starts.size(), 2 * tables);
        UNIT_ASSERT_VALUES_EQUAL(restoreTxIds.size(), tables);
    }

    Y_UNIT_TEST_TWIN(TableChecksumParallelRecovery, IsFs) {
        TableChecksumParallel<IsFs>(false);
    }

    Y_UNIT_TEST_TWIN(TableChecksumParallelCancellation, IsFs) {
        TableChecksumParallel<IsFs>(true);
    }

    Y_UNIT_TEST_TWIN(TableChecksumDisabledOnRecovery, IsFs) {
        TableChecksumParallel<IsFs>(false, true);
    }
}
