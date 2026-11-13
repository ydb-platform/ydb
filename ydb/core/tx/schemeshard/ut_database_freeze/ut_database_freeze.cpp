#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tablet_flat/defs.h>
#include <ydb/core/tablet_flat/flat_redo_player.h>
#include <ydb/core/tablet_flat/flat_sausage_slicer.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/hook/hook.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <cstring>
#include <utility>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

using TStage = NKikimrTxDataShard::TDatabaseFreezeState;

struct TFreezeEnv {
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId = 100;
    ui64 Owner = TTestTxConfig::SchemeShard;
    ui64 PathId = 1;
    TString Root = "/MyRoot";

    TFreezeEnv()
        : Env(Runtime, TTestEnvOptions()
            .EnableProtoSourceIdInfo(true)
            .EnableChangefeedInitialScan(true)
            .EnableBackgroundCompaction(false))
    {}

    TString Path(const TString& name) const {
        return name.StartsWith("/") ? name : Root + '/' + name;
    }

    void CreateTable(const TString& name = "Table", ui32 partitions = 1) {
        TestCreateTable(Runtime, Owner, ++TxId, Root, Sprintf(R"(
            Name: "%s"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: %u
        )", name.c_str(), partitions));
        Env.TestWaitNotification(Runtime, TxId, Owner);
    }

    TVector<ui64> Shards(const TString& name = "Table", bool privateTable = false) {
        const auto desc = privateTable
            ? DescribePrivatePath(Runtime, Owner, Path(name), true)
            : DescribePath(Runtime, Owner, Path(name), true);
        TestDescribeResult(desc, {NLs::PathExist});
        TVector<ui64> result;
        for (const auto& part : desc.GetPathDescription().GetTablePartitions()) {
            result.push_back(part.GetDatashardId());
        }
        UNIT_ASSERT(!result.empty());
        return result;
    }

    NKikimrScheme::TEvDatabaseFreezeResult Request(ui64 epoch, bool freeze, ui64 pathId = 0) {
        const auto sender = Runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvSchemeShard::TEvDatabaseFreeze>();
        request->Record.SetPathId(pathId ? pathId : PathId);
        request->Record.SetEpoch(epoch);
        request->Record.SetFreeze(freeze);
        ForwardToTablet(Runtime, Owner, sender, request.Release());
        return Runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDatabaseFreezeResult>(sender)->Get()->Record;
    }

    void Wait(ui64 epoch, bool freeze, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        for (ui32 attempt = 0; attempt < 300; ++attempt) {
            const auto result = Request(epoch, freeze);
            if (result.GetReady()) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(status), result.ShortDebugString());
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), result.ShortDebugString());
            Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        UNIT_FAIL("Timed out waiting for SchemeShard freeze: epoch=" << epoch << " freeze=" << freeze);
    }

    void CheckPending(ui64 epoch = 1, bool freeze = true) {
        const auto result = Request(epoch, freeze);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), result.ShortDebugString());
        UNIT_ASSERT_C(!result.GetReady(), result.ShortDebugString());
    }

    void AdvancePlanStep(const TString& name = "Table") {
        // Freeze can move the write edge into the next step. Unlike a real
        // coordinator, TTestEnv's fake coordinator does not send empty steps
        // automatically, so explicitly let post-thaw writes reach that edge.
        const auto step = Env.GetCoordinatorStep() + 1;
        const auto sender = Runtime.AllocateEdgeActor();
        for (const auto shard : Shards(name)) {
            ForwardToTablet(Runtime, shard, sender, new TEvTxProcessing::TEvPlanStep(step, 0, shard));
            const auto accepted = Runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvPlanStepAccepted>(sender);
            UNIT_ASSERT_VALUES_EQUAL(accepted->Get()->Record.GetStep(), step);
        }
    }

    void Write(const TString& name = "Table", bool success = true, ui32 key = 1, int partition = 0) {
        WriteRow(Runtime, Owner, ++TxId, Path(name), partition, key, "value", success);
    }

    void CheckSchemaBlocked() {
        TestMkDir(Runtime, Owner, ++TxId, Root, "Blocked", {{NKikimrScheme::StatusPreconditionFailed}});
    }

    void CheckSchemaAvailable() {
        ++TxId;
        TestMkDir(Runtime, Owner, TxId, Root, Sprintf("Allowed%lu", TxId));
        Env.TestWaitNotification(Runtime, TxId, Owner);
    }

    void CreateStream(bool initialScan = false) {
        TestCreateCdcStream(Runtime, Owner, ++TxId, Root, Sprintf(R"(
            TableName: "Table"
            StreamDescription {
                Name: "Stream"
                Mode: ECdcStreamModeUpdate
                Format: ECdcStreamFormatJson
                State: %s
            }
        )", initialScan ? "ECdcStreamStateScan" : "ECdcStreamStateReady"));
        Env.TestWaitNotification(Runtime, TxId, Owner);
    }
};

// Identify the commit containing this record's actual erase, rather than an
// unrelated DataShard commit or the earlier TEvRemoveRecords notification.
bool RemovesChangeRecord(const TEvTablet::TEvCommit& commit, ui32 changeRecordsTableId, ui64 order) {
    if (commit.IsSnapshot || commit.EmbeddedLogBody.empty()) {
        return false;
    }
    const auto redo = NPageCollection::TSlicer::Lz4()->Decode(commit.EmbeddedLogBody);
    NTable::NRedo::TReader reader({redo.data(), redo.size()});
    while (const auto chunk = reader.Next()) {
        const auto* label = reinterpret_cast<const NTable::NRedo::TChunk*>(chunk.data());
        if (label->Event != NTable::NRedo::ERedo::Update && label->Event != NTable::NRedo::ERedo::UpdateV) {
            continue;
        }
        UNIT_ASSERT(chunk.size() >= sizeof(NTable::NRedo::TEvUpdate));
        const auto* update = reinterpret_cast<const NTable::NRedo::TEvUpdate*>(chunk.data());
        if (update->Table != changeRecordsTableId
            || update->Rop != NTable::ERowOp::Erase)
        {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL(update->Keys, 1);
        size_t keyOffset = sizeof(*update);
        if (label->Event == NTable::NRedo::ERedo::UpdateV) {
            keyOffset += sizeof(NTable::NRedo::TEvUpdateV);
        }
        UNIT_ASSERT(chunk.size() >= keyOffset + sizeof(NTable::NRedo::TValue));
        const auto* key = reinterpret_cast<const NTable::NRedo::TValue*>(chunk.data() + keyOffset);
        UNIT_ASSERT_VALUES_EQUAL(ui32(key->TypeId), ui32(NScheme::NTypeIds::Uint64));
        UNIT_ASSERT_VALUES_EQUAL(key->Size, sizeof(ui64));
        keyOffset += sizeof(*key);
        UNIT_ASSERT(chunk.size() >= keyOffset + sizeof(ui64));
        ui64 removedOrder;
        std::memcpy(&removedOrder, chunk.data() + keyOffset, sizeof(removedOrder));
        if (removedOrder == order) {
            return true;
        }
    }
    return false;
}

void CheckGlobalBarrier(TStage::EStage heldStage) {
    TFreezeEnv env;
    TestCreateIndexedTable(env.Runtime, ++env.TxId, env.Root, R"(
        TableDescription {
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2
        }
        IndexDescription {
            Name: "ByValue"
            KeyColumnNames: ["value"]
            Type: EIndexTypeGlobalAsync
        }
    )");
    env.Env.TestWaitNotification(env.Runtime, env.TxId);
    auto shards = env.Shards();
    const auto indexShards = env.Shards("Table/ByValue/indexImplTable", true);
    shards.insert(shards.end(), indexShards.begin(), indexShards.end());
    UNIT_ASSERT(shards.size() >= 3);

    bool advanced = false;
    THashSet<ui64> participants;
    auto requests = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto& ev) {
        const auto& request = ev->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(request.GetSchemeShardId(), env.Owner);
        UNIT_ASSERT_VALUES_EQUAL(request.GetDomainOwnerId(), env.Owner);
        UNIT_ASSERT_VALUES_EQUAL(request.GetDomainLocalPathId(), env.PathId);
        advanced |= request.GetStage() > heldStage;
    });
    auto replies = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
        participants.insert(ev->Get()->Record.GetTabletId());
    });
    TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> held(env.Runtime, [&](const auto& ev) {
        return ev->Get()->Record.GetStage() == heldStage && ev->Get()->Record.GetTabletId() == shards.front();
    });
    env.CheckPending();
    env.Runtime.WaitFor("held participant acknowledgement", [&] { return !held.empty(); });
    env.Runtime.SimulateSleep(TDuration::Seconds(2));
    env.CheckPending();
    UNIT_ASSERT(!advanced);
    env.CheckSchemaBlocked();
    held.Stop().Unblock();
    env.Wait(1, true);
    for (const auto shard : shards) {
        UNIT_ASSERT_C(participants.contains(shard), "Missing participant " << shard);
    }
    UNIT_ASSERT(advanced);
    env.Wait(2, false);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(SchemeShardDatabaseFreeze) {
    Y_UNIT_TEST(EmptyDatabaseAndEpochValidation) {
        TFreezeEnv env;
        UNIT_ASSERT_VALUES_EQUAL(env.Request(0, true).GetStatus(), ui32(Ydb::StatusIds::NOT_FOUND));
        UNIT_ASSERT_VALUES_EQUAL(env.Request(1, true, Max<ui64>()).GetStatus(), ui32(Ydb::StatusIds::NOT_FOUND));
        env.Wait(1, true);
        env.CheckSchemaBlocked();
        const auto conflict = env.Request(1, false);
        UNIT_ASSERT(conflict.GetReady());
        UNIT_ASSERT_VALUES_EQUAL(conflict.GetStatus(), ui32(Ydb::StatusIds::BAD_REQUEST));
        env.Wait(2, false);
        UNIT_ASSERT_VALUES_EQUAL(env.Request(1, true).GetStatus(), ui32(Ydb::StatusIds::ABORTED));
        env.CheckSchemaAvailable();
    }

    Y_UNIT_TEST(FreezeAllTablesReadWriteAndThaw) {
        TFreezeEnv env;
        env.CreateTable("Table", 2);
        env.CreateTable("Other");
        env.Write();
        env.Write("Other");
        env.Wait(1, true);
        env.Write("Table", false, 2);
        env.Write("Table", false, 2147483648u, 1);
        env.Write("Other", false, 2);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(env.Runtime, "/MyRoot/Table"), 1);
        UNIT_ASSERT_VALUES_EQUAL(CountRows(env.Runtime, "/MyRoot/Other"), 1);
        env.CheckSchemaBlocked();
        TestDropTable(env.Runtime, ++env.TxId, env.Root, "Table", {{NKikimrScheme::StatusPreconditionFailed}});
        env.Wait(2, false);
        env.AdvancePlanStep();
        env.AdvancePlanStep("Other");
        env.Write("Table", true, 2);
        env.Write("Other", true, 2);
        env.CheckSchemaAvailable();
        UNIT_ASSERT_VALUES_EQUAL(CountRows(env.Runtime, "/MyRoot/Table"), 2);
    }

    Y_UNIT_TEST(TtlSchedulingPausesUntilThaw) {
        TFreezeEnv env;
        TestCreateTable(env.Runtime, ++env.TxId, env.Root, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "ts" Type: "Timestamp" }
            KeyColumnNames: ["key"]
            TTLSettings { Enabled { ColumnName: "ts" } }
        )");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);

        ui32 requests = 0;
        ui32 completed = 0;
        ui32 ticks = 0;
        auto eraseRequests = env.Runtime.AddObserver<TEvDataShard::TEvConditionalEraseRowsRequest>([&](auto&) {
            ++requests;
        });
        auto eraseReplies = env.Runtime.AddObserver<TEvDataShard::TEvConditionalEraseRowsResponse>([&](auto& ev) {
            if (ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvConditionalEraseRowsResponse::OK) {
                ++completed;
            }
        });
        auto timer = env.Runtime.AddObserver<NSchemeShard::TEvPrivate::TEvRunConditionalErase>([&](auto&) {
            ++ticks;
        });
        env.Runtime.AdvanceCurrentTime(TDuration::Hours(1));
        env.Runtime.WaitFor("initial TTL round completed", [&] { return completed != 0; });

        env.Wait(1, true);
        const auto frozenRequests = requests;
        const auto frozenTicks = ticks;
        env.Runtime.AdvanceCurrentTime(TDuration::Hours(1));
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_C(ticks > frozenTicks, "TTL timer must run while the database is frozen");
        UNIT_ASSERT_VALUES_EQUAL(requests, frozenRequests);

        env.Wait(2, false);
        const auto previousCompleted = completed;
        env.Runtime.AdvanceCurrentTime(TDuration::Hours(1));
        env.Runtime.WaitFor("TTL round after thaw completed", [&] { return completed > previousCompleted; });
        UNIT_ASSERT(requests > frozenRequests);
    }

    Y_UNIT_TEST(AllShardsMustFenceBeforeDrainingIndexes) {
        CheckGlobalBarrier(TStage::FENCE_WRITES);
    }

    Y_UNIT_TEST(AllIndexesMustDrainBeforeClosingChangeExchange) {
        CheckGlobalBarrier(TStage::DRAIN_INDEXES);
    }

    Y_UNIT_TEST(AlreadyAcceptedSchemaWorkFinishesBeforeFixingParticipants) {
        TFreezeEnv env;
        env.CreateTable();
        ui32 shardRequests = 0;
        auto observer = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto&) { ++shardRequests; });
        TBlockEvents<TEvDataShard::TEvProposeTransaction> creation(env.Runtime);
        TestCreateTable(env.Runtime, ++env.TxId, env.Root, R"(
            Name: "NewTable"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        const auto createTx = env.TxId;
        env.Runtime.WaitFor("accepted table creation at DataShard", [&] { return !creation.empty(); });
        env.CheckPending();
        UNIT_ASSERT_VALUES_EQUAL(shardRequests, 0);
        const auto pending = env.Request(1, true);
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "Waiting for previously accepted schema operations");
        env.CheckSchemaBlocked();
        creation.Stop().Unblock();
        env.Env.TestWaitNotification(env.Runtime, createTx);
        env.Wait(1, true);
        env.Write("NewTable", false);
        env.Wait(2, false);
        env.Write("NewTable");
    }

    Y_UNIT_TEST(FreezeWaitsForEarlierCdcRemovalCleanup) {
        TFreezeEnv env;
        env.CreateTable();
        env.CreateStream();
        const auto shard = env.Shards().front();
        const auto shardActor = ResolveTablet(env.Runtime, shard);
        const TPathId streamId(env.Owner, DescribePath(env.Runtime, env.Path("Table/Stream")).GetPathId());
        // Resolve the local table ID without depending on DataShard's private schema.
        NTabletFlatScheme::TSchemeChanges localSchema;
        TString schemaError;
        const auto schemaStatus = LocalSchemeTx(env.Runtime, shard, "", true, localSchema, schemaError);
        UNIT_ASSERT_VALUES_EQUAL_C(ui32(schemaStatus), ui32(NKikimrProto::OK), schemaError);
        TMaybe<ui32> changeRecordsTableId;
        for (const auto& delta : localSchema.GetDelta()) {
            if (delta.GetDeltaType() == NTabletFlatScheme::TAlterRecord::AddTable
                && delta.GetTableName() == "ChangeRecords")
            {
                changeRecordsTableId = delta.GetTableId();
                break;
            }
        }
        UNIT_ASSERT_C(changeRecordsTableId, localSchema.ShortDebugString());
        TMaybe<ui64> recordOrder;
        auto records = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>([&](auto& ev) {
            if (ev->Sender == shardActor && !recordOrder) {
                for (const auto& record : ev->Get()->Records) {
                    if (record.PathId == streamId) {
                        recordOrder = record.Order;
                        break;
                    }
                }
            }
        });
        // Hold the write itself: an ACK delay would still allow the topic to
        // persist the row and would not reproduce discarded, undelivered data.
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
        TBlockEvents<NChangeExchange::TEvChangeExchange::TEvRemoveRecords> cleanup(env.Runtime, [&](const auto& ev) {
            if (ev->GetRecipientRewrite() == shardActor && recordOrder) {
                for (const auto order : ev->Get()->Records) {
                    if (order == *recordOrder) {
                        return true;
                    }
                }
            }
            return false;
        });
        env.Write();
        env.Runtime.WaitFor("CDC write before reaching the topic", [&] {
            return recordOrder && !delivery.empty();
        }, TDuration::Seconds(10));
        const auto queued = ReadSystemTable(env.Runtime, shard, "ChangeRecords", {"Order"}, {"Order"});
        const auto queuedRows = NClient::TValue::Create(queued)["Result"]["List"];
        UNIT_ASSERT_VALUES_EQUAL(queuedRows.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ui64(queuedRows[0]["Order"]), *recordOrder);

        TestDropCdcStream(env.Runtime, ++env.TxId, env.Root, R"(
            TableName: "Table" StreamName: "Stream"
        )");
        const auto dropTx = env.TxId;
        env.Runtime.WaitFor("discard cleanup for the removed CDC stream", [&] {
            return !cleanup.empty();
        }, TDuration::Seconds(10));
        // Allow the accepted DDL to advance independently of its blocked
        // asynchronous cleanup. Do not assume DDL completion implies cleanup.
        env.Runtime.SimulateSleep(TDuration::Seconds(2));

        TMaybe<std::pair<ui32, ui32>> cleanupCommit;
        auto commits = env.Runtime.AddObserver<TEvTablet::TEvCommit>([&](auto& ev) {
            const auto& commit = *ev->Get();
            if (commit.TabletID == shard && RemovesChangeRecord(commit, *changeRecordsTableId, *recordOrder)) {
                UNIT_ASSERT(!cleanupCommit);
                cleanupCommit = std::make_pair(commit.Generation, commit.Step);
            }
        });
        auto isCleanupCommit = [&](const TEvTablet::TEvCommitResult& result) {
            return result.TabletID == shard && cleanupCommit
                && cleanupCommit->first == result.Generation && cleanupCommit->second == result.Step;
        };
        TBlockEvents<TEvTablet::TEvCommitResult> commitAck(env.Runtime, [&](const auto& ev) {
            return isCleanupCommit(*ev->Get());
        });
        bool cleanupCommitted = false;
        bool fenceInstalled = false;
        // Register after the blocker: a held ACK must not count as committed
        // from the executor's perspective until it is actually released.
        auto committed = env.Runtime.AddObserver<TEvTablet::TEvCommitResult>([&](auto& ev) {
            if (isCleanupCommit(*ev->Get())) {
                UNIT_ASSERT_VALUES_EQUAL(ui32(ev->Get()->Status), ui32(NKikimrProto::OK));
                cleanupCommitted = true;
            }
        });
        auto replies = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
            const auto& result = ev->Get()->Record;
            if (result.GetTabletId() == shard && result.GetEpoch() == 1 && result.GetStage() == TStage::FENCE_WRITES
                && result.GetStatus() == Ydb::StatusIds::SUCCESS && result.GetReady())
            {
                fenceInstalled = true;
            }
        });
        env.CheckPending();
        // A removed stream has no delivery obligation. Its queued records do
        // not prevent installing the write fence, but still need cleanup.
        env.Runtime.WaitFor("write fence while CDC removal cleanup is blocked", [&] {
            return fenceInstalled;
        }, TDuration::Seconds(10));
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        env.CheckPending();
        env.CheckSchemaBlocked();
        env.Write("Table", false, 2);
        UNIT_ASSERT(!cleanupCommit);
        UNIT_ASSERT(!cleanupCommitted);

        cleanup.Stop().Unblock();
        env.Runtime.WaitFor("cleanup commit awaiting its durable acknowledgement", [&] {
            return !commitAck.empty();
        }, TDuration::Seconds(10));
        // The change record is already erased in Execute(), but cleanup has
        // not completed. An empty queue must not complete the overall freeze.
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        env.CheckPending();
        UNIT_ASSERT(!cleanupCommitted);
        UNIT_ASSERT(fenceInstalled);

        commitAck.Stop().Unblock();
        env.Runtime.WaitFor("cleanup commit acknowledged to DataShard", [&] {
            return cleanupCommitted;
        }, TDuration::Seconds(10));
        env.Env.TestWaitNotification(env.Runtime, dropTx);
        env.Wait(1, true);
        const auto removed = ReadSystemTable(env.Runtime, shard, "ChangeRecords", {"Order"}, {"Order"});
        UNIT_ASSERT_VALUES_EQUAL(NClient::TValue::Create(removed)["Result"]["List"].Size(), 0);
        // Keep the old write blocked: delivery to the intentionally deleted
        // destination is outside this freeze boundary and is not required.
        UNIT_ASSERT(!delivery.empty());
        delivery.Stop();
        env.Write("Table", false, 2);
        env.Wait(2, false);
        env.AdvancePlanStep();
        env.Write("Table", true, 2);
    }

    Y_UNIT_TEST(AlreadyRunningExportFinishesBeforeFreeze) {
        const auto port = NTesting::GetFreePort();
        using NWrappers::NTestHelpers::TS3Mock;
        TS3Mock s3(TS3Mock::TSettings{port});
        UNIT_ASSERT_C(s3.Start(), s3.GetError());

        TFreezeEnv env;
        env.Runtime.GetAppData().FeatureFlags.SetEnableExportAutoDropping(true);
        env.CreateTable();
        env.Write();
        auto exportRequest = [&](const TString& prefix) {
            return Sprintf(R"(
                ExportToS3Settings {
                    endpoint: "localhost:%u"
                    scheme: HTTP
                    items { source_path: "/MyRoot/Table" destination_prefix: "%s" }
                }
            )", ui32(port), prefix.c_str());
        };
        ui64 copyTxId = 0;
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> copyTables(env.Runtime, [&](const auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransaction()) {
                if (tx.GetInternal() && tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables) {
                    copyTxId = ev->Get()->Record.GetTxId();
                    return true;
                }
            }
            return false;
        });
        const auto exportId = ++env.TxId;
        TestExport(env.Runtime, env.Owner, exportId, env.Root, exportRequest("before-freeze"));
        env.Runtime.WaitFor("accepted export between directory creation and table copying", [&] {
            return !copyTables.empty();
        }, TDuration::Seconds(30));
        UNIT_ASSERT(copyTxId);
        UNIT_ASSERT_VALUES_EQUAL(copyTables.size(), 1);
        const auto exportDirectory = env.Path(Sprintf("export-%lu", exportId));
        TestDescribeResult(DescribePath(env.Runtime, env.Owner, exportDirectory), {NLs::PathExist});
        const auto running = TestGetExport(env.Runtime, env.Owner, exportId, env.Root);
        UNIT_ASSERT_VALUES_EQUAL(running.GetResponse().GetEntry().GetProgress(),
            Ydb::Export::ExportProgress::PROGRESS_PREPARING);
        // The directory's schema transaction can finish while the next request
        // stays in transit. The export itself must keep schema draining active.
        env.Runtime.SimulateSleep(TDuration::Seconds(2));

        ui32 fenceRequests = 0;
        auto fences = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto& ev) {
            const auto& request = ev->Get()->Record;
            if (request.GetEpoch() == 1 && request.GetStage() == TStage::FENCE_WRITES) {
                ++fenceRequests;
            }
        });
        const auto started = env.Request(1, true);
        UNIT_ASSERT_VALUES_EQUAL_C(started.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), started.ShortDebugString());
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        const auto pending = env.Request(1, true);
        UNIT_ASSERT_VALUES_EQUAL_C(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS), pending.ShortDebugString());
        UNIT_ASSERT_C(!pending.GetReady(),
            "Freeze completed while an accepted export was waiting to copy tables: " << pending.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(fenceRequests, 0,
            "Freeze must wait for the accepted export before fixing DataShard participants");
        env.CheckSchemaBlocked();
        TestExport(env.Runtime, env.Owner, ++env.TxId, env.Root, exportRequest("rejected-during-drain"), "", "",
            Ydb::StatusIds::PRECONDITION_FAILED);

        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> exportSteps(env.Runtime, [&](const auto& ev) {
            for (const auto& tx : ev->Get()->Record.GetTransaction()) {
                if (tx.GetInternal() && tx.GetWorkingDir() == exportDirectory
                    && (tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpBackup
                        || tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpDropTable))
                {
                    return true;
                }
            }
            return false;
        });
        TMaybe<NKikimrScheme::TEvModifySchemeTransactionResult> copyResult;
        auto replies = env.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>([&](auto& ev) {
            if (ev->Get()->Record.GetTxId() == copyTxId) {
                copyResult = ev->Get()->Record;
            }
        });
        copyTables.Stop().Unblock();
        env.Runtime.WaitFor("accepted export's table-copy proposal", [&] {
            return bool(copyResult);
        }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(copyResult->GetStatus(), NKikimrScheme::StatusAccepted, copyResult->ShortDebugString());
        for (const auto step : {NKikimrSchemeOp::EOperationType::ESchemeOpBackup,
            NKikimrSchemeOp::EOperationType::ESchemeOpDropTable})
        {
            env.Runtime.WaitFor("accepted export awaiting its next internal schema step", [&] {
                return !exportSteps.empty();
            }, TDuration::Seconds(30));
            const auto& request = exportSteps.front()->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL_C(ui32(request.GetTransaction(0).GetOperationType()), ui32(step), request.ShortDebugString());
            env.CheckPending();
            UNIT_ASSERT_VALUES_EQUAL_C(fenceRequests, 0,
                "Freeze must wait for export data transfer and auto cleanup");
            exportSteps.Unblock(1);
        }
        exportSteps.Stop();
        env.Env.TestWaitNotification(env.Runtime, exportId, env.Owner);
        const auto exported = TestGetExport(env.Runtime, env.Owner, exportId, env.Root);
        UNIT_ASSERT_VALUES_EQUAL(exported.GetResponse().GetEntry().GetProgress(),
            Ydb::Export::ExportProgress::PROGRESS_DONE);
        UNIT_ASSERT(s3.GetData().contains("/before-freeze/scheme.pb"));
        UNIT_ASSERT(s3.GetData().contains("/before-freeze/data_00.csv"));
        TestDescribeResult(DescribePath(env.Runtime, env.Owner, exportDirectory), {NLs::PathNotExist});

        env.Wait(1, true);
        UNIT_ASSERT(fenceRequests);
        TestExport(env.Runtime, env.Owner, ++env.TxId, env.Root, exportRequest("rejected-while-frozen"), "", "",
            Ydb::StatusIds::PRECONDITION_FAILED);
        env.Write("Table", false, 2);
        env.Wait(2, false);
        env.AdvancePlanStep();
        env.Write("Table", true, 2);
        const auto nextExportId = ++env.TxId;
        TestExport(env.Runtime, env.Owner, nextExportId, env.Root, exportRequest("after-thaw"));
        env.Env.TestWaitNotification(env.Runtime, nextExportId, env.Owner);
        TestGetExport(env.Runtime, env.Owner, nextExportId, env.Root);
        UNIT_ASSERT(s3.GetData().contains("/after-thaw/data_00.csv"));
    }

    Y_UNIT_TEST(AlreadyRunningImportBuildsIndexesBeforeFreeze) {
        const auto port = NTesting::GetFreePort();
        using NWrappers::NTestHelpers::TS3Mock;
        const THashMap<TString, TString> backup = {
            {"/metadata.json", R"({"version": 0})"},
            {"/scheme.pb", R"(
                columns {
                    name: "key"
                    type { optional_type { item { type_id: UINT32 } } }
                }
                columns {
                    name: "value"
                    type { optional_type { item { type_id: UTF8 } } }
                }
                primary_key: "key"
                indexes {
                    name: "ByValue"
                    index_columns: "value"
                    global_index {}
                }
            )"},
            {"/data_00.csv", "1,\"value\"\n"},
        };
        TS3Mock s3(backup, TS3Mock::TSettings(port));
        UNIT_ASSERT_C(s3.Start(), s3.GetError());

        TFreezeEnv env;
        env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(false);
        ui64 buildTxId = 0;
        TBlockEvents<TEvIndexBuilder::TEvCreateRequest> buildIndex(env.Runtime, [&](const auto& ev) {
            const auto& request = ev->Get()->Record;
            if (request.GetInternal() && request.GetSettings().source_path() == env.Path("Imported")
                && (!buildTxId || request.GetTxId() == buildTxId))
            {
                UNIT_ASSERT_VALUES_EQUAL(request.GetDatabaseName(), env.Root);
                UNIT_ASSERT_VALUES_EQUAL(request.GetSettings().index().name(), "ByValue");
                buildTxId = request.GetTxId();
                return true;
            }
            return false;
        });
        const auto importId = ++env.TxId;
        TestImport(env.Runtime, env.Owner, importId, env.Root, Sprintf(R"(
            ImportFromS3Settings {
                endpoint: "localhost:%u"
                scheme: HTTP
                no_acl: true
                items {
                    source_prefix: ""
                    destination_path: "/MyRoot/Imported"
                }
            }
        )", ui32(port)));
        env.Runtime.WaitFor("accepted import requesting its index build", [&] {
            return !buildIndex.empty();
        }, TDuration::Seconds(30));
        UNIT_ASSERT(buildTxId);
        UNIT_ASSERT_VALUES_EQUAL(buildIndex.size(), 1);
        const auto pendingImport = TestGetImport(env.Runtime, env.Owner, importId, env.Root);
        UNIT_ASSERT_VALUES_EQUAL(pendingImport.GetResponse().GetEntry().GetProgress(),
            Ydb::Import::ImportProgress::PROGRESS_BUILD_INDEXES);

        ui32 fenceRequests = 0;
        auto fences = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto& ev) {
            const auto& request = ev->Get()->Record;
            if (request.GetEpoch() == 1 && request.GetStage() == TStage::FENCE_WRITES) {
                ++fenceRequests;
            }
        });
        THashSet<ui64> participants;
        auto freezeReplies = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
            const auto& result = ev->Get()->Record;
            if (result.GetEpoch() == 1 && result.GetStage() == TStage::FENCE_WRITES
                && result.GetStatus() == Ydb::StatusIds::SUCCESS && result.GetReady())
            {
                participants.insert(result.GetTabletId());
            }
        });
        env.CheckPending();
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        env.CheckPending();
        env.CheckSchemaBlocked();
        UNIT_ASSERT_VALUES_EQUAL_C(fenceRequests, 0, "Freeze must wait for the accepted import before fencing DataShards");
        // New client builds stay forbidden while the accepted import drains.
        TestBuildIndex(env.Runtime, ++env.TxId, env.Owner, env.Root, env.Path("Imported"), "NewIndex", {"value"},
            Ydb::StatusIds::PRECONDITION_FAILED);
        const auto importBuildRequest = buildIndex.front()->Get()->Record;
        auto checkUnrelatedInternalBuildBlocked = [&] {
            auto request = MakeHolder<TEvIndexBuilder::TEvCreateRequest>();
            request->Record.CopyFrom(importBuildRequest);
            request->Record.SetTxId(++env.TxId);
            request->Record.ClearOperationParams();
            request->Record.MutableSettings()->mutable_index()->set_name("UnrelatedIndex");
            const auto sender = env.Runtime.AllocateEdgeActor();
            ForwardToTablet(env.Runtime, env.Owner, sender, request.Release());
            const auto response = env.Runtime.GrabEdgeEventRethrow<TEvIndexBuilder::TEvCreateResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED,
                response->Get()->Record.ShortDebugString());
        };
        checkUnrelatedInternalBuildBlocked();

        TMaybe<NKikimrIndexBuilder::TEvCreateResponse> buildResult;
        auto replies = env.Runtime.AddObserver<TEvIndexBuilder::TEvCreateResponse>([&](auto& ev) {
            if (ev->Get()->Record.GetTxId() == buildTxId) {
                buildResult = ev->Get()->Record;
            }
        });
        buildIndex.Stop().Unblock();
        env.Runtime.WaitFor("index build response for the accepted import", [&] {
            return bool(buildResult);
        }, TDuration::Seconds(10));
        env.Env.TestWaitNotification(env.Runtime, importId, env.Owner);
        const auto imported = TestGetImport(env.Runtime, env.Owner, importId, env.Root,
            TVector<Ydb::StatusIds::StatusCode>{Ydb::StatusIds::SUCCESS, Ydb::StatusIds::CANCELLED});
        const auto& entry = imported.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL_C(ui32(buildResult->GetStatus()), ui32(Ydb::StatusIds::SUCCESS),
            TStringBuilder() << "Index build continuing an accepted import was rejected during freeze: "
                << buildResult->ShortDebugString() << "; import: " << entry.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(ui32(entry.GetStatus()), ui32(Ydb::StatusIds::SUCCESS), entry.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);

        env.Wait(1, true);
        checkUnrelatedInternalBuildBlocked();
        for (const auto shard : env.Shards("Imported")) {
            UNIT_ASSERT_C(participants.contains(shard), "Imported table was not fenced: " << shard);
        }
        for (const auto shard : env.Shards("Imported/ByValue/indexImplTable", true)) {
            UNIT_ASSERT_C(participants.contains(shard), "Imported index was not fenced: " << shard);
        }
        env.Write("Imported", false, 2);
        env.Wait(2, false);
        env.AdvancePlanStep("Imported");
        env.Write("Imported", true, 2);
    }

    Y_UNIT_TEST(AlreadyRunningIndexBuildFinishesBeforeFreeze) {
        TFreezeEnv env;
        env.CreateTable();
        env.Write();
        TBlockEvents<TEvDataShard::TEvBuildIndexProgressResponse> scan(env.Runtime);
        const auto buildTx = ++env.TxId;
        TestBuildIndex(env.Runtime, buildTx, env.Owner, env.Root, env.Path("Table"), "ByValue", {"value"});
        env.Runtime.WaitFor("index build progress", [&] { return !scan.empty(); });
        env.CheckPending();
        env.CheckSchemaBlocked();
        scan.Stop().Unblock();
        env.Env.TestWaitNotification(env.Runtime, buildTx);
        env.Wait(1, true);
        UNIT_ASSERT(!env.Shards("Table/ByValue/indexImplTable", true).empty());
        env.Write("Table", false, 2);
        env.Wait(2, false);
        env.Write("Table", true, 2);
    }

    Y_UNIT_TEST(AlreadyRunningSplitFreezesSuccessorsAndExcludesRetiredSource) {
        TFreezeEnv env;
        env.CreateTable();
        env.CreateStream();
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
        env.Write();
        env.Runtime.WaitFor("CDC delivery before split", [&] { return !delivery.empty(); });
        const auto source = env.Shards().front();
        TBlockEvents<TEvDataShard::TEvInitSplitMergeDestination> split(env.Runtime);
        const auto splitTx = ++env.TxId;
        TestSplitTable(env.Runtime, splitTx, env.Path("Table"), Sprintf(R"(
            SourceTabletId: %lu
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 100 } } } }
        )", source));
        env.Runtime.WaitFor("accepted split proposal", [&] { return !split.empty(); });
        THashSet<ui64> participants;
        auto observer = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
            participants.insert(ev->Get()->Record.GetTabletId());
        });
        env.CheckPending();
        UNIT_ASSERT(participants.empty());
        split.Stop().Unblock();
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        env.CheckPending();
        UNIT_ASSERT_C(participants.empty(), "Split must drain its source CDC before fixing freeze participants");
        delivery.Stop().Unblock();
        env.Env.TestWaitNotification(env.Runtime, splitTx);
        const auto successors = env.Shards();
        UNIT_ASSERT_VALUES_EQUAL(successors.size(), 2);
        env.Wait(1, true);
        UNIT_ASSERT(!participants.contains(source));
        for (const auto shard : successors) {
            UNIT_ASSERT(participants.contains(shard));
        }
        env.Write("Table", false, 1, 0);
        env.Write("Table", false, 101, 1);
        env.Wait(2, false);
        env.AdvancePlanStep();
        env.Write("Table", true, 1, 0);
        env.Write("Table", true, 101, 1);
    }

    Y_UNIT_TEST(NewSplitIsRejectedWhileFrozen) {
        TFreezeEnv env;
        env.CreateTable();
        const auto source = env.Shards().front();
        env.Wait(1, true);
        const auto description = Sprintf(R"(
            SourceTabletId: %lu
            SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: 100 } } } }
        )", source);
        TestSplitTable(env.Runtime, ++env.TxId, env.Path("Table"), description,
            {{NKikimrScheme::StatusPreconditionFailed}});
        env.Wait(2, false);
        TestSplitTable(env.Runtime, ++env.TxId, env.Path("Table"), description);
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        UNIT_ASSERT_VALUES_EQUAL(env.Shards().size(), 2);
    }

    Y_UNIT_TEST(PendingFreezeAndCompletedStateSurviveSchemeShardRestart) {
        TFreezeEnv env;
        env.CreateTable();
        TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> held(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::DRAIN_CDC;
        });
        env.CheckPending();
        env.Runtime.WaitFor("pending CDC stage", [&] { return !held.empty(); });
        const auto oldReplies = held.size();
        RebootTablet(env.Runtime, env.Owner, env.Runtime.AllocateEdgeActor());
        env.CheckPending();
        env.Runtime.WaitFor("restarted coordinator CDC stage", [&] { return held.size() > oldReplies; });
        env.CheckSchemaBlocked();
        held.Stop().Unblock();
        env.Wait(1, true);
        RebootTablet(env.Runtime, env.Owner, env.Runtime.AllocateEdgeActor());
        const auto completed = env.Request(1, true);
        UNIT_ASSERT(completed.GetReady());
        UNIT_ASSERT_VALUES_EQUAL(completed.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
        env.CheckSchemaBlocked();
        env.Write("Table", false);
        env.Wait(2, false);
        env.CheckSchemaAvailable();
    }

    Y_UNIT_TEST(PendingFreezeSurvivesDataShardRestart) {
        TFreezeEnv env;
        env.CreateTable();
        const auto shard = env.Shards().front();
        TBlockEvents<TEvDataShard::TEvDatabaseFreeze> held(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::DRAIN_CDC;
        });
        env.CheckPending();
        env.Runtime.WaitFor("pending CDC request", [&] { return !held.empty(); });
        RebootTablet(env.Runtime, shard, env.Runtime.AllocateEdgeActor());
        env.CheckPending();
        env.Write("Table", false);
        held.Stop().Unblock();
        env.Wait(1, true);
        env.Wait(2, false);
        env.Write();
    }

    Y_UNIT_TEST(ThawSupersedesPendingFreezeAndIgnoresLateReplies) {
        TFreezeEnv env;
        env.CreateTable();
        TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> held(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::DRAIN_CDC;
        });
        env.CheckPending();
        env.Runtime.WaitFor("late freeze reply", [&] { return !held.empty(); });
        env.Wait(2, false);
        env.Write();
        held.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(env.Request(1, true).GetStatus(), ui32(Ydb::StatusIds::ABORTED));
        env.Wait(2, false);
        env.CheckSchemaAvailable();
        env.Write("Table", true, 2);
    }

    Y_UNIT_TEST(ThawWaitsForAllParticipantsBeforeOpeningSchema) {
        TFreezeEnv env;
        env.CreateTable("Table", 2);
        env.Wait(1, true);
        const auto shard = env.Shards().front();
        TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> held(env.Runtime, [&](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::OPEN && ev->Get()->Record.GetTabletId() == shard;
        });
        env.CheckPending(2, false);
        env.Runtime.WaitFor("pending thaw acknowledgement", [&] { return !held.empty(); });
        env.CheckSchemaBlocked();
        env.CheckPending(2, false);
        held.Stop().Unblock();
        env.Wait(2, false);
        env.CheckSchemaAvailable();
        env.Write();
    }

    Y_UNIT_TEST(FailedFreezeKeepsSchemaFencedUntilThaw) {
        TFreezeEnv env;
        env.CreateTable();
        auto failure = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([](auto& ev) {
            if (ev->Get()->Record.GetStage() == TStage::DRAIN_CDC) {
                ev->Get()->Record.SetReady(true);
                ev->Get()->Record.SetStatus(Ydb::StatusIds::PRECONDITION_FAILED);
                ev->Get()->Record.SetIssue("Injected CDC delivery failure");
            }
        });
        env.Wait(1, true, Ydb::StatusIds::PRECONDITION_FAILED);
        env.CheckSchemaBlocked();
        env.Write("Table", false);
        RebootTablet(env.Runtime, env.Owner, env.Runtime.AllocateEdgeActor());
        env.Wait(1, true, Ydb::StatusIds::PRECONDITION_FAILED);
        env.CheckSchemaBlocked();
        failure.Remove();
        env.Wait(2, false);
        env.CheckSchemaAvailable();
        env.Write();
    }

    Y_UNIT_TEST(FailedThawKeepsSchemaFencedUntilSuccessfulRetry) {
        TFreezeEnv env;
        env.CreateTable();
        env.Wait(1, true);
        auto failure = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([](auto& ev) {
            if (ev->Get()->Record.GetStage() == TStage::OPEN) {
                ev->Get()->Record.SetReady(true);
                ev->Get()->Record.SetStatus(Ydb::StatusIds::UNAVAILABLE);
                ev->Get()->Record.SetIssue("Injected thaw failure");
            }
        });
        env.Wait(2, false, Ydb::StatusIds::UNAVAILABLE);
        env.CheckSchemaBlocked();
        RebootTablet(env.Runtime, env.Owner, env.Runtime.AllocateEdgeActor());
        env.CheckSchemaBlocked();
        failure.Remove();
        env.Wait(3, false);
        env.CheckSchemaAvailable();
        env.Write();
    }

    Y_UNIT_TEST(FailedFreezeCanBeRetriedWithNewEpoch) {
        TFreezeEnv env;
        env.CreateTable();
        auto failure = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([](auto& ev) {
            if (ev->Get()->Record.GetStage() == TStage::DRAIN_CDC) {
                ev->Get()->Record.SetReady(true);
                ev->Get()->Record.SetStatus(Ydb::StatusIds::PRECONDITION_FAILED);
                ev->Get()->Record.SetIssue("Injected freeze failure");
            }
        });
        env.Wait(1, true, Ydb::StatusIds::PRECONDITION_FAILED);
        failure.Remove();
        // Polling a completed epoch must not restart it, even after recovery.
        env.Wait(1, true, Ydb::StatusIds::PRECONDITION_FAILED);
        RebootTablet(env.Runtime, env.Owner, env.Runtime.AllocateEdgeActor());
        env.Wait(1, true, Ydb::StatusIds::PRECONDITION_FAILED);
        env.Wait(2, true);
        const auto completed = env.Request(2, true);
        UNIT_ASSERT(completed.GetReady());
        UNIT_ASSERT(completed.GetIssue().empty());
        env.CheckSchemaBlocked();
        env.Write("Table", false);
        env.Wait(3, false);
        env.AdvancePlanStep();
        env.CheckSchemaAvailable();
        env.Write();
    }

    Y_UNIT_TEST(PendingFreezeReportsParticipantsAndPipeDisconnect) {
        TFreezeEnv env;
        env.CreateTable("Table", 2);
        const auto shard = env.Shards().front();
        TActorId worker;
        THashMap<TActorId, THashMap<ui64, TActorId>> connections;
        auto requests = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto& ev) {
            worker = ev->Sender;
        });
        auto pipes = env.Runtime.AddObserver<TEvTabletPipe::TEvClientConnected>([&](auto& ev) {
            if (ev->Get()->Status == NKikimrProto::OK) {
                connections[ev->GetRecipientRewrite()][ev->Get()->TabletId] = ev->Get()->ClientId;
            }
        });
        TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> held(env.Runtime, [&](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::DRAIN_CDC
                && ev->Get()->Record.GetTabletId() == shard;
        });
        env.CheckPending();
        env.Runtime.WaitFor("pending participant", [&] { return !held.empty(); });
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        auto pending = env.Request(1, true);
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "DRAIN_CDC: acknowledged 1 of 2 DataShards; pending 1");
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), ToString(shard));

        const auto pipe = connections.at(worker).at(shard);
        env.Runtime.Send(new IEventHandle(worker, env.Runtime.AllocateEdgeActor(),
            new TEvTabletPipe::TEvClientDestroyed(shard, pipe, TActorId())));
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        pending = env.Request(1, true);
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "tablet pipe disconnected");
        const auto reconnectedPipe = connections.at(worker).at(shard);
        env.Runtime.Send(new IEventHandle(worker, env.Runtime.AllocateEdgeActor(),
            new TEvTabletPipe::TEvClientConnected(shard, NKikimrProto::ERROR, reconnectedPipe,
                TActorId(), false, false, 0)));
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        pending = env.Request(1, true);
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "tablet pipe connection failed");
        held.Stop().Unblock();
        env.Wait(1, true);
        UNIT_ASSERT(env.Request(1, true).GetIssue().empty());
        env.Wait(2, false);
        UNIT_ASSERT(env.Request(2, false).GetIssue().empty());
    }

    Y_UNIT_TEST(PendingParticipantIssuesAreBoundedAndRebuiltAfterRestart) {
        TFreezeEnv env;
        env.CreateTable();
        auto issues = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([](auto& ev) {
            if (ev->Get()->Record.GetStage() == TStage::DRAIN_CDC) {
                ev->Get()->Record.SetReady(false);
                ev->Get()->Record.SetIssue("Unknown table while draining: " + TString(8192, 'x'));
            }
        });
        auto checkProgress = [&] {
            for (ui32 attempt = 0; attempt < 300; ++attempt) {
                const auto pending = env.Request(1, true);
                UNIT_ASSERT(!pending.GetReady());
                UNIT_ASSERT_VALUES_EQUAL(pending.GetStatus(), ui32(Ydb::StatusIds::SUCCESS));
                if (pending.GetIssue().Contains("Unknown table while draining")) {
                    UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "DRAIN_CDC");
                    UNIT_ASSERT_C(pending.GetIssue().size() <= 4096, pending.GetIssue().size());
                    return;
                }
                env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
            }
            UNIT_FAIL("Participant issue was not propagated");
        };
        env.CheckPending();
        checkProgress();
        RebootTablet(env.Runtime, env.Owner, env.Runtime.AllocateEdgeActor());
        env.CheckPending();
        checkProgress();
        issues.Remove();
        env.Wait(1, true);
        UNIT_ASSERT(env.Request(1, true).GetIssue().empty());
        env.Wait(2, false);
    }

    Y_UNIT_TEST(PendingProgressLimitsParticipantList) {
        TFreezeEnv env;
        env.CreateTable("Table", 12);
        TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> held(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::FENCE_WRITES;
        });
        env.CheckPending();
        env.Runtime.WaitFor("all participant replies held", [&] { return held.size() >= 12; });
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        const auto pending = env.Request(1, true);
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "FENCE_WRITES: acknowledged 0 of 12 DataShards; pending 12");
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "4 more participants");
        UNIT_ASSERT_C(pending.GetIssue().size() <= 4096, pending.GetIssue().size());
        held.Stop().Unblock();
        env.Wait(1, true);
        env.Wait(2, false);
    }

    Y_UNIT_TEST(ProgressFromReplacedWorkerCannotOverwriteNewEpoch) {
        TFreezeEnv env;
        env.CreateTable();
        TActorId oldWorker;
        auto requests = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto& ev) {
            if (ev->Get()->Record.GetEpoch() == 1) {
                oldWorker = ev->Sender;
            }
        });
        TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> cdc(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::DRAIN_CDC;
        });
        TBlockEvents<TEvSchemeShard::TEvDatabaseFreezeResult> progress(env.Runtime, [&](const auto& ev) {
            return oldWorker && ev->Sender == oldWorker && ev->Get()->Record.GetEpoch() == 1
                && !ev->Get()->Record.GetReady();
        });
        env.CheckPending();
        env.Runtime.WaitFor("old worker CDC request", [&] { return !cdc.empty(); });
        env.Runtime.WaitFor("old worker progress", [&] { return !progress.empty(); });
        TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> thaw(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStage() == TStage::OPEN;
        });
        env.CheckPending(2, false);
        env.Runtime.WaitFor("new worker thaw request", [&] { return !thaw.empty(); });
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_STRING_CONTAINS(env.Request(2, false).GetIssue(), "OPEN");

        // Even a matching epoch cannot make another actor's progress valid.
        auto stale = MakeHolder<TEvSchemeShard::TEvDatabaseFreezeResult>();
        stale->Record.CopyFrom(progress.front()->Get()->Record);
        stale->Record.SetEpoch(2);
        stale->Record.SetIssue("Obsolete worker progress");
        env.Runtime.Send(new IEventHandle(progress.front()->GetRecipientRewrite(), oldWorker, stale.Release()));
        progress.Stop().Unblock();
        env.Runtime.SimulateSleep(TDuration::Seconds(1));
        const auto pending = env.Request(2, false);
        UNIT_ASSERT(!pending.GetReady());
        UNIT_ASSERT_STRING_CONTAINS(pending.GetIssue(), "OPEN");
        UNIT_ASSERT(!pending.GetIssue().Contains("Obsolete"));
        thaw.Stop().Unblock();
        env.Wait(2, false);
        cdc.Stop().Unblock();
        UNIT_ASSERT(env.Request(2, false).GetIssue().empty());
    }

    Y_UNIT_TEST(UnsupportedColumnTableDoesNotFenceRowTables) {
        TFreezeEnv env;
        env.CreateTable();
        CreateTestTable(env.Runtime, ++env.TxId, env.Root, true);
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        ui32 shardRequests = 0;
        auto observer = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto&) { ++shardRequests; });
        env.Wait(1, true, Ydb::StatusIds::UNSUPPORTED);
        UNIT_ASSERT_VALUES_EQUAL(shardRequests, 0);
        env.Write();
        env.CheckSchemaAvailable();
    }

    Y_UNIT_TEST(SharedSchemeShardFreezesOnlyRequestedDatabase) {
        TFreezeEnv env;
        for (const auto name : {"First", "Second"}) {
            TestCreateSubDomain(env.Runtime, ++env.TxId, "/MyRoot", Sprintf(R"(
                Name: "%s" PlanResolution: 50 Coordinators: 1 Mediators: 1 TimeCastBucketsPerMediator: 2
            )", name));
            env.Env.TestWaitNotification(env.Runtime, env.TxId);
            env.Root = TString("/MyRoot/") + name;
            env.CreateTable();
        }
        env.Root = "/MyRoot/First";
        env.PathId = DescribePath(env.Runtime, env.Root).GetPathId();
        env.Wait(1, true);
        env.Write("Table", false);
        env.CheckSchemaBlocked();
        env.Write("/MyRoot/Second/Table");
        TestDropSubDomain(env.Runtime, ++env.TxId, "/MyRoot", "First", {{NKikimrScheme::StatusPreconditionFailed}});
        TestMkDir(env.Runtime, ++env.TxId, "/MyRoot", "OutsideFrozenDatabase");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.Wait(2, false);
        env.Write();
        env.CheckSchemaAvailable();
    }

    Y_UNIT_TEST(DroppedDatabaseFreezeStateIsRemoved) {
        TFreezeEnv env;
        TestCreateSubDomain(env.Runtime, ++env.TxId, "/MyRoot", R"(Name: "Frozen")");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.Root = "/MyRoot/Frozen";
        env.PathId = DescribePath(env.Runtime, env.Root).GetPathId();
        auto freezeRows = [&] {
            const auto result = ReadSystemTable(env.Runtime, env.Owner, "DatabaseFreezes", {"PathId"}, {"PathId"});
            return NClient::TValue::Create(result)["Result"]["List"].Size();
        };
        UNIT_ASSERT_VALUES_EQUAL(freezeRows(), 0);
        env.Wait(1, true);
        env.Wait(2, false);
        UNIT_ASSERT_VALUES_EQUAL(freezeRows(), 1);
        const TPathId domainId(env.Owner, env.PathId);
        env.Env.AddExtSubdomainCleanupObserver(env.Runtime, domainId);
        TestDropSubDomain(env.Runtime, ++env.TxId, "/MyRoot", "Frozen");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.Env.WaitForExtSubdomainCleanup(env.Runtime, domainId);
        UNIT_ASSERT_VALUES_EQUAL(freezeRows(), 0);
        RebootTablet(env.Runtime, env.Owner, env.Runtime.AllocateEdgeActor());
        UNIT_ASSERT_VALUES_EQUAL(freezeRows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Request(2, false).GetStatus(), ui32(Ydb::StatusIds::NOT_FOUND));

        // A new database with the same name gets an independent epoch space.
        TestCreateSubDomain(env.Runtime, ++env.TxId, "/MyRoot", R"(Name: "Frozen")");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.PathId = DescribePath(env.Runtime, env.Root).GetPathId();
        UNIT_ASSERT(env.PathId != domainId.LocalPathId);
        env.Wait(1, true);
        env.Wait(2, false);
    }

    Y_UNIT_TEST(PayloadTargetsCannotBypassFrozenDomain) {
        TFreezeEnv env;
        TestCreateSubDomain(env.Runtime, ++env.TxId, "/MyRoot", R"(
            Name: "Frozen" PlanResolution: 50 Coordinators: 1 Mediators: 1 TimeCastBucketsPerMediator: 2
        )");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.Root = "/MyRoot/Frozen";
        env.PathId = DescribePath(env.Runtime, env.Root).GetPathId();
        env.CreateTable();
        const auto tableId = DescribePath(env.Runtime, env.Path("Table")).GetPathId();
        TestCreateSequence(env.Runtime, ++env.TxId, env.Root, R"(Name: "Sequence")");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.Wait(1, true);

        // Neither WorkingDir nor a missing WorkingDir may hide a frozen target.
        const auto alter = Sprintf(R"(
            PathId { OwnerId: %lu LocalId: %lu }
            Columns { Name: "extra" Type: "Uint32" }
        )", env.Owner, tableId);
        TestAlterTable(env.Runtime, ++env.TxId, "/MyRoot", alter, {{NKikimrScheme::StatusPreconditionFailed}});
        TestAlterTable(env.Runtime, ++env.TxId, "", Sprintf(R"(
            Id_Deprecated: %lu Columns { Name: "extra" Type: "Uint32" }
        )", tableId), {{NKikimrScheme::StatusPreconditionFailed}});
        TestDropTable(env.Runtime, ++env.TxId, tableId, {{NKikimrScheme::StatusPreconditionFailed}});
        TestUserAttrs(env.Runtime, ++env.TxId, "/MyRoot", "Frozen",
            {{NKikimrScheme::StatusPreconditionFailed}}, AlterUserAttrs({{"test", "value"}}));
        TestModifyACL(env.Runtime, ++env.TxId, "/MyRoot", "Frozen", "", "", NKikimrScheme::StatusPreconditionFailed);
        TestCreateTable(env.Runtime, ++env.TxId, "/MyRoot", R"(
            Name: "Frozen/NewTable"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )", {{NKikimrScheme::StatusPreconditionFailed}});
        TestMoveSequence(env.Runtime, ++env.TxId, env.Path("Sequence"), env.Path("Renamed"),
            {{NKikimrScheme::StatusPreconditionFailed}});

        TestMkDir(env.Runtime, ++env.TxId, "/MyRoot", "Outside");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.Wait(2, false);
        TestAlterTable(env.Runtime, ++env.TxId, "/MyRoot", alter);
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        TestMoveSequence(env.Runtime, ++env.TxId, env.Path("Sequence"), env.Path("Renamed"));
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        TestDescribeResult(DescribePath(env.Runtime, env.Path("Renamed")), {NLs::PathExist});
    }

    Y_UNIT_TEST(TenantSchemeShardUsesItsOwnDomainPath) {
        TFreezeEnv env;
        TestCreateExtSubDomain(env.Runtime, ++env.TxId, "/MyRoot", R"(Name: "Tenant")");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        TestAlterExtSubDomain(env.Runtime, ++env.TxId, "/MyRoot", R"(
            Name: "Tenant" ExternalSchemeShard: true
            PlanResolution: 50 Coordinators: 1 Mediators: 1 TimeCastBucketsPerMediator: 2
            StoragePools { Name: "pool-1" Kind: "pool-kind-1" }
        )");
        env.Env.TestWaitNotification(env.Runtime, env.TxId);
        env.Root = "/MyRoot/Tenant";
        TestDescribeResult(DescribePath(env.Runtime, env.Root), {NLs::ExtractTenantSchemeshard(&env.Owner)});
        UNIT_ASSERT(env.Owner != TTestTxConfig::SchemeShard);
        env.PathId = 1;
        env.CreateTable();
        env.Wait(1, true);
        env.Write("Table", false);
        env.CheckSchemaBlocked();
        env.Wait(2, false);
        env.Write();
        env.CheckSchemaAvailable();
    }

    Y_UNIT_TEST(NewIndexBuildIsRejectedWhileFrozen) {
        TFreezeEnv env;
        env.CreateTable();
        env.Wait(1, true);
        TestBuildIndex(env.Runtime, ++env.TxId, env.Owner, env.Root, env.Path("Table"), "ByValue", {"value"},
            Ydb::StatusIds::PRECONDITION_FAILED);
        env.Wait(2, false);
        const auto buildTx = ++env.TxId;
        TestBuildIndex(env.Runtime, buildTx, env.Owner, env.Root, env.Path("Table"), "ByValue", {"value"});
        env.Env.TestWaitNotification(env.Runtime, buildTx);
    }

    Y_UNIT_TEST(NewImportIsRejectedWhileFrozen) {
        TFreezeEnv env;
        env.CreateTable();
        env.Wait(1, true);
        auto request = MakeHolder<TEvImport::TEvCreateImportRequest>();
        request->Record.SetTxId(++env.TxId);
        request->Record.SetDatabaseName(env.Root);
        auto* settings = request->Record.MutableRequest()->MutableImportFromS3Settings();
        settings->set_endpoint("localhost:1");
        settings->set_bucket("bucket");
        auto* item = settings->add_items();
        item->set_source_prefix("table");
        item->set_destination_path(env.Path("Imported"));
        const auto sender = env.Runtime.AllocateEdgeActor();
        ForwardToTablet(env.Runtime, env.Owner, sender, request.Release());
        const auto response = env.Runtime.GrabEdgeEventRethrow<TEvImport::TEvCreateImportResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ui32(response->Get()->Record.GetResponse().GetEntry().GetStatus()),
            ui32(Ydb::StatusIds::PRECONDITION_FAILED));
        env.Wait(2, false);
        env.CheckSchemaAvailable();
    }

    Y_UNIT_TEST(NewColumnConstraintIsRejectedWhileFrozen) {
        TFreezeEnv env;
        env.Runtime.GetAppData().FeatureFlags.SetEnableSetColumnConstraint(true);
        env.CreateTable();
        env.Wait(1, true);
        const auto result = TestSetColumnConstraint(env.Runtime, ++env.TxId, env.Owner,
            env.Root, env.Path("Table"), {"value"});
        UNIT_ASSERT_VALUES_EQUAL(ui32(result.GetStatus()), ui32(Ydb::StatusIds::PRECONDITION_FAILED));
        env.Wait(2, false);
        env.Write();
    }

    Y_UNIT_TEST(FreezeWaitsForTopicAcknowledgement) {
        TFreezeEnv env;
        env.CreateTable();
        env.CreateStream();
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        env.Write();
        env.Runtime.WaitFor("CDC topic acknowledgement", [&] { return !ack.empty(); });
        env.CheckPending();
        env.Runtime.SimulateSleep(TDuration::Seconds(2));
        env.CheckPending();
        env.Write("Table", false, 2);
        env.CheckSchemaBlocked();
        UNIT_ASSERT_VALUES_EQUAL(CountRows(env.Runtime, env.Path("Table")), 1);
        ack.Stop().Unblock();
        env.Wait(1, true);
        env.Wait(2, false);
        env.AdvancePlanStep();
        env.Write("Table", true, 2);
    }

    Y_UNIT_TEST(UnrelatedCdcScanPayloadCannotBypassFreeze) {
        TFreezeEnv env;
        env.CreateTable();
        env.Wait(1, true);

        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(++env.TxId, env.Owner);
        auto& tx = *request->Record.AddTransaction();
        tx.SetWorkingDir(env.Root);
        tx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        auto& table = *tx.MutableCreateTable();
        table.SetName("Unexpected");
        auto& column = *table.AddColumns();
        column.SetName("key");
        column.SetType("Uint32");
        table.AddKeyColumnNames("key");
        table.SetUniformPartitionsCount(1);
        // This unrelated payload must not grant the CDC scan exception to
        // CreateTable: execution dispatches using OperationType.
        tx.MutableAlterCdcStream()->MutableGetReady();

        const auto sender = env.Runtime.AllocateEdgeActor();
        ForwardToTablet(env.Runtime, env.Owner, sender, request.Release());
        const auto response = env.Runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvModifySchemeTransactionResult>(sender);
        const auto& result = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(ui32(result.GetStatus()), ui32(NKikimrScheme::StatusPreconditionFailed),
            result.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetReason(), "Database is frozen for schema changes");
        TestDescribeResult(DescribePath(env.Runtime, env.Owner, env.Path("Unexpected")), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(InitialCdcScanFinalizationRemainsAllowed) {
        TFreezeEnv env;
        env.CreateTable();
        env.Write();
        TBlockEvents<TEvDataShard::TEvCdcStreamScanResponse> scan(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE;
        });
        env.CreateStream(true);
        env.Runtime.WaitFor("initial CDC scan completion", [&] { return !scan.empty(); });
        env.CheckPending();
        env.CheckSchemaBlocked();
        scan.Stop().Unblock();
        env.Wait(1, true);
        env.Wait(2, false);
        env.Write("Table", true, 2);
    }
}
