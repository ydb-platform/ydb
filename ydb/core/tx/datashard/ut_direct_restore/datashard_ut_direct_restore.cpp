#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/library/aws_init/aws.h>

#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#ifndef KIKIMR_DISABLE_S3_OPS

using namespace NKikimr;
using namespace NKikimr::NDataShard;
using namespace NKikimr::NWrappers::NTestHelpers;
using namespace NKikimr::Tests;

namespace {

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPI) {
    NKikimr::ShutdownAwsAPI();
}

const TString EmptyTableState;

TString ZstdCompress(const TStringBuf src) {
    TStringStream out;
    {
        TZstdCompress compress(&out);
        compress.Write(src.data(), src.size());
    }
    return out.Str();
}

struct TTestEnv {
    // Declaration order matters: PortManager must be constructed before Server.
    TPortManager PortManager;
    Tests::TServer::TPtr Server;
    TTestActorRuntime& Runtime;
    TActorId Sender;
    THolder<TS3Mock> S3Mock;
    ui16 S3Port = 0;
    ui64 NextTxId = 100;

    explicit TTestEnv()
        : Server([&] {
            TServerSettings settings(PortManager.GetPort(2134));
            settings.SetDomainName("Root").SetUseRealThreads(false);
            return new TServer(settings);
        }())
        , Runtime(*Server->GetRuntime())
        , Sender(Runtime.AllocateEdgeActor())
    {
        Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(true);
        Runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        Runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NLog::PRI_TRACE);
        InitRoot(Server, Sender);
    }

    ui64 AllocateTxId() {
        return ++NextTxId;
    }

    void StartS3(const TString& data, const TString& ext = ".csv", ui32 shardNum = 0) {
        S3Port = PortManager.GetPort();
        THashMap<TString, TString> objects;
        objects[Sprintf("/data_%02d%s", shardNum, ext.c_str())] = data;
        S3Mock.Reset(new TS3Mock(std::move(objects), TS3Mock::TSettings(S3Port)));
        UNIT_ASSERT(S3Mock->Start());
    }

    std::tuple<TVector<ui64>, TTableId> CreateUtf8Table(const TString& name = "Table", ui64 shards = 1) {
        auto opts = TShardedTableOptions()
            .Shards(shards)
            .Columns({
                {"key", "Utf8", true, false},
                {"value", "Utf8", false, false},
            });
        return CreateShardedTable(Server, Sender, "/Root", name, opts);
    }

    std::tuple<TVector<ui64>, TTableId> CreateUint64Table(const TString& name = "Table") {
        auto opts = TShardedTableOptions()
            .Shards(1)
            .Columns({
                {"key", "Uint64", true, false},
                {"value", "Utf8", false, false},
            });
        return CreateShardedTable(Server, Sender, "/Root", name, opts);
    }

    std::tuple<TVector<ui64>, TTableId> CreateUint32Table(const TString& name = "Table", ui64 shards = 1) {
        auto opts = TShardedTableOptions()
            .Shards(shards)
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "Utf8", false, false},
            });
        return CreateShardedTable(Server, Sender, "/Root", name, opts);
    }

    NKikimrSchemeOp::TTableDescription TableDescription(
        ui64 shardId,
        const TTableId& tableId,
        const TVector<TString>& keyColumnNames = {"key"})
    {
        auto [tables, _] = GetTablesByPathId(Server, shardId);
        auto desc = tables.at(tableId.PathId).GetDescription();
        // GetInfo description may omit KeyColumnNames; restore CheckScheme requires them.
        if (desc.KeyColumnNamesSize() == 0) {
            for (const auto& name : keyColumnNames) {
                desc.AddKeyColumnNames(name);
            }
        }
        return desc;
    }

    TString BuildRestoreBody(
        const TTableId& tableId,
        const NKikimrSchemeOp::TTableDescription& desc,
        ui32 shardNum,
        ui32 readBatchSize,
        ui64 txId,
        const TString& tableName = "Table")
    {
        NKikimrTxDataShard::TFlatSchemeTransaction tx;
        // Must exceed LastSchemeOpSeqNo left by CreateTable via SchemeShard.
        tx.MutableSeqNo()->SetGeneration(1'000'000);
        tx.MutableSeqNo()->SetRound(txId);

        auto& restore = *tx.MutableRestore();
        restore.SetTableName(tableName);
        restore.SetTableId(tableId.PathId.LocalPathId);
        restore.SetShardNum(shardNum);
        restore.MutableTableDescription()->CopyFrom(desc);
        UNIT_ASSERT_C(restore.GetTableDescription().KeyColumnNamesSize() > 0,
            restore.GetTableDescription().ShortDebugString());

        UNIT_ASSERT(S3Port);
        auto& s3 = *restore.MutableS3Settings();
        s3.SetEndpoint(Sprintf("localhost:%d", S3Port));
        s3.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        s3.MutableLimits()->SetReadBatchSize(readBatchSize);

        return tx.SerializeAsString();
    }

    // Restore scheme ops notify via SchemaChanged.OpResult (not ProposeTransactionResult COMPLETE).
    NKikimrTxDataShard::TShardOpResult ProposeAndPlanRestore(
        ui64 shardId,
        const TTableId& tableId,
        const NKikimrSchemeOp::TTableDescription& desc,
        ui32 shardNum = 0,
        ui32 readBatchSize = 128,
        const TString& tableName = "Table")
    {
        const ui64 txId = AllocateTxId();
        const ui64 schemeshardId = tableId.PathId.OwnerId;
        const auto body = BuildRestoreBody(tableId, desc, shardNum, readBatchSize, txId, tableName);

        TMaybe<NKikimrTxDataShard::TShardOpResult> opResult;
        auto prev = Runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvSchemaChanged) {
                const auto& record = ev->Get<TEvDataShard::TEvSchemaChanged>()->Record;
                if (record.GetTxId() == txId && record.HasOpResult()) {
                    opResult = record.GetOpResult();
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Runtime.SendToPipe(
            shardId,
            Sender,
            new TEvDataShard::TEvProposeTransaction(
                NKikimrTxDataShard::TX_KIND_SCHEME,
                schemeshardId,
                Sender,
                txId,
                body,
                NKikimrSubDomains::TProcessingParams()),
            0,
            GetPipeConfigWithRetries());

        auto proposeEv = Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(Sender);
        UNIT_ASSERT(proposeEv);
        UNIT_ASSERT_VALUES_EQUAL(proposeEv->Get()->GetTxId(), txId);
        UNIT_ASSERT_C(proposeEv->Get()->IsPrepared(), proposeEv->Get()->Record.ShortDebugString());

        const auto& prepare = proposeEv->Get()->Record;
        UNIT_ASSERT_C(prepare.DomainCoordinatorsSize() > 0, prepare.ShortDebugString());
        SendProposeToCoordinator(Runtime, Sender, {shardId}, {
            .TxId = txId,
            .Coordinator = prepare.GetDomainCoordinators(0),
            .MinStep = prepare.GetMinStep(),
            .MaxStep = prepare.GetMaxStep(),
        });

        if (!opResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&opResult](IEventHandle&) {
                return opResult.Defined();
            });
            Runtime.DispatchEvents(opts);
        }

        Runtime.SetObserverFunc(prev);
        UNIT_ASSERT(opResult);
        return *opResult;
    }
};

TString MakeCsv(ui32 count, const TString& keyPrefix = "a") {
    TStringBuilder csv;
    const auto numWidth = ToString(count).size();
    for (ui32 i = 1; i <= count; ++i) {
        const auto keyValue = !keyPrefix.empty()
            ? TStringBuilder() << keyPrefix << LeftPad(i, numWidth, '0')
            : TStringBuilder() << i;
        if (!keyPrefix.empty()) {
            csv << "\"" << keyValue << "\",";
        } else {
            csv << keyValue << ",";
        }
        csv << "\"value" << i << "\"" << Endl;
    }
    return csv;
}

TString ExpectedUtf8TableState(ui32 count, const TString& keyPrefix = "a") {
    TStringBuilder out;
    const auto numWidth = ToString(count).size();
    for (ui32 i = 1; i <= count; ++i) {
        const auto keyValue = !keyPrefix.empty()
            ? TStringBuilder() << keyPrefix << LeftPad(i, numWidth, '0')
            : TStringBuilder() << i;
        out << "key = " << keyValue << ", value = value" << i << Endl;
    }
    return out;
}

TString ExpectedUint32TableState(ui32 count) {
    TStringBuilder out;
    for (ui32 i = 1; i <= count; ++i) {
        out << "key = " << i << ", value = value" << i << Endl;
    }
    return out;
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardDirectRestore) {

Y_UNIT_TEST(ShouldSucceedOnRawCsv) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(MakeCsv(1));

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 1u);

    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUtf8TableState(1));
}

Y_UNIT_TEST(ShouldSucceedOnZstd) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(ZstdCompress(MakeCsv(1)), ".csv.zst");

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 1u);

    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUtf8TableState(1));
}

Y_UNIT_TEST(ShouldSucceedOnLargeData) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint32Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    const auto csv = MakeCsv(100, "");
    UNIT_ASSERT(csv.size() > 128);
    env.StartS3(csv);

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc, 0, 128);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 100u);

    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(100));
}

Y_UNIT_TEST(ShouldCountWrittenBytesAndRows) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(MakeCsv(2));

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetBytesProcessed(), 16u);
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 2u);
}

Y_UNIT_TEST(ShouldFailOnEmptyToken) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3("\"a1\",\n");

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(!result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(ShouldFailOnInvalidValue) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint64Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3("\"a1\",\"value1\"\n");

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(!result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(ShouldFailOnFileWithoutNewLines) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3("\"a1\",\"value1\"");

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(!result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(ShouldFailOnOutboundKey) {
    TTestEnv env;
    // UniformPartitionsCount=2 for Uint32 splits near 2^31; key 50 belongs to the low shard.
    auto [shards, tableId] = env.CreateUint32Table("Table", /*shards=*/2);
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

    const ui64 highShardId = shards[1];
    const auto desc = env.TableDescription(highShardId, tableId);
    env.StartS3("50,\"value1\"\n");

    // data_00 is used for ShardNum=0 naming; content is still validated against this shard's range.
    const auto result = env.ProposeAndPlanRestore(highShardId, tableId, desc, /*shardNum=*/0);
    UNIT_ASSERT(!result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(ShouldFailOnWrongOrderedCsv) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    // Direct part import requires strictly ascending keys.
    env.StartS3("\"a2\",\"value2\"\n\"a1\",\"value1\"\n");

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(!result.GetSuccess());
    UNIT_ASSERT(result.GetExplain().Contains("strictly ascending"));
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(CancelUponDirectWriteFinish) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(MakeCsv(1));

    TBlockEvents<TEvDataShard::TEvS3DirectWriteFinish> blockedFinish(env.Runtime);

    const ui64 txId = env.AllocateTxId();
    const ui64 schemeshardId = tableId.PathId.OwnerId;
    const auto body = env.BuildRestoreBody(tableId, desc, 0, 128, txId);

    env.Runtime.SendToPipe(
        shardId,
        env.Sender,
        new TEvDataShard::TEvProposeTransaction(
            NKikimrTxDataShard::TX_KIND_SCHEME,
            schemeshardId,
            env.Sender,
            txId,
            body,
            NKikimrSubDomains::TProcessingParams()),
        0,
        GetPipeConfigWithRetries());

    {
        auto proposeEv = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(env.Sender);
        UNIT_ASSERT(proposeEv->Get()->IsPrepared());
        const auto& prepare = proposeEv->Get()->Record;
        UNIT_ASSERT(prepare.DomainCoordinatorsSize() > 0);
        SendProposeToCoordinator(env.Runtime, env.Sender, {shardId}, {
            .TxId = txId,
            .Coordinator = prepare.GetDomainCoordinators(0),
            .MinStep = prepare.GetMinStep(),
            .MaxStep = prepare.GetMaxStep(),
        });
    }

    env.Runtime.WaitFor("direct write finish", [&] {
        return !blockedFinish.empty();
    });

    env.Runtime.SendToPipe(
        shardId,
        env.Sender,
        new TEvDataShard::TEvCancelRestore(txId, tableId.PathId.LocalPathId),
        0,
        GetPipeConfigWithRetries());

    // Drop the blocked finish so the part is never attached.
    blockedFinish.Stop();

    auto resultEv = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(env.Sender);
    UNIT_ASSERT(resultEv);
    UNIT_ASSERT_VALUES_EQUAL(resultEv->Get()->GetTxId(), txId);
    UNIT_ASSERT_C(
        resultEv->Get()->IsComplete() || resultEv->Get()->IsError() || resultEv->Get()->IsExecError(),
        resultEv->Get()->Record.ShortDebugString());

    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(WriteBlockedDuringRestore) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint32Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(MakeCsv(1, ""));

    const ui64 txId = env.AllocateTxId();
    TMaybe<NKikimrTxDataShard::TShardOpResult> opResult;
    auto schemaChanged = env.Runtime.AddObserver<TEvDataShard::TEvSchemaChanged>(
        [&](TEvDataShard::TEvSchemaChanged::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetTxId() == txId && record.HasOpResult()) {
                opResult = record.GetOpResult();
            }
        });
    TBlockEvents<TEvDataShard::TEvS3DirectWriteFinish> blockedFinish(env.Runtime);

    const ui64 schemeshardId = tableId.PathId.OwnerId;
    const auto body = env.BuildRestoreBody(tableId, desc, 0, 128, txId);

    env.Runtime.SendToPipe(
        shardId,
        env.Sender,
        new TEvDataShard::TEvProposeTransaction(
            NKikimrTxDataShard::TX_KIND_SCHEME,
            schemeshardId,
            env.Sender,
            txId,
            body,
            NKikimrSubDomains::TProcessingParams()),
        0,
        GetPipeConfigWithRetries());

    {
        auto proposeEv = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(env.Sender);
        UNIT_ASSERT(proposeEv->Get()->IsPrepared());
        const auto& prepare = proposeEv->Get()->Record;
        UNIT_ASSERT(prepare.DomainCoordinatorsSize() > 0);
        SendProposeToCoordinator(env.Runtime, env.Sender, {shardId}, {
            .TxId = txId,
            .Coordinator = prepare.GetDomainCoordinators(0),
            .MinStep = prepare.GetMinStep(),
            .MaxStep = prepare.GetMaxStep(),
        });
    }

    env.Runtime.WaitFor("direct write finish", [&] {
        return !blockedFinish.empty();
    });

    // Prepared writes cannot be planned while a non-readonly schema op is in flight.
    const ui32 writeKey = 42;
    const TString writeValue = "blocked";
    const std::vector<TCell> cells = {
        TCell::Make(writeKey),
        TCell(writeValue.data(), writeValue.size()),
    };
    const auto writeResult = Upsert(
        env.Runtime,
        env.Sender,
        shardId,
        tableId,
        env.AllocateTxId(),
        NKikimrDataEvents::TEvWrite::MODE_PREPARE,
        {1, 2},
        cells,
        NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
    UNIT_ASSERT_VALUES_EQUAL(writeResult.GetIssues().size(), 1);
    UNIT_ASSERT(writeResult.GetIssues(0).message().Contains("blocked shard"));

    blockedFinish.Unblock().Stop();
    env.Runtime.WaitFor("restore complete", [&] {
        return opResult.Defined();
    });
    schemaChanged.Remove();

    UNIT_ASSERT(opResult);
    UNIT_ASSERT(opResult->GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(1));
}

Y_UNIT_TEST(CompactionAfterRestore) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint32Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(MakeCsv(10, ""));

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(10));

    // Restored data is a single SST part; force compaction of single-parted shards.
    {
        auto sender = env.Runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvDataShard::TEvCompactTable>(tableId.PathId);
        request->Record.SetCompactSinglePartedShards(true);
        env.Runtime.SendToPipe(shardId, sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto ev = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(
            ev->Get()->Record.GetStatus(),
            NKikimrTxDataShard::TEvCompactTableResult::OK);
    }

    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(10));
}

Y_UNIT_TEST(WriteAfterRestore) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint32Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(MakeCsv(3, ""));

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(3));

    const TString updatedValue = "updated";
    const ui32 newKey = 4;
    const TString newValue = "value4";
    {
        const ui32 existingKey = 2;
        const std::vector<TCell> cells = {
            TCell::Make(existingKey),
            TCell(updatedValue.data(), updatedValue.size()),
            TCell::Make(newKey),
            TCell(newValue.data(), newValue.size()),
        };
        Upsert(
            env.Runtime,
            env.Sender,
            shardId,
            tableId,
            env.AllocateTxId(),
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            {1, 2},
            cells);
    }

    const TString expectedAfterWrite =
        "key = 1, value = value1\n"
        "key = 2, value = updated\n"
        "key = 3, value = value3\n"
        "key = 4, value = value4\n";
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), expectedAfterWrite);

    // Memtable + restored part: ordinary compaction is enough.
    {
        const auto compactionResult = CompactTable(env.Runtime, shardId, tableId, false);
        UNIT_ASSERT_VALUES_EQUAL(
            compactionResult.GetStatus(),
            NKikimrTxDataShard::TEvCompactTableResult::OK);
    }

    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), expectedAfterWrite);
}

Y_UNIT_TEST(SplitAfterRestore) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint32Table();
    UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    env.StartS3(MakeCsv(10, ""));

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(10));

    SetSplitMergePartCountLimit(&env.Runtime, -1);
    const ui64 splitTxId = AsyncSplitTable(env.Server, env.Sender, "/Root/Table", shardId, /*splitKey=*/5);
    WaitTxNotification(env.Server, env.Sender, splitTxId);

    const auto splitShards = GetTableShards(env.Server, env.Sender, "/Root/Table");
    UNIT_ASSERT_VALUES_EQUAL(splitShards.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, splitShards, tableId), ExpectedUint32TableState(10));
}

} // Y_UNIT_TEST_SUITE(DataShardDirectRestore)

#endif // KIKIMR_DISABLE_S3_OPS
