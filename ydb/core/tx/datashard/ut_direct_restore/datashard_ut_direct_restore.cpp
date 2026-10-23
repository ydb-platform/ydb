#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/const.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/wrappers/s3_wrapper.h>
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
    bool ValidateChecksums = false;

    explicit TTestEnv()
        : Server([&] {
            TServerSettings settings(PortManager.GetPort(2134));
            settings.SetDomainName("Root").SetUseRealThreads(false);
            return new TServer(settings);
        }())
        , Runtime(*Server->GetRuntime())
        , Sender(Runtime.AllocateEdgeActor())
    {
        // The S3 mock uses real HTTP threads; allow replies to arrive before simulated deadlines expire.
        Runtime.SetFastSimulatedTime(false);
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
        if (ValidateChecksums) {
            objects[NBackup::ChecksumKey(Sprintf("/data_%02d.csv", shardNum))] = NBackup::ComputeChecksum(data);
        }
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
        restore.SetValidateChecksums(ValidateChecksums);
        restore.SetNumberOfRetries(3);
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

        StartRestore(shardId, schemeshardId, txId, body);

        if (!opResult) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&opResult](IEventHandle&) {
                return opResult.Defined();
            });
            Runtime.DispatchEvents(opts, TDuration::Minutes(5));
        }

        Runtime.SetObserverFunc(prev);
        UNIT_ASSERT(opResult);
        return *opResult;
    }

    void StartRestore(ui64 shardId, ui64 schemeshardId, ui64 txId, const TString& body) {
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
    }

    void CancelRestore(ui64 shardId, const TTableId& tableId, ui64 txId) {
        Runtime.SendToPipe(shardId, Sender,
            new TEvDataShard::TEvCancelRestore(txId, tableId.PathId.LocalPathId),
            0, GetPipeConfigWithRetries());
        auto result = Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetTxId(), txId);
        UNIT_ASSERT_C(result->Get()->IsComplete() || result->Get()->IsError() || result->Get()->IsExecError(),
            result->Get()->Record.ShortDebugString());
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

Y_UNIT_TEST_TWIN(RestorePreservesExistingSnapshot, direct) {
    TTestEnv env;
    env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(direct);
    auto [shards, tableId] = env.CreateUint32Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    const auto snapshot = CreateVolatileSnapshot(env.Server, {"/Root/Table"}, TDuration::Hours(1));
    auto readSnapshot = [&](ui64 readId) {
        auto request = GetBaseReadRequest(tableId, desc, readId, NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
        AddFullRangeQuery(*request);
        auto result = SendRead(env.Server, shards[0], request.release(), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result->Record.GetFinished());
        return result->GetRowsCount();
    };
    UNIT_ASSERT_VALUES_EQUAL(readSnapshot(2000), 0u);

    ui32 directBegins = 0;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBegin>(
        [&](auto&) { ++directBegins; });
    env.StartS3(MakeCsv(2, ""));
    const auto result = env.ProposeAndPlanRestore(shards[0], tableId, desc);
    UNIT_ASSERT_C(result.GetSuccess(), result.GetExplain());
    UNIT_ASSERT_VALUES_EQUAL(directBegins, direct ? 1u : 0u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(2));
    UNIT_ASSERT_VALUES_EQUAL_C(readSnapshot(2001), 0u, "Restore changed a pre-existing snapshot");

    {
        auto sender = env.Runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvDataShard::TEvCompactTable>(tableId.PathId);
        request->Record.SetCompactSinglePartedShards(true);
        env.Runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto ev = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvCompactTableResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSnapshot(2002), 0u);
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    UNIT_ASSERT_VALUES_EQUAL(readSnapshot(2003), 0u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(2));
}

Y_UNIT_TEST_QUAD(RestoreKeySizeLimit, direct, oversized) {
    TTestEnv env;
    env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(direct);
    auto [shards, tableId] = CreateShardedTable(env.Server, env.Sender, "/Root", "Table",
        TShardedTableOptions().Shards(1).Columns({
            {"key1", "Utf8", true, false},
            {"key2", "Utf8", true, false},
            {"value", "Utf8", false, false},
        }));
    const auto desc = env.TableDescription(shards[0], tableId, {"key1", "key2"});
    // Each cell is below the limit; the limit applies to the combined key.
    const TString key1(NLimits::MaxWriteKeySize / 2, 'a');
    const TString key2(NLimits::MaxWriteKeySize - key1.size() + oversized, 'b');
    env.StartS3(TStringBuilder() << "\"" << key1 << "\",\"" << key2 << "\",\"value\"\n");
    const auto result = env.ProposeAndPlanRestore(shards[0], tableId, desc, 0, 4 * 1024 * 1024);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetSuccess(), !oversized, result.GetExplain());
    if (oversized) {
        UNIT_ASSERT_C(result.GetExplain().Contains("Row key size"), result.GetExplain());
        UNIT_ASSERT_C(result.GetExplain().Contains("larger than the allowed threshold"), result.GetExplain());
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 1u);
        auto request = GetBaseReadRequest(tableId, desc, 2000, NKikimrDataEvents::FORMAT_CELLVEC);
        AddFullRangeQuery(*request);
        auto read = SendRead(env.Server, shards[0], request.release(), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(read->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(read->GetRowsCount(), 1u);
        const auto cells = read->GetCells(0);
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), 3u);
        UNIT_ASSERT_C(cells[0].AsBuf() == key1, "First key cell differs");
        UNIT_ASSERT_C(cells[1].AsBuf() == key2, "Second key cell differs");
        UNIT_ASSERT_VALUES_EQUAL(cells[2].AsBuf(), "value");
    }
}

Y_UNIT_TEST_QUAD(RestoreValueSizeLimit, direct, oversized) {
    TTestEnv env;
    env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(direct);
    auto [shards, tableId] = env.CreateUint32Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    const TString value(NLimits::MaxWriteValueSize + oversized, 'v');
    env.StartS3(TStringBuilder() << "1,\"" << value << "\"\n");
    const auto result = env.ProposeAndPlanRestore(shards[0], tableId, desc, 0, 32 * 1024 * 1024);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetSuccess(), !oversized, result.GetExplain());
    if (oversized) {
        UNIT_ASSERT_C(result.GetExplain().Contains("Row cell size"), result.GetExplain());
        UNIT_ASSERT_C(result.GetExplain().Contains("larger than the allowed threshold"), result.GetExplain());
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 1u);
        auto request = GetBaseReadRequest(tableId, desc, 2000, NKikimrDataEvents::FORMAT_CELLVEC);
        AddFullRangeQuery(*request);
        auto read = SendRead(env.Server, shards[0], request.release(), env.Sender);
        UNIT_ASSERT_VALUES_EQUAL(read->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(read->GetRowsCount(), 1u);
        const auto cells = read->GetCells(0);
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(cells[0].AsValue<ui32>(), 1u);
        UNIT_ASSERT_C(cells[1].AsBuf() == value, "Value differs at the size limit");
    }
}

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

Y_UNIT_TEST_QUAD(EmptyZstdRestoreSurvivesRestart, direct, populated) {
    TTestEnv env;
    env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(direct);
    auto [shards, tableId] = env.CreateUint32Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    if (populated) {
        const ui32 key = 1;
        const TString value = "original";
        Upsert(env.Runtime, env.Sender, shards[0], tableId, env.AllocateTxId(),
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, {1, 2},
            {TCell::Make(key), TCell(value.data(), value.size())});
    }
    const TString expected = populated ? "key = 1, value = original\n" : TString();
    ui32 directBegins = 0;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBegin>(
        [&](auto&) { ++directBegins; });
    // The compressed object is non-empty, but produces no rows or parts.
    env.StartS3(ZstdCompress(""), ".csv.zst");
    const auto result = env.ProposeAndPlanRestore(shards[0], tableId, desc);
    UNIT_ASSERT_C(result.GetSuccess(), result.GetExplain());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 0u);
    if (!populated) {
        UNIT_ASSERT_VALUES_EQUAL(directBegins, direct ? 1u : 0u);
    }
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), expected);
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), expected);

    // Recovery must leave the table writable, with subsequent writes durable.
    const ui32 key = 2;
    const TString value = "after restore";
    Upsert(env.Runtime, env.Sender, shards[0], tableId, env.AllocateTxId(),
        NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, {1, 2},
        {TCell::Make(key), TCell(value.data(), value.size())});
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId),
        expected + "key = 2, value = after restore\n");
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

    blockedFinish.Stop();

    auto resultEv = env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(env.Sender);
    UNIT_ASSERT(resultEv);
    UNIT_ASSERT_VALUES_EQUAL(resultEv->Get()->GetTxId(), txId);
    UNIT_ASSERT_C(
        resultEv->Get()->IsComplete() || resultEv->Get()->IsError() || resultEv->Get()->IsExecError(),
        resultEv->Get()->Record.ShortDebugString());

    TMaybe<bool> finishSucceeded;
    auto finishObserver = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteFinishResult>(
        [&](auto& ev) { finishSucceeded = ev->Get()->Success; });
    blockedFinish.Unblock();
    env.Runtime.WaitFor("late finish response", [&] { return finishSucceeded.Defined(); }, TDuration::Seconds(30));
    UNIT_ASSERT_C(!*finishSucceeded, "A cancelled restore attached its delayed part");
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

Y_UNIT_TEST(ReportsTableStatsAfterRestore) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint32Table();
    const ui64 shardId = shards[0];
    const auto desc = env.TableDescription(shardId, tableId);

    // Wait for the first report, which means the shard has already built stats for
    // the still empty table. Otherwise the restore below would ride on that initial
    // build instead of having to invalidate the stats itself.
    WaitTableStats(env.Runtime, shardId, [](const NKikimrTableStats::TTableStats& stats) {
        return stats.GetRowCount() == 0;
    });

    env.StartS3(MakeCsv(100, ""));

    const auto result = env.ProposeAndPlanRestore(shardId, tableId, desc);
    UNIT_ASSERT(result.GetSuccess());
    UNIT_ASSERT_VALUES_EQUAL(result.GetRowsProcessed(), 100u);

    // Nothing went through the memtable and no compaction ran, so the shard must
    // invalidate cached stats in TTxS3DirectWriteFinish itself.
    const auto stats = WaitTableStats(env.Runtime, shardId, [](const NKikimrTableStats::TTableStats& stats) {
        return stats.GetRowCount() > 0;
    }).GetTableStats();

    UNIT_ASSERT_VALUES_EQUAL(stats.GetRowCount(), 100u);
    UNIT_ASSERT_VALUES_EQUAL(stats.GetPartCount(), 1u);
    UNIT_ASSERT_GT(stats.GetDataSize(), 0u);
    UNIT_ASSERT_GT(stats.GetLastUpdateTime(), 0u);
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


Y_UNIT_TEST_TWIN(RestoreIntoNonEmptyTable, compact) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUint32Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    const ui32 key = 1;
    const ui32 retainedKey = 3;
    const TString oldValue = "old";
    Upsert(env.Runtime, env.Sender, shards[0], tableId, env.AllocateTxId(),
        NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE, {1, 2},
        {TCell::Make(key), TCell(oldValue.data(), oldValue.size()),
         TCell::Make(retainedKey), TCell(oldValue.data(), oldValue.size())});
    if (compact) {
        UNIT_ASSERT_VALUES_EQUAL(CompactTable(env.Runtime, shards[0], tableId, false).GetStatus(),
            NKikimrTxDataShard::TEvCompactTableResult::OK);
    }
    ui32 directBegins = 0;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBegin>(
        [&](auto&) { ++directBegins; });
    env.StartS3(MakeCsv(2, ""));
    const auto result = env.ProposeAndPlanRestore(shards[0], tableId, desc);
    UNIT_ASSERT_C(result.GetSuccess(), result.GetExplain());
    UNIT_ASSERT_VALUES_EQUAL(directBegins, 0u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId),
        "key = 1, value = value1\nkey = 2, value = value2\nkey = 3, value = old\n");
}

Y_UNIT_TEST(ResumeLegacyRestoreAfterEnablingDirectImport) {
    TTestEnv env;
    env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(false);
    auto [shards, tableId] = env.CreateUint32Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    env.StartS3(MakeCsv(10, ""));
    TBlockEvents<TEvDataShard::TEvS3UploadRowsResponse> uploaded(env.Runtime);
    const ui64 txId = env.AllocateTxId();
    TMaybe<NKikimrTxDataShard::TShardOpResult> result;
    auto done = env.Runtime.AddObserver<TEvDataShard::TEvSchemaChanged>([&](auto& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetTxId() == txId && record.HasOpResult()) {
            result = record.GetOpResult();
        }
    });
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 16, txId));
    env.Runtime.WaitFor("first committed legacy batch", [&] { return !uploaded.empty(); });
    uploaded.Stop();
    env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(true);
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    env.Runtime.WaitFor("resumed restore", [&] { return result.Defined(); }, TDuration::Minutes(3));
    UNIT_ASSERT_C(result->GetSuccess(), result->GetExplain());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(10));
}

Y_UNIT_TEST_TWIN(ResumeDirectRestoreAfterDisablingDirectImport, committed) {
    TTestEnv env;
    env.ValidateChecksums = true;
    auto [shards, tableId] = env.CreateUint32Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    const auto data = MakeCsv(10, "");
    env.StartS3(data);
    const ui64 txId = env.AllocateTxId();

    TMaybe<NKikimrTxDataShard::TShardOpResult> result;
    auto done = env.Runtime.AddObserver<TEvDataShard::TEvSchemaChanged>([&](auto& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetTxId() == txId && record.HasOpResult()) {
            result = record.GetOpResult();
        }
    });
    ui32 directBegins = 0;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBegin>(
        [&](auto&) { ++directBegins; });
    ui32 rowUploads = 0;
    auto upload = env.Runtime.AddObserver<TEvDataShard::TEvS3UploadRowsRequest>(
        [&](auto&) { ++rowUploads; });

    // Stop either before attachment, with durable but unreferenced part blobs,
    // or after its commit, before the downloader can report completion.
    TBlockEvents<TEvDataShard::TEvS3DirectWriteFinish> finish(env.Runtime,
        [](const auto&) { return !committed; });
    TBlockEvents<TEvDataShard::TEvS3DirectWriteFinishResult> finishResult(env.Runtime,
        [](const auto&) { return committed; });
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 16, txId));
    env.Runtime.WaitFor("direct import before completion", [&] {
        return committed ? !finishResult.empty() : !finish.empty();
    });
    UNIT_ASSERT_VALUES_EQUAL(directBegins, 1u);
    UNIT_ASSERT_VALUES_EQUAL(rowUploads, 0u);
    UNIT_ASSERT(!result);
    if (committed) {
        UNIT_ASSERT_C(finishResult.front()->Get()->Success, finishResult.front()->Get()->Error);
    }

    TMaybe<TS3Download> recoveredInfo;
    auto checkpoint = env.Runtime.AddObserver<TEvDataShard::TEvS3DownloadInfo>([&](auto& ev) {
        if (!recoveredInfo) {
            recoveredInfo = ev->Get()->Info;
        }
    });
    env.Runtime.GetAppData().FeatureFlags.SetEnableDataShardDirectPartImport(false);
    finish.Stop();
    finishResult.Stop();
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    env.Runtime.WaitFor("restore after disabling direct import", [&] { return result.Defined(); },
        TDuration::Minutes(3));

    UNIT_ASSERT(recoveredInfo);
    UNIT_ASSERT(recoveredInfo->DataETag);
    UNIT_ASSERT_VALUES_EQUAL(recoveredInfo->ProcessedBytes, committed ? data.size() : 0u);
    UNIT_ASSERT_VALUES_EQUAL(recoveredInfo->WrittenRows, committed ? 10u : 0u);
    UNIT_ASSERT_VALUES_EQUAL(directBegins, 1u);
    UNIT_ASSERT_VALUES_EQUAL(rowUploads == 0, committed);
    UNIT_ASSERT_C(result->GetSuccess(), result->GetExplain());
    UNIT_ASSERT_VALUES_EQUAL(result->GetRowsProcessed(), 10u);
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(10));

    // The recovered contents must remain correct after another tablet restart.
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), ExpectedUint32TableState(10));
}

Y_UNIT_TEST(CancelBeforeDirectWriteBegin) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    env.StartS3(MakeCsv(1));
    TBlockEvents<TEvDataShard::TEvS3DirectWriteBegin> blockedBegin(env.Runtime);
    const ui64 txId = env.AllocateTxId();
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 128, txId));
    env.Runtime.WaitFor("direct write begin", [&] { return !blockedBegin.empty(); });
    env.CancelRestore(shards[0], tableId, txId);
    TMaybe<bool> beginSucceeded;
    auto observer = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBeginResult>(
        [&](auto& ev) { beginSucceeded = ev->Get()->Success; });
    blockedBegin.Unblock().Stop();
    env.Runtime.WaitFor("late begin response", [&] { return beginSucceeded.Defined(); }, TDuration::Seconds(30));
    UNIT_ASSERT_C(!*beginSucceeded, "A cancelled restore reserved a new writer and GC barrier");
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(CancelBeforeDirectWriteBeginResult) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    auto barriers = [&]() -> ui64 {
        // Executor gauges are refreshed every 15 seconds.
        env.Runtime.SimulateSleep(TDuration::Seconds(30));
        const auto sender = env.Runtime.AllocateEdgeActor();
        env.Runtime.SendToPipe(shards[0], sender, new TEvTablet::TEvGetCounters(), 0, GetPipeConfigWithRetries());
        auto response = env.Runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(sender);
        for (const auto& counter : response->Get()->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters()) {
            if (counter.GetName() == "GcBarriersActive") {
                return counter.GetValue();
            }
        }
        UNIT_FAIL("GcBarriersActive counter not found");
        return 0;
    };
    const auto initialBarriers = barriers();
    env.StartS3(MakeCsv(1));
    TBlockEvents<TEvDataShard::TEvS3DirectWriteBeginResult> begin(env.Runtime);
    const ui64 txId = env.AllocateTxId();
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 128, txId));
    env.Runtime.WaitFor("reserved direct writer", [&] { return !begin.empty(); });
    UNIT_ASSERT(begin.front()->Get()->Success);
    UNIT_ASSERT_C(barriers() > initialBarriers, "The direct writer did not reserve a GC barrier");
    env.CancelRestore(shards[0], tableId, txId);
    UNIT_ASSERT_VALUES_EQUAL_C(barriers(), initialBarriers, "Cancellation leaked the unclaimed writer reservation");
    begin.Unblock().Stop();
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(BlobAcknowledgementDuringChecksumRetry) {
    TTestEnv env;
    env.ValidateChecksums = true;
    auto [shards, tableId] = env.CreateUint32Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    TStringBuilder csv;
    const TString value(128 * 1024, 'x');
    for (ui32 key = 1; key <= 128; ++key) {
        csv << key << ",\"" << value << "\"\n";
    }
    env.StartS3(csv);

    TActorId downloader;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBeginResult>(
        [&](auto& ev) { downloader = ev->Recipient; });
    TBlockEvents<TEvBlobStorage::TEvPutResult> puts(env.Runtime,
        [&](const auto& ev) { return ev->Recipient == downloader; });
    bool failed = false;
    auto failRead = env.Runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NWrappers::NExternalStorage::EvGetObjectResponse) {
                return;
            }
            const auto* msg = ev->Get<NWrappers::NExternalStorage::TEvGetObjectResponse>();
            if (failed || puts.empty() || !msg->Key || !msg->Key->EndsWith(".csv")) {
                return;
            }
            failed = true;
            env.Runtime.EnableScheduleForActor(ev->Recipient);
            Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> outcome(
                Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::INTERNAL_FAILURE, "InternalError", "injected failure", true)));
            ev.Reset(new IEventHandle(ev->Recipient, ev->Sender,
                new NWrappers::NExternalStorage::TEvGetObjectResponse(msg->Key, msg->GetReadInterval(), outcome),
                ev->Flags, ev->Cookie));
        });
    // Storage replies may have a synthetic sender without a runtime mailbox;
    // hold them directly instead of resolving the sender's actor name in TBlockEvents.
    TDeque<NWrappers::NExternalStorage::TEvHeadObjectResponse::TPtr> checksum;
    auto checksumObserver = env.Runtime.AddObserver<NWrappers::NExternalStorage::TEvHeadObjectResponse>(
        [&](auto& ev) {
            if (failed && ev->Get()->Key && ev->Get()->Key->EndsWith(".sha256")) {
                checksum.emplace_back(std::move(ev));
            }
        });
    const ui64 txId = env.AllocateTxId();
    TMaybe<NKikimrTxDataShard::TShardOpResult> result;
    auto done = env.Runtime.AddObserver<TEvDataShard::TEvSchemaChanged>([&](auto& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetTxId() == txId && record.HasOpResult()) {
            result = record.GetOpResult();
        }
    });
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 1024 * 1024, txId));
    env.Runtime.WaitFor("checksum retry with blob puts outstanding", [&] { return !checksum.empty(); },
        TDuration::Minutes(3));
    UNIT_ASSERT(!puts.empty());
    puts.Unblock().Stop();
    // Keep checksum HEAD blocked until the acknowledgements have been delivered.
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    checksumObserver.Remove();
    for (auto& ev : checksum) {
        env.Runtime.Send(ev.Release(), 0, true);
    }
    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return result.Defined(); };
    options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
    env.Runtime.DispatchEvents(options, TDuration::Minutes(3));
    UNIT_ASSERT_C(result, "Restore lost a blob acknowledgement during checksum retry");
    UNIT_ASSERT_C(result->GetSuccess(), result->GetExplain());
    UNIT_ASSERT_VALUES_EQUAL(result->GetRowsProcessed(), 128);
    TStringBuilder expected;
    for (ui32 key = 1; key <= 128; ++key) {
        expected << "key = " << key << ", value = " << value << "\n";
    }
    UNIT_ASSERT_VALUES_EQUAL(NBackup::ComputeChecksum(ReadTable(env.Server, shards, tableId)),
        NBackup::ComputeChecksum(expected));
}

Y_UNIT_TEST_TWIN(RejectDiskQuotaExceeded, beforeBegin) {
    TTestEnv env;
    using TUpdated = TEvTxProxySchemeCache::TEvWatchNotifyUpdated;
    TAutoPtr<IEventHandle> notification;
    auto quota = env.Runtime.AddObserver<TUpdated>([&](auto& ev) {
        auto* msg = ev->Get();
        NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
        record.MutablePathDescription()->MutableDomainDescription()->MutableDomainState()
            ->SetDiskQuotaExceeded(beforeBegin);
        msg->Result = NSchemeCache::TDescribeResult::Create(record);
        notification.Reset(new IEventHandle(ev->Recipient, ev->Sender,
            new TUpdated(msg->Key, msg->Path, msg->PathId, msg->Result)));
    });
    auto [shards, tableId] = env.CreateUtf8Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    env.Runtime.WaitFor("domain state notification", [&] { return bool(notification); });
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
    quota.Remove();
    env.StartS3(MakeCsv(1));
    TBlockEvents<TEvDataShard::TEvS3DirectWriteFinish> finish(env.Runtime);
    const ui64 txId = env.AllocateTxId();
    TMaybe<NKikimrTxDataShard::TShardOpResult> result;
    auto done = env.Runtime.AddObserver<TEvDataShard::TEvSchemaChanged>([&](auto& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetTxId() == txId && record.HasOpResult()) {
            result = record.GetOpResult();
        }
    });
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 128, txId));
    env.Runtime.WaitFor("restore admitted or rejected", [&] { return result || !finish.empty(); });
    if (!beforeBegin) {
        UNIT_ASSERT(!finish.empty());
        auto* msg = notification->Get<TUpdated>();
        NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
        record.MutablePathDescription()->MutableDomainDescription()->MutableDomainState()->SetDiskQuotaExceeded(true);
        msg->Result = NSchemeCache::TDescribeResult::Create(record);
        env.Runtime.Send(notification.Release(), 0, true);
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return result.Defined(); };
        options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
        env.Runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT_C(result, "Disk quota change did not stop the active direct import");
    }
    finish.Unblock().Stop();
    env.Runtime.WaitFor("restore result", [&] { return result.Defined(); }, TDuration::Seconds(30));
    UNIT_ASSERT_C(!result->GetSuccess(), "Direct import bypassed the database disk quota");
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(YellowStopReportSurvivesCancellation) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    env.StartS3(MakeCsv(1));

    TActorId downloader;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBeginResult>(
        [&](auto& ev) { downloader = ev->Recipient; });
    TBlockEvents<TEvTablet::TEvCheckBlobstorageStatusResult> statusChecks(env.Runtime);
    auto yellow = env.Runtime.AddObserver<TEvBlobStorage::TEvPutResult>([&](auto& ev) {
        if (ev->Recipient == downloader) {
            const_cast<TStorageStatusFlags&>(ev->Get()->StatusFlags) = TStorageStatusFlags(
                NKikimrBlobStorage::StatusIsValid | NKikimrBlobStorage::StatusDiskSpaceYellowStop);
        }
    });
    TBlockEvents<TEvDataShard::TEvS3DirectWriteAbort> abort(env.Runtime);
    TBlockEvents<TEvDataShard::TEvAsyncJobComplete> complete(env.Runtime,
        [&](const auto& ev) { return ev->Sender == downloader; });
    const ui64 txId = env.AllocateTxId();
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 128, txId));
    env.Runtime.WaitFor("writer yellow-stop report", [&] { return !abort.empty(); });
    UNIT_ASSERT(!abort.front()->Get()->YellowStopChannels.empty());
    yellow.Remove();

    // Cancellation releases the reservation before its owner's health report arrives.
    env.CancelRestore(shards[0], tableId, txId);
    abort.Unblock().Stop();
    complete.Stop();
    const auto result = env.ProposeAndPlanRestore(shards[0], tableId, desc);
    UNIT_ASSERT_C(!result.GetSuccess(), "Cancellation discarded the writer's yellow-stop report");
    UNIT_ASSERT_STRING_CONTAINS(result.GetExplain(), "out of disk space");
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST(YellowStopFromUnrelatedCommitAbortsDirectImport) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    env.StartS3(MakeCsv(1));

    TActorId downloader;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBeginResult>(
        [&](auto& ev) { downloader = ev->Recipient; });
    TBlockEvents<TEvBlobStorage::TEvPutResult> puts(env.Runtime,
        [&](const auto& ev) { return ev->Recipient == downloader; });
    TBlockEvents<TEvTablet::TEvCheckBlobstorageStatusResult> statusChecks(env.Runtime);
    const ui64 txId = env.AllocateTxId();
    TMaybe<NKikimrTxDataShard::TShardOpResult> result;
    auto done = env.Runtime.AddObserver<TEvDataShard::TEvSchemaChanged>([&](auto& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetTxId() == txId && record.HasOpResult()) {
            result = record.GetOpResult();
        }
    });
    env.StartRestore(shards[0], tableId.PathId.OwnerId, txId,
        env.BuildRestoreBody(tableId, desc, 0, 128, txId));
    env.Runtime.WaitFor("direct writer with outstanding puts", [&] { return !puts.empty(); });

    // Commit an unrelated system-table update while the direct writer is waiting.
    // Its own puts never report yellow stop and cannot finish the import yet.
    bool injected = false;
    auto yellow = env.Runtime.AddObserver<TEvBlobStorage::TEvPutResult>([&](auto& ev) {
        if (ev->Get()->Id.TabletID() == shards[0] && ev->Recipient != downloader) {
            const_cast<TStorageStatusFlags&>(ev->Get()->StatusFlags) = TStorageStatusFlags(
                NKikimrBlobStorage::StatusIsValid | NKikimrBlobStorage::StatusDiskSpaceYellowStop);
            injected = true;
        }
    });
    TS3Download info;
    info.DataETag = "unrelated";
    const auto sender = env.Runtime.AllocateEdgeActor();
    env.Runtime.SendToPipe(shards[0], sender,
        new TEvDataShard::TEvStoreS3DownloadInfo(env.AllocateTxId(), info), 0, GetPipeConfigWithRetries());
    env.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvS3DownloadInfo>(sender);
    UNIT_ASSERT(injected);
    yellow.Remove();

    // Allow the shard's existing five-second maintenance tick to notice yellow
    // stop while the direct writer's acknowledgements remain blocked.
    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return result.Defined(); };
    options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
    env.Runtime.DispatchEvents(options, TDuration::Seconds(30));
    UNIT_ASSERT_C(result, "Yellow stop from a tablet commit did not abort the active import");
    UNIT_ASSERT_C(!result->GetSuccess(), "The import succeeded while its blob acknowledgements were blocked");
    UNIT_ASSERT_STRING_CONTAINS(result->GetExplain(), "out of disk space");
    puts.Unblock().Stop();
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
    RebootTablet(env.Runtime, shards[0], env.Runtime.AllocateEdgeActor());
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

Y_UNIT_TEST_TWIN(RejectYellowStopDuringBlobWrite, beforeBegin) {
    TTestEnv env;
    auto [shards, tableId] = env.CreateUtf8Table();
    const auto desc = env.TableDescription(shards[0], tableId);
    env.StartS3(MakeCsv(1));
    TActorId downloader;
    auto begin = env.Runtime.AddObserver<TEvDataShard::TEvS3DirectWriteBeginResult>(
        [&](auto& ev) { downloader = ev->Recipient; });
    bool injected = false;
    TBlockEvents<TEvTablet::TEvCheckBlobstorageStatusResult> statusChecks(env.Runtime);
    auto yellow = env.Runtime.AddObserver<TEvBlobStorage::TEvPutResult>([&](auto& ev) {
        if (beforeBegin ? ev->Get()->Id.TabletID() == shards[0] : ev->Recipient == downloader) {
            const_cast<TStorageStatusFlags&>(ev->Get()->StatusFlags) = TStorageStatusFlags(NKikimrBlobStorage::StatusIsValid | NKikimrBlobStorage::StatusDiskSpaceYellowStop);
            injected = true;
        }
    });
    const auto result = env.ProposeAndPlanRestore(shards[0], tableId, desc);
    UNIT_ASSERT(injected);
    UNIT_ASSERT_C(!result.GetSuccess(), "Direct import ignored yellow stop on its blob writes");
    UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), EmptyTableState);
}

} // Y_UNIT_TEST_SUITE(DataShardDirectRestore)

#endif // KIKIMR_DISABLE_S3_OPS
