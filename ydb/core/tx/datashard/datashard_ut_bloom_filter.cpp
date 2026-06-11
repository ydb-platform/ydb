#include <ydb/core/tablet_flat/bloom_filter_defaults.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace Tests;

// Datashard-level coverage for the bloom filter logic
Y_UNIT_TEST_SUITE(DataShardBloomFilter) {

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> CreateServer() {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        return {runtime, server, sender};
    }

    // The shard's own bloom prefix lengths -> FPP map, from its persisted schema.
    TMap<ui32, double> ShardBloomPrefixes(TTestActorRuntime& runtime, ui64 shard) {
        auto edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(shard, edge, new TEvDataShard::TEvGetInfoRequest(),
            0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto* resp = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
        UNIT_ASSERT(resp);
        UNIT_ASSERT_VALUES_EQUAL(resp->Record.UserTablesSize(), 1u);
        const auto& pc = resp->Record.GetUserTables(0).GetDescription().GetPartitionConfig();
        TMap<ui32, double> result;
        for (const auto& p : pc.GetByKeyFilterPrefixes()) {
            result[p.GetPrefixLength()] = p.GetFalsePositiveProbability();
        }
        return result;
    }

    TVector<ui32> ShardBloomPrefixLengths(TTestActorRuntime& runtime, ui64 shard) {
        TVector<ui32> out;
        for (const auto& [len, fpp] : ShardBloomPrefixes(runtime, shard)) {
            out.push_back(len);
        }
        return out;
    }

    // The engine ByKeyFilterPrefixes the shard persists must track CREATE / ALTER ADD INDEX, and
    // must be cleared when KEY_BLOOM_FILTER=DISABLED wipes the whole config. The clear case is the
    // important one: the shard had prefixes and ApplyAlter must drop them from the engine, not keep
    // a stale filter.
    Y_UNIT_TEST(PrefixesUpdatedOnAlter) {
        auto [runtime, server, sender] = CreateServer();

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/T` (
                    Key1 Uint64, Key2 Uint64, Value String,
                    PRIMARY KEY (Key1, Key2),
                    INDEX idx1 LOCAL USING bloom_filter ON (Key1)
                );
            )"), "SUCCESS");

        auto shards = GetTableShards(server, sender, "/Root/T");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        const ui64 shard = shards.at(0);

        // ApplyConfig carried the prefix to the datashard.
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{1}));

        // ALTER ADD INDEX -> ApplyAlter adds the second prefix alongside the first.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                ALTER TABLE `/Root/T` ADD INDEX idx2 LOCAL USING bloom_filter ON (Key1, Key2);
            )"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{1, 2}));

        // KEY_BLOOM_FILTER = DISABLED clears the whole bloom config -> ApplyAlter must drop every
        // prefix from the engine.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` SET (KEY_BLOOM_FILTER = DISABLED);"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{}));

        // Re-adding a bloom index after disable should correctly re-establish bloom state.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` ADD INDEX idx3 LOCAL USING bloom_filter ON (Key1);"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{1}));
    }

    // The EnableFilterByKey (whole-key KEY_BLOOM_FILTER) branch: the whole-key filter is unified
    // into the engine prefix list as an entry of length == number of key columns, must coexist with
    // shorter prefix blooms, and be cleared by KEY_BLOOM_FILTER=DISABLED.
    Y_UNIT_TEST(FullKeyCoexistsWithPrefix) {
        auto [runtime, server, sender] = CreateServer();

        // (Key1, Key2) primary key => whole-key bloom has prefix length 2.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/T` (
                    Key1 Uint64, Key2 Uint64, Value String,
                    PRIMARY KEY (Key1, Key2),
                    INDEX idx1 LOCAL USING bloom_filter ON (Key1)
                );
            )"), "SUCCESS");
        auto shards = GetTableShards(server, sender, "/Root/T");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        const ui64 shard = shards.at(0);
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{1}));

        // Enable the whole-key filter -> length-2 entry added alongside the length-1 prefix.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` SET (KEY_BLOOM_FILTER = ENABLED);"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{1, 2}));

        // Disabling KEY_BLOOM_FILTER clears all bloom state on the shard.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` SET (KEY_BLOOM_FILTER = DISABLED);"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{}));

        // Re-enabling KEY_BLOOM_FILTER should correctly re-establish the whole-key filter.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` SET (KEY_BLOOM_FILTER = ENABLED);"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{2}));

        // Adding a new prefix bloom index after disable should also work correctly.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` ADD INDEX idx3 LOCAL USING bloom_filter ON (Key1);"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{1, 2}));
    }

    // The per-prefix false-positive probability resolved by ApplyConfig
    // (explicit value, or NTable::DefaultBloomFilterFpp when unspecified) must reach the shard.
    Y_UNIT_TEST(FppPropagatedToShard) {
        auto [runtime, server, sender] = CreateServer();

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/T` (
                    Key1 Uint64, Key2 Uint64, Value String,
                    PRIMARY KEY (Key1, Key2),
                    INDEX idx1 LOCAL USING bloom_filter ON (Key1) WITH (false_positive_probability = 0.02),
                    INDEX idx2 LOCAL USING bloom_filter ON (Key1, Key2)
                );
            )"), "SUCCESS");
        auto shards = GetTableShards(server, sender, "/Root/T");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        auto prefixes = ShardBloomPrefixes(runtime, shards.at(0));
        UNIT_ASSERT_VALUES_EQUAL(prefixes.size(), 2u);
        // Explicit FPP is preserved; the unspecified one falls back to the engine default.
        UNIT_ASSERT_DOUBLES_EQUAL(prefixes.at(1), 0.02, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(prefixes.at(2), NTable::DefaultBloomFilterFpp, 1e-9);
    }

} // Y_UNIT_TEST_SUITE(DataShardBloomFilter)

} // namespace NKikimr
