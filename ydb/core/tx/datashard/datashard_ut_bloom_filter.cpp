#include <ydb/core/tablet_flat/bloom_filter_defaults.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include "datashard_ut_common_kqp.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace Tests;

// Datashard-level coverage for the bloom filter logic in TUserTable::ApplyConfig/ApplyAlter
// (datashard_user_table.cpp). These tests boot a real datashard, drive CREATE/ALTER/DROP through
// SQL, and read the shard's OWN persisted table description via TEvGetInfoRequest (built from
// TUserTable::GetSchema) — i.e. exactly what ApplyConfig/ApplyAlter wrote, not the schemeshard view.
Y_UNIT_TEST_SUITE(DataShardBloomFilter) {

    std::tuple<TTestActorRuntime&, Tests::TServer::TPtr, TActorId> CreateServer() {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);
        serverSettings.FeatureFlags.SetEnableLocalIndexAsSchemeObject(true);

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

    Y_UNIT_TEST(PrefixesUpdatedOnAlter) {
        // The engine ByKeyFilterPrefixes the shard persists must track CREATE / ALTER ADD INDEX /
        // DROP INDEX. In particular, dropping the LAST prefix must clear the filter entirely — the
        // delta carries no explicit bloom field in that case, and before the fix it was ignored,
        // leaving a stale filter on the shard until restart.
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

        // DROP one of two indexes -> ApplyAlter removes just that prefix.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` DROP INDEX idx1;"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{2}));

        // DROP the LAST index -> ApplyAlter must clear the filter entirely (regression target).
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` DROP INDEX idx2;"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{}));
    }

    Y_UNIT_TEST(FullKeyCoexistsWithPrefix) {
        // The EnableFilterByKey (whole-key KEY_BLOOM_FILTER) branch: the whole-key filter is unified
        // into the engine prefix list as an entry of length == number of key columns, and must
        // coexist with shorter prefix blooms, survive dropping the last shorter prefix, and be
        // cleared by KEY_BLOOM_FILTER=DISABLED.
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

        // Drop the only shorter prefix bloom -> whole-key (length 2) must remain, NOT clear to empty.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` DROP INDEX idx1;"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{2}));

        // Disabling KEY_BLOOM_FILTER clears all bloom state on the shard.
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, "ALTER TABLE `/Root/T` SET (KEY_BLOOM_FILTER = DISABLED);"), "SUCCESS");
        UNIT_ASSERT_VALUES_EQUAL(ShardBloomPrefixLengths(runtime, shard), (TVector<ui32>{}));
    }

    Y_UNIT_TEST(FppPropagatedToShard) {
        // The per-prefix false-positive probability resolved by ApplyConfig/ApplyAlter
        // (explicit value, or NTable::DefaultBloomFilterFpp when unspecified) must reach the shard.
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
