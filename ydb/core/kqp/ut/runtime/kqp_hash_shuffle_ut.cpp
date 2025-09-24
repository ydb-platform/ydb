#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <util/system/fs.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(HashShuffle) {

    void RunHashShuffleCompatibility(bool shuffleElimination, bool hashV2) {
        TKikimrSettings settings;

        settings.AppConfig.MutableTableServiceConfig()->SetDefaultEnableShuffleElimination(shuffleElimination);
        settings.AppConfig.MutableTableServiceConfig()->SetDefaultHashShuffleFuncType(
            hashV2 ? NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V2
                   : NKikimrConfig::TTableServiceConfig_EHashKind_HASH_V1
        );
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableSpillingNodes("All");
        settings.AppConfig.MutableTableServiceConfig()->SetEnableQueryServiceSpilling(true);
        settings.AppConfig.MutableTableServiceConfig()->SetBlockChannelsMode(NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO);

        TKikimrRunner kikimr(settings);

        // we need following:
        //
        // 1. hash join
        // 2. scalar input (forced by scalar combiner) and possible block input (propagated from CS read)
        // 3. optional field in shuffle (caused by left join)
        //
        // HashV1 should prevent block propagation (both inputs are scalar)
        // HashV2 should provide correct answer in case of scalar vs block shuffles

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table1` (
                    k1 Uint32 NOT NULL,
                    k2 Uint32,
                    PRIMARY KEY (k1)
                )
                PARTITION BY HASH (k1)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table2` (
                    k2 Uint32,
                    k3 Uint32 NOT NULL,
                    PRIMARY KEY (k3)
                )
                PARTITION BY HASH (k3)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table1` (k1, k2) VALUES (0, 1), (1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 5);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table2` (k2, k3) VALUES (1, 1), (2, 2), (3, 3), (5, 4), (6, 5), (7, 6);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString query = R"(
                PRAGMA ydb.OptimizerHints = 'JoinType(t1 t2 Shuffle)';
                SELECT SUM(t2.k3) FROM (
                    SELECT COUNT(*) as cnt, k2 FROM `/Root/Table1` GROUP BY k1, k2
                ) as t1 LEFT JOIN `/Root/Table2` as t2 on t1.k2 = t2.k2
            )";

            NYdb::NTable::TExecDataQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            Cerr << *result.GetStats()->GetAst() << Endl;
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), R"([[[16u]]])");
        }
    }

    Y_UNIT_TEST_QUAD(Compatibility, ShuffleElimination, HashV2) {
        RunHashShuffleCompatibility(ShuffleElimination, HashV2);
    }


} // suite

} // namespace NKqp
} // namespace NKikimr
