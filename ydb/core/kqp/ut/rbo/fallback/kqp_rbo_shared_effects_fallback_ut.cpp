#include <ydb/core/kqp/ut/rbo/effects/kqp_effects_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

// Supported-syntax counterpart of the disabled RandomWithIndex test in kqp_effects_ut.cpp.
Y_UNIT_TEST_SUITE(KqpRboSharedEffectsFallback) {

    Y_UNIT_TEST_QUAD(RandomWithIndexOn, NewRbo, UseSecondaryIndex) {
        // Both REPLACE statements must use the same generated rowKey. Independent
        // evaluation by RBO used to break the link between the two tables, so the
        // RBO variants require fallback and compare the persisted keys directly.
        auto config = NewRboAppConfig();
        config.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        config.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/Rows` (
                    rowKey Uint64 NOT NULL,
                    v1 Uint64 NOT NULL,
                    v2 Uint64 NOT NULL,
                    %s
                    PRIMARY KEY (rowKey)
                );
            )", (UseSecondaryIndex ? "INDEX idx_2 GLOBAL ON (v2)," : ""));
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                CREATE TABLE `/Root/GlobalKeyToRows` (
                    globalKey Uint64 NOT NULL,
                    rowKey Uint64 NOT NULL,
                    PRIMARY KEY (globalKey, rowKey)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                $globalKey = 1;
                $addRows = SELECT 1 AS v1, 2 AS v2;

                $rowKeys = SELECT rowKey FROM `/Root/GlobalKeyToRows` WHERE globalKey = $globalKey;

                $existingRows = SELECT rowKey, v1, v2 FROM `/Root/Rows` WHERE rowKey IN $rowKeys;

                $rowUpdates = SELECT
                    r.rowKey ?? CAST(RandomNumber(r.rowKey) AS Uint64) AS rowKey,
                    l.v1 AS v1,
                    l.v2 AS v2
                FROM $addRows AS l
                LEFT JOIN $existingRows AS r
                ON l.v1 = r.v1 AND l.v2 = r.v2;

                REPLACE INTO `/Root/Rows`
                SELECT rowKey, v1, v2 FROM $rowUpdates;

                REPLACE INTO `/Root/GlobalKeyToRows`
                SELECT $globalKey AS globalKey, rowKey FROM $rowUpdates;
            )";
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            if constexpr (NewRbo) {
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Compilation with the new RBO failed");
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "shared non-repeatable input");
            }
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT rowKey FROM `/Root/Rows`;
                SELECT rowKey FROM `/Root/GlobalKeyToRows`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(1).RowsCount(), 1);
            // Compare persisted keys directly, independently of join planning.
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)),
                "both effects must use the same generated rowKey");
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                $rowKeys = SELECT rowKey FROM `/Root/GlobalKeyToRows` WHERE globalKey = 1;
                SELECT COUNT(*) FROM `/Root/Rows` WHERE rowKey IN $rowKeys;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[1u]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

} // suite

} // namespace NKikimr::NKqp
