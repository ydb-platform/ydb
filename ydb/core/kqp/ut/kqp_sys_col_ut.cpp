#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NExperimental;

TDataQueryResult ExecuteDataQuery(TKikimrRunner& kikimr, const TString& query) {
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto txControl = TTxControl::BeginTx().CommitTx();
    auto result = session.ExecuteDataQuery(
        query,
        txControl,
        TExecDataQuerySettings()
        .OperationTimeout(TDuration::Seconds(10))
    ).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result;
}

TDataQueryResult ExecuteDataQuery(const TString& query) {
    TKikimrRunner kikimr;
    return ExecuteDataQuery(kikimr, query);
}

void SelectRowAsteriskCommon() {
    TStringBuilder query;
    query << R"(
        PRAGMA kikimr.EnableSystemColumns = "true";
        SELECT * FROM `/Root/TwoShard` WHERE Key = 1;
    )";
    auto result = ExecuteDataQuery(query);
    UNIT_ASSERT(result.GetResultSets().size());
    CompareYson(R"([[[1u];["One"];[-1]]])",
        FormatResultSetYson(result.GetResultSet(0)));
}

TScanQueryPartIterator ExecuteStreamQuery(TKikimrRunner& kikimr, const TString& query) {
    auto db = kikimr.GetTableClient();
    auto it = db.StreamExecuteScanQuery(query).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    return it;
}

void SelectRowByIdCommon() {
    TStringBuilder query;
    query << R"(
        PRAGMA kikimr.EnableSystemColumns = "true";
        SELECT * FROM `/Root/TwoShard` WHERE _yql_partition_id = 72075186224037888ul;
    )";
    auto result = ExecuteDataQuery(query);
    UNIT_ASSERT(result.GetResultSets().size());
    CompareYson(R"([[[1u];["One"];[-1]];[[2u];["Two"];[0]];[[3u];["Three"];[1]]])",
        FormatResultSetYson(result.GetResultSet(0)));
}

void SelectRangeCommon() {
    TStringBuilder query;
    query << R"(
        PRAGMA kikimr.EnableSystemColumns = "true";
        SELECT _yql_partition_id FROM `/Root/TwoShard` WHERE Key < 3;
    )";
    auto result = ExecuteDataQuery(query);
    UNIT_ASSERT(result.GetResultSets().size());
    CompareYson(R"([[[72075186224037888u]];[[72075186224037888u]]])",
        FormatResultSetYson(result.GetResultSet(0)));
}

Y_UNIT_TEST_SUITE(KqpSysColV0) {
    Y_UNIT_TEST(SelectRowAsterisk) {
        SelectRowAsteriskCommon();
    }

    Y_UNIT_TEST(SelectRowById) {
        SelectRowByIdCommon();
    }

    Y_UNIT_TEST(SelectRange) {
        SelectRangeCommon();
    }

    Y_UNIT_TEST(UpdateAndDelete) {
        TKikimrRunner kikimr;
        {
            auto query = R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                REPLACE INTO `/Root/TwoShard` (Key, Value1, Value2)
                VALUES (4u, "Four", -4);
            )";
            auto result = ExecuteDataQuery(kikimr, query);
        }
        {
            auto query = R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT COUNT(*) FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -4;
            )";
            auto result = ExecuteDataQuery(kikimr, query);
            UNIT_ASSERT(result.GetResultSets().size());
            CompareYson(R"([[1u]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto query = R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                UPDATE `/Root/TwoShard` SET Value2 = -44
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -4;
            )";
            ExecuteDataQuery(kikimr, query);
        }
        {
            auto query = R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT COUNT(*) FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -44;
            )";
            auto result = ExecuteDataQuery(kikimr, query);
            UNIT_ASSERT(result.GetResultSets().size());
            CompareYson(R"([[1u]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto query = R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                DELETE FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -44;
            )";
            auto result = ExecuteDataQuery(kikimr, query);
        }
        {
            auto query = R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT COUNT(*) FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -44;
            )";
            auto result = ExecuteDataQuery(kikimr, query);
            UNIT_ASSERT(result.GetResultSets().size());
            CompareYson(R"([[0u]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InnerJoinTables) {
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN `/Root/Join2` AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )";
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[108u];["One"];[8];["Value5"];[108u];["One"];#;["Value31"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InnerJoinSelect) {
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN (SELECT Key1, Key2, Value2 FROM `/Root/Join2`) AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )";
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[108u];["One"];[8];["Value5"];[108u];["One"];["Value31"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InnerJoinSelectAsterisk) {
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN (SELECT * FROM `/Root/Join2`) AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )";
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[108u];["One"];[8];["Value5"];[108u];["One"];#;["Value31"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST_SUITE(KqpSysColV1) {
    Y_UNIT_TEST(SelectRowAsterisk) {
        auto query = Q_(R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1;
        )");
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[1u];["One"];[-1]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SelectRowById) {
        auto query = Q_(R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT * FROM `/Root/TwoShard` WHERE _yql_partition_id = 72075186224037888ul;
        )");
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[1u];["One"];[-1]];[[2u];["Two"];[0]];[[3u];["Three"];[1]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SelectRange) {
        auto query = Q_(R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT _yql_partition_id FROM `/Root/TwoShard` WHERE Key < 3;
        )");
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[72075186224037888u]];[[72075186224037888u]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(UpdateAndDelete) {
        TKikimrRunner kikimr;
        {
            auto query = Q_(R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                REPLACE INTO `/Root/TwoShard` (Key, Value1, Value2)
                VALUES (4u, "Four", -4);
            )");
            auto result = ExecuteDataQuery(kikimr, query);
        }
        {
            auto query = Q_(R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT COUNT(*) FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -4;
            )");
            auto result = ExecuteDataQuery(kikimr, query);
            UNIT_ASSERT(result.GetResultSets().size());
            CompareYson(R"([[1u]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto query = Q_(R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                UPDATE `/Root/TwoShard` SET Value2 = -44
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -4;
            )");
            ExecuteDataQuery(kikimr, query);
        }
        {
            auto query = Q_(R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT COUNT(*) FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -44;
            )");
            auto result = ExecuteDataQuery(kikimr, query);
            UNIT_ASSERT(result.GetResultSets().size());
            CompareYson(R"([[1u]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto query = Q_(R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                DELETE FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -44;
            )");
            auto result = ExecuteDataQuery(kikimr, query);
        }
        {
            auto query = Q_(R"(
                PRAGMA kikimr.EnableSystemColumns = "true";
                SELECT COUNT(*) FROM `/Root/TwoShard`
                WHERE _yql_partition_id = 72075186224037888ul AND Value2 = -44;
            )");
            auto result = ExecuteDataQuery(kikimr, query);
            UNIT_ASSERT(result.GetResultSets().size());
            CompareYson(R"([[0u]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InnerJoinTables) {
        auto query = Q_(R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN `/Root/Join2` AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )");
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[108u];["One"];[8];["Value5"];[108u];["One"];#;["Value31"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InnerJoinSelect) {
        auto query = Q_(R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN (SELECT Key1, Key2, Value2 FROM `/Root/Join2`) AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )");
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[108u];["One"];[8];["Value5"];[108u];["One"];["Value31"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(InnerJoinSelectAsterisk) {
        auto query = Q_(R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN (SELECT * FROM `/Root/Join2`) AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )");
        auto result = ExecuteDataQuery(query);
        UNIT_ASSERT(result.GetResultSets().size());
        CompareYson(R"([[[108u];["One"];[8];["Value5"];[108u];["One"];#;["Value31"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(StreamSelectRowAsterisk) {
        TKikimrRunner kikimr;
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1;
        )";
        auto it = ExecuteStreamQuery(kikimr, query);
        CompareYson(R"([[[1u];["One"];[-1]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamSelectRowById) {
        TKikimrRunner kikimr;
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT * FROM `/Root/TwoShard` WHERE _yql_partition_id = 72075186224037888ul;
        )";
        auto it = ExecuteStreamQuery(kikimr, query);
        CompareYson(R"([[[1u];["One"];[-1]];[[2u];["Two"];[0]];[[3u];["Three"];[1]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamSelectRange) {
        TKikimrRunner kikimr;
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT _yql_partition_id FROM `/Root/TwoShard` WHERE Key < 3;
        )";
        auto it = ExecuteStreamQuery(kikimr, query);
        CompareYson(R"([[[72075186224037888u]];[[72075186224037888u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamInnerJoinTables) {
        TKikimrRunner kikimr;
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            PRAGMA DisableSimpleColumns;
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN `/Root/Join2` AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )";
        auto it = ExecuteStreamQuery(kikimr, query);
        auto yson = StreamResultToYson(it);
        Cerr << yson << Endl;
        CompareYson(R"([[[108u];["One"];[8];["Value5"];[108u];["One"];#;["Value31"]]])", yson);
//      CompareYson(R"([[[108u];["One"];[8];[108u];["One"];#;["Value5"];["Value31"]]])", yson);
    }

    Y_UNIT_TEST(StreamInnerJoinSelect) {
        TKikimrRunner kikimr;
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN (SELECT Key1, Key2, Value2 FROM `/Root/Join2`) AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )";
        auto it = ExecuteStreamQuery(kikimr, query);
        CompareYson(R"([[[108u];["One"];[8];[108u];["One"];["Value5"];["Value31"]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(StreamInnerJoinSelectAsterisk) {
        TKikimrRunner kikimr;
        auto query = R"(
            PRAGMA kikimr.EnableSystemColumns = "true";
            SELECT * FROM `/Root/Join1` AS t1
            INNER JOIN (SELECT * FROM `/Root/Join2`) AS t2
            ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
            WHERE t1.Value == "Value5" AND t2.Value2 == "Value31";
        )";
        auto it = ExecuteStreamQuery(kikimr, query);
        CompareYson(R"([[[108u];["One"];[8];[108u];["One"];#;["Value5"];["Value31"]]])", StreamResultToYson(it));
    }
}

} // namespace NKqp
} // namespace NKikimr
