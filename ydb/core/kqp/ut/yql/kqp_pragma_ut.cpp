#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpPragma) {
    Y_UNIT_TEST(Auth) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            PRAGMA kikimr.Auth = "default_kikimr";
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_PRAGMA_NOT_SUPPORTED));
    }

    Y_UNIT_TEST(ResetPerQuery) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            PRAGMA kikimr.EnableSystemColumns = "true";

            SELECT COUNT(_yql_partition_id) FROM `/Root/KeyValue` WHERE Key = 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        CompareYson(R"([[1u]])", FormatResultSetYson(result.GetResultSet(0)));

        /*result = session.ExecuteDataQuery(R"(
            SELECT COUNT(_yql_partition_id) FROM `/Root/KeyValue` WHERE Key = 1;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::CORE_TYPE_ANN));*/
    }

    Y_UNIT_TEST(OrderedColumns) {
        TKikimrRunner kikimr;
        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_v1
            CREATE TABLE `/Root/NewTable` (
                Column3 Uint32,
                Column2 Uint32,
                Column1 Uint32,
                PRIMARY KEY (Column1)
            );
            COMMIT;

            INSERT INTO `/Root/NewTable` (Column1, Column2, Column3) VALUES (1, 2, 3);
            COMMIT;

            PRAGMA OrderedColumns;
            SELECT * FROM `/Root/NewTable`;
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[3u];[2u];[1u]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Warning) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            PRAGMA Warning("disable", "1108");

            SELECT * FROM `/Root/KeyValue` WHERE Key IN (1, 2);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().Empty(), result.GetIssues().ToString());
    }
}

} // namspace NKqp
} // namespace NKikimr
