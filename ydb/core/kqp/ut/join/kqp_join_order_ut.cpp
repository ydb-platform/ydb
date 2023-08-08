#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

/**
 * A basic join order test. We define 5 tables sharing the same
 * key attribute and construct various full clique join queries
*/
static void CreateSampleTable(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/R` (
            id Int32,
            payload1 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/S` (
            id Int32,
            payload2 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/T` (
            id Int32,
            payload3 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/U` (
            id Int32,
            payload4 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/V` (
            id Int32,
            payload5 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(

        REPLACE INTO `/Root/R` (id, payload1) VALUES
            (1, "blah");

        REPLACE INTO `/Root/S` (id, payload2) VALUES
            (1, "blah");

        REPLACE INTO `/Root/T` (id, payload3) VALUES
            (1, "blah");

        REPLACE INTO `/Root/U` (id, payload4) VALUES
            (1, "blah");

        REPLACE INTO `/Root/V` (id, payload5) VALUES
            (1, "blah");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());
}

static TKikimrRunner GetKikimrWithJoinSettings(){
    TVector<NKikimrKqp::TKqpSetting> settings;
    NKikimrKqp::TKqpSetting setting;
    setting.SetName("OptEnableCostBasedOptimization");
    setting.SetValue("true");
    settings.push_back(setting);

    return TKikimrRunner(settings);
}


Y_UNIT_TEST_SUITE(KqpJoinOrder) {
    Y_UNIT_TEST(FiveWayJoin) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

     Y_UNIT_TEST(FiveWayJoinWithPreds) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithComplexPreds) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND ( S.payload2  || T.payload3 = U.payload4 )
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithComplexPreds2) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE (R.payload1 || V.payload5 = 'blah') AND U.payload4 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithPredsAndEquiv) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND R.id = 1
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }
}

}
}

