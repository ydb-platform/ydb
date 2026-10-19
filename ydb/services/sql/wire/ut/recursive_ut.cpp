#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(Recursive) {
    Y_UNIT_TEST(ShortSeriesStopsAtTheBound) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE seed (id int8 PRIMARY KEY)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO seed (id) VALUES (1)").Ok, "insert");

        const auto series = sql.Exec(
            "WITH RECURSIVE nums AS (SELECT id FROM seed WHERE id = 1 UNION ALL SELECT id + 1 FROM nums WHERE id < 4) SELECT id FROM nums");
        UNIT_ASSERT_C(series.Ok, series.SqlState + " " + series.Message);
        UNIT_ASSERT_VALUES_EQUAL(series.CommandTag, "SELECT 4");
        UNIT_ASSERT_VALUES_EQUAL(series.Rows.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(series.Rows[0][0].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(series.Rows[1][0].Text, "2");
        UNIT_ASSERT_VALUES_EQUAL(series.Rows[2][0].Text, "3");
        UNIT_ASSERT_VALUES_EQUAL(series.Rows[3][0].Text, "4");
    }

    Y_UNIT_TEST(UnfixedSeriesHitsTheLimit) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        settings.AppConfig->MutableSqlConfig()->SetRecursionLimit(2);
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE seed (id int8 PRIMARY KEY)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO seed (id) VALUES (1)").Ok, "insert");

        const auto series = sql.Exec(
            "WITH RECURSIVE nums AS (SELECT id FROM seed WHERE id = 1 UNION ALL SELECT id + 1 FROM nums WHERE id < 100) SELECT id FROM nums");
        UNIT_ASSERT(!series.Ok);
        UNIT_ASSERT_VALUES_EQUAL(series.SqlState, "54001");
    }
}
