#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(Having) {
    Y_UNIT_TEST(DropsTheGroupThatFails) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE sales (id int8 PRIMARY KEY, kind text, amount int8)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO sales (id, kind, amount) VALUES (1, 'a', 10)").Ok, "1");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO sales (id, kind, amount) VALUES (2, 'a', 5)").Ok, "2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO sales (id, kind, amount) VALUES (3, 'b', 7)").Ok, "3");

        const auto kept = sql.Exec("SELECT kind, sum(amount) FROM sales WHERE id >= 1 GROUP BY kind HAVING sum > 10");
        UNIT_ASSERT_C(kept.Ok, kept.SqlState + " " + kept.Message);
        UNIT_ASSERT_VALUES_EQUAL(kept.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(kept.Rows[0][0].Text, "a");
        UNIT_ASSERT_VALUES_EQUAL(kept.Rows[0][1].Text, "15");
    }
}
