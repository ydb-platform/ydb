#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(With) {
    Y_UNIT_TEST(NameUsedTwiceReturnsTheSameRows) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (1, 'a')").Ok, "a");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (2, 'b')").Ok, "b");

        const auto twice = sql.Exec("WITH s AS (SELECT note FROM items WHERE id >= 1) SELECT note FROM s UNION ALL SELECT note FROM s");
        UNIT_ASSERT_C(twice.Ok, twice.SqlState + " " + twice.Message);
        UNIT_ASSERT_VALUES_EQUAL(twice.CommandTag, "SELECT 4");
        UNIT_ASSERT_VALUES_EQUAL(twice.Rows.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(twice.Rows[0][0].Text, twice.Rows[2][0].Text);
        UNIT_ASSERT_VALUES_EQUAL(twice.Rows[1][0].Text, twice.Rows[3][0].Text);
        UNIT_ASSERT_VALUES_EQUAL(twice.Rows[0][0].Text, "a");
        UNIT_ASSERT_VALUES_EQUAL(twice.Rows[1][0].Text, "b");
    }
}
