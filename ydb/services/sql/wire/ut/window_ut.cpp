#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(Window) {
    Y_UNIT_TEST(RowNumberNumbersAPartition) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, kind text, note text)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, kind, note) VALUES (1, 'a', 'x')").Ok, "1");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, kind, note) VALUES (2, 'a', 'x')").Ok, "2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, kind, note) VALUES (3, 'b', 'y')").Ok, "3");

        const auto numbered = sql.Exec(
            "SELECT kind, row_number() OVER (PARTITION BY kind ORDER BY id) FROM items WHERE id >= 1");
        UNIT_ASSERT_C(numbered.Ok, numbered.SqlState + " " + numbered.Message);
        UNIT_ASSERT_VALUES_EQUAL(numbered.Rows.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(numbered.Rows[0][0].Text, "a");
        UNIT_ASSERT_VALUES_EQUAL(numbered.Rows[0][1].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(numbered.Rows[1][0].Text, "a");
        UNIT_ASSERT_VALUES_EQUAL(numbered.Rows[1][1].Text, "2");
        UNIT_ASSERT_VALUES_EQUAL(numbered.Rows[2][0].Text, "b");
        UNIT_ASSERT_VALUES_EQUAL(numbered.Rows[2][1].Text, "1");

        const auto ranked = sql.Exec(
            "SELECT rank() OVER (PARTITION BY kind ORDER BY note) FROM items WHERE id >= 1");
        UNIT_ASSERT_C(ranked.Ok, ranked.SqlState + " " + ranked.Message);
        UNIT_ASSERT_VALUES_EQUAL(ranked.Rows.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(ranked.Rows[0][0].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(ranked.Rows[1][0].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(ranked.Rows[2][0].Text, "1");

        const auto other = sql.Exec("SELECT lag(id) OVER (ORDER BY id) FROM items WHERE id >= 1");
        UNIT_ASSERT(!other.Ok);
        UNIT_ASSERT_VALUES_EQUAL(other.SqlState, "0A000");
    }
}
