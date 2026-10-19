#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(OffsetFetch) {
    Y_UNIT_TEST(SkipsTheFirstRowAndKeepsOne) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (1, 'a')").Ok, "1");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (2, 'b')").Ok, "2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (3, 'c')").Ok, "3");

        const auto page = sql.Exec(
            "SELECT note FROM items WHERE id >= 1 ORDER BY id OFFSET 1 FETCH FIRST 1 ROW ONLY");
        UNIT_ASSERT_C(page.Ok, page.SqlState + " " + page.Message);
        UNIT_ASSERT_VALUES_EQUAL(page.CommandTag, "SELECT 1");
        UNIT_ASSERT_VALUES_EQUAL(page.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page.Rows[0][0].Text, "b");
    }
}
