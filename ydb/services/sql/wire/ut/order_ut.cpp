#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(OrderBy) {
    Y_UNIT_TEST(PrimaryKeyDescUsesReverseRead) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (1, 'a')").Ok, "1");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (2, 'c')").Ok, "2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (3, 'b')").Ok, "3");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (4, NULL)").Ok, "4");

        const auto descending = sql.Exec("SELECT note FROM items WHERE id >= 1 ORDER BY id DESC");
        UNIT_ASSERT_C(descending.Ok, descending.SqlState + " " + descending.Message);
        UNIT_ASSERT_VALUES_EQUAL(descending.AccessPath, "Reverse");
        UNIT_ASSERT_VALUES_EQUAL(descending.Rows.size(), 4u);
        UNIT_ASSERT(descending.Rows[0][0].Null);
        UNIT_ASSERT_VALUES_EQUAL(descending.Rows[1][0].Text, "b");
        UNIT_ASSERT_VALUES_EQUAL(descending.Rows[2][0].Text, "c");
        UNIT_ASSERT_VALUES_EQUAL(descending.Rows[3][0].Text, "a");

        const auto byNote = sql.Exec("SELECT id FROM items WHERE id >= 1 ORDER BY note NULLS FIRST");
        UNIT_ASSERT_C(byNote.Ok, byNote.SqlState + " " + byNote.Message);
        UNIT_ASSERT_VALUES_EQUAL(byNote.AccessPath, "");
        UNIT_ASSERT_VALUES_EQUAL(byNote.Rows.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(byNote.Rows[0][0].Text, "4");
        UNIT_ASSERT_VALUES_EQUAL(byNote.Rows[1][0].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(byNote.Rows[2][0].Text, "3");
        UNIT_ASSERT_VALUES_EQUAL(byNote.Rows[3][0].Text, "2");
    }
}
