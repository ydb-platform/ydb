#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(UpdateDeleteReturning) {
    Y_UNIT_TEST(UpdateShowsTheNewValueAndDeleteShowsTheOldRow) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (1, 'old')").Ok, "insert");

        const auto updated = sql.Exec("UPDATE items SET note = 'new' WHERE id = 1 RETURNING note");
        UNIT_ASSERT_C(updated.Ok, updated.SqlState + " " + updated.Message);
        UNIT_ASSERT_VALUES_EQUAL(updated.CommandTag, "UPDATE 1");
        UNIT_ASSERT_VALUES_EQUAL(updated.Columns.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(updated.Columns[0].Name, "note");
        UNIT_ASSERT_VALUES_EQUAL(updated.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(updated.Rows[0][0].Text, "new");

        const auto expression = sql.Exec("UPDATE items SET note = 'x' WHERE id = 1 RETURNING note || 'x'");
        UNIT_ASSERT(!expression.Ok);
        UNIT_ASSERT_VALUES_EQUAL(expression.SqlState, "0A000");

        const auto removed = sql.Exec("DELETE FROM items WHERE id = 1 RETURNING id, note");
        UNIT_ASSERT_C(removed.Ok, removed.SqlState + " " + removed.Message);
        UNIT_ASSERT_VALUES_EQUAL(removed.CommandTag, "DELETE 1");
        UNIT_ASSERT_VALUES_EQUAL(removed.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(removed.Rows[0][0].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(removed.Rows[0][1].Text, "new");

        const auto gone = sql.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_C(gone.Ok, gone.SqlState + " " + gone.Message);
        UNIT_ASSERT_VALUES_EQUAL(gone.Rows.size(), 0u);
    }
}
