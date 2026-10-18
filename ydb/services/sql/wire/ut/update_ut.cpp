#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(UpdateRow) {
    Y_UNIT_TEST(UpdateChangesTheColumnAndASyncIndexStays) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (1, 'old')").Ok, "insert");

        const auto updated = sql.Exec("UPDATE items SET note = 'new' WHERE id = 1");
        UNIT_ASSERT_C(updated.Ok, updated.SqlState + " " + updated.Message);
        UNIT_ASSERT_VALUES_EQUAL(updated.CommandTag, "UPDATE 1");

        const auto seen = sql.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_C(seen.Ok, seen.SqlState + " " + seen.Message);
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows[0][0].Text, "new");

        const auto indexed = sql.Exec("CREATE TABLE kept (id int8 PRIMARY KEY, note text)");
        UNIT_ASSERT_C(indexed.Ok, indexed.SqlState + " " + indexed.Message);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO kept (id, note) VALUES (1, 'stay')").Ok, "insert kept");
        const auto index = sql.Exec("CREATE INDEX note_idx ON kept (note)");
        UNIT_ASSERT_C(index.Ok, index.SqlState + " " + index.Message);

        const auto blocked = sql.Exec("UPDATE kept SET note = 'changed' WHERE id = 1");
        UNIT_ASSERT(!blocked.Ok);
        UNIT_ASSERT_VALUES_EQUAL(blocked.SqlState, "0A000");

        const auto still = sql.Exec("SELECT note FROM kept WHERE id = 1");
        UNIT_ASSERT_C(still.Ok, still.SqlState + " " + still.Message);
        UNIT_ASSERT_VALUES_EQUAL(still.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(still.Rows[0][0].Text, "stay");
    }
}
