#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(InsertRow) {
    Y_UNIT_TEST(InsertedRowIsReadBack) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);

        const auto inserted = sql.Exec("INSERT INTO items (id, note) VALUES (7, 'hello')");
        UNIT_ASSERT_C(inserted.Ok, inserted.SqlState + " " + inserted.Message);
        UNIT_ASSERT_VALUES_EQUAL(inserted.CommandTag, "INSERT 0 1");

        const auto selected = sql.Exec("SELECT note FROM items WHERE id = 7");
        UNIT_ASSERT_C(selected.Ok, selected.SqlState + " " + selected.Message);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][0].Text, "hello");

        const auto duplicate = sql.Exec("INSERT INTO items (id, note) VALUES (7, 'other')");
        UNIT_ASSERT(!duplicate.Ok);
        UNIT_ASSERT_VALUES_EQUAL(duplicate.SqlState, "23505");

        const auto again = sql.Exec("SELECT note FROM items WHERE id = 7");
        UNIT_ASSERT_C(again.Ok, again.SqlState + " " + again.Message);
        UNIT_ASSERT_VALUES_EQUAL(again.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(again.Rows[0][0].Text, "hello");

        const auto fromSelect = sql.Exec("INSERT INTO items SELECT id, note FROM items");
        UNIT_ASSERT(!fromSelect.Ok);
        UNIT_ASSERT_VALUES_EQUAL(fromSelect.SqlState, "0A000");

        const auto indexed = sql.Exec("CREATE TABLE keyed (id int8 PRIMARY KEY, email text, UNIQUE (email))");
        UNIT_ASSERT_C(indexed.Ok, indexed.SqlState + " " + indexed.Message);
        const auto blocked = sql.Exec("INSERT INTO keyed (id, email) VALUES (1, 'a')");
        UNIT_ASSERT(!blocked.Ok);
        UNIT_ASSERT_VALUES_EQUAL(blocked.SqlState, "0A000");
    }

    Y_UNIT_TEST(OnConflictUpdatesOnlyTheSetColumns) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text, extra text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        const auto inserted = sql.Exec("INSERT INTO items (id, note, extra) VALUES (7, 'hello', 'keep')");
        UNIT_ASSERT_C(inserted.Ok, inserted.SqlState + " " + inserted.Message);

        const auto skipped = sql.Exec("INSERT INTO items (id, note, extra) VALUES (7, 'other', 'gone') ON CONFLICT (id) DO NOTHING");
        UNIT_ASSERT_C(skipped.Ok, skipped.SqlState + " " + skipped.Message);
        UNIT_ASSERT_VALUES_EQUAL(skipped.CommandTag, "INSERT 0 0");

        const auto updated = sql.Exec("INSERT INTO items (id, note, extra) VALUES (7, 'changed', 'gone') ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note");
        UNIT_ASSERT_C(updated.Ok, updated.SqlState + " " + updated.Message);
        UNIT_ASSERT_VALUES_EQUAL(updated.CommandTag, "INSERT 0 1");

        const auto selected = sql.Exec("SELECT note, extra FROM items WHERE id = 7");
        UNIT_ASSERT_C(selected.Ok, selected.SqlState + " " + selected.Message);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][0].Text, "changed");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][1].Text, "keep");

        const auto held = sql.Exec("INSERT INTO items (id, note, extra) VALUES (7, 'no', 'gone') ON CONFLICT (id) DO UPDATE SET note = 'no' WHERE note = 'missing'");
        UNIT_ASSERT_C(held.Ok, held.SqlState + " " + held.Message);
        UNIT_ASSERT_VALUES_EQUAL(held.CommandTag, "INSERT 0 0");
        const auto again = sql.Exec("SELECT note, extra FROM items WHERE id = 7");
        UNIT_ASSERT_C(again.Ok, again.SqlState + " " + again.Message);
        UNIT_ASSERT_VALUES_EQUAL(again.Rows[0][0].Text, "changed");
        UNIT_ASSERT_VALUES_EQUAL(again.Rows[0][1].Text, "keep");

        const auto wrongTarget = sql.Exec("INSERT INTO items (id, note) VALUES (8, 'x') ON CONFLICT (note) DO NOTHING");
        UNIT_ASSERT(!wrongTarget.Ok);
        UNIT_ASSERT_VALUES_EQUAL(wrongTarget.SqlState, "0A000");
    }
}
