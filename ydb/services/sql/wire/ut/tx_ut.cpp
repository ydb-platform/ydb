#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(SqlTransaction) {
    Y_UNIT_TEST(RollbackHidesTheRowAndAnErrorBlocksUntilRollback) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);

        const auto begun = sql.Exec("BEGIN");
        UNIT_ASSERT_C(begun.Ok, begun.SqlState + " " + begun.Message);
        UNIT_ASSERT_VALUES_EQUAL(begun.CommandTag, "BEGIN");
        UNIT_ASSERT_VALUES_EQUAL(begun.TxStatus, 'T');

        const auto hidden = sql.Exec("INSERT INTO items (id, note) VALUES (1, 'hidden')");
        UNIT_ASSERT_C(hidden.Ok, hidden.SqlState + " " + hidden.Message);
        UNIT_ASSERT_VALUES_EQUAL(hidden.TxStatus, 'T');

        const auto rolled = sql.Exec("ROLLBACK");
        UNIT_ASSERT_C(rolled.Ok, rolled.SqlState + " " + rolled.Message);
        UNIT_ASSERT_VALUES_EQUAL(rolled.CommandTag, "ROLLBACK");
        UNIT_ASSERT_VALUES_EQUAL(rolled.TxStatus, 'I');

        const auto missing = sql.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_C(missing.Ok, missing.SqlState + " " + missing.Message);
        UNIT_ASSERT_VALUES_EQUAL(missing.Rows.size(), 0u);

        const auto again = sql.Exec("BEGIN");
        UNIT_ASSERT_C(again.Ok, again.SqlState + " " + again.Message);
        const auto kept = sql.Exec("INSERT INTO items (id, note) VALUES (2, 'kept')");
        UNIT_ASSERT_C(kept.Ok, kept.SqlState + " " + kept.Message);
        const auto committed = sql.Exec("COMMIT");
        UNIT_ASSERT_C(committed.Ok, committed.SqlState + " " + committed.Message);
        UNIT_ASSERT_VALUES_EQUAL(committed.CommandTag, "COMMIT");
        UNIT_ASSERT_VALUES_EQUAL(committed.TxStatus, 'I');

        const auto seen = sql.Exec("SELECT note FROM items WHERE id = 2");
        UNIT_ASSERT_C(seen.Ok, seen.SqlState + " " + seen.Message);
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows[0][0].Text, "kept");

        const auto broken = sql.Exec("BEGIN");
        UNIT_ASSERT_C(broken.Ok, broken.SqlState + " " + broken.Message);
        const auto bad = sql.Exec("INSERT INTO missing (id, note) VALUES (3, 'no')");
        UNIT_ASSERT(!bad.Ok);
        UNIT_ASSERT_VALUES_EQUAL(bad.SqlState, "42P01");
        UNIT_ASSERT_VALUES_EQUAL(bad.TxStatus, 'E');

        const auto blocked = sql.Exec("INSERT INTO items (id, note) VALUES (4, 'later')");
        UNIT_ASSERT(!blocked.Ok);
        UNIT_ASSERT_VALUES_EQUAL(blocked.SqlState, "25P02");
        UNIT_ASSERT_VALUES_EQUAL(blocked.TxStatus, 'E');

        const auto ddl = sql.Exec("ROLLBACK");
        UNIT_ASSERT_C(ddl.Ok, ddl.SqlState + " " + ddl.Message);

        const auto open = sql.Exec("BEGIN");
        UNIT_ASSERT_C(open.Ok, open.SqlState + " " + open.Message);
        const auto scheme = sql.Exec("CREATE TABLE other (id int8 PRIMARY KEY)");
        UNIT_ASSERT(!scheme.Ok);
        UNIT_ASSERT_VALUES_EQUAL(scheme.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(scheme.TxStatus, 'E');
        const auto after = sql.Exec("SELECT note FROM items WHERE id = 2");
        UNIT_ASSERT(!after.Ok);
        UNIT_ASSERT_VALUES_EQUAL(after.SqlState, "25P02");
        const auto done = sql.Exec("ROLLBACK");
        UNIT_ASSERT_C(done.Ok, done.SqlState + " " + done.Message);
        UNIT_ASSERT_VALUES_EQUAL(done.TxStatus, 'I');

        const auto still = sql.Exec("SELECT note FROM items WHERE id = 2");
        UNIT_ASSERT_C(still.Ok, still.SqlState + " " + still.Message);
        UNIT_ASSERT_VALUES_EQUAL(still.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(still.Rows[0][0].Text, "kept");
    }
}
