#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(IndexScan) {
    Y_UNIT_TEST(SyncIndexReturnsTheRowAndACoveringListSkipsTheTable) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text, extra text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note, extra) VALUES (1, 'hello', 'body')").Ok, "insert");
        const auto index = sql.Exec("CREATE INDEX note_idx ON items (note)");
        UNIT_ASSERT_C(index.Ok, index.SqlState + " " + index.Message);

        const auto scanned = sql.Exec("SELECT extra FROM items WHERE note = 'hello'");
        UNIT_ASSERT_C(scanned.Ok, scanned.SqlState + " " + scanned.Message);
        UNIT_ASSERT_VALUES_EQUAL(scanned.AccessPath, "IndexScan");
        UNIT_ASSERT_VALUES_EQUAL(scanned.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(scanned.Rows[0][0].Text, "body");

        const auto only = sql.Exec("SELECT note FROM items WHERE note = 'hello'");
        UNIT_ASSERT_C(only.Ok, only.SqlState + " " + only.Message);
        UNIT_ASSERT_VALUES_EQUAL(only.AccessPath, "IndexOnlyScan");
        UNIT_ASSERT_VALUES_EQUAL(only.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(only.Rows[0][0].Text, "hello");

        const auto primary = sql.Exec("SELECT extra FROM items WHERE id = 1");
        UNIT_ASSERT_C(primary.Ok, primary.SqlState + " " + primary.Message);
        UNIT_ASSERT_VALUES_EQUAL(primary.AccessPath, "");
        UNIT_ASSERT_VALUES_EQUAL(primary.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(primary.Rows[0][0].Text, "body");

        const auto covered = sql.Exec("CREATE TABLE covered (id int8 PRIMARY KEY, note text, extra text)");
        UNIT_ASSERT_C(covered.Ok, covered.SqlState + " " + covered.Message);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO covered (id, note, extra) VALUES (1, 'hello', 'body')").Ok, "insert covered");
        const auto include = sql.Exec("CREATE INDEX note_idx ON covered (note) INCLUDE (extra)");
        UNIT_ASSERT_C(include.Ok, include.SqlState + " " + include.Message);

        const auto fromIndex = sql.Exec("SELECT extra FROM covered WHERE note = 'hello'");
        UNIT_ASSERT_C(fromIndex.Ok, fromIndex.SqlState + " " + fromIndex.Message);
        UNIT_ASSERT_VALUES_EQUAL(fromIndex.AccessPath, "IndexOnlyScan");
        UNIT_ASSERT_VALUES_EQUAL(fromIndex.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fromIndex.Rows[0][0].Text, "body");
    }
}
