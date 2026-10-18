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

    Y_UNIT_TEST(AsyncIndexSeesTheRowAfterTheChangeIsApplied) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;

        NKikimrSchemeOp::TTableDescription table;
        table.SetName("items");
        auto* key = table.AddColumns();
        key->SetName("id");
        key->SetType("Uint64");
        key->SetNotNull(true);
        auto* note = table.AddColumns();
        note->SetName("note");
        note->SetType("Uint64");
        auto* extra = table.AddColumns();
        extra->SetName("extra");
        extra->SetType("Utf8");
        table.AddKeyColumnNames("id");
        UNIT_ASSERT_VALUES_EQUAL(client.CreateTableWithUniformShardedIndex(
            database, table, "note_idx", {"note"}, NKikimrSchemeOp::EIndexTypeGlobalAsync, {}), NMsgBusProxy::MSTATUS_OK);

        TSqlClient sql(server, database);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note, extra) VALUES (1, 7, 'body')").Ok, "insert");

        TSqlClient::TResult seen;
        for (int attempt = 0; attempt < 30; ++attempt) {
            seen = sql.Exec("SELECT extra FROM items WHERE note = 7");
            if (seen.Ok && seen.AccessPath == "IndexScan" && seen.Rows.size() == 1) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_C(seen.Ok, seen.SqlState + " " + seen.Message);
        UNIT_ASSERT_VALUES_EQUAL(seen.AccessPath, "IndexScan");
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows[0][0].Text, "body");
    }

    Y_UNIT_TEST(InsertIsVisibleThroughTheIndexAndARepeatedUniqueKeyIsRejected) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text, extra text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        const auto index = sql.Exec("CREATE UNIQUE INDEX note_idx ON items (note)");
        UNIT_ASSERT_C(index.Ok, index.SqlState + " " + index.Message);
        const auto inserted = sql.Exec("INSERT INTO items (id, note, extra) VALUES (1, 'hello', 'body')");
        UNIT_ASSERT_C(inserted.Ok, inserted.SqlState + " " + inserted.Message);
        UNIT_ASSERT_VALUES_EQUAL(inserted.CommandTag, "INSERT 0 1");

        const auto seen = sql.Exec("SELECT extra FROM items WHERE note = 'hello'");
        UNIT_ASSERT_C(seen.Ok, seen.SqlState + " " + seen.Message);
        UNIT_ASSERT_VALUES_EQUAL(seen.AccessPath, "IndexScan");
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows[0][0].Text, "body");

        const auto again = sql.Exec("INSERT INTO items (id, note, extra) VALUES (2, 'hello', 'other')");
        UNIT_ASSERT(!again.Ok);
        UNIT_ASSERT_VALUES_EQUAL(again.SqlState, "23505");
    }

    Y_UNIT_TEST(UpdateMovesTheIndexKeyAndDeleteRemovesIt) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE moved (id int8 PRIMARY KEY, note text, extra text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO moved (id, note, extra) VALUES (1, 'old', 'body')").Ok, "insert");
        const auto index = sql.Exec("CREATE INDEX note_idx ON moved (note)");
        UNIT_ASSERT_C(index.Ok, index.SqlState + " " + index.Message);

        const auto updated = sql.Exec("UPDATE moved SET note = 'new' WHERE id = 1");
        UNIT_ASSERT_C(updated.Ok, updated.SqlState + " " + updated.Message);

        const auto found = sql.Exec("SELECT extra FROM moved WHERE note = 'new'");
        UNIT_ASSERT_C(found.Ok, found.SqlState + " " + found.Message);
        UNIT_ASSERT_VALUES_EQUAL(found.AccessPath, "IndexScan");
        UNIT_ASSERT_VALUES_EQUAL(found.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(found.Rows[0][0].Text, "body");

        const auto gone = sql.Exec("SELECT extra FROM moved WHERE note = 'old'");
        UNIT_ASSERT_C(gone.Ok, gone.SqlState + " " + gone.Message);
        UNIT_ASSERT_VALUES_EQUAL(gone.AccessPath, "IndexScan");
        UNIT_ASSERT_VALUES_EQUAL(gone.Rows.size(), 0u);

        const auto removed = sql.Exec("DELETE FROM moved WHERE id = 1");
        UNIT_ASSERT_C(removed.Ok, removed.SqlState + " " + removed.Message);
        const auto after = sql.Exec("SELECT extra FROM moved WHERE note = 'new'");
        UNIT_ASSERT_C(after.Ok, after.SqlState + " " + after.Message);
        UNIT_ASSERT_VALUES_EQUAL(after.Rows.size(), 0u);
    }

    Y_UNIT_TEST(ConflictOnTheUniqueIndexUpdatesTheExistingPrimaryKey) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text, extra text)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        const auto index = sql.Exec("CREATE UNIQUE INDEX note_idx ON items (note)");
        UNIT_ASSERT_C(index.Ok, index.SqlState + " " + index.Message);
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note, extra) VALUES (1, 'hello', 'body')").Ok, "insert");

        const auto updated = sql.Exec(
            "INSERT INTO items (id, note, extra) VALUES (2, 'hello', 'other') ON CONFLICT (note) DO UPDATE SET extra = EXCLUDED.extra");
        UNIT_ASSERT_C(updated.Ok, updated.SqlState + " " + updated.Message);
        UNIT_ASSERT_VALUES_EQUAL(updated.CommandTag, "INSERT 0 1");

        const auto kept = sql.Exec("SELECT extra FROM items WHERE id = 1");
        UNIT_ASSERT_C(kept.Ok, kept.SqlState + " " + kept.Message);
        UNIT_ASSERT_VALUES_EQUAL(kept.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(kept.Rows[0][0].Text, "other");
        const auto absent = sql.Exec("SELECT extra FROM items WHERE id = 2");
        UNIT_ASSERT_C(absent.Ok, absent.SqlState + " " + absent.Message);
        UNIT_ASSERT_VALUES_EQUAL(absent.Rows.size(), 0u);
        const auto indexed = sql.Exec("SELECT extra FROM items WHERE note = 'hello'");
        UNIT_ASSERT_C(indexed.Ok, indexed.SqlState + " " + indexed.Message);
        UNIT_ASSERT_VALUES_EQUAL(indexed.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(indexed.Rows[0][0].Text, "other");

        const auto skipped = sql.Exec(
            "INSERT INTO items (id, note, extra) VALUES (3, 'hello', 'no') ON CONFLICT (note) DO NOTHING");
        UNIT_ASSERT_C(skipped.Ok, skipped.SqlState + " " + skipped.Message);
        UNIT_ASSERT_VALUES_EQUAL(skipped.CommandTag, "INSERT 0 0");
        const auto still = sql.Exec("SELECT extra FROM items WHERE id = 1");
        UNIT_ASSERT_VALUES_EQUAL(still.Rows[0][0].Text, "other");

        const auto fresh = sql.Exec(
            "INSERT INTO items (id, note, extra) VALUES (4, 'next', 'row') ON CONFLICT (note) DO NOTHING");
        UNIT_ASSERT_C(fresh.Ok, fresh.SqlState + " " + fresh.Message);
        UNIT_ASSERT_VALUES_EQUAL(fresh.CommandTag, "INSERT 0 1");

        const auto mismatch = sql.Exec("INSERT INTO items (id, note) VALUES (5, 'z') ON CONFLICT (extra) DO NOTHING");
        UNIT_ASSERT(!mismatch.Ok);
        UNIT_ASSERT_VALUES_EQUAL(mismatch.SqlState, "42P10");
    }
}
