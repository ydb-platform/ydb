#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(RowLock) {
    Y_UNIT_TEST(ForUpdateBlocksUntilRollback) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;
        TSqlClient holder(server, database);
        TSqlClient writer(server, database);
        TSqlClient reader(server, database);

        UNIT_ASSERT_C(holder.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)").Ok, "create");
        UNIT_ASSERT_C(holder.Exec("INSERT INTO items (id, note) VALUES (1, 'old')").Ok, "insert");

        const auto begun = holder.Exec("BEGIN");
        UNIT_ASSERT_C(begun.Ok, begun.SqlState + " " + begun.Message);
        const auto locked = holder.Exec("SELECT note FROM items WHERE id = 1 FOR UPDATE");
        UNIT_ASSERT_C(locked.Ok, locked.SqlState + " " + locked.Message);
        UNIT_ASSERT_VALUES_EQUAL(locked.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(locked.Rows[0][0].Text, "old");

        UNIT_ASSERT_C(writer.Exec("BEGIN").Ok, "writer begin");
        writer.Post("UPDATE items SET note = 'new' WHERE id = 1");
        const auto saved = server.GetRuntime()->SetDispatchTimeout(TDuration::Seconds(1));
        UNIT_ASSERT(!writer.Take(TDuration::MilliSeconds(300)));
        server.GetRuntime()->SetDispatchTimeout(saved);

        const auto during = reader.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_C(during.Ok, during.SqlState + " " + during.Message);
        UNIT_ASSERT_VALUES_EQUAL(during.Rows[0][0].Text, "old");

        const auto rolled = holder.Exec("ROLLBACK");
        UNIT_ASSERT_C(rolled.Ok, rolled.SqlState + " " + rolled.Message);

        server.GetRuntime()->SetDispatchTimeout(TDuration::Seconds(8));
        const auto updated = writer.Take(TDuration::Seconds(3));
        server.GetRuntime()->SetDispatchTimeout(saved);
        const TString updateState = updated ? updated->SqlState + " " + updated->Message + " " + updated->CommandTag : TString("still waiting");
        UNIT_ASSERT_C(updated && updated->Ok, updateState);
        UNIT_ASSERT_VALUES_EQUAL(updated->CommandTag, "UPDATE 1");

        const auto hidden = reader.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_VALUES_EQUAL(hidden.Rows[0][0].Text, "old");
        UNIT_ASSERT_C(writer.Exec("COMMIT").Ok, "writer commit");
        const auto seen = reader.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_C(seen.Ok, seen.SqlState + " " + seen.Message);
        UNIT_ASSERT_VALUES_EQUAL(seen.Rows[0][0].Text, "new");

        UNIT_ASSERT_C(holder.Exec("BEGIN").Ok, "holder begin again");
        UNIT_ASSERT_C(holder.Exec("SELECT note FROM items WHERE id = 1 FOR UPDATE").Ok, "lock again");
        UNIT_ASSERT_C(writer.Exec("BEGIN").Ok, "writer begin again");
        writer.Post("UPDATE items SET note = 'newer' WHERE id = 1");
        server.GetRuntime()->SetDispatchTimeout(TDuration::Seconds(1));
        UNIT_ASSERT(!writer.Take(TDuration::MilliSeconds(300)));
        server.GetRuntime()->SetDispatchTimeout(saved);
        const auto committed = holder.Exec("COMMIT");
        UNIT_ASSERT_C(committed.Ok, committed.SqlState + " " + committed.Message);
        server.GetRuntime()->SetDispatchTimeout(TDuration::Seconds(8));
        const auto afterCommit = writer.Take(TDuration::Seconds(3));
        server.GetRuntime()->SetDispatchTimeout(saved);
        const TString commitState = afterCommit ? afterCommit->SqlState + " " + afterCommit->Message + " " + afterCommit->CommandTag : TString("still waiting");
        UNIT_ASSERT_C(afterCommit && afterCommit->Ok, commitState);
        UNIT_ASSERT_C(writer.Exec("COMMIT").Ok, "writer commit again");
        const auto newest = reader.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_VALUES_EQUAL(newest.Rows[0][0].Text, "newer");

        UNIT_ASSERT_C(holder.Exec("BEGIN").Ok, "holder begin delete");
        UNIT_ASSERT_C(holder.Exec("SELECT note FROM items WHERE id = 1 FOR UPDATE").Ok, "lock for delete");
        UNIT_ASSERT_C(writer.Exec("BEGIN").Ok, "writer begin delete");
        writer.Post("DELETE FROM items WHERE id = 1");
        server.GetRuntime()->SetDispatchTimeout(TDuration::Seconds(1));
        UNIT_ASSERT(!writer.Take(TDuration::MilliSeconds(300)));
        server.GetRuntime()->SetDispatchTimeout(saved);
        UNIT_ASSERT_C(holder.Exec("ROLLBACK").Ok, "rollback delete lock");
        server.GetRuntime()->SetDispatchTimeout(TDuration::Seconds(8));
        const auto removed = writer.Take(TDuration::Seconds(3));
        server.GetRuntime()->SetDispatchTimeout(saved);
        const TString deleteState = removed ? removed->SqlState + " " + removed->Message + " " + removed->CommandTag : TString("still waiting");
        UNIT_ASSERT_C(removed && removed->Ok, deleteState);
        UNIT_ASSERT_VALUES_EQUAL(removed->CommandTag, "DELETE 1");
        UNIT_ASSERT_C(writer.Exec("COMMIT").Ok, "writer commit delete");
        const auto gone = reader.Exec("SELECT note FROM items WHERE id = 1");
        UNIT_ASSERT_C(gone.Ok, gone.SqlState + " " + gone.Message);
        UNIT_ASSERT_VALUES_EQUAL(gone.Rows.size(), 0u);

        const auto nowait = holder.Exec("SELECT note FROM items WHERE id = 1 FOR UPDATE NOWAIT");
        UNIT_ASSERT(!nowait.Ok);
        UNIT_ASSERT_VALUES_EQUAL(nowait.SqlState, "0A000");
    }
}
