#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(ExplainPlan) {
    Y_UNIT_TEST(PointSelectNamesKeysAndOneShard) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;
        TSqlClient sql(server, database);

        const auto created = sql.Exec("CREATE TABLE explained (id int8 PRIMARY KEY, qty int8)");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        const auto inserted = sql.Exec("INSERT INTO explained (id, qty) VALUES (1, 7)");
        UNIT_ASSERT_C(inserted.Ok, inserted.SqlState + " " + inserted.Message);

        const auto explained = sql.Exec("EXPLAIN SELECT qty FROM explained WHERE id = 1");
        UNIT_ASSERT_C(explained.Ok, explained.SqlState + " " + explained.Message);
        UNIT_ASSERT_VALUES_EQUAL(explained.CommandTag, "EXPLAIN");
        UNIT_ASSERT_VALUES_EQUAL(explained.Columns.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(explained.Columns[0].Name, "QUERY PLAN");
        UNIT_ASSERT_VALUES_EQUAL(explained.Rows.size(), 1u);
        UNIT_ASSERT(explained.Rows[0][0].Text.Contains("Keys"));
        UNIT_ASSERT(explained.Rows[0][0].Text.Contains("Shards 1"));

        const auto selected = sql.Exec("SELECT qty FROM explained WHERE id = 1");
        UNIT_ASSERT_C(selected.Ok, selected.SqlState + " " + selected.Message);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][0].Text, "7");

        const auto analyze = sql.Exec("EXPLAIN ANALYZE SELECT qty FROM explained WHERE id = 1");
        UNIT_ASSERT(!analyze.Ok);
        UNIT_ASSERT_VALUES_EQUAL(analyze.SqlState, "0A000");

        const auto still = sql.Exec("SELECT qty FROM explained WHERE id = 1");
        UNIT_ASSERT_C(still.Ok, still.SqlState + " " + still.Message);
        UNIT_ASSERT_VALUES_EQUAL(still.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(still.Rows[0][0].Text, "7");
    }
}
