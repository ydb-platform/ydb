#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(Union) {
    Y_UNIT_TEST(AllKeepsTheRepeatedRow) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE lefts (id int8 PRIMARY KEY, note text)").Ok, "left");
        UNIT_ASSERT_C(sql.Exec("CREATE TABLE rights (id int8 PRIMARY KEY, note text)").Ok, "right");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO lefts (id, note) VALUES (1, 'a')").Ok, "a");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO lefts (id, note) VALUES (2, 'b')").Ok, "b");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO rights (id, note) VALUES (1, 'b')").Ok, "b2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO rights (id, note) VALUES (2, 'c')").Ok, "c");

        const auto all = sql.Exec("SELECT note FROM lefts WHERE id >= 1 UNION ALL SELECT note FROM rights WHERE id >= 1");
        UNIT_ASSERT_C(all.Ok, all.SqlState + " " + all.Message);
        UNIT_ASSERT_VALUES_EQUAL(all.CommandTag, "SELECT 4");

        const auto once = sql.Exec("SELECT note FROM lefts WHERE id >= 1 UNION SELECT note FROM rights WHERE id >= 1");
        UNIT_ASSERT_C(once.Ok, once.SqlState + " " + once.Message);
        UNIT_ASSERT_VALUES_EQUAL(once.CommandTag, "SELECT 3");
        UNIT_ASSERT(all.Rows.size() > once.Rows.size());
    }

    Y_UNIT_TEST(IntersectKeepsOnlyTheSharedRow) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE lefts (id int8 PRIMARY KEY, note text)").Ok, "left");
        UNIT_ASSERT_C(sql.Exec("CREATE TABLE rights (id int8 PRIMARY KEY, note text)").Ok, "right");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO lefts (id, note) VALUES (1, 'a')").Ok, "a");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO lefts (id, note) VALUES (2, 'b')").Ok, "b");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO rights (id, note) VALUES (1, 'b')").Ok, "b2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO rights (id, note) VALUES (2, 'c')").Ok, "c");

        const auto shared = sql.Exec("SELECT note FROM lefts WHERE id >= 1 INTERSECT SELECT note FROM rights WHERE id >= 1");
        UNIT_ASSERT_C(shared.Ok, shared.SqlState + " " + shared.Message);
        UNIT_ASSERT_VALUES_EQUAL(shared.CommandTag, "SELECT 1");
        UNIT_ASSERT_VALUES_EQUAL(shared.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(shared.Rows[0][0].Text, "b");
    }
}
