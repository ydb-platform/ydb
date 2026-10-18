#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(Join) {
    Y_UNIT_TEST(InnerJoinReturnsOnlyMatchingRows) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE lefts (id int8 PRIMARY KEY, note text)").Ok, "left");
        UNIT_ASSERT_C(sql.Exec("CREATE TABLE rights (id int8 PRIMARY KEY, extra text)").Ok, "right");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO lefts (id, note) VALUES (1, 'one')").Ok, "insert 1");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO lefts (id, note) VALUES (2, 'two')").Ok, "insert 2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO rights (id, extra) VALUES (1, 'body')").Ok, "insert right");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO rights (id, extra) VALUES (3, 'no')").Ok, "insert other");

        const auto joined = sql.Exec("SELECT lefts.note, rights.extra FROM lefts INNER JOIN rights ON lefts.id = rights.id");
        UNIT_ASSERT_C(joined.Ok, joined.SqlState + " " + joined.Message);
        UNIT_ASSERT_VALUES_EQUAL(joined.CommandTag, "SELECT 1");
        UNIT_ASSERT_VALUES_EQUAL(joined.AccessPath, "HashJoin 1");
        UNIT_ASSERT_VALUES_EQUAL(joined.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(joined.Rows[0][0].Text, "one");
        UNIT_ASSERT_VALUES_EQUAL(joined.Rows[0][1].Text, "body");

        const auto plain = sql.Exec("SELECT a FROM t JOIN u ON t.id = u.id");
        UNIT_ASSERT(!plain.Ok);
        UNIT_ASSERT_VALUES_EQUAL(plain.SqlState, "42P01");
    }
}
