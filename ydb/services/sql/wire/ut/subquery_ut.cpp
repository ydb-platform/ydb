#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(Subquery) {
    Y_UNIT_TEST(ScalarAndFromReturnTheInnerRow) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE lefts (id int8 PRIMARY KEY, note text)").Ok, "left");
        UNIT_ASSERT_C(sql.Exec("CREATE TABLE rights (id int8 PRIMARY KEY, note text)").Ok, "right");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO lefts (id, note) VALUES (1, 'one')").Ok, "left row");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO rights (id, note) VALUES (1, 'body')").Ok, "right row");

        const auto scalar = sql.Exec("SELECT id, (SELECT note FROM rights WHERE id = 1) FROM lefts WHERE id = 1");
        UNIT_ASSERT_C(scalar.Ok, scalar.SqlState + " " + scalar.Message);
        UNIT_ASSERT_VALUES_EQUAL(scalar.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(scalar.Rows[0][0].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(scalar.Rows[0][1].Text, "body");

        const auto derived = sql.Exec("SELECT note FROM (SELECT note FROM lefts WHERE id = 1) s");
        UNIT_ASSERT_C(derived.Ok, derived.SqlState + " " + derived.Message);
        UNIT_ASSERT_VALUES_EQUAL(derived.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(derived.Rows[0][0].Text, "one");
    }
}
