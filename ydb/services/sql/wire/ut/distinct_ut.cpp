#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(Distinct) {
    Y_UNIT_TEST(DropsRepeatedRows) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (1, 'a')").Ok, "1");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (2, 'a')").Ok, "2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO items (id, note) VALUES (3, 'b')").Ok, "3");

        const auto distinct = sql.Exec("SELECT DISTINCT note FROM items WHERE id >= 1");
        UNIT_ASSERT_C(distinct.Ok, distinct.SqlState + " " + distinct.Message);
        UNIT_ASSERT_VALUES_EQUAL(distinct.Rows.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(distinct.Rows[0][0].Text, "a");
        UNIT_ASSERT_VALUES_EQUAL(distinct.Rows[1][0].Text, "b");

        const auto first = sql.Exec("SELECT DISTINCT ON (note) note, id FROM items WHERE id >= 1 ORDER BY note, id");
        UNIT_ASSERT_C(first.Ok, first.SqlState + " " + first.Message);
        UNIT_ASSERT_VALUES_EQUAL(first.Rows.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(first.Rows[0][0].Text, "a");
        UNIT_ASSERT_VALUES_EQUAL(first.Rows[0][1].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(first.Rows[1][0].Text, "b");
        UNIT_ASSERT_VALUES_EQUAL(first.Rows[1][1].Text, "3");
    }
}
