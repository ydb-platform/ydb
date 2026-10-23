#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(GroupBy) {
    Y_UNIT_TEST(CountAndSumFollowTheGroup) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        UNIT_ASSERT_C(sql.Exec("CREATE TABLE sales (id int8 PRIMARY KEY, kind text, amount int8)").Ok, "create");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO sales (id, kind, amount) VALUES (1, 'a', 10)").Ok, "1");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO sales (id, kind, amount) VALUES (2, 'a', 5)").Ok, "2");
        UNIT_ASSERT_C(sql.Exec("INSERT INTO sales (id, kind, amount) VALUES (3, 'b', 7)").Ok, "3");

        const auto grouped = sql.Exec("SELECT kind, count(*), sum(amount), min(amount), max(amount) FROM sales WHERE id >= 1 GROUP BY kind");
        UNIT_ASSERT_C(grouped.Ok, grouped.SqlState + " " + grouped.Message);
        UNIT_ASSERT_VALUES_EQUAL(grouped.Rows.size(), 2u);
        const TVector<TQueryCell>* alpha = nullptr;
        const TVector<TQueryCell>* beta = nullptr;
        for (const auto& row : grouped.Rows) {
            if (row[0].Text == "a") {
                alpha = &row;
            }
            if (row[0].Text == "b") {
                beta = &row;
            }
        }
        UNIT_ASSERT(alpha && beta);
        UNIT_ASSERT_VALUES_EQUAL((*alpha)[1].Text, "2");
        UNIT_ASSERT_VALUES_EQUAL((*alpha)[2].Text, "15");
        UNIT_ASSERT_VALUES_EQUAL((*alpha)[3].Text, "5");
        UNIT_ASSERT_VALUES_EQUAL((*alpha)[4].Text, "10");
        UNIT_ASSERT_VALUES_EQUAL((*beta)[1].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL((*beta)[2].Text, "7");
        UNIT_ASSERT_VALUES_EQUAL((*beta)[3].Text, "7");
        UNIT_ASSERT_VALUES_EQUAL((*beta)[4].Text, "7");

        const auto total = sql.Exec("SELECT count(*), sum(amount) FROM sales WHERE id >= 1");
        UNIT_ASSERT_C(total.Ok, total.SqlState + " " + total.Message);
        UNIT_ASSERT_VALUES_EQUAL(total.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(total.Rows[0][0].Text, "3");
        UNIT_ASSERT_VALUES_EQUAL(total.Rows[0][1].Text, "22");

        const auto avg = sql.Exec("SELECT avg(amount) FROM sales WHERE id = 1");
        UNIT_ASSERT(!avg.Ok);
        UNIT_ASSERT_VALUES_EQUAL(avg.SqlState, "0A000");
    }
}
