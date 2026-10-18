#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(SqlSession) {
    Y_UNIT_TEST(ExecutesWithoutTheProtocol) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;
        TSqlClient sql(server, database);

        const TSqlClient::TResult created = sql.Exec("CREATE SCHEMA app");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);
        UNIT_ASSERT_VALUES_EQUAL(created.CommandTag, "CREATE SCHEMA");

        const TSqlClient::TResult selected = sql.Exec("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'app'");
        UNIT_ASSERT_C(selected.Ok, selected.SqlState + " " + selected.Message);
        UNIT_ASSERT_VALUES_EQUAL(selected.CommandTag, "SELECT 1");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0].size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][0].Text, "app");

        const TSqlClient::TResult rejected = sql.Exec("INSERT INTO app.t (id) VALUES (1)");
        UNIT_ASSERT(!rejected.Ok);
        UNIT_ASSERT_VALUES_EQUAL(rejected.SqlState, "42P01");
    }
}
