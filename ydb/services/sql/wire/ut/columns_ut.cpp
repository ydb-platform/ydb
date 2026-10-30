#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(InformationSchemaColumns) {
    Y_UNIT_TEST(ColumnMatchesTheTable) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int8 PRIMARY KEY, note text NOT NULL DEFAULT 'hi')");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);

        const auto selected = sql.Exec(
            "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'items'");
        UNIT_ASSERT_C(selected.Ok, selected.SqlState + " " + selected.Message);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][0].Text, "id");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][1].Text, "int8");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][2].Text, "NO");
        UNIT_ASSERT(selected.Rows[0][3].Null);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[1][0].Text, "note");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[1][1].Text, "text");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[1][2].Text, "NO");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[1][3].Text, "hi");

        const auto extra = sql.Exec("SELECT extra FROM information_schema.columns");
        UNIT_ASSERT(!extra.Ok);
        UNIT_ASSERT_VALUES_EQUAL(extra.SqlState, "0A000");
    }
}
