#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(InformationSchemaConstraints) {
    Y_UNIT_TEST(PrimaryKeyAndUniqueIndex) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE TABLE items (id int4 PRIMARY KEY, email text, UNIQUE (email))");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);

        const auto constraints = sql.Exec(
            "SELECT constraint_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'items'");
        UNIT_ASSERT_C(constraints.Ok, constraints.SqlState + " " + constraints.Message);
        TVector<std::pair<TString, TString>> kinds;
        for (const auto& row : constraints.Rows) {
            kinds.push_back({row[0].Text, row[1].Text});
        }
        std::sort(kinds.begin(), kinds.end());
        UNIT_ASSERT_VALUES_EQUAL(kinds.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(kinds[0].first, "items_email_key");
        UNIT_ASSERT_VALUES_EQUAL(kinds[0].second, "UNIQUE");
        UNIT_ASSERT_VALUES_EQUAL(kinds[1].first, "items_pkey");
        UNIT_ASSERT_VALUES_EQUAL(kinds[1].second, "PRIMARY KEY");

        const auto usage = sql.Exec(
            "SELECT constraint_name, column_name FROM information_schema.key_column_usage WHERE table_name = 'items'");
        UNIT_ASSERT_C(usage.Ok, usage.SqlState + " " + usage.Message);
        TVector<std::pair<TString, TString>> columns;
        for (const auto& row : usage.Rows) {
            columns.push_back({row[0].Text, row[1].Text});
        }
        std::sort(columns.begin(), columns.end());
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(columns[0].first, "items_email_key");
        UNIT_ASSERT_VALUES_EQUAL(columns[0].second, "email");
        UNIT_ASSERT_VALUES_EQUAL(columns[1].first, "items_pkey");
        UNIT_ASSERT_VALUES_EQUAL(columns[1].second, "id");

        const auto checks = sql.Exec(
            "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'items' AND constraint_type = 'CHECK'");
        UNIT_ASSERT(checks.Ok);
        UNIT_ASSERT(checks.Rows.empty());
    }
}
