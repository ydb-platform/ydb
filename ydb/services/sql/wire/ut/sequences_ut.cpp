#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(InformationSchemaSequences) {
    Y_UNIT_TEST(SequenceMatchesTheDescription) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto created = sql.Exec("CREATE SEQUENCE seq AS integer INCREMENT BY 2 MINVALUE 1 MAXVALUE 20 START WITH 5 CYCLE");
        UNIT_ASSERT_C(created.Ok, created.SqlState + " " + created.Message);

        const auto selected = sql.Exec(
            "SELECT data_type, numeric_precision, start_value, minimum_value, maximum_value, increment, cycle_option "
            "FROM information_schema.sequences WHERE sequence_name = 'seq'");
        UNIT_ASSERT_C(selected.Ok, selected.SqlState + " " + selected.Message);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][0].Text, "int4");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][1].Text, "32");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][2].Text, "5");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][3].Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][4].Text, "20");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][5].Text, "2");
        UNIT_ASSERT_VALUES_EQUAL(selected.Rows[0][6].Text, "YES");

        const auto current = sql.Exec("SELECT last_value FROM information_schema.sequences");
        UNIT_ASSERT(!current.Ok);
        UNIT_ASSERT_VALUES_EQUAL(current.SqlState, "0A000");
    }
}
