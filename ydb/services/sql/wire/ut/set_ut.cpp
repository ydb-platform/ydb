#include "sql_client.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(SessionParameters) {
    Y_UNIT_TEST(ShowReturnsTheValueAfterSetAndEncodingStaysUtf8) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        TSqlClient sql(server, "/" + settings.DomainName);

        const auto initial = sql.Exec("SHOW DateStyle");
        UNIT_ASSERT_C(initial.Ok, initial.SqlState + " " + initial.Message);
        UNIT_ASSERT_VALUES_EQUAL(initial.CommandTag, "SHOW");
        UNIT_ASSERT_VALUES_EQUAL(initial.Rows.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(initial.Rows[0][0].Text, "ISO, MDY");

        const auto changed = sql.Exec("SET DateStyle TO 'ISO, DMY'");
        UNIT_ASSERT_C(changed.Ok, changed.SqlState + " " + changed.Message);
        UNIT_ASSERT_VALUES_EQUAL(changed.CommandTag, "SET");
        UNIT_ASSERT_VALUES_EQUAL(changed.GucName, "DateStyle");
        UNIT_ASSERT_VALUES_EQUAL(changed.GucValue, "ISO, DMY");

        const auto shown = sql.Exec("SHOW DateStyle");
        UNIT_ASSERT_C(shown.Ok, shown.SqlState + " " + shown.Message);
        UNIT_ASSERT_VALUES_EQUAL(shown.Rows[0][0].Text, "ISO, DMY");

        const auto zone = sql.Exec("SET TimeZone = 'UTC'");
        UNIT_ASSERT_C(zone.Ok, zone.SqlState + " " + zone.Message);
        const auto zoneShown = sql.Exec("SHOW TimeZone");
        UNIT_ASSERT_VALUES_EQUAL(zoneShown.Rows[0][0].Text, "UTC");

        const auto strings = sql.Exec("SET standard_conforming_strings TO off");
        UNIT_ASSERT_C(strings.Ok, strings.SqlState + " " + strings.Message);
        const auto stringsShown = sql.Exec("SHOW standard_conforming_strings");
        UNIT_ASSERT_VALUES_EQUAL(stringsShown.Rows[0][0].Text, "off");

        const auto latin = sql.Exec("SET client_encoding TO 'LATIN1'");
        UNIT_ASSERT(!latin.Ok);
        UNIT_ASSERT_VALUES_EQUAL(latin.SqlState, "0A000");
        const auto encoding = sql.Exec("SHOW client_encoding");
        UNIT_ASSERT_C(encoding.Ok, encoding.SqlState + " " + encoding.Message);
        UNIT_ASSERT_VALUES_EQUAL(encoding.Rows[0][0].Text, "UTF8");

        const auto path = sql.Exec("SET search_path TO public");
        UNIT_ASSERT(!path.Ok);
        UNIT_ASSERT_VALUES_EQUAL(path.SqlState, "0A000");
        const auto transaction = sql.Exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        UNIT_ASSERT(!transaction.Ok);
        UNIT_ASSERT_VALUES_EQUAL(transaction.SqlState, "0A000");
    }
}
