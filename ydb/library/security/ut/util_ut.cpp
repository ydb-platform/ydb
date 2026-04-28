
#include <ydb/library/security/util.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(Util) {
    Y_UNIT_TEST(IsQueryWithSensitiveInfo) {
        // false
        UNIT_ASSERT(!NKikimr::IsQueryWithSensitiveInfo("SELECT 1"));
        UNIT_ASSERT(!NKikimr::IsQueryWithSensitiveInfo("CREATE TABLE t (id Int32, PRIMARY KEY(id))"));
        UNIT_ASSERT(!NKikimr::IsQueryWithSensitiveInfo(""));
        UNIT_ASSERT(!NKikimr::IsQueryWithSensitiveInfo("SELECT UserSID, Query FROM `.sys/query_sessions`"));
        UNIT_ASSERT(!NKikimr::IsQueryWithSensitiveInfo("GRANT x TO user1"));
        UNIT_ASSERT(!NKikimr::IsQueryWithSensitiveInfo("CREATE TABLE tableWithSecrets (id Int32, PRIMARY KEY(id))"));

        // true – one marker
        UNIT_ASSERT(NKikimr::IsQueryWithSensitiveInfo("CREATE USER user1 PASSWORD 'p@ss'"));

        // true – two markers
        UNIT_ASSERT(NKikimr::IsQueryWithSensitiveInfo("CReaTE    SECRET x WITH (VALUE = '123')"));
        UNIT_ASSERT(NKikimr::IsQueryWithSensitiveInfo("ALTER\t\n\rSecret x SET VALUE '123'"));

        // true – many lines
        UNIT_ASSERT(NKikimr::IsQueryWithSensitiveInfo(
            "CREATE\n"
            "\t\r secret\n"
            "x\n"
            "WITH (VALUE = '123')"));

        // false positives
        UNIT_ASSERT(NKikimr::IsQueryWithSensitiveInfo("SELECT 1 -- create secret comment"));
        UNIT_ASSERT(NKikimr::IsQueryWithSensitiveInfo("SELECT 1 /* password hint */"));
    }

    Y_UNIT_TEST(ProtectQueryForLoggingIfSensitive) {
        // false
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("SELECT 1"),
            "SELECT 1");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("CREATE TABLE t (id Int32, PRIMARY KEY(id))"),
            "CREATE TABLE t (id Int32, PRIMARY KEY(id))");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive(""),
            "");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("SELECT UserSID, Query FROM `.sys/query_sessions`"),
            "SELECT UserSID, Query FROM `.sys/query_sessions`");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("GRANT x TO user1"),
            "GRANT x TO user1");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("CREATE TABLE tableWithSecrets (id Int32, PRIMARY KEY(id))"),
            "CREATE TABLE tableWithSecrets (id Int32, PRIMARY KEY(id))");

        // true – one marker
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("CREATE USER user1 PASSWORD 'p@ss'"),
            "Query text is hidden due to a sensitive marker: password");

        // true – two markers
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("CReaTE    SECRET x WITH (VALUE = '123')"),
            "Query text is hidden due to a sensitive marker: create secret");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("ALTER\t\n\rSecret x SET VALUE '123'"),
            "Query text is hidden due to a sensitive marker: alter secret");

        // true – many lines
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive(
                "CREATE\n"
                "\t\r secret\n"
                "x\n"
                "WITH (VALUE = '123')"),
            "Query text is hidden due to a sensitive marker: create secret");

        // false positives
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("SELECT 1 -- create secret comment"),
            "Query text is hidden due to a sensitive marker: create secret");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::ProtectQueryForLoggingIfSensitive("SELECT 1 /* password hint */"),
            "Query text is hidden due to a sensitive marker: password");
    }

    Y_UNIT_TEST(MaskTicket) {
        TString ticket = "my_secret_abaabaabaaba";
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::MaskTicket(ticket), "my_s****aaba (47A7C701)");
    }

    Y_UNIT_TEST(MaskIAMTicket) {
        struct TTestCase {
            TString input;
            TString expected;
        };

        const TString hiddenValue = "*** hidden ***";

        std::vector<TTestCase> cases = {
            {
                "",
                ""
            },
            {
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                hiddenValue
            },
            {
                "t1.a",
                hiddenValue
            },
            {
                "t1.a.a.a",
                hiddenValue
            },
            {
                "c1.a.a",
                hiddenValue
            },
            {
                "AaAa****6789",
                hiddenValue
            },
            {
                "t1.9eudmZ2JmpyVkI6PiYrLnJKMlZeby-3rnZmdiYvJkIuYy56cm8aRyM-WjMnl8_cIPXh8-e8PXDYt_d3z90hrdXz57w9cNi39.DEADBEAF",
                "t1.9eudmZ2JmpyVkI6PiYrLnJKMlZeby-3rnZmdiYvJkIuYy56cm8aRyM-WjMnl8_cIPXh8-e8PXDYt_d3z90hrdXz57w9cNi39.**** (28240C80)"
            }
        };

        for (const auto& [input, expected] : cases) {
            auto result = NKikimr::MaskIAMTicket(input);
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }
    }

    Y_UNIT_TEST(SanitizeNebiusTicket) {
        {
            // Right format: ne1 + token + .signature => leave token without signature
            TString ticket = "ne1ABCD0123456789abcd.signature";
            UNIT_ASSERT_VALUES_EQUAL(NKikimr::SanitizeNebiusTicket(ticket), "ne1ABCD0123456789abcd.**");
        }

        {
            // version not recognized
            TString ticket = "ABCD0123456789abcd.signature";
            UNIT_ASSERT_VALUES_EQUAL(NKikimr::SanitizeNebiusTicket(ticket), "ABCD****ture (E1B4EFAB)");
        }

        {
            // version recognized, no signature part
            TString ticket = "ne1ABCD0123456789abcd";
            UNIT_ASSERT_VALUES_EQUAL(NKikimr::SanitizeNebiusTicket(ticket), "ne1A****abcd (A09B232A)");
        }

        {
            // version recognized, no signature part
            TString ticket = "ne1ABCD0123456789abcd.";
            UNIT_ASSERT_VALUES_EQUAL(NKikimr::SanitizeNebiusTicket(ticket), "ne1A****bcd. (95475F6D)");
        }
    }

    Y_UNIT_TEST(MaskNebiusTicket) {
        TString ticket = "ne1ABCD0123456789abcd.signature";
        // try not to show signature
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::MaskNebiusTicket(ticket), "ne1A****abcd (A09B232A)");
    }
}
