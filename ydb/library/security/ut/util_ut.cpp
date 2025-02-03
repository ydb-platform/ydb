
#include <ydb/library/security/util.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(Util) {
    Y_UNIT_TEST(MaskTicket) {
        TString ticket = "my_secret_abaabaabaaba";
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::MaskTicket(ticket), "my_s****aaba (47A7C701)");
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
