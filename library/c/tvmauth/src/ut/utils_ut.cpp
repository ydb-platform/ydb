#include <library/c/tvmauth/src/utils.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(UtilsTest) {
    using namespace NTvmAuth;
    using namespace NTvmAuthC::NUtils;

    Y_UNIT_TEST(CppErrorCodeToCTest) {
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::Ok), TA_EC_OK);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::Expired), TA_EC_EXPIRED_TICKET);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::InvalidBlackboxEnv), TA_EC_INVALID_BLACKBOX_ENV);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::InvalidDst), TA_EC_INVALID_DST);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::InvalidTicketType), TA_EC_INVALID_TICKET_TYPE);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::Malformed), TA_EC_MALFORMED_TICKET);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::MissingKey), TA_EC_MISSING_KEY);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::SignBroken), TA_EC_SIGN_BROKEN);
        UNIT_ASSERT_EQUAL(CppErrorCodeToC(ETicketStatus::UnsupportedVersion), TA_EC_UNSUPPORTED_VERSION);
    }
}
