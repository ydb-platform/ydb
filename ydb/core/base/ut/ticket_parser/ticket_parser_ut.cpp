#include <ydb/core/base/ticket_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <type_traits>
#include <utility>

namespace NKikimr {

using TAuthorizeTicketResult = TEvTicketParser::TEvAuthorizeTicketResult;
static_assert(std::is_const_v<decltype(TAuthorizeTicketResult::Ticket)>);
static_assert(std::is_same_v<decltype(std::declval<const TAuthorizeTicketResult&>().GetError()), const TEvTicketParser::TError&>);
static_assert(std::is_same_v<decltype(std::declval<const TAuthorizeTicketResult&>().GetToken()), TIntrusiveConstPtr<NACLib::TUserToken>>);

Y_UNIT_TEST_SUITE(TAuthorizeTicketResult) {
    Y_UNIT_TEST(NullTokenIsError) {
        TEvTicketParser::TEvAuthorizeTicketResult result("ticket", nullptr);

        UNIT_ASSERT(result.HasError());
        UNIT_ASSERT(!result.GetError().empty());
        UNIT_ASSERT_EXCEPTION(result.GetToken(), std::bad_variant_access);
    }

    Y_UNIT_TEST(SuccessContainsToken) {
        const auto token = MakeIntrusive<NACLib::TUserToken>("user", TVector<TString>{});
        TEvTicketParser::TEvAuthorizeTicketResult result("ticket", token);

        UNIT_ASSERT(!result.HasError());
        UNIT_ASSERT(result.GetToken() == token);
    }

    Y_UNIT_TEST(TokenCanOutliveResult) {
        TIntrusiveConstPtr<NACLib::TUserToken> token;
        {
            TEvTicketParser::TEvAuthorizeTicketResult result(
                "ticket", MakeIntrusive<NACLib::TUserToken>("user", TVector<TString>{}));

            UNIT_ASSERT(!result.HasError());
            token = result.GetToken();
            UNIT_ASSERT(token == result.GetToken());
        }

        UNIT_ASSERT_VALUES_EQUAL(token->GetUserSID(), "user");
    }

    Y_UNIT_TEST(ErrorContainsNoToken) {
        TEvTicketParser::TEvAuthorizeTicketResult result("ticket", TEvTicketParser::TError{.Message = "denied", .Retryable = false});

        UNIT_ASSERT(result.HasError());
        UNIT_ASSERT_EXCEPTION(result.GetToken(), std::bad_variant_access);
        UNIT_ASSERT_VALUES_EQUAL(result.GetError().Message, "denied");
        UNIT_ASSERT(!result.GetError().Retryable);
    }

    Y_UNIT_TEST(EmptyErrorRemainsError) {
        for (const bool retryable : {false, true}) {
            TEvTicketParser::TEvAuthorizeTicketResult result("ticket", TEvTicketParser::TError{.Retryable = retryable});

            UNIT_ASSERT(result.HasError());
            UNIT_ASSERT(result.GetError().empty());
            UNIT_ASSERT_VALUES_EQUAL(result.GetError().Retryable, retryable);
            UNIT_ASSERT_EXCEPTION(result.GetToken(), std::bad_variant_access);
        }
    }
}

} // namespace NKikimr
