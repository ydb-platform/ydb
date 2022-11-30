#include "common.h"

#include <library/cpp/tvmauth/client/mocked_updater.h>
#include <library/cpp/tvmauth/client/misc/default_uid_checker.h>
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h>

#include <library/cpp/tvmauth/type.h>
#include <library/cpp/tvmauth/unittest.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(DefaultUidChecker) {
    Y_UNIT_TEST(Check) {
        NRoles::TRolesPtr roles = std::make_shared<NRoles::TRoles>(
            NRoles::TRoles::TMeta{},
            NRoles::TRoles::TTvmConsumers{},
            NRoles::TRoles::TUserConsumers{
                {12345, std::make_shared<NRoles::TConsumerRoles>(
                            THashMap<TString, NRoles::TEntitiesPtr>())},
            },
            std::make_shared<TString>());

        TAsyncUpdaterPtr u = new TMockedUpdater({.Roles = roles});
        auto r = u->GetRoles();

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TDefaultUidChecker::Check(NUnittest::CreateUserTicket(ETicketStatus::Expired, 12345, {}), r),
            TIllegalUsage,
            "User ticket must be valid");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TDefaultUidChecker::Check(NUnittest::CreateUserTicket(ETicketStatus::Ok, 12345, {}, {}, EBlackboxEnv::Test), r),
            TIllegalUsage,
            "User ticket must be from ProdYateam, got from Test");

        TCheckedUserTicket ticket;
        UNIT_ASSERT_NO_EXCEPTION(
            ticket = TDefaultUidChecker::Check(NUnittest::CreateUserTicket(ETicketStatus::Ok, 12345, {}, {}, EBlackboxEnv::ProdYateam), r));
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok, ticket.GetStatus());

        UNIT_ASSERT_NO_EXCEPTION(
            ticket = TDefaultUidChecker::Check(NUnittest::CreateUserTicket(ETicketStatus::Ok, 9999, {}, {}, EBlackboxEnv::ProdYateam), r));
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::NoRoles, ticket.GetStatus());
    }
}
