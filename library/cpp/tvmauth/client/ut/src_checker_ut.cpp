#include "common.h"

#include <library/cpp/tvmauth/client/mocked_updater.h>
#include <library/cpp/tvmauth/client/misc/src_checker.h>
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h>

#include <library/cpp/tvmauth/type.h>
#include <library/cpp/tvmauth/unittest.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(SrcChecker) {
    Y_UNIT_TEST(Ctor) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TSrcChecker(new TMockedUpdater),
            TBrokenTvmClientSettings,
            "Need to use TClientSettings::EnableRolesFetching");
    }

    Y_UNIT_TEST(Check) {
        NRoles::TRolesPtr roles = std::make_shared<NRoles::TRoles>(
            NRoles::TRoles::TMeta{},
            NRoles::TRoles::TTvmConsumers{
                {12345, std::make_shared<NRoles::TConsumerRoles>(
                            THashMap<TString, NRoles::TEntitiesPtr>())},
            },
            NRoles::TRoles::TUserConsumers{},
            std::make_shared<TString>());
        const TSrcChecker checker(new TMockedUpdater({.Roles = roles}));

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            checker.Check(NUnittest::CreateServiceTicket(ETicketStatus::Expired, 12345)),
            TIllegalUsage,
            "Service ticket must be valid");

        TCheckedServiceTicket ticket;
        UNIT_ASSERT_NO_EXCEPTION(
            ticket = checker.Check(NUnittest::CreateServiceTicket(ETicketStatus::Ok, 12345)));
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok, ticket.GetStatus());

        UNIT_ASSERT_NO_EXCEPTION(
            ticket = checker.Check(NUnittest::CreateServiceTicket(ETicketStatus::Ok, 9999)));
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::NoRoles, ticket.GetStatus());
    }
}
