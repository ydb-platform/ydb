#include "common.h" 
 
#include <library/cpp/tvmauth/client/mocked_updater.h> 
#include <library/cpp/tvmauth/client/misc/default_uid_checker.h> 
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h> 
 
#include <library/cpp/tvmauth/type.h> 
#include <library/cpp/tvmauth/unittest.h> 
 
#include <library/cpp/testing/unittest/registar.h> 
 
using namespace NTvmAuth; 
 
Y_UNIT_TEST_SUITE(DefaultUidChecker) { 
    Y_UNIT_TEST(Ctor) { 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDefaultUidChecker(new TMockedUpdater), 
            TBrokenTvmClientSettings, 
            "Need to use TClientSettings::EnableRolesFetching"); 
    } 
 
    Y_UNIT_TEST(Check) { 
        NRoles::TRolesPtr roles = std::make_shared<NRoles::TRoles>( 
            NRoles::TRoles::TMeta{}, 
            NRoles::TRoles::TTvmConsumers{}, 
            NRoles::TRoles::TUserConsumers{ 
                {12345, std::make_shared<NRoles::TConsumerRoles>( 
                            THashMap<TString, NRoles::TEntitiesPtr>())}, 
            }, 
            std::make_shared<TString>()); 
        const TDefaultUidChecker checker(new TMockedUpdater({.Roles = roles})); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            checker.Check(NUnittest::CreateUserTicket(ETicketStatus::Expired, 12345, {})), 
            TIllegalUsage, 
            "User ticket must be valid"); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            checker.Check(NUnittest::CreateUserTicket(ETicketStatus::Ok, 12345, {}, {}, EBlackboxEnv::Test)), 
            TIllegalUsage, 
            "User ticket must be from ProdYateam, got from Test"); 
 
        TCheckedUserTicket ticket; 
        UNIT_ASSERT_NO_EXCEPTION( 
            ticket = checker.Check(NUnittest::CreateUserTicket(ETicketStatus::Ok, 12345, {}, {}, EBlackboxEnv::ProdYateam))); 
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok, ticket.GetStatus()); 
 
        UNIT_ASSERT_NO_EXCEPTION( 
            ticket = checker.Check(NUnittest::CreateUserTicket(ETicketStatus::Ok, 9999, {}, {}, EBlackboxEnv::ProdYateam))); 
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::NoRoles, ticket.GetStatus()); 
    } 
} 
