#include <library/cpp/tvmauth/client/ut/common.h>

#include <library/cpp/tvmauth/client/exception.h>
#include <library/cpp/tvmauth/client/misc/roles/roles.h>

#include <library/cpp/tvmauth/unittest.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

using namespace NTvmAuth;
using namespace NTvmAuth::NRoles;

Y_UNIT_TEST_SUITE(Roles) {
    Y_UNIT_TEST(EntContains) {
        TEntities ent(CreateEntitiesIndex());

        UNIT_ASSERT(ent.Contains({{"key#1", "value#11"}}));
        UNIT_ASSERT(ent.Contains({
            {"key#1", "value#13"},
            {"key#3", "value#33"},
        }));
        UNIT_ASSERT(!ent.Contains({{"key#111", "value#11"}}));
        UNIT_ASSERT(!ent.Contains({
            {"key#111", "value#13"},
            {"key#3", "value#33"},
        }));

        // valid calls
        {
            std::array<const std::pair<TStringBuf, TString>, 1> arr = {{{"key#1", "value#11"}}};
            UNIT_ASSERT(ent.ContainsSortedUnique<TStringBuf>({arr.begin(), arr.end()}));
        }
        {
            std::array<const std::pair<TString, TStringBuf>, 2> arr = {{
                {"key#1", "value#13"},
                {"key#3", "value#33"},
            }};
            bool res = ent.ContainsSortedUnique<TString, TStringBuf>({arr.begin(), arr.end()});
            UNIT_ASSERT(res);
        }
        {
            std::array<const std::pair<TStringBuf, TStringBuf>, 1> arr = {{{"key#111", "value#11"}}};
            bool res = ent.ContainsSortedUnique<TStringBuf, TStringBuf>({arr.begin(), arr.end()});
            UNIT_ASSERT(!res);
        }
        {
            std::array<const std::pair<TString, TString>, 2> arr = {{
                {"key#111", "value#13"},
                {"key#3", "value#33"},
            }};
            UNIT_ASSERT(!ent.ContainsSortedUnique({arr.begin(), arr.end()}));
        }

        // invalid calls
        {
            std::array<const std::pair<TString, TString>, 2> arr = {{
                {"key#3", "value#33"},
                {"key#1", "value#13"},
            }};
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                ent.ContainsSortedUnique({arr.begin(), arr.end()}),
                TIllegalUsage,
                "attrs are not sorted: 'key#3' before 'key#1'");
        }
        {
            std::array<const std::pair<TString, TString>, 2> arr = {{
                {"key#1", "value#13"},
                {"key#1", "value#13"},
            }};
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                ent.ContainsSortedUnique({arr.begin(), arr.end()}),
                TIllegalUsage,
                "attrs are not unique: 'key#1'");
        }
    }

    Y_UNIT_TEST(EntWithAttrs) {
        TEntities ent(CreateEntitiesIndex());

        UNIT_ASSERT_VALUES_EQUAL(
            ent.GetEntitiesWithAttrs({{"key#1", "value#11"}}),
            std::vector<TEntityPtr>({
                std::make_shared<TEntity>(TEntity{
                    {"key#1", "value#11"},
                }),
                std::make_shared<TEntity>(TEntity{
                    {"key#1", "value#11"},
                    {"key#2", "value#22"},
                    {"key#3", "value#33"},
                }),
                std::make_shared<TEntity>(TEntity{
                    {"key#1", "value#11"},
                    {"key#2", "value#23"},
                    {"key#3", "value#33"},
                }),
            }));
        UNIT_ASSERT_VALUES_EQUAL(
            ent.GetEntitiesWithAttrs({{"key#111", "value#11"}}),
            std::vector<TEntityPtr>());

        // valid calls
        {
            std::array<const std::pair<TStringBuf, TString>, 2> arr = {{
                {"key#1", "value#11"},
                {"key#3", "value#33"},
            }};
            auto vec = ent.GetEntitiesWithSortedUniqueAttrs<TStringBuf>({arr.begin(), arr.end()});
            UNIT_ASSERT_VALUES_EQUAL(
                vec,
                std::vector<TEntityPtr>({
                    std::make_shared<TEntity>(TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#22"},
                        {"key#3", "value#33"},
                    }),
                    std::make_shared<TEntity>(TEntity{
                        {"key#1", "value#11"},
                        {"key#2", "value#23"},
                        {"key#3", "value#33"},
                    }),
                }));
        }
        {
            std::array<const std::pair<TString, TString>, 2> arr = {{
                {"key#111", "value#13"},
                {"key#3", "value#33"},
            }};
            UNIT_ASSERT_VALUES_EQUAL(
                ent.GetEntitiesWithSortedUniqueAttrs({arr.begin(), arr.end()}),
                std::vector<TEntityPtr>());
        }

        // invalid calls
        {
            std::array<const std::pair<TString, TString>, 2> arr = {{
                {"key#3", "value#33"},
                {"key#1", "value#13"},
            }};
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                ent.GetEntitiesWithSortedUniqueAttrs({arr.begin(), arr.end()}),
                TIllegalUsage,
                "attrs are not sorted: 'key#3' before 'key#1'");
        }
        {
            std::array<const std::pair<TString, TString>, 2> arr = {{
                {"key#1", "value#13"},
                {"key#1", "value#13"},
            }};
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                ent.GetEntitiesWithSortedUniqueAttrs({arr.begin(), arr.end()}),
                TIllegalUsage,
                "attrs are not unique: 'key#1'");
        }
    }

    Y_UNIT_TEST(Consumer) {
        TConsumerRoles c({
            {"read", std::make_shared<TEntities>(CreateEntitiesIndex())},
            {"write", std::make_shared<TEntities>(CreateEntitiesIndex())},
        });

        UNIT_ASSERT(c.HasRole("read"));
        UNIT_ASSERT(c.HasRole("write"));
        UNIT_ASSERT(!c.HasRole("access"));

        UNIT_ASSERT_EQUAL(nullptr, c.GetEntitiesForRole("access"));

        TEntitiesPtr ent = c.GetEntitiesForRole("read");
        UNIT_ASSERT_UNEQUAL(nullptr, ent);
        UNIT_ASSERT(ent->Contains({{"key#1", "value#11"}}));
        UNIT_ASSERT(!ent->Contains({{"key#111", "value#11"}}));

        UNIT_ASSERT(c.CheckRoleForExactEntity("read", {{"key#1", "value#11"}}));
        UNIT_ASSERT(!c.CheckRoleForExactEntity("acess", {{"key#1", "value#11"}}));
        UNIT_ASSERT(!c.CheckRoleForExactEntity("read", {{"key#111", "value#11"}}));
        UNIT_ASSERT(!c.CheckRoleForExactEntity("read", {}));
    }

    Y_UNIT_TEST(RolesService) {
        TRoles r(
            {},
            {
                {100500, std::make_shared<TConsumerRoles>(THashMap<TString, TEntitiesPtr>{
                             {"write", std::make_shared<TEntities>(CreateEntitiesIndex())},
                         })},
            },
            {},
            std::make_shared<TString>());

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            r.GetRolesForService(NUnittest::CreateServiceTicket(
                ETicketStatus::InvalidDst,
                100500)),
            TIllegalUsage,
            "Service ticket must be valid, got: InvalidDst");

        TConsumerRolesPtr cons;
        UNIT_ASSERT_NO_EXCEPTION(
            cons = r.GetRolesForService(NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100501)));
        UNIT_ASSERT_EQUAL(nullptr, cons);

        cons = r.GetRolesForService(NUnittest::CreateServiceTicket(
            ETicketStatus::Ok,
            100500));
        UNIT_ASSERT_UNEQUAL(nullptr, cons);
        UNIT_ASSERT(!cons->HasRole("read"));
        UNIT_ASSERT(cons->HasRole("write"));

        ////shortcuts
        // no tvmid
        UNIT_ASSERT(!r.CheckServiceRole(
            NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100501),
            "write"));

        // no role
        UNIT_ASSERT(!r.CheckServiceRole(
            NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100500),
            "read"));

        // success
        UNIT_ASSERT(r.CheckServiceRole(
            NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100500),
            "write"));

        // no tvmid
        UNIT_ASSERT(!r.CheckServiceRoleForExactEntity(
            NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100501),
            "write",
            {{"key#1", "value#11"}}));

        // no role
        UNIT_ASSERT(!r.CheckServiceRoleForExactEntity(
            NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100500),
            "read",
            {{"key#1", "value#11"}}));

        // no entity
        UNIT_ASSERT(!r.CheckServiceRoleForExactEntity(
            NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100500),
            "write",
            {{"key#111", "value#11"}}));

        // success
        UNIT_ASSERT(r.CheckServiceRoleForExactEntity(
            NUnittest::CreateServiceTicket(
                ETicketStatus::Ok,
                100500),
            "write",
            {{"key#1", "value#11"}}));
    }

    Y_UNIT_TEST(RolesUser) {
        TRoles r(
            {},
            {},
            {
                {789654, std::make_shared<TConsumerRoles>(THashMap<TString, TEntitiesPtr>{
                             {"read", std::make_shared<TEntities>(CreateEntitiesIndex())},
                         })},
            },
            std::make_shared<TString>("some roles"));

        UNIT_ASSERT_VALUES_EQUAL("some roles", r.GetRaw());

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            r.GetRolesForUser(NUnittest::CreateUserTicket(
                ETicketStatus::Malformed,
                789654,
                {})),
            TIllegalUsage,
            "User ticket must be valid, got: Malformed");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            r.GetRolesForUser(NUnittest::CreateUserTicket(
                                  ETicketStatus::Ok,
                                  789654,
                                  {}),
                              789123),
            TIllegalUsage,
            "User ticket must be from ProdYateam, got from Test");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            r.GetRolesForUser(NUnittest::CreateUserTicket(
                                  ETicketStatus::Ok,
                                  789654,
                                  {},
                                  {},
                                  EBlackboxEnv::ProdYateam),
                              789123),
            TIllegalUsage,
            "selectedUid must be in user ticket but it's not: 789123");

        TConsumerRolesPtr cons;
        UNIT_ASSERT_NO_EXCEPTION(
            cons = r.GetRolesForUser(NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789123,
                {},
                {},
                EBlackboxEnv::ProdYateam)));
        UNIT_ASSERT_EQUAL(nullptr, cons);

        cons = r.GetRolesForUser(NUnittest::CreateUserTicket(
            ETicketStatus::Ok,
            789654,
            {},
            {},
            EBlackboxEnv::ProdYateam));
        UNIT_ASSERT_UNEQUAL(nullptr, cons);
        UNIT_ASSERT(cons->HasRole("read"));
        UNIT_ASSERT(!cons->HasRole("write"));

        cons = r.GetRolesForUser(NUnittest::CreateUserTicket(
                                     ETicketStatus::Ok,
                                     789123,
                                     {},
                                     {789654, 789741},
                                     EBlackboxEnv::ProdYateam),
                                 789654);
        UNIT_ASSERT_UNEQUAL(nullptr, cons);
        UNIT_ASSERT(cons->HasRole("read"));
        UNIT_ASSERT(!cons->HasRole("write"));

        ////shortcuts
        // no uid
        UNIT_ASSERT(!r.CheckUserRole(
            NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789123,
                {},
                {},
                EBlackboxEnv::ProdYateam),
            "read"));

        // no role
        UNIT_ASSERT(!r.CheckUserRole(
            NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789654,
                {},
                {},
                EBlackboxEnv::ProdYateam),
            "wrire"));

        // success
        UNIT_ASSERT(r.CheckUserRole(
            NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789654,
                {},
                {},
                EBlackboxEnv::ProdYateam),
            "read"));

        // no uid
        UNIT_ASSERT(!r.CheckUserRoleForExactEntity(
            NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789123,
                {},
                {},
                EBlackboxEnv::ProdYateam),
            "read",
            {{"key#1", "value#11"}}));

        // no role
        UNIT_ASSERT(!r.CheckUserRoleForExactEntity(
            NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789654,
                {},
                {},
                EBlackboxEnv::ProdYateam),
            "wrire",
            {{"key#1", "value#11"}}));

        // no entity
        UNIT_ASSERT(!r.CheckUserRoleForExactEntity(
            NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789654,
                {},
                {},
                EBlackboxEnv::ProdYateam),
            "read",
            {{"key#111", "value#11"}}));

        // success
        UNIT_ASSERT(r.CheckUserRoleForExactEntity(
            NUnittest::CreateUserTicket(
                ETicketStatus::Ok,
                789654,
                {},
                {},
                EBlackboxEnv::ProdYateam),
            "read",
            {{"key#1", "value#11"}}));
    }
}
