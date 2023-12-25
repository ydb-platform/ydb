#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include "login.h"

using namespace NLogin;

Y_UNIT_TEST_SUITE(Login) {
    void none() {}

    Y_UNIT_TEST(TestSuccessfulLogin1) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();
        TLoginProvider::TCreateUserRequest request1;
        request1.User = "user1";
        request1.Password = "password1";
        auto response1 = provider.CreateUser(request1);
        UNIT_ASSERT(!response1.Error);
        TLoginProvider::TLoginUserRequest request2;
        request2.User = request1.User;
        request2.Password = request1.Password;
        auto response2 = provider.LoginUser(request2);
        UNIT_ASSERT_VALUES_EQUAL(response2.Error, "");
        TLoginProvider::TValidateTokenRequest request3;
        request3.Token = response2.Token;
        auto response3 = provider.ValidateToken(request3);
        UNIT_ASSERT_VALUES_EQUAL(response3.Error, "");
        UNIT_ASSERT(response3.User == request1.User);
    }

    Y_UNIT_TEST(TestFailedLogin1) {
        TLoginProvider provider;
        provider.RotateKeys();
        TLoginProvider::TCreateUserRequest request1;
        request1.User = "user1";
        request1.Password = "password1";
        auto response1 = provider.CreateUser(request1);
        UNIT_ASSERT(!response1.Error);
        TLoginProvider::TLoginUserRequest request2;
        request2.User = request1.User;
        request2.Password = "wrong password";
        auto response2 = provider.LoginUser(request2);
        UNIT_ASSERT(response2.Error == "Invalid password");
    }

    Y_UNIT_TEST(TestFailedLogin2) {
        TLoginProvider provider;
        provider.RotateKeys();
        TLoginProvider::TCreateUserRequest request1;
        request1.User = "user1";
        request1.Password = "password1";
        auto response1 = provider.CreateUser(request1);
        UNIT_ASSERT(!response1.Error);
        TLoginProvider::TLoginUserRequest request2;
        request2.User = "wrong user";
        request2.Password = request1.Password;
        auto response2 = provider.LoginUser(request2);
        UNIT_ASSERT(response2.Error == "Invalid user");
    }

    Y_UNIT_TEST(TestFailedLogin3) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();
        TLoginProvider::TCreateUserRequest request1;
        request1.User = "user1";
        request1.Password = "password1";
        auto response1 = provider.CreateUser(request1);
        UNIT_ASSERT(!response1.Error);
        TLoginProvider::TLoginUserRequest request2;
        request2.User = request1.User;
        request2.Password = request1.Password;
        auto response2 = provider.LoginUser(request2);
        UNIT_ASSERT(response2.Error.empty());
        TLoginProvider::TValidateTokenRequest request3;
        request3.Token = response2.Token;
        provider.Audience = "test_audience2";
        auto response3 = provider.ValidateToken(request3);
        UNIT_ASSERT_VALUES_EQUAL(response3.Error, "Wrong audience");
    }

    Y_UNIT_TEST(TestGroups) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();
        {
            auto response1 = provider.CreateUser({.User = "user1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.CreateGroup({.Group = "group1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.CreateGroup({.Group = "group1"});
            UNIT_ASSERT(response1.Error == "Group already exists");
        }
        {
            auto response1 = provider.CreateGroup({.Group = "group2"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.CreateGroup({.Group = "group3"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.CreateGroup({.Group = "group4"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.CreateGroup({.Group = "group5"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group1", .Member = "group2"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group1", .Member = "group3"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group2", .Member = "group4"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group3", .Member = "group5"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group4", .Member = "user1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto groups = provider.GetGroupsMembership("user1");
            UNIT_ASSERT(groups.size() == 3);
            UNIT_ASSERT(Count(groups, "group1") == 1);
            UNIT_ASSERT(Count(groups, "group2") == 1);
            UNIT_ASSERT(Count(groups, "group4") == 1);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group5", .Member = "user1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto groups = provider.GetGroupsMembership("user1");
            UNIT_ASSERT(groups.size() == 5);
            UNIT_ASSERT(Count(groups, "group1") == 1);
            UNIT_ASSERT(Count(groups, "group2") == 1);
            UNIT_ASSERT(Count(groups, "group3") == 1);
            UNIT_ASSERT(Count(groups, "group4") == 1);
            UNIT_ASSERT(Count(groups, "group5") == 1);
        }
        {
            auto response1 = provider.RenameGroup({.Group = "group3", .NewName = "group33"});
            UNIT_ASSERT(!response1.Error);

            auto sids = provider.Sids;
            UNIT_ASSERT(sids.size() == 6);
            UNIT_ASSERT(sids.count("user1") == 1);
            UNIT_ASSERT(sids.count("group1") == 1);
            UNIT_ASSERT(sids.count("group2") == 1);
            UNIT_ASSERT(sids.count("group33") == 1);
            UNIT_ASSERT(sids.count("group4") == 1);
            UNIT_ASSERT(sids.count("group5") == 1);

            auto groups = provider.GetGroupsMembership("user1");
            UNIT_ASSERT(groups.size() == 5);
            UNIT_ASSERT(Count(groups, "group1") == 1);
            UNIT_ASSERT(Count(groups, "group2") == 1);
            UNIT_ASSERT(Count(groups, "group33") == 1);
            UNIT_ASSERT(Count(groups, "group4") == 1);
            UNIT_ASSERT(Count(groups, "group5") == 1);

            groups = provider.GetGroupsMembership("group33");
            UNIT_ASSERT(groups.size() == 1);
            UNIT_ASSERT(Count(groups, "group1") == 1);

            groups = provider.GetGroupsMembership("group5");
            UNIT_ASSERT(groups.size() == 2);
            UNIT_ASSERT(Count(groups, "group33") == 1);
            UNIT_ASSERT(Count(groups, "group1") == 1);

            groups = provider.GetGroupsMembership("group4");
            UNIT_ASSERT(groups.size() == 2);
            UNIT_ASSERT(Count(groups, "group2") == 1);
            UNIT_ASSERT(Count(groups, "group1") == 1);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group2", .Member = {"group4"}});
            UNIT_ASSERT(!response1.Error);
            UNIT_ASSERT(response1.Notice == "Role \"group4\" is already a member of role \"group2\"");
        }
        {
            auto response1 = provider.RemoveGroupMembership({.Group = "group2", .Member = {"group4"}});
            UNIT_ASSERT(!response1.Error);
            UNIT_ASSERT(!response1.Warning);
        }
        {
            auto response1 = provider.RemoveGroupMembership({.Group = "group2", .Member = {"group4"}});
            UNIT_ASSERT(!response1.Error);
            UNIT_ASSERT(response1.Warning == "Role \"group4\" is not a member of role \"group2\"");
        }
        {
            auto groups = provider.GetGroupsMembership("user1");
            UNIT_ASSERT(groups.size() == 4);
            UNIT_ASSERT(Count(groups, "group1") == 1);
            UNIT_ASSERT(Count(groups, "group33") == 1);
            UNIT_ASSERT(Count(groups, "group4") == 1);
            UNIT_ASSERT(Count(groups, "group5") == 1);
        }
        {
            auto response1 = provider.RemoveUser({.User = "user1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto groups = provider.GetGroupsMembership("user1");
            UNIT_ASSERT(groups.empty());
        }
    }

    Y_UNIT_TEST(TestTokenWithGroups) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();
        {
            auto response1 = provider.CreateUser({.User = "user1", .Password = "password1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.CreateGroup({.Group = "group1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.AddGroupMembership({.Group = "group1", .Member = "user1"});
            UNIT_ASSERT(!response1.Error);
        }
        {
            auto response1 = provider.LoginUser({.User = "user1", .Password = "password1", .Options = {.WithUserGroups = true}});
            UNIT_ASSERT(!response1.Error);
            auto response2 = provider.ValidateToken({.Token = response1.Token});
            UNIT_ASSERT(!response2.Error);
            UNIT_ASSERT(response2.Groups);
            UNIT_ASSERT(response2.Groups.value().size() == 1);
            UNIT_ASSERT(response2.Groups.value()[0] == "group1");
        }
    }

    Y_UNIT_TEST(TestCreateTokenForExternalAuth) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();
        TLoginProvider::TCreateUserRequest request1;
        request1.User = "user1";
        request1.Password = "password1";
        auto response1 = provider.CreateUser(request1);
        UNIT_ASSERT(!response1.Error);
        {
            TLoginProvider::TLoginUserRequest request2;
            request2.User = "external_user";
            request2.ExternalAuth = "ldap";
            auto response2 = provider.LoginUser(request2);
            UNIT_ASSERT_VALUES_EQUAL(response2.Error, "");
            TLoginProvider::TValidateTokenRequest request3;
            request3.Token = response2.Token;
            auto response3 = provider.ValidateToken(request3);
            UNIT_ASSERT_VALUES_EQUAL(response3.Error, "");
            UNIT_ASSERT(response3.User == request2.User);
            UNIT_ASSERT(!response3.ExternalAuth.empty());
            UNIT_ASSERT(response3.ExternalAuth == request2.ExternalAuth);
        }
        {
            TLoginProvider::TLoginUserRequest request2;
            request2.User = request1.User;
            request2.Password = request1.Password;
            auto response2 = provider.LoginUser(request2);
            UNIT_ASSERT_VALUES_EQUAL(response2.Error, "");
            TLoginProvider::TValidateTokenRequest request3;
            request3.Token = response2.Token;
            auto response3 = provider.ValidateToken(request3);
            UNIT_ASSERT_VALUES_EQUAL(response3.Error, "");
            UNIT_ASSERT(response3.User == request1.User);
            UNIT_ASSERT(response3.ExternalAuth.empty());
        }
    }
}
