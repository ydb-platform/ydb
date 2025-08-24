#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <ydb/library/login/password_checker/password_checker.h>
#include <ydb/library/login/account_lockout/account_lockout.h>
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

    Y_UNIT_TEST(TestModifyUser) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();
        TLoginProvider::TCreateUserRequest createUser1Request {
            .User = "user1",
            .Password = "password1"
        };
        auto createUser1Response = provider.CreateUser(createUser1Request);
        UNIT_ASSERT(!createUser1Response.Error);
        TLoginProvider::TLoginUserRequest loginUser1Request1 {
            .User = createUser1Request.User,
            .Password = createUser1Request.Password
        };
        auto loginUser1Response1 = provider.LoginUser(loginUser1Request1);
        UNIT_ASSERT_VALUES_EQUAL(loginUser1Response1.Error, "");
        TLoginProvider::TValidateTokenRequest validateUser1TokenRequest1 {
            .Token = loginUser1Response1.Token
        };
        auto validateUser1TokenResponse1 = provider.ValidateToken(validateUser1TokenRequest1);
        UNIT_ASSERT_VALUES_EQUAL(validateUser1TokenResponse1.Error, "");
        UNIT_ASSERT(validateUser1TokenResponse1.User == createUser1Request.User);

        TPasswordComplexity passwordComplexity({
            .MinLength = 8,
            .MinLowerCaseCount = 2,
            .MinUpperCaseCount = 2,
            .MinNumbersCount = 2,
            .MinSpecialCharsCount = 2
        });

        provider.UpdatePasswordCheckParameters(passwordComplexity);

        TLoginProvider::TModifyUserRequest modifyUser1RequestBad {
            .User = createUser1Request.User,
            .Password = "UserPassword1"
        };

        TLoginProvider::TBasicResponse modifyUser1ResponseBad = provider.ModifyUser(modifyUser1RequestBad);
        UNIT_ASSERT(!modifyUser1ResponseBad.Error.empty());
        UNIT_ASSERT_STRINGS_EQUAL(modifyUser1ResponseBad.Error, "Incorrect password format: should contain at least 2 number, should contain at least 2 special character");

        TLoginProvider::TModifyUserRequest modifyUser1Request {
            .User = createUser1Request.User,
            .Password = "paS*sw1oR#d7"
        };

        TLoginProvider::TBasicResponse modifyUser1Response = provider.ModifyUser(modifyUser1Request);
        UNIT_ASSERT_VALUES_EQUAL(modifyUser1Response.Error, "");

        TLoginProvider::TLoginUserRequest loginUser1Request2  = {
            .User = modifyUser1Request.User,
            .Password = modifyUser1Request.Password.value()
        };
        TLoginProvider::TLoginUserResponse loginUser1Response2 = provider.LoginUser(loginUser1Request2);
        UNIT_ASSERT_VALUES_EQUAL(loginUser1Response2.Error, "");
        TLoginProvider::TValidateTokenRequest validateUser1TokenRequest2  = {
            .Token = loginUser1Response2.Token
        };
        TLoginProvider::TValidateTokenResponse validateUser1TokenResponse2 = provider.ValidateToken(validateUser1TokenRequest2);
        UNIT_ASSERT_VALUES_EQUAL(validateUser1TokenResponse2.Error, "");
        UNIT_ASSERT(validateUser1TokenResponse2.User == createUser1Request.User);
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
            auto response1 = provider.RemoveUser("user1");
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

    Y_UNIT_TEST(SanitizeJwtToken) {
        UNIT_ASSERT_VALUES_EQUAL(TLoginProvider::SanitizeJwtToken("123.456"), "123.**");
        UNIT_ASSERT_VALUES_EQUAL(TLoginProvider::SanitizeJwtToken("123.456.789"), "123.456.**");
        UNIT_ASSERT_VALUES_EQUAL(TLoginProvider::SanitizeJwtToken("token_without_dot"), "");
        UNIT_ASSERT_VALUES_EQUAL(TLoginProvider::SanitizeJwtToken("token_without_signature."), "");
    }

    Y_UNIT_TEST(CheckTimeOfUserCreating) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();

        {
            std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
            TLoginProvider::TCreateUserRequest request {
                .User = "user1",
                .Password = "password1"
            };
            auto response = provider.CreateUser(request);
            std::chrono::time_point<std::chrono::system_clock> finish = std::chrono::system_clock::now();
            UNIT_ASSERT(!response.Error);
            const auto& sid = provider.Sids["user1"];
            UNIT_ASSERT(sid.CreatedAt >= start && sid.CreatedAt <= finish);
        }
        {
            std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
            TLoginProvider::TCreateUserRequest request {
                .User = "user2",
                .Password = "password2"
            };
            auto response = provider.CreateUser(request);
            std::chrono::time_point<std::chrono::system_clock> finish = std::chrono::system_clock::now();
            UNIT_ASSERT(!response.Error);
            const auto& sid = provider.Sids["user2"];
            UNIT_ASSERT(sid.CreatedAt >= start && sid.CreatedAt <= finish);
        }

        {
            const auto& sid1 = provider.Sids["user1"];
            const auto& sid2 = provider.Sids["user2"];
            UNIT_ASSERT(sid1.CreatedAt < sid2.CreatedAt);
        }
    }

    Y_UNIT_TEST(CannotCheckLockoutNonExistentUser) {
        TLoginProvider provider;
        provider.Audience = "test_audience1";
        provider.RotateKeys();

        {
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = "nonExistentUser"});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::INVALID_USER);
            UNIT_ASSERT_VALUES_EQUAL(checkLockoutResponse.Error, "Cannot find user: nonExistentUser");
        }

        {
            auto createGroupResponse = provider.CreateGroup({.Group = "group1"});
            UNIT_ASSERT(!createGroupResponse.Error);

            auto checkLockoutResponse = provider.CheckLockOutUser({.User = "group1"});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::INVALID_USER);
            UNIT_ASSERT_VALUES_EQUAL(checkLockoutResponse.Error, "group1 is a group");
        }
    }

    Y_UNIT_TEST(AccountLockoutAndAutomaticallyUnlock) {
        TAccountLockout::TInitializer accountLockoutInitializer {.AttemptThreshold = 4, .AttemptResetDuration = "3s"};
        TLoginProvider provider(accountLockoutInitializer);
        provider.Audience = "test_audience1";
        provider.RotateKeys();

        TLoginProvider::TCreateUserRequest createUserRequest {
            .User = "user1",
            .Password = "password1"
        };
        auto createUserResponse = provider.CreateUser(createUserRequest);
        UNIT_ASSERT(!createUserResponse.Error);

        {
            for (size_t attempt = 0; attempt < accountLockoutInitializer.AttemptThreshold; attempt++) {
                UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
                auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
                UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
                auto loginUserResponse = provider.LoginUser({.User = createUserRequest.User, .Password = TStringBuilder() << "wrongpassword" << attempt});
                UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD);
                UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "Invalid password");
            }
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), true);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::SUCCESS);
        }

        Sleep(TDuration::Seconds(4));

        {
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::RESET);
            auto loginUserResponse = provider.LoginUser({.User = createUserRequest.User, .Password = TStringBuilder() << "wrongpassword" << accountLockoutInitializer.AttemptThreshold});
            UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD);
            UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "Invalid password");
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
            auto loginUserResponse = provider.LoginUser({.User = createUserRequest.User, .Password = createUserRequest.Password});
            UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "");

            auto validateTokenResponse = provider.ValidateToken({.Token = loginUserResponse.Token});
            UNIT_ASSERT_VALUES_EQUAL(validateTokenResponse.Error, "");
            UNIT_ASSERT(validateTokenResponse.User == createUserRequest.User);
        }
    }

    Y_UNIT_TEST(ResetFailedAttemptCount) {
        TAccountLockout::TInitializer accountLockoutInitializer {.AttemptThreshold = 4, .AttemptResetDuration = "3s"};
        TLoginProvider provider(accountLockoutInitializer);
        provider.Audience = "test_audience1";
        provider.RotateKeys();

        TLoginProvider::TCreateUserRequest createUserRequest {
            .User = "user1",
            .Password = "password1"
        };
        auto createUserResponse = provider.CreateUser(createUserRequest);
        UNIT_ASSERT(!createUserResponse.Error);

        {
            for (size_t attempt = 0; attempt < accountLockoutInitializer.AttemptThreshold - 1; attempt++) {
                UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
                auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
                UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
                auto loginUserResponse = provider.LoginUser({.User = createUserRequest.User, .Password = TStringBuilder() << "wrongpassword" << attempt});
                UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD);
                UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "Invalid password");
            }
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
        }

        Sleep(TDuration::Seconds(4));

        {
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::RESET);
            auto loginUserResponse = provider.LoginUser({.User = createUserRequest.User, .Password = "wrongpassword1"});
            UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD);
            UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "Invalid password");
        }

        {
            for (size_t attempt = 0; attempt < accountLockoutInitializer.AttemptThreshold - 2; attempt++) {
                UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
                auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
                UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
                auto loginUserResponse = provider.LoginUser({.User = createUserRequest.User, .Password = TStringBuilder() << "wrongpassword1" << attempt});
                UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD);
                UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "Invalid password");
            }
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[createUserRequest.User]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = createUserRequest.User});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
            auto loginUserResponse = provider.LoginUser({.User = createUserRequest.User, .Password = createUserRequest.Password});
            UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "");

            auto validateTokenResponse = provider.ValidateToken({.Token = loginUserResponse.Token});
            UNIT_ASSERT_VALUES_EQUAL(validateTokenResponse.Error, "");
            UNIT_ASSERT(validateTokenResponse.User == createUserRequest.User);
        }
    }

    Y_UNIT_TEST(ResetFailedAttemptCountWithAlterUserLogin) {
        TAccountLockout::TInitializer accountLockoutInitializer {.AttemptThreshold = 4, .AttemptResetDuration = "3s"};
        TLoginProvider provider(accountLockoutInitializer);
        provider.Audience = "test_audience1";
        provider.RotateKeys();

        TString userName = "user1";
        TString userPassword = "password1";

        TLoginProvider::TCreateUserRequest createUserRequest {
            .User = userName,
            .Password = userPassword
        };

        auto createUserResponse = provider.CreateUser(createUserRequest);
        UNIT_ASSERT(!createUserResponse.Error);

        for (size_t attempt = 0; attempt < accountLockoutInitializer.AttemptThreshold; attempt++) {
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[userName]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = userName});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
            auto loginUserResponse = provider.LoginUser({.User = userName, .Password = TStringBuilder() << "wrongpassword" << attempt});
            UNIT_ASSERT_EQUAL(loginUserResponse.Status, TLoginProvider::TLoginUserResponse::EStatus::INVALID_PASSWORD);
            UNIT_ASSERT_VALUES_EQUAL(loginUserResponse.Error, "Invalid password");
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[userName]), true);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = userName});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::SUCCESS);
            UNIT_ASSERT_STRING_CONTAINS(checkLockoutResponse.Error, TStringBuilder() << "User " << userName << " is not permitted to log in");
        }

        {
            TLoginProvider::TModifyUserRequest alterRequest;
            alterRequest.User = userName;
            alterRequest.CanLogin = true;
            auto alterResponse = provider.ModifyUser(alterRequest);
            UNIT_ASSERT(!alterResponse.Error);
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(provider.IsLockedOut(provider.Sids[userName]), false);
            auto checkLockoutResponse = provider.CheckLockOutUser({.User = userName});
            UNIT_ASSERT_EQUAL(checkLockoutResponse.Status, TLoginProvider::TCheckLockOutResponse::EStatus::UNLOCKED);
        }
    }

    Y_UNIT_TEST(CreateAlterUserWithHash) {
        TLoginProvider provider;
        provider.RotateKeys();

        {
            TString user = "user1";
            TString password = "password1";
            TString hash = R"(
                {
                    "hash":"ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
                    "salt":"HTkpQjtVJgBoA0CZu+i3zg==",
                    "type":"argon2id"
                }
            )";

            {
                TLoginProvider::TCreateUserRequest createRequest;
                createRequest.User = user;
                createRequest.Password = hash;
                createRequest.IsHashedPassword = true;
                auto createResponse = provider.CreateUser(createRequest);
                UNIT_ASSERT(!createResponse.Error);
            }

            {
                TLoginProvider::TLoginUserRequest loginRequest;
                loginRequest.User = user;
                loginRequest.Password = password;
                auto loginResponse = provider.LoginUser(loginRequest);
                UNIT_ASSERT(!loginResponse.Error);
            }
        }

        {
            TString user = "user2";
            TString hash = R"(
            {
                "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
                "salt": "Not in base64 format =) ",
                "type": "argon2id"
            }
            )";

            {
                TLoginProvider::TCreateUserRequest createRequest;
                createRequest.User = user;
                createRequest.Password = hash;
                createRequest.IsHashedPassword = true;
                auto createResponse = provider.CreateUser(createRequest);
                UNIT_ASSERT_STRING_CONTAINS(createResponse.Error, "Field \'salt\' must be in base64 format");
            }

            {
                TLoginProvider::TLoginUserRequest loginRequest;
                loginRequest.User = user;
                loginRequest.Password = "somePassword";
                auto loginResponse = provider.LoginUser(loginRequest);
                UNIT_ASSERT_STRING_CONTAINS(loginResponse.Error, "Invalid user");

                auto sids = provider.Sids;
                UNIT_ASSERT(!sids.contains(user));
            }
        }

        {
            TString user = "user3";
            TString tempPassword = "password0";
            TString password = "password1";
            TString hash = R"(
                {
                    "hash":"ZO37rNB37kP9hzmKRGfwc4aYrboDt4OBDsF1TBn5oLw=",
                    "salt":"HTkpQjtVJgBoA0CZu+i3zg==",
                    "type":"argon2id"
                }
            )";

            {
                TLoginProvider::TCreateUserRequest createRequest;
                createRequest.User = user;
                createRequest.Password = tempPassword;
                auto createResponse = provider.CreateUser(createRequest);
                UNIT_ASSERT(!createResponse.Error);
            }

            {
                TLoginProvider::TModifyUserRequest alterRequest;
                alterRequest.User = user;
                alterRequest.Password = hash;
                alterRequest.IsHashedPassword = true;
                auto alterResponse = provider.ModifyUser(alterRequest);
                UNIT_ASSERT(!alterResponse.Error);
            }

            {
                TLoginProvider::TLoginUserRequest loginRequest;
                loginRequest.User = user;
                loginRequest.Password = password;
                auto loginResponse = provider.LoginUser(loginRequest);
                UNIT_ASSERT(!loginResponse.Error);
            }

            {
                TLoginProvider::TLoginUserRequest loginRequest;
                loginRequest.User = user;
                loginRequest.Password = tempPassword;
                auto loginResponse = provider.LoginUser(loginRequest);
                UNIT_ASSERT_STRING_CONTAINS(loginResponse.Error, "Invalid password");
            }
        }
    }
}
