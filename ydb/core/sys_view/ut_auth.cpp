#include "ut_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void SetupAuthEnvironment(TTestEnv& env) {
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);
    CreateTenantsAndTables(env, true);
}

void SetupAuthAccessEnvironment(TTestEnv& env) {
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back("root@builtin");
    env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back("user1rootadmin");
    env.GetServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableDatabaseAdmin(true);
    env.GetClient().SetSecurityToken("root@builtin");
    CreateTenantsAndTables(env, true);

    env.GetClient().CreateUser("/Root", "user1rootadmin", "password1");
    env.GetClient().CreateUser("/Root", "user2", "password2");
    env.GetClient().CreateUser("/Root/Tenant1", "user3", "password3");
    env.GetClient().CreateUser("/Root/Tenant1", "user4", "password4");
    env.GetClient().CreateUser("/Root/Tenant2", "user5", "password5");

    // Note: in real scenarios user6tenant1admin should be created in /Root/Tenant1
    // but it isn't supported by test framework
    env.GetClient().CreateUser("/Root", "user6tenant1admin", "password6");
    env.GetClient().ModifyOwner("/Root", "Tenant1", "user6tenant1admin");

    {
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1rootadmin");
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user2");
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user6tenant1admin");
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, "root@builtin");
        env.GetClient().ModifyACL("", "Root", acl.SerializeAsString());
    }
}

void CheckAuthAdministratorAccessIsRequired(TScanQueryPartIterator& it) {
    NKqp::StreamResultToYson(it, false, EStatus::UNAUTHORIZED,
        "Administrator access is required");
}

void CheckEmpty(TScanQueryPartIterator& it) {
    auto expected = R"([

    ])";
    NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
}

} // namespace

Y_UNIT_TEST_SUITE(AuthSystemView) {

    Y_UNIT_TEST(AuthUsers) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root/Tenant1", "group2");
        env.GetClient().CreateGroup("/Root/Tenant2", "group3");
        env.GetClient().CreateGroup("/Root/Tenant2", "group4");

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, LastSuccessfulAttemptAt, LastFailedAttemptAt, FailedAttemptCount
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"];[%true];[%false];#;#;[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PasswordHash
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto actual = NKqp::StreamResultToYson(it);
            UNIT_ASSERT_STRING_CONTAINS(actual, "hash");
            UNIT_ASSERT_STRING_CONTAINS(actual, "salt");
            UNIT_ASSERT_STRING_CONTAINS(actual, "type");
            UNIT_ASSERT_STRING_CONTAINS(actual, "argon2id");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, LastSuccessfulAttemptAt, LastFailedAttemptAt, FailedAttemptCount
                FROM `Root/Tenant1/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user2"];[%true];[%false];#;#;[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, LastSuccessfulAttemptAt, LastFailedAttemptAt, FailedAttemptCount
                FROM `Root/Tenant2/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user3"];[%true];[%false];#;#;[0u]];
                [["user4"];[%true];[%false];#;#;[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthUsers_LockUnlock) {
        NKikimrProto::TAuthConfig authConfig;
        auto accountLockout = authConfig.MutableAccountLockout();
        accountLockout->SetAttemptResetDuration("3s");
        TTestEnv env(1, 4, {.AuthConfig = authConfig});
        SetupAuthEnvironment(env);

        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, LastSuccessfulAttemptAt, LastFailedAttemptAt, FailedAttemptCount
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"];[%true];[%false];#;#;[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }


        {
            auto loginResult = env.GetClient().Login(*(env.GetServer().GetRuntime()), "user1", "password1");
            UNIT_ASSERT_EQUAL(loginResult.GetError(), "");
        }

        {
            for (size_t i = 0; i < 4; i++) {
                auto loginResult = env.GetClient().Login(*(env.GetServer().GetRuntime()), "user1", "wrongPassword");
                UNIT_ASSERT_EQUAL(loginResult.GetError(), "Invalid password");
            }
        }

        // After some attempts login with wrong password user must be locked out. Flag IsLockedOut must be true
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, FailedAttemptCount
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"];[%true];[%true];[4u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        // Check that user is locked out and cannot login
        {
            auto loginResult = env.GetClient().Login(*(env.GetServer().GetRuntime()), "user1", "password1");
            UNIT_ASSERT_EQUAL(loginResult.GetError(), "User user1 login denied: too many failed password attempts");
        }

        Sleep(TDuration::Seconds(5));

        // User can login after 5 seconds. Flag IsLockedOut is false
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, FailedAttemptCount
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"];[%true];[%false];[4u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        // User can login
        {
            auto loginResult = env.GetClient().Login(*(env.GetServer().GetRuntime()), "user1", "password1");
            UNIT_ASSERT_EQUAL(loginResult.GetError(), "");
        }

        // Check that FailedAttemptCount is reset
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, IsEnabled, IsLockedOut, FailedAttemptCount
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"];[%true];[%false];[0u]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthUsers_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        { // anonymous login doesn't give administrative access as `AdministrationAllowedSIDs` isn't empty
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }
        }

        { // user1rootadmin is /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user1rootadmin"]];
                    [["user2"]];
                    [["user6tenant1admin"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user3"]];
                    [["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user5"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // user2 isn't /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user2",
                    .Password = "password2",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }
        }

        { // user6tenant1admin is /Root/Tenant1 admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user6tenant1admin",
                    .Password = "password6",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user6tenant1admin"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_users`
                )").GetValueSync();

                auto expected = R"([
                    [["user3"]];
                    [["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_users`
                )").GetValueSync();

                CheckEmpty(it);
            }
        }
    }

    Y_UNIT_TEST(AuthUsers_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto user : {
            "user3",
            "user1",
            "user2",
            "user",
            "user33",
            "user21",
            "user22",
            "userrr",
            "u",
            "asdf",
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT Sid
            FROM `Root/.sys/auth_users`
        )").GetValueSync();

        auto expected = R"([
            [["asdf"]];
            [["u"]];
            [["user"]];
            [["user1"]];
            [["user2"]];
            [["user21"]];
            [["user22"]];
            [["user3"]];
            [["user33"]];
            [["userrr"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthUsers_TableRange) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto user : {
            "user1",
            "user2",
            "user3",
            "user4"
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
            )").GetValueSync();

            auto expected = R"([
                [["user1"]];
                [["user2"]];
                [["user3"]];
                [["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid >= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["user2"]];
                [["user3"]];
                [["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid > "user2"
            )").GetValueSync();

            auto expected = R"([
                [["user3"]];
                [["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid <= "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user1"]];
                [["user2"]];
                [["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid < "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user1"]];
                [["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid > "user1" AND Sid <= "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user2"]];
                [["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_users`
                WHERE Sid >= "user2" AND Sid < "user3"
            )").GetValueSync();

            auto expected = R"([
                [["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroups) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root/Tenant1", "group2");
        env.GetClient().CreateGroup("/Root/Tenant2", "group3");
        env.GetClient().CreateGroup("/Root/Tenant2", "group4");

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_groups`
            )").GetValueSync();

            auto expected = R"([
                [["group1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant1/.sys/auth_groups`
            )").GetValueSync();

            auto expected = R"([
                [["group2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant2/.sys/auth_groups`
            )").GetValueSync();

            auto expected = R"([
                [["group3"]];
                [["group4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroups_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root", "group2");
        env.GetClient().CreateGroup("/Root/Tenant1", "group3");
        env.GetClient().CreateGroup("/Root/Tenant1", "group4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group5");

        { // anonymous login doesn't give administrative access as `AdministrationAllowedSIDs` isn't empty
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user1rootadmin is /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group1"]];
                    [["group2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"]];
                    [["group4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group5"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // user2 isn't /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user2",
                    .Password = "password2",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user6tenant1admin is /Root/Tenant1 admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user6tenant1admin",
                    .Password = "password6",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant1/.sys/auth_groups`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"]];
                    [["group4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT Sid
                    FROM `Root/Tenant2/.sys/auth_groups`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }
    }

    Y_UNIT_TEST(AuthGroups_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group3",
            "group1",
            "group2",
            "group",
            "group33",
            "group21",
            "group22",
            "grouprr",
            "g",
            "asdf",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `Root/.sys/auth_groups`
        )").GetValueSync();

        auto expected = R"([
            [["asdf"]];
            [["g"]];
            [["group"]];
            [["group1"]];
            [["group2"]];
            [["group21"]];
            [["group22"]];
            [["group3"]];
            [["group33"]];
            [["grouprr"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthGroups_TableRange) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group1",
            "group2",
            "group3",
            "group4",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid
                FROM `Root/.sys/auth_groups`
                WHERE Sid > "group1" AND Sid <= "group3"
            )").GetValueSync();

            auto expected = R"([
                [["group2"]];
                [["group3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroupMembers) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateUser("/Root", "user1", "password1");
        env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
        env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
        env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root/Tenant1", "group2");
        env.GetClient().CreateGroup("/Root/Tenant2", "group3");
        env.GetClient().CreateGroup("/Root/Tenant2", "group4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group5");

        env.GetClient().AddGroupMembership("/Root", "group1", "user1");
        env.GetClient().AddGroupMembership("/Root/Tenant1", "group2", "user2");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group3", "user4");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "user3");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "user4");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "group3");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group4", "group4");

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant1/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/Tenant2/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group3"];["user4"]];
                [["group4"];["group3"]];
                [["group4"];["group4"]];
                [["group4"];["user3"]];
                [["group4"];["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthGroupMembers_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);
        TTableClient client(env.GetDriver());

        env.GetClient().CreateGroup("/Root", "group1");
        env.GetClient().CreateGroup("/Root", "group2");
        env.GetClient().CreateGroup("/Root/Tenant1", "group3");
        env.GetClient().CreateGroup("/Root/Tenant1", "group4");
        env.GetClient().CreateGroup("/Root/Tenant2", "group5");

        env.GetClient().AddGroupMembership("/Root", "group1", "user1rootadmin");
        env.GetClient().AddGroupMembership("/Root", "group2", "user2");
        env.GetClient().AddGroupMembership("/Root/Tenant1", "group3", "user3");
        env.GetClient().AddGroupMembership("/Root/Tenant1", "group4", "user4");
        env.GetClient().AddGroupMembership("/Root/Tenant2", "group5", "user5");

        { // anonymous login doesn't give administrative access as `AdministrationAllowedSIDs` isn't empty
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint());
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user1rootadmin is /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group1"];["user1rootadmin"]];
                    [["group2"];["user2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"];["user3"]];
                    [["group4"];["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group5"];["user5"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // user2 isn't /Root admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user2",
                    .Password = "password2",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }

        { // user6tenant1admin is /Root/Tenant1 admin
            auto driverConfig = TDriverConfig()
                .SetEndpoint(env.GetEndpoint())
                .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user6tenant1admin",
                    .Password = "password6",
                }));
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant1/.sys/auth_group_members`
                )").GetValueSync();

                auto expected = R"([
                    [["group3"];["user3"]];
                    [["group4"];["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `Root/Tenant2/.sys/auth_group_members`
                )").GetValueSync();

                CheckAuthAdministratorAccessIsRequired(it);
            }
        }
    }

    Y_UNIT_TEST(AuthGroupMembers_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group3",
            "group1",
            "group2",
            "group",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        for (auto user : {
            "user1",
            "user2",
            "user"
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        for (auto membership : TVector<std::pair<TString, TString>>{
            {"group3", "user1"},
            {"group3", "user2"},
            {"group2", "user"},
            {"group2", "user1"},
            {"group2", "user2"},
            {"group", "user2"},
        }) {
            env.GetClient().AddGroupMembership("/Root", membership.first, membership.second);
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `Root/.sys/auth_group_members`
        )").GetValueSync();

        auto expected = R"([
            [["group"];["user2"]];
            [["group2"];["user"]];
            [["group2"];["user1"]];
            [["group2"];["user2"]];
            [["group3"];["user1"]];
            [["group3"];["user2"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(AuthGroupMembers_TableRange) {
        TTestEnv env;
        SetupAuthEnvironment(env);
        TTableClient client(env.GetDriver());

        for (auto group : {
            "group1",
            "group2",
            "group3",
        }) {
            env.GetClient().CreateGroup("/Root", group);
        }

        for (auto user : {
            "user1",
            "user2",
            "user3"
        }) {
            env.GetClient().CreateUser("/Root", user, "password");
        }

        for (auto membership : TVector<std::pair<TString, TString>>{
            {"group1", "user1"},
            {"group1", "user2"},
            {"group2", "user1"},
            {"group2", "user2"},
            {"group2", "user3"},
            {"group3", "user1"},
            {"group3", "user2"},
        }) {
            env.GetClient().AddGroupMembership("/Root", membership.first, membership.second);
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
                [["group1"];["user2"]];
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid > "group1" AND GroupSid <= "group3"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid >= "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid > "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group3"];["user1"]];
                [["group3"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid <= "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
                [["group1"];["user2"]];
                [["group2"];["user1"]];
                [["group2"];["user2"]];
                [["group2"];["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid < "group2"
            )").GetValueSync();

            auto expected = R"([
                [["group1"];["user1"]];
                [["group1"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid >= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user2"]];
                [["group2"];["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid > "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user3"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid <= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
                [["group2"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `Root/.sys/auth_group_members`
                WHERE GroupSid = "group2" AND MemberSid < "user2"
            )").GetValueSync();

            auto expected = R"([
                [["group2"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST_TWIN(AuthOwners, EnableRealSystemViewPaths) {
        TTestEnv env({ .EnableRealSystemViewPaths = EnableRealSystemViewPaths });
        SetupAuthEnvironment(env);

        {
            env.GetClient().CreateUser("/Root", "user1", "password1");
            env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
            env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
            env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
            env.GetClient().CreateGroup("/Root/Tenant2", "group1");

            env.GetClient().MkDir("/Root", "Dir1/SubDir1");
            env.GetClient().ModifyOwner("/Root", "Dir1", "user1");
            env.GetClient().ModifyOwner("/Root/Dir1", "SubDir1", "user1");

            env.GetClient().MkDir("/Root/Tenant1", "Dir2/SubDir2");
            env.GetClient().ModifyOwner("/Root/Tenant1", "Dir2", "user2");
            env.GetClient().ModifyOwner("/Root/Tenant1/Dir2", "SubDir2", "user2");

            env.GetClient().MkDir("/Root/Tenant2", "Dir3/SubDir33");
            env.GetClient().MkDir("/Root/Tenant2", "Dir3/SubDir34");
            env.GetClient().MkDir("/Root/Tenant2", "Dir4/SubDir45");
            env.GetClient().MkDir("/Root/Tenant2", "Dir4/SubDir46");
            env.GetClient().ModifyOwner("/Root/Tenant2", "Dir3", "user3");
            env.GetClient().ModifyOwner("/Root/Tenant2", "Dir4", "user4");
            env.GetClient().ModifyOwner("/Root/Tenant2/Dir3", "SubDir33", "group1");
            env.GetClient().ModifyOwner("/Root/Tenant2/Dir4", "SubDir46", "user4");
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);

        {
            driverConfig.SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
            )").GetValueSync();

            TString expectedYson;
            if (EnableRealSystemViewPaths) {
                expectedYson = R"([
                    [["/Root"];["root@builtin"]];
                    [["/Root/.metadata"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/.sys"];["metadata@system"]];[["/Root/.sys/auth_effective_permissions"];["metadata@system"]];
                    [["/Root/.sys/auth_group_members"];["metadata@system"]];
                    [["/Root/.sys/auth_groups"];["metadata@system"]];
                    [["/Root/.sys/auth_owners"];["metadata@system"]];
                    [["/Root/.sys/auth_permissions"];["metadata@system"]];
                    [["/Root/.sys/auth_users"];["metadata@system"]];
                    [["/Root/.sys/compile_cache_queries"];["metadata@system"]];
                    [["/Root/.sys/ds_groups"];["metadata@system"]];
                    [["/Root/.sys/ds_pdisks"];["metadata@system"]];
                    [["/Root/.sys/ds_storage_pools"];["metadata@system"]];
                    [["/Root/.sys/ds_storage_stats"];["metadata@system"]];
                    [["/Root/.sys/ds_vslots"];["metadata@system"]];
                    [["/Root/.sys/hive_tablets"];["metadata@system"]];
                    [["/Root/.sys/nodes"];["metadata@system"]];
                    [["/Root/.sys/partition_stats"];["metadata@system"]];
                    [["/Root/.sys/pg_class"];["metadata@system"]];
                    [["/Root/.sys/pg_tables"];["metadata@system"]];
                    [["/Root/.sys/query_metrics_one_minute"];["metadata@system"]];
                    [["/Root/.sys/query_sessions"];["metadata@system"]];
                    [["/Root/.sys/resource_pool_classifiers"];["metadata@system"]];
                    [["/Root/.sys/resource_pools"];["metadata@system"]];
                    [["/Root/.sys/streaming_queries"];["metadata@system"]];
                    [["/Root/.sys/tables"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_by_tli_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_by_tli_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_cpu_time_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_cpu_time_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_duration_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_duration_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_read_bytes_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_read_bytes_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_request_units_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_request_units_one_minute"];["metadata@system"]];
                    [["/Root/Dir1"];["user1"]];
                    [["/Root/Dir1/SubDir1"];["user1"]];
                    [["/Root/Table0"];["root@builtin"]];
                ])";
            } else {
                expectedYson = R"([
                    [["/Root"];["root@builtin"]];
                    [["/Root/.metadata"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/Dir1"];["user1"]];
                    [["/Root/Dir1/SubDir1"];["user1"]];
                    [["/Root/Table0"];["root@builtin"]];
                ])";
            }

            NKqp::CompareYson(expectedYson, NKqp::StreamResultToYson(it));
        }

        {
            driverConfig.SetDatabase("/Root/Tenant1");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/Tenant1/.sys/auth_owners`
            )").GetValueSync();

            TString expectedYson;
            if (EnableRealSystemViewPaths) {
                expectedYson = R"([
                    [["/Root/Tenant1"];["root@builtin"]];
                    [["/Root/Tenant1/.metadata"];["metadata@system"]];
                    [["/Root/Tenant1/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/Tenant1/.sys"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/auth_effective_permissions"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/auth_group_members"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/auth_groups"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/auth_owners"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/auth_permissions"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/auth_users"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/compile_cache_queries"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/nodes"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/partition_stats"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/pg_class"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/pg_tables"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/query_metrics_one_minute"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/query_sessions"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/resource_pool_classifiers"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/resource_pools"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/streaming_queries"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/tables"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_partitions_by_tli_one_hour"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_partitions_by_tli_one_minute"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_partitions_one_hour"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_partitions_one_minute"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_cpu_time_one_hour"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_cpu_time_one_minute"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_duration_one_hour"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_duration_one_minute"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_read_bytes_one_hour"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_request_units_one_hour"];["metadata@system"]];
                    [["/Root/Tenant1/.sys/top_queries_by_request_units_one_minute"];["metadata@system"]];
                    [["/Root/Tenant1/Dir2"];["user2"]];[["/Root/Tenant1/Dir2/SubDir2"];["user2"]];
                    [["/Root/Tenant1/Table1"];["root@builtin"]];
                ])";
            } else {
                expectedYson = R"([
                    [["/Root/Tenant1"];["root@builtin"]];
                    [["/Root/Tenant1/.metadata"];["metadata@system"]];
                    [["/Root/Tenant1/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/Tenant1/Dir2"];["user2"]];
                    [["/Root/Tenant1/Dir2/SubDir2"];["user2"]];
                    [["/Root/Tenant1/Table1"];["root@builtin"]];
                ])";
            }

            NKqp::CompareYson(expectedYson, NKqp::StreamResultToYson(it));
        }

        {
            driverConfig.SetDatabase("/Root/Tenant2");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/Tenant2/.sys/auth_owners`
            )").GetValueSync();

            TString expectedYson;
            if (EnableRealSystemViewPaths) {
                expectedYson = R"([
                    [["/Root/Tenant2"];["root@builtin"]];
                    [["/Root/Tenant2/.metadata"];["metadata@system"]];
                    [["/Root/Tenant2/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/Tenant2/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/Tenant2/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/Tenant2/.sys"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/auth_effective_permissions"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/auth_group_members"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/auth_groups"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/auth_owners"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/auth_permissions"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/auth_users"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/compile_cache_queries"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/nodes"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/partition_stats"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/pg_class"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/pg_tables"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/query_metrics_one_minute"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/query_sessions"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/resource_pool_classifiers"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/resource_pools"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/streaming_queries"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/tables"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_partitions_by_tli_one_hour"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_partitions_by_tli_one_minute"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_partitions_one_hour"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_partitions_one_minute"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_cpu_time_one_hour"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_cpu_time_one_minute"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_duration_one_hour"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_duration_one_minute"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_read_bytes_one_hour"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_read_bytes_one_minute"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_request_units_one_hour"];["metadata@system"]];
                    [["/Root/Tenant2/.sys/top_queries_by_request_units_one_minute"];["metadata@system"]];
                    [["/Root/Tenant2/Dir3"];["user3"]];
                    [["/Root/Tenant2/Dir3/SubDir33"];["group1"]];
                    [["/Root/Tenant2/Dir3/SubDir34"];["root@builtin"]];
                    [["/Root/Tenant2/Dir4"];["user4"]];
                    [["/Root/Tenant2/Dir4/SubDir45"];["root@builtin"]];
                    [["/Root/Tenant2/Dir4/SubDir46"];["user4"]];
                    [["/Root/Tenant2/Table2"];["root@builtin"]];
                ])";
            } else {
                expectedYson = R"([
                    [["/Root/Tenant2"];["root@builtin"]];
                    [["/Root/Tenant2/.metadata"];["metadata@system"]];
                    [["/Root/Tenant2/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/Tenant2/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/Tenant2/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/Tenant2/Dir3"];["user3"]];
                    [["/Root/Tenant2/Dir3/SubDir33"];["group1"]];
                    [["/Root/Tenant2/Dir3/SubDir34"];["root@builtin"]];
                    [["/Root/Tenant2/Dir4"];["user4"]];
                    [["/Root/Tenant2/Dir4/SubDir45"];["root@builtin"]];
                    [["/Root/Tenant2/Dir4/SubDir46"];["user4"]];
                    [["/Root/Tenant2/Table2"];["root@builtin"]];
                ])";
            }

            NKqp::CompareYson(expectedYson, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthOwners_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);

        {
            env.GetClient().MkDir("/Root", "Dir1");
            env.GetClient().MkDir("/Root", "Dir2");
            env.GetClient().MkDir("/Root/Tenant1", "Dir3");
            env.GetClient().MkDir("/Root/Tenant1", "Dir4");
            env.GetClient().ModifyOwner("/Root", "Dir1", "user1rootadmin");
            env.GetClient().ModifyOwner("/Root/Tenant1", "Dir3", "user3");
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);

        { // anonymous login gives `ydb.granular.describe_schema` access
            driverConfig.SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system entries
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/Dir1"];["user1rootadmin"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Table0"];["root@builtin"]]
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        { // user1rootadmin has /Root GenericUse access
            driverConfig.SetCredentialsProviderFactory(
                NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                })
            );

            {
                driverConfig.SetDatabase("/Root");
                auto driver = TDriver(driverConfig);

                TTableClient client(driver);
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `/Root/.sys/auth_owners`
                    WHERE Path NOT LIKE "%/.sys%"    -- not list system entries
                    AND Path NOT LIKE "%/.metadata%"
                )").GetValueSync();

                auto expected = R"([
                    [["/Root"];["root@builtin"]];
                    [["/Root/Dir1"];["user1rootadmin"]];
                    [["/Root/Dir2"];["root@builtin"]];
                    [["/Root/Table0"];["root@builtin"]]
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                driverConfig.SetDatabase("/Root/Tenant1");
                auto driver = TDriver(driverConfig);

                TTableClient client(driver);
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `/Root/Tenant1/.sys/auth_owners`
                    WHERE Path NOT LIKE "%/.sys%"    -- not list system entries
                    AND Path NOT LIKE "%/.metadata%"
                )").GetValueSync();

                auto expected = R"([
                    [["/Root/Tenant1"];["user6tenant1admin"]];
                    [["/Root/Tenant1/Dir3"];["user3"]];
                    [["/Root/Tenant1/Dir4"];["root@builtin"]];
                    [["/Root/Tenant1/Table1"];["root@builtin"]]
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // revoke user1rootadmin /Root/Dir2 GenericUse access
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "user1rootadmin");
            env.GetClient().ModifyACL("/Root", "Dir2", acl.SerializeAsString());

            driverConfig.SetCredentialsProviderFactory(
                NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                })
            );
            driverConfig.SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system entries
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/Dir1"];["user1rootadmin"]];
                [["/Root/Table0"];["root@builtin"]]
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthOwners_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);

        {
            for (auto path : {
                "Dir2/SubDir2",
                "Dir1/SubDir1",
                "Dir2/SubDir1",
                "Dir1/SubDir2",
                "Dir2/SubDir3",
                "Dir1/SubDir3",
                "Dir11/SubDir",
                "Dir/SubDir",
            }) {
                env.GetClient().MkDir("/Root", path);
            }
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT *
            FROM `/Root/.sys/auth_owners`
            WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
            AND Path NOT LIKE "%/.metadata%"
        )").GetValueSync();

        auto expected = R"([
            [["/Root"];["root@builtin"]];
            [["/Root/Dir"];["root@builtin"]];
            [["/Root/Dir/SubDir"];["root@builtin"]];
            [["/Root/Dir1"];["root@builtin"]];
            [["/Root/Dir1/SubDir1"];["root@builtin"]];
            [["/Root/Dir1/SubDir2"];["root@builtin"]];
            [["/Root/Dir1/SubDir3"];["root@builtin"]];
            [["/Root/Dir11"];["root@builtin"]];
            [["/Root/Dir11/SubDir"];["root@builtin"]];
            [["/Root/Dir2"];["root@builtin"]];
            [["/Root/Dir2/SubDir1"];["root@builtin"]];
            [["/Root/Dir2/SubDir2"];["root@builtin"]];
            [["/Root/Dir2/SubDir3"];["root@builtin"]];
            [["/Root/Table0"];["root@builtin"]]
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST_TWIN(AuthOwners_TableRange, EnableRealSystemViewPaths) {
        TTestEnv env({ .EnableRealSystemViewPaths = EnableRealSystemViewPaths });
        SetupAuthEnvironment(env);

        {
            for (auto path : {
                "Dir0/SubDir0",
                "Dir0/SubDir1",
                "Dir0/SubDir2",
                "Dir1/SubDir0",
                "Dir1/SubDir1",
                "Dir1/SubDir2",
                "Dir2/SubDir0",
                "Dir2/SubDir1",
                "Dir2/SubDir2",
                "Dir3/SubDir0",
                "Dir3/SubDir1",
                "Dir3/SubDir2",
            }) {
                env.GetClient().MkDir("/Root", path);
            }

            env.GetClient().CreateUser("/Root", "user0", "password0");
            env.GetClient().CreateUser("/Root", "user1", "password1");
            env.GetClient().CreateUser("/Root", "user2", "password2");
            env.GetClient().ModifyOwner("/Root/Dir1", "SubDir0", "user0");
            env.GetClient().ModifyOwner("/Root/Dir1", "SubDir1", "user1");
            env.GetClient().ModifyOwner("/Root/Dir1", "SubDir2", "user2");
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
            )").GetValueSync();

            TString expectedYson;
            if (EnableRealSystemViewPaths) {
                expectedYson = R"([
                    [["/Root"];["root@builtin"]];
                    [["/Root/.metadata"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/.sys"];["metadata@system"]];[["/Root/.sys/auth_effective_permissions"];["metadata@system"]];
                    [["/Root/.sys/auth_group_members"];["metadata@system"]];
                    [["/Root/.sys/auth_groups"];["metadata@system"]];
                    [["/Root/.sys/auth_owners"];["metadata@system"]];
                    [["/Root/.sys/auth_permissions"];["metadata@system"]];
                    [["/Root/.sys/auth_users"];["metadata@system"]];
                    [["/Root/.sys/compile_cache_queries"];["metadata@system"]];
                    [["/Root/.sys/ds_groups"];["metadata@system"]];
                    [["/Root/.sys/ds_pdisks"];["metadata@system"]];
                    [["/Root/.sys/ds_storage_pools"];["metadata@system"]];
                    [["/Root/.sys/ds_storage_stats"];["metadata@system"]];
                    [["/Root/.sys/ds_vslots"];["metadata@system"]];
                    [["/Root/.sys/hive_tablets"];["metadata@system"]];
                    [["/Root/.sys/nodes"];["metadata@system"]];
                    [["/Root/.sys/partition_stats"];["metadata@system"]];
                    [["/Root/.sys/pg_class"];["metadata@system"]];
                    [["/Root/.sys/pg_tables"];["metadata@system"]];
                    [["/Root/.sys/query_metrics_one_minute"];["metadata@system"]];
                    [["/Root/.sys/query_sessions"];["metadata@system"]];
                    [["/Root/.sys/resource_pool_classifiers"];["metadata@system"]];
                    [["/Root/.sys/resource_pools"];["metadata@system"]];
                    [["/Root/.sys/streaming_queries"];["metadata@system"]];
                    [["/Root/.sys/tables"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_by_tli_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_by_tli_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_partitions_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_cpu_time_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_cpu_time_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_duration_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_duration_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_read_bytes_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_read_bytes_one_minute"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_request_units_one_hour"];["metadata@system"]];
                    [["/Root/.sys/top_queries_by_request_units_one_minute"];["metadata@system"]];
                    [["/Root/Dir0"];["root@builtin"]];
                    [["/Root/Dir0/SubDir0"];["root@builtin"]];
                    [["/Root/Dir0/SubDir1"];["root@builtin"]];
                    [["/Root/Dir0/SubDir2"];["root@builtin"]];
                    [["/Root/Dir1"];["root@builtin"]];
                    [["/Root/Dir1/SubDir0"];["user0"]];
                    [["/Root/Dir1/SubDir1"];["user1"]];
                    [["/Root/Dir1/SubDir2"];["user2"]];
                    [["/Root/Dir2"];["root@builtin"]];
                    [["/Root/Dir2/SubDir0"];["root@builtin"]];
                    [["/Root/Dir2/SubDir1"];["root@builtin"]];
                    [["/Root/Dir2/SubDir2"];["root@builtin"]];
                    [["/Root/Dir3"];["root@builtin"]];
                    [["/Root/Dir3/SubDir0"];["root@builtin"]];
                    [["/Root/Dir3/SubDir1"];["root@builtin"]];
                    [["/Root/Dir3/SubDir2"];["root@builtin"]];
                    [["/Root/Table0"];["root@builtin"]];
                ])";
            } else {
                expectedYson = R"([
                    [["/Root"];["root@builtin"]];
                    [["/Root/.metadata"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools"];["metadata@system"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["metadata@system"]];
                    [["/Root/Dir0"];["root@builtin"]];
                    [["/Root/Dir0/SubDir0"];["root@builtin"]];
                    [["/Root/Dir0/SubDir1"];["root@builtin"]];
                    [["/Root/Dir0/SubDir2"];["root@builtin"]];
                    [["/Root/Dir1"];["root@builtin"]];
                    [["/Root/Dir1/SubDir0"];["user0"]];
                    [["/Root/Dir1/SubDir1"];["user1"]];
                    [["/Root/Dir1/SubDir2"];["user2"]];
                    [["/Root/Dir2"];["root@builtin"]];
                    [["/Root/Dir2/SubDir0"];["root@builtin"]];
                    [["/Root/Dir2/SubDir1"];["root@builtin"]];
                    [["/Root/Dir2/SubDir2"];["root@builtin"]];
                    [["/Root/Dir3"];["root@builtin"]];
                    [["/Root/Dir3/SubDir0"];["root@builtin"]];
                    [["/Root/Dir3/SubDir1"];["root@builtin"]];
                    [["/Root/Dir3/SubDir2"];["root@builtin"]];
                    [["/Root/Table0"];["root@builtin"]];
                ])";
            }

            NKqp::CompareYson(expectedYson, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system entries
                AND Path NOT LIKE "%/.metadata%"
                AND Path >= "/A" AND Path <= "/Z"
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["root@builtin"]];
                [["/Root/Dir0"];["root@builtin"]];
                [["/Root/Dir0/SubDir0"];["root@builtin"]];
                [["/Root/Dir0/SubDir1"];["root@builtin"]];
                [["/Root/Dir0/SubDir2"];["root@builtin"]];
                [["/Root/Dir1"];["root@builtin"]];
                [["/Root/Dir1/SubDir0"];["user0"]];
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
                [["/Root/Dir2/SubDir1"];["root@builtin"]];
                [["/Root/Dir2/SubDir2"];["root@builtin"]];
                [["/Root/Dir3"];["root@builtin"]];
                [["/Root/Dir3/SubDir0"];["root@builtin"]];
                [["/Root/Dir3/SubDir1"];["root@builtin"]];
                [["/Root/Dir3/SubDir2"];["root@builtin"]];
                [["/Root/Table0"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path >= "/Root/Dir1" AND Path < "/Root/Dir3"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1"];["root@builtin"]];
                [["/Root/Dir1/SubDir0"];["user0"]];
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
                [["/Root/Dir2/SubDir1"];["root@builtin"]];
                [["/Root/Dir2/SubDir2"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path >= "/Root/Dir1/SubDir1" AND Path <= "/Root/Dir2/SubDir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
                [["/Root/Dir2/SubDir1"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path > "/Root/Dir1/SubDir1" AND Path < "/Root/Dir2/SubDir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir2"];["user2"]];
                [["/Root/Dir2"];["root@builtin"]];
                [["/Root/Dir2/SubDir0"];["root@builtin"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path >= "/Root/Dir1/SubDir0" AND Sid >= "user1" AND Path < "/Root/Dir2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
                [["/Root/Dir1/SubDir2"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid > "user0"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid < "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid >= "user1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid <= "user1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid > "user1"
            )").GetValueSync();

            auto expected = R"([

            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid < "user1"
            )").GetValueSync();

            auto expected = R"([

            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Sid, Path
                FROM `/Root/.sys/auth_owners`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid >= "user1"
            )").GetValueSync();

            auto expected = R"([
                [["user1"];["/Root/Dir1/SubDir1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions) {
        TTestEnv env;
        SetupAuthEnvironment(env);

        {
            env.GetClient().CreateUser("/Root", "user1", "password1");
            env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");
            env.GetClient().CreateUser("/Root/Tenant2", "user3", "password3");
            env.GetClient().CreateUser("/Root/Tenant2", "user4", "password4");
            env.GetClient().CreateGroup("/Root/Tenant2", "group1");

            env.GetClient().MkDir("/Root", "Dir1/SubDir1");
            env.GetClient().MkDir("/Root/Tenant1", "Dir2/SubDir2");
            env.GetClient().MkDir("/Root/Tenant2", "Dir3/SubDir3");
            env.GetClient().MkDir("/Root/Tenant2", "Dir4/SubDir4");
        }

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            env.GetClient().ModifyACL("/", "Root", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1");
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user1");
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Deny, NACLib::UpdateRow, "user1");
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user2");
            env.GetClient().ModifyACL("/Root", "Tenant1", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root/Tenant1/Dir2", "SubDir2", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user3");
            env.GetClient().ModifyACL("/Root", "Tenant2", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root/Tenant2", "Dir3", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user4");
            env.GetClient().ModifyACL("/Root/Tenant2/Dir4", "SubDir4", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "group1");
            env.GetClient().ModifyACL("/Root/Tenant2", "Dir4", acl.SerializeAsString());
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);

        {
            driverConfig.SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["ydb.generic.use"];["user1"]];
                [["/Root/Dir1"];["ydb.generic.use"];["user1"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user1"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user1"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            driverConfig.SetDatabase("/Root/Tenant1");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/Tenant1/.sys/auth_permissions`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Tenant1"];["ydb.generic.use"];["user2"]];
                [["/Root/Tenant1/Dir2/SubDir2"];["ydb.generic.use"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            driverConfig.SetDatabase("/Root/Tenant2");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/Tenant2/.sys/auth_permissions`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Tenant2"];["ydb.generic.use"];["user3"]];
                [["/Root/Tenant2/Dir3"];["ydb.generic.use"];["user3"]];
                [["/Root/Tenant2/Dir4"];["ydb.generic.use"];["group1"]];
                [["/Root/Tenant2/Dir4/SubDir4"];["ydb.generic.use"];["user4"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions_Access) {
        TTestEnv env;
        SetupAuthAccessEnvironment(env);

        {
            env.GetClient().MkDir("/Root", "Dir1");
            env.GetClient().MkDir("/Root", "Dir2");
            env.GetClient().MkDir("/Root/Tenant1", "Dir3");
            env.GetClient().MkDir("/Root/Tenant1", "Dir4");
        }

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1rootadmin");
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user2");
            env.GetClient().ModifyACL("/Root", "Dir2", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user3");
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user4");
            env.GetClient().ModifyACL("/Root/Tenant1", "Dir3", acl.SerializeAsString());
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);

        { // anonymous login gives `ydb.granular.describe_schema` access
            driverConfig.SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["ydb.generic.full"];["root@builtin"]];
                [["/Root"];["ydb.generic.use"];["user1rootadmin"]];
                [["/Root"];["ydb.generic.use"];["user2"]];
                [["/Root"];["ydb.generic.use"];["user6tenant1admin"]];
                [["/Root/Dir1"];["ydb.granular.select_row"];["user1rootadmin"]];
                [["/Root/Dir2"];["ydb.granular.erase_row"];["user2"]];
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        { // user1rootadmin has /Root GenericUse access
            driverConfig.SetCredentialsProviderFactory(
                NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                })
            );

            {
                driverConfig.SetDatabase("/Root");
                auto driver = TDriver(driverConfig);

                TTableClient client(driver);
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `/Root/.sys/auth_permissions`
                    WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                    AND Path NOT LIKE "%/.metadata%"
                )").GetValueSync();

                auto expected = R"([
                    [["/Root"];["ydb.generic.full"];["root@builtin"]];
                    [["/Root"];["ydb.generic.use"];["user1rootadmin"]];
                    [["/Root"];["ydb.generic.use"];["user2"]];
                    [["/Root"];["ydb.generic.use"];["user6tenant1admin"]];
                    [["/Root/Dir1"];["ydb.granular.select_row"];["user1rootadmin"]];
                    [["/Root/Dir2"];["ydb.granular.erase_row"];["user2"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }

            {
                driverConfig.SetDatabase("/Root/Tenant1");
                auto driver = TDriver(driverConfig);

                TTableClient client(driver);
                auto it = client.StreamExecuteScanQuery(R"(
                    SELECT *
                    FROM `/Root/Tenant1/.sys/auth_permissions`
                    WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                    AND Path NOT LIKE "%/.metadata%"
                )").GetValueSync();

                auto expected = R"([
                    [["/Root/Tenant1/Dir3"];["ydb.granular.select_row"];["user3"]];
                    [["/Root/Tenant1/Dir3"];["ydb.granular.erase_row"];["user4"]];
                ])";
                NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
            }
        }

        { // revoke user1rootadmin /Root/Dir2 GenericUse access
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Deny, NACLib::GenericUse, "user1rootadmin");
            env.GetClient().ModifyACL("/Root", "Dir2", acl.SerializeAsString());

            driverConfig.SetCredentialsProviderFactory(
                NYdb::CreateLoginCredentialsProviderFactory({
                    .User = "user1rootadmin",
                    .Password = "password1",
                })
            );
            driverConfig.SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
                AND Path NOT LIKE "%/.metadata%"
            )").GetValueSync();

            auto expected = R"([
                [["/Root"];["ydb.generic.full"];["root@builtin"]];
                [["/Root"];["ydb.generic.use"];["user1rootadmin"]];
                [["/Root"];["ydb.generic.use"];["user2"]];
                [["/Root"];["ydb.generic.use"];["user6tenant1admin"]];
                [["/Root/Dir1"];["ydb.granular.select_row"];["user1rootadmin"]];
            ])";
            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions_ResultOrder) {
        TTestEnv env;
        SetupAuthEnvironment(env);

        {
            for (auto user : {
                "user1",
                "user2",
                "user"
            }) {
                env.GetClient().CreateUser("/Root", user, "password");
            }

            for (auto dir : {
                "Dir",
                "Dir1",
                "Dir2",
                "Dir/SubDir1",
                "Dir/SubDir2"
            }) {
                env.GetClient().MkDir("/Root", dir);
            }

            for (auto acl : TVector<std::tuple<TString, TString, TString, NACLib::EAccessRights>>{
                {"/", "Root", "user1", NACLib::SelectRow},
                {"/", "Root", "user1", NACLib::EraseRow},
                {"/", "Root", "user1", NACLib::AlterSchema},
                {"/", "Root", "user2", NACLib::GenericUse},
                {"/Root", "Dir1", "user2", NACLib::GenericUse},
                {"/Root", "Dir1", "user1", NACLib::GenericUse},
                {"/Root", "Dir2", "user2", NACLib::GenericUse},
                {"/Root", "Dir2", "user", NACLib::GenericUse},
                {"/Root", "Dir2", "user1", NACLib::GenericUse},
                {"/Root", "Dir", "user1", NACLib::GenericUse},
                {"/Root", "Dir1", "user1", NACLib::AlterSchema},
                {"/Root/Dir1", "SubDir1", "user1", NACLib::AlterSchema},
                {"/Root/Dir1", "SubDir2", "user2", NACLib::AlterSchema},
                {"/Root/Dir1", "SubDir2", "user1", NACLib::AlterSchema}
            }) {
                NACLib::TDiffACL diffAcl;
                diffAcl.AddAccess(NACLib::EAccessType::Allow, std::get<3>(acl), std::get<2>(acl));
                env.GetClient().ModifyACL(std::get<0>(acl), std::get<1>(acl), diffAcl.SerializeAsString());
            }
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT Path, Sid, Permission
            FROM `/Root/.sys/auth_permissions`
            WHERE Path NOT LIKE "%/.sys%"    -- not list system dirs and files
            AND Path NOT LIKE "%/.metadata%"
        )").GetValueSync();

        auto expected = R"([
            [["/Root"];["user1"];["ydb.granular.alter_schema"]];
            [["/Root"];["user1"];["ydb.granular.erase_row"]];
            [["/Root"];["user1"];["ydb.granular.select_row"]];
            [["/Root"];["user2"];["ydb.generic.use"]];
            [["/Root/Dir"];["user1"];["ydb.generic.use"]];
            [["/Root/Dir1"];["user1"];["ydb.generic.use"]];
            [["/Root/Dir1"];["user1"];["ydb.granular.alter_schema"]];
            [["/Root/Dir1"];["user2"];["ydb.generic.use"]];
            [["/Root/Dir2"];["user"];["ydb.generic.use"]];
            [["/Root/Dir2"];["user1"];["ydb.generic.use"]];
            [["/Root/Dir2"];["user2"];["ydb.generic.use"]];
        ])";

        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST_TWIN(AuthEffectivePermissions, EnableRealSystemViewPaths) {
        TTestEnv env({ .EnableRealSystemViewPaths = EnableRealSystemViewPaths });
        SetupAuthEnvironment(env);

        {
            env.GetClient().CreateUser("/Root", "user1", "password1");
            env.GetClient().CreateUser("/Root/Tenant1", "user2", "password2");

            env.GetClient().MkDir("/Root", "Dir1");
            env.GetClient().MkDir("/Root/Tenant1", "Dir2");
        }

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            env.GetClient().ModifyACL("/", "Root", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user2");
            env.GetClient().ModifyACL("/Root/Tenant1", "Dir2", acl.SerializeAsString());
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);

        {
            driverConfig.SetDatabase("/Root");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_effective_permissions`
            )").GetValueSync();

            TString expectedYson;
            if (EnableRealSystemViewPaths) {
                expectedYson = R"([
                    [["/Root"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata/workload_manager"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata/workload_manager/pools"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/auth_effective_permissions"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/auth_group_members"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/auth_groups"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/auth_owners"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/auth_permissions"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/auth_users"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/compile_cache_queries"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/ds_groups"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/ds_pdisks"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/ds_storage_pools"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/ds_storage_stats"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/ds_vslots"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/hive_tablets"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/nodes"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/partition_stats"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/pg_class"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/pg_tables"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/query_metrics_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/query_sessions"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/resource_pool_classifiers"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/resource_pools"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/streaming_queries"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/tables"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_partitions_by_tli_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_partitions_by_tli_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_partitions_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_partitions_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_cpu_time_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_cpu_time_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_duration_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_duration_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_read_bytes_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_read_bytes_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_request_units_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/.sys/top_queries_by_request_units_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Dir1"];["ydb.generic.use"];["user1"]];
                    [["/Root/Table0"];["ydb.generic.use"];["user1"]];
                ])";
            } else {
                expectedYson = R"([
                    [["/Root"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata/workload_manager"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata/workload_manager/pools"];["ydb.generic.use"];["user1"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                    [["/Root/.metadata/workload_manager/pools/default"];["ydb.generic.use"];["user1"]];
                    [["/Root/Dir1"];["ydb.generic.use"];["user1"]];
                    [["/Root/Table0"];["ydb.generic.use"];["user1"]];
                ])";
            }

            NKqp::CompareYson(expectedYson, NKqp::StreamResultToYson(it));
        }

        {
            driverConfig.SetDatabase("/Root/Tenant1");
            auto driver = TDriver(driverConfig);

            TTableClient client(driver);
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/Tenant1/.sys/auth_effective_permissions`
            )").GetValueSync();

            TString expectedYson;
            if (EnableRealSystemViewPaths) {
                expectedYson = R"([
                    [["/Root/Tenant1"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata/workload_manager"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/auth_effective_permissions"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/auth_group_members"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/auth_groups"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/auth_owners"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/auth_permissions"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/auth_users"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/compile_cache_queries"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/nodes"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/partition_stats"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/pg_class"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/pg_tables"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/query_metrics_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/query_sessions"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/resource_pool_classifiers"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/resource_pools"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/streaming_queries"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/tables"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_partitions_by_tli_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_partitions_by_tli_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_partitions_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_partitions_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_cpu_time_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_cpu_time_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_duration_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_duration_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_read_bytes_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_request_units_one_hour"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.sys/top_queries_by_request_units_one_minute"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/Dir2"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/Dir2"];["ydb.granular.select_row"];["user2"]];
                    [["/Root/Tenant1/Table1"];["ydb.generic.use"];["user1"]];
                ])";
            } else {
                expectedYson = R"([
                    [["/Root/Tenant1"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata/workload_manager"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["all-users@well-known"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["all-users@well-known"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.describe_schema"];["root@builtin"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.granular.select_row"];["root@builtin"]];
                    [["/Root/Tenant1/.metadata/workload_manager/pools/default"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/Dir2"];["ydb.generic.use"];["user1"]];
                    [["/Root/Tenant1/Dir2"];["ydb.granular.select_row"];["user2"]];
                    [["/Root/Tenant1/Table1"];["ydb.generic.use"];["user1"]];
                ])";
            }

            NKqp::CompareYson(expectedYson, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(AuthPermissions_Selects) {
        TTestEnv env;
        SetupAuthEnvironment(env);

        {
            env.GetClient().CreateUser("/Root", "user1", "password1");
            env.GetClient().CreateUser("/Root", "user2", "password2");

            env.GetClient().MkDir("/Root", "Dir1/SubDir1");
            env.GetClient().MkDir("/Root", "Dir1/SubDir2");
        }

        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericUse, "user1");
            env.GetClient().ModifyACL("/", "Root", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user2");
            env.GetClient().ModifyACL("/Root", "Dir1", acl.SerializeAsString());
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::EraseRow, "user2");
            env.GetClient().ModifyACL("/Root/Dir1", "SubDir1", acl.SerializeAsString());
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1"];["ydb.generic.use"];["user1"]];
                [["/Root/Dir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Sid = "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1"];["ydb.granular.select_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid >= "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user2"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user2" AND Permission >= "ydb.granular.erase_row"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.erase_row"];["user2"]];
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT *
                FROM `/Root/.sys/auth_permissions`
                WHERE Path = "/Root/Dir1/SubDir1" AND Sid = "user2" AND Permission > "ydb.granular.erase_row"
            )").GetValueSync();

            auto expected = R"([
                [["/Root/Dir1/SubDir1"];["ydb.granular.select_row"];["user2"]];
            ])";

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }
}

} // NSysView
} // NKikimr
