#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/storage_pools.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>

#include "ydb_common_ut.h"

namespace NKikimr {

using namespace Tests;
using namespace NYdb;

namespace {

struct TServerInitialization {
    bool EnforceUserToken = true;
    std::vector<TString> AdministrationAllowedSids = {};
    std::vector<TString> MonitoringAllowedSids = {};
    std::vector<TString> ViewerAllowedSids = {};
    std::vector<TString> DatabaseAllowedSids = {};
};

NKikimrConfig::TAppConfig GetWhoAmIAppConfig(const TServerInitialization& serverInitialization) {
    auto config = NKikimrConfig::TAppConfig();

    auto& securityConfig = *config.MutableDomainsConfig()->MutableSecurityConfig();
    securityConfig.SetEnforceUserTokenRequirement(serverInitialization.EnforceUserToken);

    for (const auto& sid : serverInitialization.AdministrationAllowedSids) {
        securityConfig.AddAdministrationAllowedSIDs(sid);
    }
    for (const auto& sid : serverInitialization.MonitoringAllowedSids) {
        securityConfig.AddMonitoringAllowedSIDs(sid);
    }
    for (const auto& sid : serverInitialization.ViewerAllowedSids) {
        securityConfig.AddViewerAllowedSIDs(sid);
    }
    for (const auto& sid : serverInitialization.DatabaseAllowedSids) {
        securityConfig.AddDatabaseAllowedSIDs(sid);
    }

    return config;
}

NDiscovery::TWhoAmIResult WhoAmI(ui16 grpc, const TString& token, bool withGroups) {
    TString location = TStringBuilder() << "localhost:" << grpc;
    TDriverConfig config;
    config.SetEndpoint(location);
    config.SetAuthToken(token);
    config.SetDatabase("/Root");
    auto connection = NYdb::TDriver(config);
    NYdb::NDiscovery::TDiscoveryClient discoveryClient = NYdb::NDiscovery::TDiscoveryClient(connection);
    auto settings = NDiscovery::TWhoAmISettings().WithGroups(withGroups);
    auto result = discoveryClient.WhoAmI(settings).GetValueSync();
    connection.Stop(true);
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TWhoAmI) {

    Y_UNIT_TEST(WhoAmIBasic) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", false);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
    }

    Y_UNIT_TEST(WhoAmIWithGroups) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", true);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
        // Check that we got groups (even if empty, the call should succeed)
        auto groups = result.GetGroups();
        // User might have groups or not, depending on setup
    }

    Y_UNIT_TEST(WhoAmIWithPermissions_NoPermissions) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true,
            .AdministrationAllowedSids = {},
            .MonitoringAllowedSids = {},
            .ViewerAllowedSids = {},
            .DatabaseAllowedSids = {}
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", true);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
        UNIT_ASSERT_VALUES_EQUAL(result.IsTokenRequired(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), false);
    }

    Y_UNIT_TEST(WhoAmIWithPermissions_DatabaseAllowed) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true,
            .DatabaseAllowedSids = {"user1@builtin"}
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", true);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
        UNIT_ASSERT_VALUES_EQUAL(result.IsTokenRequired(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), true);
    }

    Y_UNIT_TEST(WhoAmIWithPermissions_ViewerAllowed) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true,
            .ViewerAllowedSids = {"user1@builtin"}
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", true);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
        UNIT_ASSERT_VALUES_EQUAL(result.IsTokenRequired(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), true);
        // Viewer implies database access
        UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), true);
    }

    Y_UNIT_TEST(WhoAmIWithPermissions_MonitoringAllowed) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true,
            .MonitoringAllowedSids = {"user1@builtin"}
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", true);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
        UNIT_ASSERT_VALUES_EQUAL(result.IsTokenRequired(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), true);
        // Monitoring implies viewer and database access
        UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), true);
    }

    Y_UNIT_TEST(WhoAmIWithPermissions_AdministrationAllowed) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true,
            .AdministrationAllowedSids = {"user1@builtin"}
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", true);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
        UNIT_ASSERT_VALUES_EQUAL(result.IsTokenRequired(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), true);
        // Administration implies all other permissions
        UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), true);
        UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), true);
    }

    Y_UNIT_TEST(WhoAmIWithPermissions_TokenNotRequired) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = false
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", true);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result.IsTokenRequired(), false);
    }

    Y_UNIT_TEST(WhoAmIWithoutGroups_NoPermissions) {
        // When withGroups is false, permission fields should not be populated
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true,
            .AdministrationAllowedSids = {"user1@builtin"}
        }));
        ui16 grpc = server.GetPort();

        auto result = WhoAmI(grpc, "user1@builtin", false);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "user1@builtin");
        // Without groups flag, permissions should not be populated (all false)
        UNIT_ASSERT_VALUES_EQUAL(result.IsTokenRequired(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), false);
    }

    Y_UNIT_TEST(WhoAmIWithPermissions_DifferentUser) {
        TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth> server(GetWhoAmIAppConfig({
            .EnforceUserToken = true,
            .AdministrationAllowedSids = {"admin@builtin"},
            .ViewerAllowedSids = {"viewer@builtin"}
        }));
        ui16 grpc = server.GetPort();

        // Test admin user
        {
            auto result = WhoAmI(grpc, "admin@builtin", true);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "admin@builtin");
            UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), true);
            UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), true);
            UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), true);
            UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), true);
        }

        // Test viewer user
        {
            auto result = WhoAmI(grpc, "viewer@builtin", true);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "viewer@builtin");
            UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), true);
            UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), true);
        }

        // Test regular user
        {
            auto result = WhoAmI(grpc, "regular@builtin", true);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "regular@builtin");
            UNIT_ASSERT_VALUES_EQUAL(result.IsAdministrationAllowed(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.IsMonitoringAllowed(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.IsViewerAllowed(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.IsDatabaseAllowed(), false);
        }
    }
}

} // namespace NKikimr
