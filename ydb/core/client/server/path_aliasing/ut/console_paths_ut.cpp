#include <ydb/core/client/server/path_aliasing/path_aliasing.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/console_tenant.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <initializer_list>
#include <utility>

namespace NKikimr::NMsgBusProxy {
    namespace {

        NPathAliasing::TPathContext Context(std::initializer_list<std::pair<const char*, const char*>> rules = {
                                                {"^/alias(/|$)", "/Root\\1"}, {"^/Root(/|$)", "/Decoy\\1"}})
        {
            NKikimrConfig::TPathRewriteConfig config;
            for (const auto& [pattern, replacement] : rules) {
                auto* rule = config.AddRules();
                rule->SetPattern(pattern);
                rule->SetReplacement(replacement);
            }
            return {NPathAliasing::TPathNormalizer(config), Nothing()};
        }

        void SetPath(NKikimrClient::TConsoleRequest& request, ui32 operation, const TString& path) {
            switch (operation) {
                case 0:
                    request.MutableCreateTenantRequest()->MutableRequest()->set_path(path);
                    break;
                case 1:
                    request.MutableGetTenantStatusRequest()->MutableRequest()->set_path(path);
                    break;
                case 2:
                    request.MutableAlterTenantRequest()->MutableRequest()->set_path(path);
                    break;
                case 3:
                    request.MutableRemoveTenantRequest()->MutableRequest()->set_path(path);
                    break;
            }
        }

        void Normalize(NKikimrClient::TConsoleRequest& request, const NPathAliasing::TPathContext& context = Context()) {
            const auto status = NormalizeMessageBusDatabasePaths(request, context);
            UNIT_ASSERT_C(status.IsSuccess(), status.GetErrorMessage());
        }

    } // namespace

    Y_UNIT_TEST_SUITE(MessageBusConsolePathAliasing) {
        Y_UNIT_TEST(AllFourDatabaseOperationsRewriteExactlyTheirOwnedOperandOnce) {
            for (ui32 operation = 0; operation < 4; ++operation) {
                NKikimrClient::TConsoleRequest request;
                request.SetDomainName("/alias/domain-name");
                request.SetSecurityToken("/alias/token");
                request.SetTimeoutMs(1234);
                SetPath(request, operation, "/alias/database");
                auto expected = request;
                SetPath(expected, operation, "/Root/database");
                Normalize(request);
                UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), expected.SerializeAsString());
            }
        }

        Y_UNIT_TEST(CreateServerlessRewritesBothDatabasePathsButNotAttributesOrPeer) {
            NKikimrClient::TConsoleRequest request;
            auto* create = request.MutableCreateTenantRequest();
            create->SetUserToken("/alias/token");
            create->SetPeerName("/alias/peer");
            create->MutableRequest()->set_path("/alias/database");
            create->MutableRequest()->mutable_serverless_resources()->set_shared_database_path("/alias/shared");
            (*create->MutableRequest()->mutable_attributes())["/alias/key"] = "/alias/value";
            auto expected = request;
            expected.MutableCreateTenantRequest()->MutableRequest()->set_path("/Root/database");
            expected.MutableCreateTenantRequest()->MutableRequest()->mutable_serverless_resources()->set_shared_database_path("/Root/shared");
            Normalize(request);
            UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), expected.SerializeAsString());
        }

        Y_UNIT_TEST(DisabledMissAndIdentityPreserveExactWireBytes) {
            const auto disabled = Context({});
            const auto unrelated = Context({{"^/never", "/unused"}});
            const auto identity = Context({{"^/alias(/|$)", "/alias\\1"}, {"^/alias", "/Root"}});
            for (const auto* context : {&disabled, &unrelated, &identity}) {
                for (ui32 operation = 0; operation < 4; ++operation) {
                    for (const TString& path : {TString("/alias/database"), TString("//alias//database/")}) {
                        NKikimrClient::TConsoleRequest request;
                        SetPath(request, operation, path);
                        const TString before = request.SerializeAsString();
                        Normalize(request, *context);
                        UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), before);
                    }
                }
            }
        }

        Y_UNIT_TEST(AbsentAndEmptyOperandsNeverBecomeRequestsOrDatabases) {
            const auto emptyMatching = Context({{"^$", "/Root/invented"}});
            NKikimrClient::TConsoleRequest absent;
            const TString absentBytes = absent.SerializeAsString();
            Normalize(absent, emptyMatching);
            UNIT_ASSERT_VALUES_EQUAL(absent.SerializeAsString(), absentBytes);
            for (ui32 operation = 0; operation < 4; ++operation) {
                NKikimrClient::TConsoleRequest request;
                switch (operation) {
                    case 0:
                        request.MutableCreateTenantRequest();
                        break;
                    case 1:
                        request.MutableGetTenantStatusRequest();
                        break;
                    case 2:
                        request.MutableAlterTenantRequest();
                        break;
                    case 3:
                        request.MutableRemoveTenantRequest();
                        break;
                }
                const TString withoutBody = request.SerializeAsString();
                Normalize(request, emptyMatching);
                UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), withoutBody);
                SetPath(request, operation, "");
                const TString emptyPath = request.SerializeAsString();
                Normalize(request, emptyMatching);
                UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), emptyPath);
            }
        }

        Y_UNIT_TEST(InvalidSharedTargetCannotPublishPartiallyRewrittenCreate) {
            NKikimrClient::TConsoleRequest request;
            auto* create = request.MutableCreateTenantRequest()->MutableRequest();
            create->set_path("/alias/database");
            create->mutable_serverless_resources()->set_shared_database_path("/alias/shared");
            const TString before = request.SerializeAsString();
            const auto status = NormalizeMessageBusDatabasePaths(request, Context({{"^/alias/database$", "/Root/database"}, {"^/alias/shared$", "relative-invalid"}}));
            UNIT_ASSERT(status.IsFail());
            UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), before);
        }

        Y_UNIT_TEST(ConfigurationSelectorsInternalPoolUpdatesAndOperationIdsRemainOpaque) {
            for (ui32 variant = 0; variant < 4; ++variant) {
                NKikimrClient::TConsoleRequest request;
                request.SetDomainName("/alias/domain");
                switch (variant) {
                    case 0:
                        request.MutableGetOperationRequest()->set_id("/alias/operation-id");
                        break;
                    case 1:
                        request.MutableUpdateTenantPoolConfig()->SetTenant("/alias/internal-tenant");
                        request.MutableUpdateTenantPoolConfig()->SetPoolType("/alias/pool-type");
                        break;
                    case 2:
                        request.MutableGetNodeConfigRequest()->MutableNode()->SetTenant("/alias/deployment-selector");
                        break;
                    case 3:
                        request.MutableGetConfigItemsRequest()->MutableTenantFilter()->AddTenants("/alias/config-selector");
                        break;
                }
                const TString before = request.SerializeAsString();
                Normalize(request);
                UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), before);
            }
        }
    } // Y_UNIT_TEST_SUITE(MessageBusConsolePathAliasing)

    Y_UNIT_TEST_SUITE(MessageBusMaintenancePathAliasing) {
        Y_UNIT_TEST(EachTenantRewritesOnceWithoutTouchingOtherMaintenanceNames) {
            NKikimrClient::TCmsRequest request;
            request.SetDomainName("/alias/domain");
            request.SetSecurityToken("/alias/token");
            auto* permission = request.MutablePermissionRequest();
            permission->SetUser("/alias/user");
            permission->SetReason("/alias/reason");
            permission->SetMaintenanceTaskId("/alias/task-id");
            for (const TString& tenant : {TString("/alias/one"), TString("/alias/two")}) {
                auto* action = permission->AddActions();
                action->SetTenant(tenant);
                action->SetHost("/alias/host");
                action->AddServices("/alias/service");
                action->AddDevices("/alias/device");
                action->SetMaintenanceTaskContext("/alias/opaque-context");
            }
            auto expected = request;
            expected.MutablePermissionRequest()->MutableActions(0)->SetTenant("/Root/one");
            expected.MutablePermissionRequest()->MutableActions(1)->SetTenant("/Root/two");
            UNIT_ASSERT(NormalizeMessageBusMaintenancePaths(request, Context()).IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), expected.SerializeAsString());
        }

        Y_UNIT_TEST(AbsentEmptyUnmatchedAndIdentityTenantsKeepRawPresenceAndSpelling) {
            const auto disabled = Context({});
            const auto unrelated = Context({{"^/never", "/unused"}});
            const auto identity = Context({{"^/alias(/|$)", "/alias\\1"}, {"^/alias", "/Root"}});
            const auto emptyMatching = Context({{"^$", "/Root/invented"}});
            for (const auto* context : {&disabled, &unrelated, &identity, &emptyMatching}) {
                NKikimrClient::TCmsRequest absent;
                const TString absentBytes = absent.SerializeAsString();
                UNIT_ASSERT(NormalizeMessageBusMaintenancePaths(absent, *context).IsSuccess());
                UNIT_ASSERT_VALUES_EQUAL(absent.SerializeAsString(), absentBytes);
                auto* permission = absent.MutablePermissionRequest();
                permission->AddActions();
                permission->AddActions()->SetTenant("");
                permission->AddActions()->SetTenant("/alias/tenant");
                permission->AddActions()->SetTenant("//alias//tenant/");
                const TString before = absent.SerializeAsString();
                UNIT_ASSERT(NormalizeMessageBusMaintenancePaths(absent, *context).IsSuccess());
                UNIT_ASSERT_VALUES_EQUAL(absent.SerializeAsString(), before);
            }
            NKikimrClient::TCmsRequest exactMatcher;
            exactMatcher.MutablePermissionRequest()->AddActions()->SetTenant("//alias//tenant/");
            const TString raw = exactMatcher.SerializeAsString();
            UNIT_ASSERT(NormalizeMessageBusMaintenancePaths(exactMatcher, Context()).IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(exactMatcher.SerializeAsString(), raw);
        }

        Y_UNIT_TEST(InvalidLaterTenantLeavesEveryActionUnchanged) {
            NKikimrClient::TCmsRequest request;
            request.MutablePermissionRequest()->AddActions()->SetTenant("/alias/one");
            request.MutablePermissionRequest()->AddActions()->SetTenant("/alias/bad");
            const TString before = request.SerializeAsString();
            const auto context = Context({{"^/alias/one$", "/Root/one"}, {"^/alias/bad$", "relative-invalid"}});
            UNIT_ASSERT(NormalizeMessageBusMaintenancePaths(request, context).IsFail());
            UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), before);
        }

        Y_UNIT_TEST(NotificationsConfigurationAndOtherRequestsRemainByteIdentical) {
            for (ui32 variant = 0; variant < 6; ++variant) {
                NKikimrClient::TCmsRequest request;
                request.SetDomainName("/alias/domain");
                switch (variant) {
                    case 0:
                        request.MutableNotification()->AddActions()->SetTenant("/alias/notification-tenant");
                        request.MutableNotification()->SetReason("/alias/notification-reason");
                        break;
                    case 1:
                        request.MutableSetConfigRequest()->MutableConfig();
                        break;
                    case 2:
                        request.MutableClusterStateRequest()->AddHosts("/alias/host");
                        break;
                    case 3:
                        request.MutableManagePermissionRequest()->AddPermissions("/alias/permission-id");
                        break;
                    case 4:
                        request.MutableCheckRequest()->SetRequestId("/alias/request-id");
                        break;
                    case 5:
                        // This legacy operation is unsupported by its owning CMS.
                        request.MutableConditionalPermissionRequest()->MutableAction()->SetTenant("/alias/tenant");
                        break;
                }
                const TString before = request.SerializeAsString();
                UNIT_ASSERT(NormalizeMessageBusMaintenancePaths(request, Context()).IsSuccess());
                UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), before);
            }
        }
    } // Y_UNIT_TEST_SUITE(MessageBusMaintenancePathAliasing)

} // namespace NKikimr::NMsgBusProxy
