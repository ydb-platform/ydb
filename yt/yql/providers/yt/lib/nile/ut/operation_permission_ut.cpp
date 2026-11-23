#include "operation_permission.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <initializer_list>
#include <utility>

namespace NYql {
    namespace {

        TGatewaysConfig MakeConfig(std::initializer_list<std::pair<TString, TString>> allowed = {}) {
            TGatewaysConfig config;
            auto* policy = config.MutableNile();
            for (const auto& [cluster, user] : allowed) {
                auto* rule = policy->AddAllowed();
                rule->SetCluster(cluster);
                rule->SetUser(user);
            }
            return config;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TNileOperationPermissionsTest) {
        Y_UNIT_TEST(MissingGatewaysConfigurationDisablesPolicy) {
            const auto permissions = BuildNileOperationPermissions(nullptr, "testuser", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(!permissions);
        }

        Y_UNIT_TEST(MissingNileConfigurationDisablesPolicy) {
            const TGatewaysConfig config;
            const auto permissions = BuildNileOperationPermissions(&config, "testuser", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(!permissions);
        }

        Y_UNIT_TEST(EmptyPolicyInitializesEmptyPermissions) {
            const auto config = ::ParseTextFormatFromString<TGatewaysConfig>("Nile {}");
            const auto permissions = BuildNileOperationPermissions(&config, "testuser", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(permissions);
            UNIT_ASSERT(permissions->empty());
        }

        Y_UNIT_TEST(RootNileConfigurationIsParsed) {
            const auto config = ::ParseTextFormatFromString<TGatewaysConfig>(R"(
                Nile {
                    Allowed { Cluster: "plato" User: "testuser" }
                    Allowed { Cluster: "shared" User: "*" }
                }
            )");
            UNIT_ASSERT(config.HasNile());
            UNIT_ASSERT(!config.HasYt());
            const auto permissions = BuildNileOperationPermissions(&config, "testuser", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(permissions);
            UNIT_ASSERT_VALUES_EQUAL(permissions->size(), 2);
            UNIT_ASSERT(permissions->contains("plato"));
            UNIT_ASSERT(permissions->contains("shared"));
        }

        Y_UNIT_TEST(MissingAttributesDisableValidation) {
            const auto config = MakeConfig();
            UNIT_ASSERT(!BuildNileOperationPermissions(&config, "testuser", Nothing()));
        }

        Y_UNIT_TEST(NonNileAttributesDisableValidation) {
            const auto config = MakeConfig();
            for (const auto attributes : {"", "{}", "{unrelated=job}", "{nile_job_uuid=job}", "{nested={yql_nile_job_uuid=job}}"}) {
                UNIT_ASSERT_C(!BuildNileOperationPermissions(&config, "testuser", TString(attributes)), attributes);
            }
        }

        Y_UNIT_TEST(NileDetectionUsesAttributePresence) {
            const auto config = MakeConfig();
            for (const auto attributes : {"{yql_nile_job_uuid=\"\"}", "{yql_nile_job_uuid=#}", "{yql_nile_job_uuid=%false}",
                                          "{yql_nile_job_uuid=0}", "{yql_nile_job_uuid={}}", "{yql_nile_job_uuid=[]}"}) {
                UNIT_ASSERT_C(BuildNileOperationPermissions(&config, "testuser", TString(attributes)), attributes);
            }
        }

        Y_UNIT_TEST(BinaryYsonAttributesAreSupported) {
            const auto config = MakeConfig();
            auto attributes = NYT::TNode::CreateMap();
            attributes["yql_nile_job_uuid"] = "job";
            const auto permissions = BuildNileOperationPermissions(&config, "testuser",
                                                                   NYT::NodeToYsonString(attributes, NYson::EYsonFormat::Binary));
            UNIT_ASSERT(permissions);
        }

        Y_UNIT_TEST(OnlyExecutionUserPermissionsAreSelected) {
            const auto config = MakeConfig({{"plato", "testuser"}, {"plato", "testuser"}, {"other", "another-user"},
                                            {"case-sensitive", "Testuser"}});
            const auto permissions = BuildNileOperationPermissions(&config, "testuser", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(permissions);
            UNIT_ASSERT_VALUES_EQUAL(permissions->size(), 1);
            UNIT_ASSERT(permissions->contains("plato"));
        }

        Y_UNIT_TEST(NoMatchingUserInitializesEmptyPermissions) {
            const auto config = MakeConfig({{"plato", "testuser"}});
            const auto permissions = BuildNileOperationPermissions(&config, "another-user", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(permissions);
            UNIT_ASSERT(permissions->empty());
        }

        Y_UNIT_TEST(WildcardAndExactPermissionsAreCombined) {
            const auto config = MakeConfig({{"kolmogorov", "*"}, {"vanga", "*"}, {"watt", "*"},
                                            {"plato", "testuser"},
                                            {"other", "another-user"}});
            const auto permissions = BuildNileOperationPermissions(&config, "testuser", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(permissions);
            UNIT_ASSERT_VALUES_EQUAL(permissions->size(), 4);
            for (const auto cluster : {"kolmogorov", "vanga", "watt", "plato"}) {
                UNIT_ASSERT_C(permissions->contains(cluster), cluster);
            }
        }

        Y_UNIT_TEST(EmptyUserReceivesOnlyMatchingPermissions) {
            const auto config = MakeConfig({{"shared", "*"}, {"explicit-empty", ""}, {"other", "testuser"}});
            const auto permissions = BuildNileOperationPermissions(&config, "", TString("{yql_nile_job_uuid=job}"));
            UNIT_ASSERT(permissions);
            UNIT_ASSERT_VALUES_EQUAL(permissions->size(), 2);
            UNIT_ASSERT(permissions->contains("shared"));
            UNIT_ASSERT(permissions->contains("explicit-empty"));
        }

        Y_UNIT_TEST(PermissionsOwnClusters) {
            auto config = MakeConfig({{"plato", "testuser"}});
            TString userName = "testuser";
            TMaybe<TString> attributes = TString("{yql_nile_job_uuid=job}");
            const auto permissions = BuildNileOperationPermissions(&config, userName, attributes);
            config.ClearNile();
            userName.clear();
            attributes.Clear();
            UNIT_ASSERT(permissions);
            UNIT_ASSERT_VALUES_EQUAL(permissions->size(), 1);
            UNIT_ASSERT(permissions->contains("plato"));
        }
    } // Y_UNIT_TEST_SUITE(TNileOperationPermissionsTest)

} // namespace NYql
