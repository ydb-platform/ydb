#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include "ydb_test_bootstrap.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

// Bindings

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageCreateBinding) {
    Y_UNIT_TEST(ShouldSucceed)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("test_binding_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckLowerCaseName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("biNdiNG").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect binding name: biNdiNG. Please use only lower case, code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("binding").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckMaxLengthName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(TString(256, '_')).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: ________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________. Name length must not exceed 255 symbols. Current length is 256 symbol(s), code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(TString(255, '_')).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckMultipleDotsName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("..").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: ... Name is not allowed path part contains only dots, code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("..!").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckAllowedSymbolsName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("binding`").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: binding`. Please make sure that name consists of following symbols: ['a'-'z'], ['0'-'9'], '!', '\\', '#', '$', '%'. '&', '(', ')', '*', '+', ',', '-', '.', ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '{', '|', '}', '~', code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("binding").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckNotAvailable)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.AddAvailableBinding("NOT_EXIST");
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: binding of the specified type is disabled, code: 1003\n");
        }

    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: connection_id's length is not in [1; 1024], code: 1003\n");
        }

        // Build successful binding request and then modify it
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        FederatedQuery::CreateBindingRequest successfulRequest;
        {
            successfulRequest = TCreateBindingBuilder{}.SetConnectionId(connectionId).Build();
            const auto [result, issues] = bootstrap.CreateBinding(successfulRequest);
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            req.clear_content();
            const auto [result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "content field is not specified");
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            req.mutable_content()->mutable_acl()->clear_visibility();
            const auto [result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "binding.acl.visibility field is not specified");
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            req.mutable_content()->clear_setting();
            const auto [result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "binding.setting field is not specified");
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            req.mutable_content()->mutable_setting()->clear_data_streams();
            const auto [result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "binding is not set");
        }


        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            req.mutable_content()->mutable_setting()->mutable_data_streams()->clear_schema();
            const auto [result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "data streams with empty schema is forbidden");
        }
    }

    Y_UNIT_TEST(ShouldValidateFormatSetting) {
        TTestBootstrap bootstrap{ __PRETTY_FUNCTION__ };
        // Build successful binding request and then modify it
        TString connectionId;
        {
            const auto[result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        FederatedQuery::CreateBindingRequest successfulRequest;
        {
            successfulRequest = TCreateBindingBuilder{}.SetConnectionId(connectionId).Build();
            auto os = successfulRequest.mutable_content()->mutable_setting()->mutable_object_storage()->add_subset();
            os->set_format("csv_with_names");
            os->set_path_pattern("/");
            (*os->mutable_format_setting())["data.interval.unit"] = "SECONDS";
            (*os->mutable_format_setting())["csv_delimiter"] = ";";
            const auto[result, issues] = bootstrap.CreateBinding(successfulRequest);
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            auto os = req.mutable_content()->mutable_setting()->mutable_object_storage()->add_subset();
            os->set_path_pattern("/");
            (*os->mutable_format_setting())["data.interval.unit"] = "XYZ";
            const auto[result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: unknown value for data.interval.unit XYZ, code: 400010");
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            auto os = req.mutable_content()->mutable_setting()->mutable_object_storage()->add_subset();
            os->set_path_pattern("/");
            (*os->mutable_format_setting())["csv_delimiter"] = "AA";
            const auto[result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: csv_delimiter should contain only one character, code: 400010");
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            auto os = req.mutable_content()->mutable_setting()->mutable_object_storage()->add_subset();
            os->set_path_pattern("/");
            os->set_format("parquet");
            (*os->mutable_format_setting())["csv_delimiter"] = ";";
            const auto[result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: csv_delimiter should be used only with format csv_with_names, code: 400010");
        }

        {
            FederatedQuery::CreateBindingRequest req = successfulRequest;
            auto os = req.mutable_content()->mutable_setting()->mutable_object_storage()->add_subset();
            os->set_path_pattern("/");
            (*os->mutable_format_setting())["AAA"] = "YYY";
            const auto[result, issues] = bootstrap.CreateBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: unknown format setting AAA, code: 400010");
        }
    }

    Y_UNIT_TEST(ShouldCheckMaxCountBindings)
    {

        NConfig::TControlPlaneStorageConfig config;
        config.SetMaxCountBindings(1);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("test_binding_name_2").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Too many bindings in folder: 1. Please remove unused bindings, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        const auto request = TCreateBindingBuilder{}.SetIdempotencyKey("aba").SetConnectionId(connectionId).Build();
        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        TString bindingIdRetry;
        {
            const auto [result, issues] = bootstrap.CreateBinding(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingIdRetry = result.binding_id();
        }

        UNIT_ASSERT_VALUES_EQUAL(bindingId, bindingIdRetry);
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageListBindings) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldFilterByName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("a").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TListBindingsBuilder{}.Build();
            request.mutable_filter()->set_name("a");
            const auto [result, issues] = bootstrap.ListBindings(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldFilterByMe)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("a").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("c").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TListBindingsBuilder{}.Build();
            request.mutable_filter()->set_created_by_me(true);
            const auto [result, issues] = bootstrap.ListBindings(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldPageToken)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListBindings(TListBindingsBuilder{}.Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.binding_size(), 10);

            const auto [result2, issues2] = bootstrap.ListBindings(TListBindingsBuilder{}.SetPageToken(result1.next_page_token()).Build());
            UNIT_ASSERT_C(!issues2, issues2.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result2.binding_size(), 5);
        }
    }

    Y_UNIT_TEST(ShouldEmptyPageToken)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        for (size_t i = 0; i < 20; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListBindings(TListBindingsBuilder{}.Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.binding_size(), 10);

            const auto [result2, issues2] = bootstrap.ListBindings(TListBindingsBuilder{}.SetPageToken(result1.next_page_token()).Build());
            UNIT_ASSERT_C(!issues2, issues2.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result2.binding_size(), 10);
            UNIT_ASSERT_VALUES_EQUAL(result2.next_page_token(), "");
        }
    }

    Y_UNIT_TEST(ShouldCheckLimit)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListBindings(TListBindingsBuilder{}.SetLimit(20).Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.binding_size(), 15);
        }
    }

    Y_UNIT_TEST(ShouldCheckScopeVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.SetLimit(20).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 15);
        }
    }

    Y_UNIT_TEST(ShouldCheckPrivateVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        for (size_t i = 0; i < 3; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(ToString(i)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 5; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(ToString(i + 3)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 3);
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 5);
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build(), TPermissions{}, "");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: scope is not specified, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        for (size_t i = 0; i < 3; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetName(ToString(i)).SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 5; i++)
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName(ToString(i + 3)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 8);
        }
    }

    Y_UNIT_TEST(ShouldCheckFilterByConnectionId)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId1;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId1 = result.connection_id();
        }
        TString connectionId2;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("test_connection_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId2 = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId1).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId2).SetName("test_binding_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 2);
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.SetConnectionId(connectionId1).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldCombineFilters)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("a").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TListBindingsBuilder{}.Build();
            request.mutable_filter()->set_created_by_me(true);
            request.mutable_filter()->set_name("a");
            const auto [result, issues] = bootstrap.ListBindings(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.binding(0).name(), "ab");
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDescribeBinding) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_1");
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckExist)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId("abra").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: binding_id's length is not in [1; 1024], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_1");
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDeleteBinding) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(bindingId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckExist)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId("aba").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId("").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: binding_id's length is not in [1; 1024], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(bindingId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(bindingId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(bindingId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionFailed)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(bindingId).SetPreviousRevision(100).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Revision of the binding has been changed already. Please restart the request with a new revision, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(bindingId).SetPreviousRevision(1).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageModifyBinding) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckLowerCaseName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("binding").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }
        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).SetName("biNdiNG").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect binding name: biNdiNG. Please use only lower case, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckMaxLengthName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("binding").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }
        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetName(TString(256, '_')).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: ________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________. Name length must not exceed 255 symbols. Current length is 256 symbol(s), code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckMultipleDotsName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("binding").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }
        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetName("..").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: ... Name is not allowed path part contains only dots, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckAllowedSymbolsName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("binding").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }
        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetName("binding`").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: binding`. Please make sure that name consists of following symbols: ['a'-'z'], ['0'-'9'], '!', '\\', '#', '$', '%'. '&', '(', ')', '*', '+', ',', '-', '.', ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '{', '|', '}', '~', code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckExist)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId("aba").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckNotExistOldName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckMoveToScope)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetName("test_binding_name_1").SetBindingId(bindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding with the same name already exists. Please choose another name, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckModifyTheSame)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().acl().visibility(), FederatedQuery::Acl::SCOPE);
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId("").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: binding_id's length is not in [1; 1024], code: 1003\n");
        }

        FederatedQuery::ModifyBindingRequest successfulRequest;
        {
            successfulRequest = TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build();
            const auto [result, issues] = bootstrap.ModifyBinding(successfulRequest);
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            FederatedQuery::ModifyBindingRequest req = successfulRequest;
            req.clear_content();
            const auto [result, issues] = bootstrap.ModifyBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "content field is not specified");
        }

        {
            FederatedQuery::ModifyBindingRequest req = successfulRequest;
            req.mutable_content()->mutable_acl()->clear_visibility();
            const auto [result, issues] = bootstrap.ModifyBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "binding.acl.visibility field is not specified");
        }

        {
            FederatedQuery::ModifyBindingRequest req = successfulRequest;
            req.mutable_content()->clear_setting();
            const auto [result, issues] = bootstrap.ModifyBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "binding.setting field is not specified");
        }

        {
            FederatedQuery::ModifyBindingRequest req = successfulRequest;
            req.mutable_content()->mutable_setting()->clear_data_streams();
            const auto [result, issues] = bootstrap.ModifyBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "binding is not set");
        }


        {
            FederatedQuery::ModifyBindingRequest req = successfulRequest;
            req.mutable_content()->mutable_setting()->mutable_data_streams()->clear_schema();
            const auto [result, issues] = bootstrap.ModifyBinding(req);
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "data streams with empty schema is forbidden");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckWithoutIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
            UNIT_ASSERT_EQUAL(result.binding().meta().revision(), 2);
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
            UNIT_ASSERT_EQUAL(result.binding().meta().revision(), 3);
        }
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
            UNIT_ASSERT_EQUAL(result.binding().meta().revision(), 2);
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
            UNIT_ASSERT_EQUAL(result.binding().meta().revision(), 2);
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionFailed)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(bindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.binding().content().name(), "test_binding_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckMoveToScopeWithPrivateConnection)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString bindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            bindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(connectionId).SetBindingId(bindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding with SCOPE visibility cannot refer to connection with PRIVATE visibility, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldNotCreateScopeeBindingWithUnavailableConnection)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString publicConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            publicConnectionId = result.connection_id();
        }

        TString privateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            privateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(publicConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), TStringBuilder{} << "<main>: Error: The connection with id " << publicConnectionId << " is overridden by the private conection with id " << privateConnectionId << " (test_connection_name_1). Please rename the private connection or use another connection, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldNotCreatePrivateBindingWithUnavailableConnection)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString publicConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            publicConnectionId = result.connection_id();
        }

        TString privateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            privateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(publicConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), TStringBuilder{} << "<main>: Error: The connection with id " << publicConnectionId << " is overridden by the private conection with id " << privateConnectionId << " (test_connection_name_1). Please rename the private connection or use another connection, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldNotCreatePrivateConnectionWithDesctructionBinding)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString publicConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            publicConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(publicConnectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());

        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), TStringBuilder{} << "<main>: Error: Connection named test_connection_name_1 overrides connection from binding test_binding_name_1. Please rename this connection, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouleCheckObjectStorageProjectionByColumns)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString publicConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            publicConnectionId = result.connection_id();
        }

        {
            FederatedQuery::ObjectStorageBinding os;
            auto& subset = *os.add_subset();
            subset.set_path_pattern("/root/");
            subset.add_partitioned_by("a");
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.CreateObjectStorage(os).SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(publicConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), TStringBuilder{} << "<main>: Error: Column a from partitioned_by does not exist in the scheme. Please add such a column to your scheme, code: 400010\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckObjectStorageProjectionByTypes)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString publicConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            publicConnectionId = result.connection_id();
        }

        {
            FederatedQuery::ObjectStorageBinding os;
            auto& subset = *os.add_subset();
            auto& column = *subset.mutable_schema()->add_column();
            column.set_name("a");
            column.mutable_type()->set_type_id(Ydb::Type::BOOL);
            subset.set_path_pattern("/root/");
            subset.add_partitioned_by("a");
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.CreateObjectStorage(os).SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(publicConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), TStringBuilder{} << "<main>: Error: Column \"a\" from projection does not support Bool type, code: 400010\n");
        }
    }
}

} // NFq
