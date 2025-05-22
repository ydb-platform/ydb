#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include "ydb_test_bootstrap.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/fq/libs/config/protos/control_plane_storage.pb.h>


namespace NFq {

using namespace NActors;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageCreateConnection) {
    Y_UNIT_TEST(ShouldSucccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("test_connection_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckNotAvailable)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.AddAvailableConnection("CLICKHOUSE_CLUSTER");
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: connection of the specified type is disabled, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldDisableCurrentIam)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetDisableCurrentIam(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: current iam authorization is disabled, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build(), TPermissions{}, "");
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: scope is not specified, code: 1003\n");
    }

    Y_UNIT_TEST(ShouldCheckMaxCountConnections)
    {

        NConfig::TControlPlaneStorageConfig config;
        config.SetMaxCountConnections(1);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("test_connection_name_2").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Too many connections in folder: 1. Please remove unused connections, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        const auto request = TCreateConnectionBuilder{}.SetIdempotencyKey("aba").Build();
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        TString connectionIdRetry;
        {
            const auto [result, issues] = bootstrap.CreateConnection(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionIdRetry = result.connection_id();
        }

        UNIT_ASSERT_VALUES_EQUAL(connectionId, connectionIdRetry);
    }

    Y_UNIT_TEST(ShouldCheckUniqueName)
    {

        NConfig::TControlPlaneStorageConfig config;
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection with the same name already exists. Please choose another name, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckLowerCaseName)
    {
        NConfig::TControlPlaneStorageConfig config;
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("coNneCTiOn").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect binding name: coNneCTiOn. Please use only lower case, code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("connection").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckMaxLengthName)
    {
        NConfig::TControlPlaneStorageConfig config;
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(TString(256, '_')).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: ________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________. Name length must not exceed 255 symbols. Current length is 256 symbol(s), code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(TString(255, '_')).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckMultipleDotsName)
    {
        NConfig::TControlPlaneStorageConfig config;
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("..").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: ... Name is not allowed path part contains only dots, code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("..!").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckAllowedSymbolsName)
    {
        NConfig::TControlPlaneStorageConfig config;
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("connection`").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: connection`. Please make sure that name consists of following symbols: ['a'-'z'], ['0'-'9'], '!', '\\', '#', '$', '%'. '&', '(', ')', '*', '+', ',', '-', '.', ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '{', '|', '}', '~', code: 1003\n");
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("connection").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckCommitTransactionWrite)
    {

        NConfig::TControlPlaneStorageConfig config;
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 50; i++) {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection with the same name already exists. Please choose another name, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckCommitTransactionReadWrite)
    {

        NConfig::TControlPlaneStorageConfig config;
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 50; i++) {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetIdempotencyKey("aba").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection with the same name already exists. Please choose another name, code: 1003\n");
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageListConnections) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldPageToken)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.connection_size(), 10);

            const auto [result2, issues2] = bootstrap.ListConnections(TListConnectionsBuilder{}.SetPageToken(result1.next_page_token()).Build());
            UNIT_ASSERT_C(!issues2, issues2.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result2.connection_size(), 5);
        }
    }

    Y_UNIT_TEST(ShouldEmptyPageToken)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 20; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.connection_size(), 10);

            const auto [result2, issues2] = bootstrap.ListConnections(TListConnectionsBuilder{}.SetPageToken(result1.next_page_token()).Build());
            UNIT_ASSERT_C(!issues2, issues2.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result2.connection_size(), 10);
            UNIT_ASSERT_VALUES_EQUAL(result2.next_page_token(), "");
        }
    }

    Y_UNIT_TEST(ShouldCheckLimit)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListConnections(TListConnectionsBuilder{}.SetLimit(20).Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.connection_size(), 15);
        }
    }

    Y_UNIT_TEST(ShouldCheckScopeVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.SetLimit(20).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 15);
        }
    }

    Y_UNIT_TEST(ShouldCheckPrivateVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 3; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 5; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i + 3)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 3);
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 5);
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build(), TPermissions{}, "");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: scope is not specified, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 3; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 5; i++)
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName(ToString(i + 3)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(),  TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 8);
        }
    }

    Y_UNIT_TEST(ShouldCheckFilterByName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("a").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("ab").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("c").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TListConnectionsBuilder{}.Build();
            request.mutable_filter()->set_name("a");
            const auto [result, issues] = bootstrap.ListConnections(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldCheckFilterByMe)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("a").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("ab").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("c").SetVisibility(FederatedQuery::Acl::SCOPE).Build(),  TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TListConnectionsBuilder{}.Build();
            request.mutable_filter()->set_created_by_me(true);
            const auto [result, issues] = bootstrap.ListConnections(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldCombineFilters)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("a").SetVisibility(FederatedQuery::Acl::SCOPE).Build(),  TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("ab").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("c").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TListConnectionsBuilder{}.Build();
            request.mutable_filter()->set_created_by_me(true);
            request.mutable_filter()->set_name("a");
            const auto [result, issues] = bootstrap.ListConnections(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.connection(0).content().name(), "ab");
        }
    }

    Y_UNIT_TEST(ShouldCheckFilterByConnectionType)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("a").SetVisibility(FederatedQuery::Acl::SCOPE).CreateYdb("test_ydb", "").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("ab").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("c").SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TListConnectionsBuilder{}.Build();
            request.mutable_filter()->set_connection_type(FederatedQuery::ConnectionSetting::YDB_DATABASE);
            const auto [result, issues] = bootstrap.ListConnections(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.connection(0).content().name(), "a");
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDescribeConnection) {
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
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_1");
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
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

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId("abra").Build());
            UNIT_ASSERT(issues);
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
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "");
            UNIT_ASSERT(issues);
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_1");
        }
    }

    Y_UNIT_TEST(ShouldNotShowClickHousePassword)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.CreateClickHouse(
                "my_db", "my_login", "my_pswd", "", "database_name").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_1");
            UNIT_ASSERT_EQUAL(result.connection().content().setting().clickhouse_cluster().password(), "");
        }
    }
}

void TestShouldNotShowPassword(NYql::EDatabaseType databaseType) {
    TTestBootstrap bootstrap{__PRETTY_FUNCTION__ + NYql::DatabaseTypeToMdbUrlPath(databaseType)};
    FederatedQuery::CreateConnectionRequest request;
    TString connectionId;
    {
        TCreateConnectionBuilder connBuilder{};
        switch(databaseType) {
            case NYql::EDatabaseType::ClickHouse:
                request = connBuilder.CreateClickHouse("my_db", "my_login", "my_pswd", "", "database_name").Build();
                break;
            case NYql::EDatabaseType::PostgreSQL:
                request = connBuilder.CreatePostgreSQL("my_db", "my_login", "my_pswd", "", "database_name").Build();
                break;
            default:
                ythrow yexception() << TStringBuilder() << "unexpected database type: " << NYql::DatabaseTypeToMdbUrlPath(databaseType);
        }
        const auto [result, issues] = bootstrap.CreateConnection(request);
        UNIT_ASSERT_C(!issues, issues.ToString());
        connectionId = result.connection_id();
    }

    {
        const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
        UNIT_ASSERT_C(!issues, issues.ToString());
        UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_1");
        UNIT_ASSERT_EQUAL(result.connection().content().setting().clickhouse_cluster().password(), "");
    }
}

Y_UNIT_TEST_SUITE(ShouldNotShowPassword) {
    Y_UNIT_TEST(ShouldNotShowPasswordClickHouse) { TestShouldNotShowPassword(NYql::EDatabaseType::ClickHouse); }
    Y_UNIT_TEST(ShouldNotShowPasswordPostgreSQL) { TestShouldNotShowPassword(NYql::EDatabaseType::PostgreSQL); }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDeleteConnection) {
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
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
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

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId("aba").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
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
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId("").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: connection_id's length is not in [1; 1024], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
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

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(connectionId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(connectionId).SetIdempotencyKey("aba").Build());
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

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(connectionId).SetPreviousRevision(100).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Revision of the connection has been changed already. Please restart the request with a new revision, code: 1003\n");
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

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(connectionId).SetPreviousRevision(1).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageModifyConnection) {
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
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
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

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId("aba").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
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

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
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
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("coNneCTiOn").SetConnectionId(connectionId).Build());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect binding name: coNneCTiOn. Please use only lower case, code: 1003\n");
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
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName(TString(256, '_')).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
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

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("..").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
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
        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("connection`").Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "<main>: Error: Incorrect connection name: connection`. Please make sure that name consists of following symbols: ['a'-'z'], ['0'-'9'], '!', '\\', '#', '$', '%'. '&', '(', ')', '*', '+', ',', '-', '.', ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '{', '|', '}', '~', code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckMoveToScope)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());

        }

        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).SetName("test_connection_name_1").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection with the same name already exists. Please choose another name, code: 1003\n");
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
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: user is empty, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_2");
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

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_2");
            UNIT_ASSERT_EQUAL(result.connection().meta().revision(), 2);
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_2");
            UNIT_ASSERT_EQUAL(result.connection().meta().revision(), 3);
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

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_2");
            UNIT_ASSERT_EQUAL(result.connection().meta().revision(), 2);
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetConnectionId(connectionId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_2");
            UNIT_ASSERT_EQUAL(result.connection().meta().revision(), 2);
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

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetPreviousRevision(10).SetConnectionId(connectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Revision of the connection has been changed already. Please restart the request with a new revision, code: 1003\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_1");
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

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetPreviousRevision(1).SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.connection().content().name(), "test_connection_name_2");
        }
    }

    Y_UNIT_TEST(ShouldMoveFromScopeToPrivateWithError)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(connectionId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user1@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Changing visibility from SCOPE to PRIVATE is forbidden. Please create a new connection with visibility PRIVATE, code: 1003\n");
        }
    }
}

} // NFq
