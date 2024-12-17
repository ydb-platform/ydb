#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include "ydb_test_bootstrap.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageCreateConnectionPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionManagePublicSuccess)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
        UNIT_ASSERT_C(!issues, issues.ToString());
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePublicFailed)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a connection with these parameters. Please receive a permission yq.resources.managePublic, code: 1000\n");
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageListConnectionsPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 3);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListConnections(TListConnectionsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.connection_size(), 4);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDescribeConnectionPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeConnection(TDescribeConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageModifyConnectionPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("1").SetConnectionId(myScopeConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(myPrivateConnectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("3").SetConnectionId(otherScopeConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(otherPrivateConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("1").SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("3").SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("1").SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("3").SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("1").SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("3").SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyConnection(TModifyConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDeleteConnectionPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Connection does not exist or permission denied. Please check the id connection or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeConnectionId = result.connection_id();
        }

        TString myPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("2").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateConnectionId = result.connection_id();
        }

        TString otherScopeConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("3").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeConnectionId = result.connection_id();
        }

        TString otherPrivateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("4").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(myPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherScopeConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteConnection(TDeleteConnectionBuilder{}.SetConnectionId(otherPrivateConnectionId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

} // NFq
