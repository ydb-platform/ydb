#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include "ydb_test_bootstrap.h"

namespace NFq {

using namespace NActors;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageCreateBindingPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionManagePublicSuccess)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }
        const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
        UNIT_ASSERT_C(!issues, issues.ToString());
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePublicFailed)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetName("1").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).Build());
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a binding with these parameters. Please receive a permission yq.resources.managePublic, code: 1000\n");
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageListBindingsPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 3);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 4);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDescribeBindingPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myScopeBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myPrivateBindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherScopeBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeBinding(TDescribeBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageModifyBindingPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").SetBindingId(myScopeBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(myPrivateBindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("3b").SetBindingId(otherScopeBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(otherPrivateBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivateAfterModify)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(myPrivateBindingId).Build(), TPermissions{});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListBindings(TListBindingsBuilder{}.Build(), TPermissions{});
            UNIT_ASSERT_VALUES_EQUAL(result.binding_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyBinding(TModifyBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDeleteBindingPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myScopeBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myPrivateBindingId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherScopeBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::VIEW_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Binding does not exist or permission denied. Please check the id binding or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("1c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myConnectionId = result.connection_id();
        }

        TString otherConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::SCOPE).SetName("2c").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherConnectionId = result.connection_id();
        }

        TString myScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("1b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeBindingId = result.binding_id();
        }

        TString myPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(myConnectionId).SetName("2b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateBindingId = result.binding_id();
        }

        TString otherScopeBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("3b").Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeBindingId = result.binding_id();
        }

        TString otherPrivateBindingId;
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(otherConnectionId).SetName("4b").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateBindingId = result.binding_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(myPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherScopeBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteBinding(TDeleteBindingBuilder{}.SetBindingId(otherPrivateBindingId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

} // NFq
