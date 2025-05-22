#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include "ydb_test_bootstrap.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageCreateQueryPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionManagePublicSuccess)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
        UNIT_ASSERT_C(!issues, issues.ToString());
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePublicFailed)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic, code: 1000\n");
    }

    Y_UNIT_TEST(ShouldApplyPermissionQueryInvokeSuccess)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE});
        UNIT_ASSERT_C(!issues, issues.ToString());
    }

    Y_UNIT_TEST(ShouldApplyPermissionQueryInvokeFailed)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a query with these parameters. Please receive a permission yq.queries.invoke, code: 1000\n");
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageListQueriesPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 3);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 4);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDescribeQueryPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewAst)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        TString queryId;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.user_token(), "xxx");
            queryId = task.query_id().value();
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope("yandexcloud://test_folder_id_1")
                                .SetQueryId(queryId)
                                .SetOwner("owner")
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetPlan("test-plan")
                                .SetAst("test-ast")
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            auto request = TDescribeQueryBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.DescribeQuery(std::move(request), TPermissions{TPermissions::VIEW_AST});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().ast().data(), "test-ast");
            UNIT_ASSERT_VALUES_EQUAL(result.query().plan().json(), "test-plan");
        }

        {
            auto request = TDescribeQueryBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.DescribeQuery(std::move(request), TPermissions{TPermissions::VIEW_QUERY_TEXT});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().text(), "SELECT 1;");
        }
    }

    Y_UNIT_TEST(ShouldNotApplyPermissionViewAstAndViewQueryText)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        TString queryId;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.user_token(), "xxx");
            queryId = task.query_id().value();
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope("yandexcloud://test_folder_id_1")
                                .SetQueryId(queryId)
                                .SetOwner("owner")
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetPlan("test-plan")
                                .SetAst("test-ast")
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            auto request = TDescribeQueryBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.DescribeQuery(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().ast().data(), "");
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().text(), "");
            UNIT_ASSERT_VALUES_EQUAL(result.query().plan().json(), "test-plan");
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageGetQueryStatusPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myScopeQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myPrivateQueryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPublicQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPrivateQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageModifyQueryPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionQueryInvokeSuccess)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionQueryInvokeFailed)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a query with these parameters. Please receive a permission yq.queries.invoke, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Permission denied to create a query with these parameters. Please receive a permission yq.resources.managePublic, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDeleteQueryPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myScopeQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myPrivateQueryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPublicQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageControlQueryPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myScopeQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myPrivateQueryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPublicQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionManagePrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::MANAGE_PRIVATE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageGetResultDataPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);

            for (const auto& task: tasks) {
                TVector<FederatedQuery::ResultSetMeta> resultSet;
                resultSet.emplace_back();

                auto request = TPingTaskBuilder{}
                                    .SetScope("yandexcloud://test_folder_id_1")
                                    .SetQueryId(task.query_id().value())
                                    .SetOwner("owner")
                                    .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                    .SetResultSetMetas(resultSet)
                                    .SetPlan("test-plan")
                                    .SetAst("test-ast")
                                    .Build();
                const auto [handler, event] = bootstrap.PingTask(std::move(request));
                UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            }
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myScopeQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myPrivateQueryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPublicQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPrivateQueryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);

            for (const auto& task: tasks) {
                TVector<FederatedQuery::ResultSetMeta> resultSet;
                resultSet.emplace_back();

                auto request = TPingTaskBuilder{}
                                    .SetScope("yandexcloud://test_folder_id_1")
                                    .SetQueryId(task.query_id().value())
                                    .SetOwner("owner")
                                    .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                    .SetResultSetMetas(resultSet)
                                    .SetPlan("test-plan")
                                    .SetAst("test-ast")
                                    .Build();
                const auto [handler, event] = bootstrap.PingTask(std::move(request));
                UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            }
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {

            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);

            for (const auto& task: tasks) {
                TVector<FederatedQuery::ResultSetMeta> resultSet;
                resultSet.emplace_back();

                auto request = TPingTaskBuilder{}
                                    .SetScope("yandexcloud://test_folder_id_1")
                                    .SetQueryId(task.query_id().value())
                                    .SetOwner("owner")
                                    .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                    .SetResultSetMetas(resultSet)
                                    .SetPlan("test-plan")
                                    .SetAst("test-ast")
                                    .Build();
                const auto [handler, event] = bootstrap.PingTask(std::move(request));
                UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            }
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);

            for (const auto& task: tasks) {
                TVector<FederatedQuery::ResultSetMeta> resultSet;
                resultSet.emplace_back();

                auto request = TPingTaskBuilder{}
                                    .SetScope("yandexcloud://test_folder_id_1")
                                    .SetQueryId(task.query_id().value())
                                    .SetOwner("owner")
                                    .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                    .SetResultSetMetas(resultSet)
                                    .SetPlan("test-plan")
                                    .SetAst("test-ast")
                                    .Build();
                const auto [handler, event] = bootstrap.PingTask(std::move(request));
                UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            }
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageListJobsPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 3);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::MANAGE_PUBLIC | TPermissions::QUERY_INVOKE}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.Build(), TPermissions{TPermissions::VIEW_PRIVATE | TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 4);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDescribeJobPermissions) {
    Y_UNIT_TEST(ShouldApplyPermissionEmpty)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        TString myScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeJobId = result.query().meta().last_job_id();
        }

        TString myPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateJobId = result.query().meta().last_job_id();
        }

        TString otherScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeJobId = result.query().meta().last_job_id();
        }

        TString otherPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateJobId = result.query().meta().last_job_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myScopeJobId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myPrivateJobId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherScopeJobId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherPrivateJobId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        TString myScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeJobId = result.query().meta().last_job_id();
        }

        TString myPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateJobId = result.query().meta().last_job_id();
        }

        TString otherScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeJobId = result.query().meta().last_job_id();
        }

        TString otherPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateJobId = result.query().meta().last_job_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myScopeJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myPrivateJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherScopeJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherPrivateJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivate)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        TString myScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeJobId = result.query().meta().last_job_id();
        }

        TString myPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateJobId = result.query().meta().last_job_id();
        }

        TString otherScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeJobId = result.query().meta().last_job_id();
        }

        TString otherPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateJobId = result.query().meta().last_job_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myScopeJobId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myPrivateJobId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherScopeJobId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherPrivateJobId).Build(), TPermissions{TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldApplyPermissionViewPrivatePublic)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        TString myScopeQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeQueryId = result.query_id();
        }

        TString myPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateQueryId = result.query_id();
        }

        TString otherPublicQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPublicQueryId = result.query_id();
        }

        TString otherPrivateQueryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{TPermissions::QUERY_INVOKE | TPermissions::MANAGE_PUBLIC}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateQueryId = result.query_id();
        }

        TString myScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myScopeQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myScopeJobId = result.query().meta().last_job_id();
        }

        TString myPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(myPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            myPrivateJobId = result.query().meta().last_job_id();
        }

        TString otherScopeJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPublicQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherScopeJobId = result.query().meta().last_job_id();
        }

        TString otherPrivateJobId;
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(otherPrivateQueryId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
            otherPrivateJobId = result.query().meta().last_job_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myScopeJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(myPrivateJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherScopeJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(otherPrivateJobId).Build(), TPermissions{TPermissions::VIEW_PUBLIC | TPermissions::VIEW_PRIVATE});
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

} // NFq
