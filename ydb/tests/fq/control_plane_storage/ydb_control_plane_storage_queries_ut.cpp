#include "ydb_test_bootstrap.h"

#include <util/datetime/base.h>

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageCreateQuery) {
    Y_UNIT_TEST(ShouldSucccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("test_query_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "");
        UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: user is empty, code: 1003\n");
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        const auto request = TCreateQueryBuilder{}.SetIdempotencyKey("aba").Build();
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        TString queryIdRetry;
        {
            const auto [result, issues] = bootstrap.CreateQuery(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryIdRetry = result.query_id();
        }

        UNIT_ASSERT_VALUES_EQUAL(queryId, queryIdRetry);
    }

    Y_UNIT_TEST(ShouldCreateJob)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.job(0).query_name(), TCreateQueryBuilder{}.Build().content().name());
        }
    }

    Y_UNIT_TEST(ShouldCheckListJobs)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetAutomatic(true).SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("test_query_name_1").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.job(0).query_name(), TCreateQueryBuilder{}.Build().content().name());
            UNIT_ASSERT_VALUES_EQUAL(result.job(0).automatic(), true);
            UNIT_ASSERT_VALUES_EQUAL(FederatedQuery::Acl_Visibility_Name(result.job(0).visibility()), "SCOPE");
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldListJobsByQuery)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldListJobsCreatedByMe)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            auto request = TListJobsBuilder{}.Build();
            request.mutable_filter()->set_created_by_me(true);
            const auto [result, issues] = bootstrap.ListJobs(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldCheckDescribeJob)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("test describe job").SetAutomatic(true).SetVisibility(FederatedQuery::Acl::SCOPE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        TString jobId;
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            jobId = result.job(0).meta().id();
        }
        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(jobId ).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job().query_name(), "test describe job");
            UNIT_ASSERT_VALUES_EQUAL(result.job().meta().id(), jobId);
            UNIT_ASSERT_VALUES_EQUAL(result.job().query_meta().common().id(), queryId);
            UNIT_ASSERT_VALUES_EQUAL(result.job().automatic(), true);
            UNIT_ASSERT_VALUES_EQUAL(FederatedQuery::Acl_Visibility_Name(result.job().acl().visibility()), "SCOPE");
        }
    }

    Y_UNIT_TEST(ShouldCheckDescribeIncorrectJob)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId("test-test").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckDescribeJobIncorrectVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("test describe job").SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user1");
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        TString jobId;
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user1");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            jobId = result.job(0).meta().id();
        }
        {
            const auto [result, issues] = bootstrap.DescribeJob(TDescribeJobBuilder{}.SetJobId(jobId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Job does not exist or permission denied. Please check the job id or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldSaveQuery)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 0);
        }
    }

    Y_UNIT_TEST(ShouldCheckQueryName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("query name test").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.job(0).query_name(), "query name test");
        }
    }

    Y_UNIT_TEST(ShouldCheckAvailableConnections)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.AddAvailableStreamingConnection("OBJECT_STORAGE");
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            // Adds data stream connection
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        TString queryId;
        { // make v1 query
            auto query = TCreateQueryBuilder{}
                .SetName("query name test")
                .SetType(FederatedQuery::QueryContent::QueryType::QueryContent_QueryType_STREAMING)
                .Build();

            const auto [result, issues] = bootstrap.CreateQuery(std::move(query));
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        { // v1 query has no connections
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "query name test");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
        }

        { // make v2 query
            auto query = TCreateQueryBuilder{}
                .SetName("query name test")
                .SetType(FederatedQuery::QueryContent::QueryType::QueryContent_QueryType_ANALYTICS)
                .Build();

            const auto [result, issues] = bootstrap.CreateQuery(std::move(query));
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        { // v2 query has connections
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "query name test");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 1);
        }
    }
}


Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageListQueries) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetAutomatic(true).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
            UNIT_ASSERT_EQUAL(result.query(0).visibility(), FederatedQuery::Acl::SCOPE);
            UNIT_ASSERT(result.query(0).automatic());
        }
    }

    Y_UNIT_TEST(ShouldPageToken)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListQueries(TListQueriesBuilder{}.Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.query_size(), 10);

            const auto [result2, issues2] = bootstrap.ListQueries(TListQueriesBuilder{}.SetPageToken(result1.next_page_token()).Build());
            UNIT_ASSERT_C(!issues2, issues2.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result2.query_size(), 5);
        }
    }

    Y_UNIT_TEST(ShouldEmptyPageToken)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 20; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListQueries(TListQueriesBuilder{}.Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.query_size(), 10);

            const auto [result2, issues2] = bootstrap.ListQueries(TListQueriesBuilder{}.SetPageToken(result1.next_page_token()).Build());
            UNIT_ASSERT_C(!issues2, issues2.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result2.query_size(), 10);
            UNIT_ASSERT_VALUES_EQUAL(result2.next_page_token(), "");
        }
    }

    Y_UNIT_TEST(ShouldCheckLimit)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result1, issues1] = bootstrap.ListQueries(TListQueriesBuilder{}.SetLimit(20).Build());
            UNIT_ASSERT_C(!issues1, issues1.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result1.query_size(), 15);
        }
    }

    Y_UNIT_TEST(ShouldCheckScopeVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 15; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i)).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.SetLimit(20).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 15);
        }
    }

    Y_UNIT_TEST(ShouldCheckPrivateVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 3; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 5; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i + 3)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 3);
            UNIT_ASSERT_EQUAL(result.query(0).visibility(), FederatedQuery::Acl::PRIVATE);
            UNIT_ASSERT(!result.query(0).automatic());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 5);
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: user is empty, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        for (size_t i = 0; i < 3; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        for (size_t i = 0; i < 5; i++)
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName(ToString(i + 3)).SetVisibility(FederatedQuery::Acl::PRIVATE).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListQueries(TListQueriesBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 8);
        }
    }

    Y_UNIT_TEST(ShouldFilterName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("a").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("Hello").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->set_name("a");
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 2);
        }

        {
            // case insensitive search by name
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->set_name("A");
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 2);
        }

        {
            // case insensitive search by name
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->set_name("heLLo");
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldFilterByMe)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->set_created_by_me(true);
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 2);

        }
    }

    Y_UNIT_TEST(ShouldFilterType)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            FederatedQuery::StreamingDisposition disposition;
            disposition.mutable_fresh();
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("a").SetType(FederatedQuery::QueryContent::STREAMING).SetDisposition(disposition).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->set_query_type(FederatedQuery::QueryContent::STREAMING);
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldFilterMode)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("a").SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->add_mode(FederatedQuery::SAVE);
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldCombineFilters)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("a").SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->add_mode(FederatedQuery::RUN);
            proto.mutable_filter()->set_name("a");
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.query(0).name(), "ab");
        }
    }

    Y_UNIT_TEST(ShouldFilterVisibility)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("a").SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->set_visibility(FederatedQuery::Acl::PRIVATE);
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.query(0).name(), "a");
        }
    }

    Y_UNIT_TEST(ShouldFilterAutomatic)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("a").SetAutomatic(true).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("ab").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("c").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto proto = TListQueriesBuilder{}.Build();
            proto.mutable_filter()->set_automatic(FederatedQuery::AutomaticType::AUTOMATIC);
            const auto [result, issues] = bootstrap.ListQueries(proto);
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.query(0).name(), "a");
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDescribeQuery) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TInstant now = TInstant::Now();
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_1");
            UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(result.query().meta().started_at()), TInstant::Zero());
            UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(result.query().meta().finished_at()), TInstant::Zero());
            UNIT_ASSERT_LE(now, NProtoInterop::CastFromProto(result.query().meta().submitted_at()));
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckExist)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId("abra").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: query_id's length is not in [1; 1024], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_1");
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageGetQueryStatus) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()), FederatedQuery::QueryMeta::ComputeStatus_Name(FederatedQuery::QueryMeta::STARTING));
            UNIT_ASSERT_VALUES_EQUAL(result.meta_revision(), 0);
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckExist)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId("abra").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: query_id's length is not in [1; 1024], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()), FederatedQuery::QueryMeta::ComputeStatus_Name(FederatedQuery::QueryMeta::STARTING));
            UNIT_ASSERT_VALUES_EQUAL(result.meta_revision(), 0);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageDeleteQuery) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckExist)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId("aba").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId("").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: query_id's length is not in [1; 1024], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionFailed)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).SetPreviousRevision(100).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Revision of the query has been changed already. Please restart the request with a new revision, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).SetPreviousRevision(1).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldProhibitDeletionOfRunningQuery)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        TInstant deadline;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            auto request = TPingTaskBuilder{}
                                .SetScope("yandexcloud://test_folder_id_1")
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::RUNNING)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DeleteQuery(TDeleteQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(issues, issues.ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(issues.ToString(), "Can't delete running query", issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageModifyQuery) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_2");
        }

        {
            const auto [result, issues] = bootstrap.GetQueryStatus(TGetQueryStatusBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.meta_revision(), 1);
        }
    }

    Y_UNIT_TEST(ShouldModifyRunningQuery)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::RUN).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            UNIT_ASSERT_VALUES_UNEQUAL(task.job_id().value(), "");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope("yandexcloud://test_folder_id_1")
                                .SetQueryId(queryId)
                                .SetResultId("here")
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(TInstant::Now())
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckPermission)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "test_user2@staff");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckExist)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId("aba").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Query does not exist or permission denied. Please check the id of the query or your access rights, code: 1000\n");
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId("").Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: query_id's length is not in [1; 1024], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckSuperUser)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "super_user@staff");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckWithoutIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_2");
            UNIT_ASSERT_EQUAL(result.query().meta().common().revision(), 2);
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Conversion from status STARTING to STARTING is not possible. Please wait for the query to complete or stop it, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_2");
            UNIT_ASSERT_EQUAL(result.query().meta().common().revision(), 2);
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_2");
            UNIT_ASSERT_EQUAL(result.query().meta().common().revision(), 2);
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionFailed)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetMode(FederatedQuery::SAVE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().content().name(), "test_query_name_2");
        }
    }

    Y_UNIT_TEST(ShouldCheckQueryName)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetName("query name test").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.job(0).query_name(), "query name test");
        }

        FederatedQuery::Query query;
        TString owner;
        TInstant deadline;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "query name test");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope("yandexcloud://test_folder_id_1")
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).SetName("query name test 2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(result.job(0).query_name(), "query name test 2");
            UNIT_ASSERT_VALUES_EQUAL(result.job(1).query_name(), "query name test");
        }
    }

    Y_UNIT_TEST(ShouldCheckAvailableConnections)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.AddAvailableStreamingConnection("OBJECT_STORAGE");
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};
        {
            // Adds data stream connection
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        TString queryId;
        { // make v1 query
            auto query = TCreateQueryBuilder{}
                .SetName("query name test")
                .SetMode(FederatedQuery::SAVE)
                .SetType(FederatedQuery::QueryContent::STREAMING)
                .Build();

            const auto [result, issues] = bootstrap.CreateQuery(std::move(query));
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            auto query = TModifyQueryBuilder{}
                .SetQueryId(queryId)
                .SetType(FederatedQuery::QueryContent::STREAMING)
                .SetName("modified query name")
                .Build();
            const auto [result, issues] = bootstrap.ModifyQuery(query);
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        { // v1 query has no connections
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "modified query name");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
        }

        { // make v2 query
            auto query = TCreateQueryBuilder{}
                .SetName("query name test")
                .SetMode(FederatedQuery::SAVE)
                .SetType(FederatedQuery::QueryContent::ANALYTICS)
                .Build();

            const auto [result, issues] = bootstrap.CreateQuery(std::move(query));
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            auto query = TModifyQueryBuilder{}
                .SetQueryId(queryId)
                .SetType(FederatedQuery::QueryContent::ANALYTICS)
                .SetName("modified query name")
                .Build();
            const auto [result, issues] = bootstrap.ModifyQuery(query);
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        { // v2 query has connections
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "modified query name");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 1);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageControlQuery) {
    Y_UNIT_TEST(ShouldSucccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::STARTING);
            UNIT_ASSERT_VALUES_EQUAL(result.query().meta().has_aborted_by(), false);
        }
        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).SetAction(FederatedQuery::ABORT).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::ABORTING_BY_USER);
            UNIT_ASSERT_VALUES_EQUAL(result.query().meta().has_aborted_by(), true);
            UNIT_ASSERT_VALUES_EQUAL(result.query().meta().aborted_by(), "test_user@staff");
        }
        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            UNIT_ASSERT_EQUAL(result.job(0).query_meta().status(), FederatedQuery::QueryMeta::ABORTING_BY_USER);
        }
    }

    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, "yandexcloud://test_folder_id_1", "");
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: user is empty, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckIdempotencyKey)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        const auto request = TCreateQueryBuilder{}.Build();
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(request);
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).SetIdempotencyKey("aba").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionFailed)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).SetPreviousRevision(100).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: Revision of the query has been changed already. Please restart the request with a new revision, code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldCheckPreviousRevisionSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).SetPreviousRevision(1).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageGetResult) {
    Y_UNIT_TEST(ShouldSuccess)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        FederatedQuery::Query query;
        TInstant deadline;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope("yandexcloud://test_folder_id_1")
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldEmpty)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        TInstant deadline;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope("yandexcloud://test_folder_id_1")
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::RUNNING)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Result doesn't exist");
        }
    }
}

} // NFq
