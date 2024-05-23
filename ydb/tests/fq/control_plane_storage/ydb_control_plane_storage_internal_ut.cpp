#include "ydb_test_bootstrap.h"

#include <util/datetime/base.h>

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

static const TString TestScope = "yandexcloud://test_folder_id_1";

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageWriteResultData) {
    Y_UNIT_TEST(ShouldValidateWrite)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.Build());
            UNIT_ASSERT(event->Issues);
        }
    }

    Y_UNIT_TEST(ShouldValidateRead)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: query_id's length is not in [1; 1024], code: 1003\n");
        }
    }

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
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultId(queryId).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageGetTask) {
    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.SetOwner("").Build());
            UNIT_ASSERT(event->Issues);
        }
    }

    Y_UNIT_TEST(ShouldWorkWithEmptyPending)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT(event->Record.tasks().empty());
        }
    }

    Y_UNIT_TEST(ShouldBatchingGetTasks)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        const size_t batchSize = 3;
        for (size_t i = 0; i < batchSize; ++i) {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(event->Record.tasks().size(), batchSize);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStoragePingTask) {
    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [handler, event] = bootstrap.PingTask(TPingTaskBuilder{}.Build());
            UNIT_ASSERT(event->Issues);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageNodesHealthCheck) {
    Y_UNIT_TEST(ShouldValidate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [handler, event] = bootstrap.NodesHealthCheck(TNodesHealthCheckBuilder{}.Build());
            UNIT_ASSERT(event->Issues);
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStorageRateLimiter) {
    class TTestBootstrap : public ::NFq::TTestBootstrap {
    public:
        TTestBootstrap(std::string tablePrefix, const NConfig::TControlPlaneStorageConfig& config = {})
            : ::NFq::TTestBootstrap(std::move(tablePrefix), config)
        {
            NActors::TActorId quotaServiceActor = Runtime->Register(NFq::CreateQuotaServiceActor(
                NConfig::TQuotasManagerConfig{},
                Config.GetStorage(),
                YqSharedResources,
                NKikimr::CreateYdbCredentialsProviderFactory,
                MakeIntrusive<NMonitoring::TDynamicCounters>(),
                {
                    TQuotaDescription(SUBJECT_TYPE_CLOUD, QUOTA_CPU_PERCENT_LIMIT, 200, 3200)
                }));
            Runtime->RegisterService(NFq::MakeQuotaServiceActorId(quotaServiceActor.NodeId()), quotaServiceActor);

            // Create resource for cloud (in normal system it is created by proxy)
            {
                NActors::TActorId edge = Runtime->AllocateEdgeActor();
                Runtime->Send(new IEventHandle(NFq::RateLimiterControlPlaneServiceId(), edge, new TEvRateLimiter::TEvCreateResource("mock_cloud", 42000)));
                auto response = Runtime->GrabEdgeEvent<TEvRateLimiter::TEvCreateResourceResponse>();
                UNIT_ASSERT_C(response->Success, response->Issues.ToOneLineString());
            }
        }
    };

    Y_UNIT_TEST(ShouldValidateCreate)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [handler, event] = bootstrap.CreateRateLimiterResource(TCreateRateLimiterResourceBuilder{}.SetScope("scope").SetTenant("tenant").Build());
            UNIT_ASSERT(event->Issues);
        }

        {
            const auto [handler, event] = bootstrap.CreateRateLimiterResource(TCreateRateLimiterResourceBuilder{}.SetQueryId("query_id").SetTenant("tenant").Build());
            UNIT_ASSERT(event->Issues);
        }

        {
            const auto [handler, event] = bootstrap.CreateRateLimiterResource(TCreateRateLimiterResourceBuilder{}.SetQueryId("query_id").SetScope("scope").Build());
            UNIT_ASSERT(event->Issues);
        }
    }

    Y_UNIT_TEST(ShouldValidateDelete)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [handler, event] = bootstrap.DeleteRateLimiterResource(TDeleteRateLimiterResourceBuilder{}.SetScope("scope").SetTenant("tenant").Build());
            UNIT_ASSERT(event->Issues);
        }

        {
            const auto [handler, event] = bootstrap.DeleteRateLimiterResource(TDeleteRateLimiterResourceBuilder{}.SetQueryId("query_id").SetTenant("tenant").Build());
            UNIT_ASSERT(event->Issues);
        }

        {
            const auto [handler, event] = bootstrap.DeleteRateLimiterResource(TDeleteRateLimiterResourceBuilder{}.SetQueryId("query_id").SetScope("scope").Build());
            UNIT_ASSERT(event->Issues);
        }
    }

    Y_UNIT_TEST(ShouldCreateRateLimiterResource)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT(event->Record.tasks_size());
        }

        {
            const auto [handler, event] = bootstrap.CreateRateLimiterResource(TCreateRateLimiterResourceBuilder{}.SetQueryId(queryId).SetScope(TestScope).SetTenant(TTestBootstrap::GetDefaultTenantName()).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldDeleteRateLimiterResource)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT(event->Record.tasks_size());
        }

        {
            const auto [handler, event] = bootstrap.CreateRateLimiterResource(TCreateRateLimiterResourceBuilder{}.SetQueryId(queryId).SetScope(TestScope).SetTenant(TTestBootstrap::GetDefaultTenantName()).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.DeleteRateLimiterResource(TDeleteRateLimiterResourceBuilder{}.SetQueryId(queryId).SetScope(TestScope).SetTenant(TTestBootstrap::GetDefaultTenantName()).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }
    }
}

Y_UNIT_TEST_SUITE(TYdbControlPlaneStoragePipeline) {
    Y_UNIT_TEST(ShouldCheckSimplePipeline)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString connectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            connectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("test_connection_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(connectionId).SetName("test_binding_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        TString queryId;
        TInstant deadline;
        const TString resId = "result_id00";
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(!tasks.empty());
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_id().value(), queryId);
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(task.user_token(), "xxx");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultId(resId).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());

        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetResultId(resId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_1");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::COMPLETED);
            UNIT_ASSERT_EQUAL(result.query().result_set_meta_size(), 1);
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows(0).int64_value(), 1);
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            UNIT_ASSERT_EQUAL(result.job(0).query_meta().status(), FederatedQuery::QueryMeta::COMPLETED);
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build(), TPermissions{}, TestScope, "test_user@staff", "token2");
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(!tasks.empty());
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(task.user_token(), "token2");
            // TBD query = task.Query;
        }
    }

    Y_UNIT_TEST(ShouldIncrementGeneration)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetText("select ListSkip(ListFromRange(0,10*1000*1000),1);").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.generation(), 1);
        }
    }

    Y_UNIT_TEST(ShouldCheckStopModifyRun)
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
            // TBD UNIT_ASSERT_VALUES_EQUAL(task.Internal.cloud_id(), "mock_cloud");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner("owner")
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT_EQUAL(event->Record.action(), FederatedQuery::ABORT);
            UNIT_ASSERT(event->Record.has_expired_at());
        }

        {
            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner("owner")
                                .SetStatus(FederatedQuery::QueryMeta::ABORTED_BY_USER)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT_EQUAL(event->Record.action(), FederatedQuery::ABORT);
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner("owner")
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT_EQUAL(event->Record.action(), FederatedQuery::QUERY_ACTION_UNSPECIFIED);
        }
    }

    Y_UNIT_TEST(ShouldCheckAbortInTerminatedState)
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
            // TBD UNIT_ASSERT_VALUES_EQUAL(task.Internal.cloud_id(), "mock_cloud");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner("owner")
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT_EQUAL(event->Record.action(), FederatedQuery::ABORT);
            UNIT_ASSERT(event->Record.has_expired_at());
        }

        {
            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner("owner")
                                .SetStatus(FederatedQuery::QueryMeta::ABORTED_BY_USER)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            UNIT_ASSERT_EQUAL(event->Record.action(), FederatedQuery::ABORT);
        }

        {
            const auto [result, issues] = bootstrap.ControlQuery(TControlQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
    }

    Y_UNIT_TEST(ShouldCheckJobMeta)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
                                .SetScope(TestScope)
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
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_1");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::COMPLETED);
            UNIT_ASSERT_EQUAL(result.query().result_set_meta_size(), 1);
            UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(result.query().meta().started_at()), TInstant::Zero());
            UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(result.query().meta().finished_at()), TInstant::Zero());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetStatedAt(TInstant::Now())
                                .SetFinishedAt(TInstant::Now() + TDuration::Seconds(1))
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_2");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::COMPLETED);
            UNIT_ASSERT_UNEQUAL(NProtoInterop::CastFromProto(result.query().meta().started_at()), NProtoInterop::CastFromProto(result.query().meta().finished_at()));
        }

        {
            const auto [result, issues] = bootstrap.ListJobs(TListJobsBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 2);
            UNIT_ASSERT_UNEQUAL(result.job(0).meta().id(), result.job(1).meta().id());
            auto createdJob0 = result.job(0).meta().created_at();
            auto createdJob1 = result.job(1).meta().created_at();
            UNIT_ASSERT_UNEQUAL(NProtoInterop::CastFromProto(createdJob0), NProtoInterop::CastFromProto(createdJob1));
            auto modifiedJob0 = result.job(0).meta().modified_at();
            auto modifiedJob1 = result.job(1).meta().modified_at();
            UNIT_ASSERT_UNEQUAL(NProtoInterop::CastFromProto(modifiedJob0), NProtoInterop::CastFromProto(modifiedJob1));
            UNIT_ASSERT_EQUAL(result.job(0).meta().created_by(), result.job(1).meta().created_by());
            UNIT_ASSERT_EQUAL(result.job(0).meta().modified_by(), result.job(1).meta().modified_by());
            UNIT_ASSERT_EQUAL(result.job(0).meta().revision(), result.job(1).meta().revision());
        }
    }

    Y_UNIT_TEST(ShouldCheckClearFields)
    {
        constexpr auto tinyStats = "{\"Graph=0\":{}}";

        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetIssues(NYql::TIssues{MakeErrorIssue(TIssuesIds::BAD_REQUEST, "failed issue")})
                                .SetTransientIssues(NYql::TIssues{MakeErrorIssue(TIssuesIds::BAD_REQUEST, "failed transient issue")})
                                .SetStatistics(tinyStats)
                                .SetPlan("test plan")
                                .SetStatus(FederatedQuery::QueryMeta::FAILED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_1");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::FAILED);
            UNIT_ASSERT_EQUAL(result.query().result_set_meta_size(), 1);
            UNIT_ASSERT_EQUAL(result.query().issue_size(), 1);
            UNIT_ASSERT_EQUAL(result.query().transient_issue_size(), 1);
            UNIT_ASSERT_EQUAL(result.query().statistics().json(), tinyStats);
            UNIT_ASSERT_EQUAL(result.query().plan().json(), "test plan");
            UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(result.query().meta().started_at()), TInstant::Zero());
            UNIT_ASSERT_EQUAL(NProtoInterop::CastFromProto(result.query().meta().finished_at()), TInstant::Zero());
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_2");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::STARTING);
            UNIT_ASSERT_EQUAL(result.query().result_set_meta_size(), 0);
            UNIT_ASSERT_EQUAL(result.query().issue_size(), 0);
            UNIT_ASSERT_EQUAL(result.query().transient_issue_size(), 0);
            UNIT_ASSERT(!result.query().has_statistics());
            UNIT_ASSERT(!result.query().has_plan());
        }
    }

    Y_UNIT_TEST(ShouldCheckNodesHealthCheck)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        {
            auto request = TNodesHealthCheckBuilder{}
                .SetTenantName("Tenant")
                .SetNodeId(100500)
                .SetActiveWorkers(100600)
                .SetInstanceId("SomeInstanceId")
                .SetHostName("host")
                .Build();
            const auto [handler, event] = bootstrap.NodesHealthCheck(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& peers = event->Record.nodes();
            UNIT_ASSERT_VALUES_EQUAL(peers.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peers[0].node_id(), 100500);
            UNIT_ASSERT_VALUES_EQUAL(peers[0].instance_id(), "SomeInstanceId");
            UNIT_ASSERT_VALUES_EQUAL(peers[0].hostname(), "host");
        }

    }

    Y_UNIT_TEST(ShouldCheckResultSetMeta)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        const TString resId = "result_id00";
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultId(resId).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());

        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetResultId(resId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_1");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::COMPLETED);
            UNIT_ASSERT_EQUAL(result.query().result_set_meta_size(), 1);
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows(0).int64_value(), 1);
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultId(resId).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());

        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetResultId(resId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_2");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::COMPLETED);
            UNIT_ASSERT_VALUES_EQUAL(result.query().result_set_meta_size(), 1);
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows(0).int64_value(), 1);
        }
    }

    Y_UNIT_TEST(ShouldCheckRemovingOldResultSet)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        const TString resId = "result_id00";
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultId(resId).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());

        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetResultId(resId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_1");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::COMPLETED);
            UNIT_ASSERT_EQUAL(result.query().result_set_meta_size(), 1);
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows(0).int64_value(), 1);
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
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
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_2");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::RUNNING);
            UNIT_ASSERT_VALUES_EQUAL(result.query().result_set_meta_size(), 1);
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Result doesn't exist");
        }
    }

    Y_UNIT_TEST(ShouldCheckPrioritySelectionEntities)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());

        }

        TString privateConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            privateConnectionId = result.connection_id();
        }

        TString publicConnectionId;
        {
            const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("test_connection_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            publicConnectionId = result.connection_id();
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(publicConnectionId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(privateConnectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }
        {
            const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(publicConnectionId).SetName("test_binding_name_2").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        TString queryId;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 2);

            UNIT_ASSERT_UNEQUAL(task.connection(0).content().acl().visibility(),
                                task.connection(1).content().acl().visibility());

            UNIT_ASSERT_UNEQUAL(task.binding(0).content().acl().visibility(),
                                task.binding(1).content().acl().visibility());
        }
    }

    Y_UNIT_TEST(ShouldSkipBindingIfDisabledConnection)
    {
        {
            TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

            {
                const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.Build());
                UNIT_ASSERT_C(!issues, issues.ToString());

            }
            TString privateConnectionId;
            {
                const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
                UNIT_ASSERT_C(!issues, issues.ToString());
                privateConnectionId = result.connection_id();
            }
            TString publicConnectionId;
            {
                const auto [result, issues] = bootstrap.CreateConnection(TCreateConnectionBuilder{}.SetName("test_connection_name_2").Build());
                UNIT_ASSERT_C(!issues, issues.ToString());
                publicConnectionId = result.connection_id();
            }
            {
                const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(publicConnectionId).Build());
                UNIT_ASSERT_C(!issues, issues.ToString());
            }
            {
                const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(privateConnectionId).SetVisibility(FederatedQuery::Acl::PRIVATE).Build());
                UNIT_ASSERT_C(!issues, issues.ToString());
            }
            {
                const auto [result, issues] = bootstrap.CreateBinding(TCreateBindingBuilder{}.SetConnectionId(publicConnectionId).SetName("test_binding_name_2").Build());
                UNIT_ASSERT_C(!issues, issues.ToString());
            }
        }

        {
            NConfig::TControlPlaneStorageConfig config;
            config.SetDisableCurrentIam(true);
            TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};

            TString queryId;
            {
                const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
                UNIT_ASSERT_C(!issues, issues.ToString());
                queryId = result.query_id();
            }

            {
                const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
                UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
                const auto& tasks = event->Record.tasks();
                UNIT_ASSERT(tasks.size() > 0);
                const auto& task = tasks[tasks.size() - 1];
                UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
                UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
                UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            }

            {
                TVector<FederatedQuery::ResultSetMeta> resultSet;
                resultSet.emplace_back();

                auto request = TPingTaskBuilder{}
                                    .SetScope(TestScope)
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
                const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
                UNIT_ASSERT_C(!issues, issues.ToString());
            }

            {
                const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
                UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
                const auto& tasks = event->Record.tasks();
                UNIT_ASSERT(tasks.size() > 0);
                const auto& task = tasks[tasks.size() - 1];
                UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
                UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
                UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            }
        }
    }

    Y_UNIT_TEST(ShouldCheckResultSetLimit)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        TInstant deadline;
        const TString resId = "result_id00";
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.user_token(), "xxx");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            Ydb::ResultSet resultSet;
            for (size_t i = 0; i < 5; i++) {
                auto& value = *resultSet.add_rows();
                value.set_int64_value(i);
            }
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultSet(resultSet).SetResultId(resId).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());

        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetResultId(resId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.DescribeQuery(TDescribeQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().content().name(), "test_query_name_1");
            UNIT_ASSERT_EQUAL(result.query().meta().status(), FederatedQuery::QueryMeta::COMPLETED);
            UNIT_ASSERT_EQUAL(result.query().result_set_meta_size(), 1);
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetLimit(2).SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows_size(), 2);
        }
    }

    Y_UNIT_TEST(ShouldCheckGetResultDataRequest)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};
        TString queryId;
        TInstant deadline;
        const TString resId = "result_id00";
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.user_token(), "xxx");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            Ydb::ResultSet resultSet;
            for (size_t i = 0; i < 1001; i++) {
                auto& value = *resultSet.add_rows();
                value.set_int64_value(i);
            }
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultSet(resultSet).SetResultId(resId).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());

        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetResultId(resId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetLimit(1000).SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows_size(), 1000);
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetLimit(1001).SetQueryId(queryId).Build());
            UNIT_ASSERT_VALUES_EQUAL(issues.ToString(), "<main>: Error: limit's value is not [1; 1000], code: 1003\n");
        }
    }

    Y_UNIT_TEST(ShouldRetryQuery)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT_C(tasks.size() == 0, "Task should not be returned");
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetResignQuery(true)
                                .SetStatusCode(NYql::NDqProto::StatusIds::UNAVAILABLE)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
        }
    }

    Y_UNIT_TEST(ShouldCheckAst)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetPlan("test-plan")
                                .SetAst("test-ast")
                                .SetDeadline(deadline)
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
            UNIT_ASSERT_VALUES_EQUAL(result.query().ast().data(), "test-ast");
            UNIT_ASSERT_VALUES_EQUAL(result.query().plan().json(), "test-plan");
            UNIT_ASSERT_VALUES_EQUAL(NProtoInterop::CastFromProto(result.query().meta().result_expire_at()), deadline);
        }

        TString jobId;
        {
            auto request = TListJobsBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.ListJobs(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            jobId = result.job(0).meta().id();
        }

        {
            auto request = TDescribeJobBuilder{}
                                .SetJobId(jobId)
                                .Build();
            const auto [result, issues] = bootstrap.DescribeJob(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job().ast().data(), "test-ast");
            UNIT_ASSERT_VALUES_EQUAL(result.job().plan().json(), "test-plan");
        }
    }

    Y_UNIT_TEST(ShouldCheckAstClear)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetPlan("test-plan")
                                .SetAst("test-ast")
                                .SetDeadline(deadline)
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
            UNIT_ASSERT_VALUES_EQUAL(result.query().ast().data(), "test-ast");
            UNIT_ASSERT_VALUES_EQUAL(result.query().plan().json(), "test-plan");
            UNIT_ASSERT_VALUES_EQUAL(NProtoInterop::CastFromProto(result.query().meta().result_expire_at()), deadline);
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            // TBD query = task.Query;
            // TBD owner = event->Owner;
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            auto request = TDescribeQueryBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.DescribeQuery(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query().ast().data(), "");
            UNIT_ASSERT_VALUES_EQUAL(result.query().plan().json(), "");
            UNIT_ASSERT_VALUES_EQUAL(NProtoInterop::CastFromProto(result.query().meta().result_expire_at()), TInstant::Zero());
        }
    }

    Y_UNIT_TEST(ShouldCheckAutomaticTtl)
    {
        NConfig::TControlPlaneStorageConfig config{};
        *config.MutableAutomaticQueriesTtl() = "0s";
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetAutomatic(true).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
            UNIT_ASSERT_LE_C(deadline, TInstant::Now(), "TTL more than current time");
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        const TString resId = "result_id00";
        {
            Ydb::ResultSet resultSet;
            for (size_t i = 0; i < 5; i++) {
                auto& value = *resultSet.add_rows();
                value.set_int64_value(i);
            }
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultSet(resultSet).SetResultId(resId).SetDeadline(deadline).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            auto request = TListQueriesBuilder{}
                                .Build();
            const auto [result, issues] = bootstrap.ListQueries(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 0);
        }

        {
            auto request = TListJobsBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.ListJobs(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 0);
        }

        {
            const auto result = bootstrap.GetTable(QUERIES_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_LE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL more than current time");
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            }
        }

        {
            const auto result = bootstrap.GetTable(JOBS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_LE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL more than current time");
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            }
        }

        {
            const auto result = bootstrap.GetTable(RESULT_SETS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_LE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL more than current time");
            }
        }
    }

    Y_UNIT_TEST(ShouldCheckNotAutomaticTtl)
    {
        NConfig::TControlPlaneStorageConfig config{};
        *config.MutableAutomaticQueriesTtl() = "0s";
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};

        const TString resId = "result_id00";
        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetAutomatic(false).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
            UNIT_ASSERT_GE_C(deadline, TInstant::Now(), "TTL less than current time");
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .SetResultId(resId)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            Ydb::ResultSet resultSet;
            for (size_t i = 0; i < 5; i++) {
                auto& value = *resultSet.add_rows();
                value.set_int64_value(i);
            }
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultSet(resultSet).SetResultId(resId).SetDeadline(deadline).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            auto request = TListQueriesBuilder{}
                                .Build();
            const auto [result, issues] = bootstrap.ListQueries(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
            UNIT_ASSERT_LE(NProtoInterop::CastFromProto(result.query(0).meta().expire_at()), TInstant::Now());
        }

        {
            auto request = TListJobsBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.ListJobs(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
            UNIT_ASSERT_LE(NProtoInterop::CastFromProto(result.job(0).expire_at()), TInstant::Now());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.result_set().rows_size(), 5);
        }

        {
            const auto result = bootstrap.GetTable(QUERIES_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL_C(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), false, *parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp());
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            } else {
                UNIT_ASSERT("Row count equal 0");
            }
        }

        {
            const auto result = bootstrap.GetTable(JOBS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL_C(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), false, *parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp());
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            } else {
                UNIT_ASSERT("Row count equal 0");
            }
        }

        {
            const auto result = bootstrap.GetTable(RESULT_SETS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_GE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL less than current time");
            }
        }
    }

    Y_UNIT_TEST(ShouldCheckChangeAutomaticTtl)
    {
        NConfig::TControlPlaneStorageConfig config{};
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.SetAutomatic(true).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
            UNIT_ASSERT_GE_C(deadline, TInstant::Now(), TString("TTL less than current time"));
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
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
            auto request = TListQueriesBuilder{}
                                .Build();
            const auto [result, issues] = bootstrap.ListQueries(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
        }

        {
            auto request = TListJobsBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.ListJobs(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 1);
        }

        {
            const auto result = bootstrap.GetTable(QUERIES_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_GE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL less than current time");
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            }
        }

        {
            const auto result = bootstrap.GetTable(JOBS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_GE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL less than current time");
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            }
        }

        {
            const auto result = bootstrap.GetTable(RESULT_SETS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_GE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL less than current time");
            }
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).SetAutomatic(false).Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_2");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
            UNIT_ASSERT_GE_C(deadline, TInstant::Now(), "TTL less than current time");
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
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
            auto request = TListQueriesBuilder{}
                                .Build();
            const auto [result, issues] = bootstrap.ListQueries(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
        }

        {
            auto request = TListJobsBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.ListJobs(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 2);
        }

        {
            const auto result = bootstrap.GetTable(QUERIES_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL_C(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), false, *parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp());
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            } else {
                UNIT_ASSERT("Row count equal 0");
            }
        }

        {
            const auto result = bootstrap.GetTable(JOBS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 2);
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL_C(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), false, *parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp());
            }
        }

        {
            const auto [result, issues] = bootstrap.ModifyQuery(TModifyQueryBuilder{}.SetQueryId(queryId).SetAutomatic(true).SetName("test_query_name_3").Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_3");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
            UNIT_ASSERT_GE_C(deadline, TInstant::Now(), "TTL less than current time");
        }

        {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
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
            auto request = TListQueriesBuilder{}
                                .Build();
            const auto [result, issues] = bootstrap.ListQueries(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.query_size(), 1);
        }

        {
            auto request = TListJobsBuilder{}
                                .SetQueryId(queryId)
                                .Build();
            const auto [result, issues] = bootstrap.ListJobs(std::move(request));
            UNIT_ASSERT_C(!issues, issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.job_size(), 3);
        }

        {
            const auto result = bootstrap.GetTable(QUERIES_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_GE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL less than current time");
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            }
        }

        {
            const auto result = bootstrap.GetTable(JOBS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 3);
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_GE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL less than current time");
            }
        }

        {
            const auto result = bootstrap.GetTable(RESULT_SETS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_GE_C(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), TInstant::Now(), "TTL less than current time");
            }
        }
    }

    Y_UNIT_TEST(ShouldCheckResultsTTL)
    {
        NConfig::TControlPlaneStorageConfig config{};
        *config.MutableResultSetsTtl() = "0s";
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

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
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetStatus(FederatedQuery::QueryMeta::COMPLETED)
                                .SetResultSetMetas(resultSet)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        const TString resId = "result_id00";
        {
            Ydb::ResultSet resultSet;
            for (size_t i = 0; i < 5; i++) {
                auto& value = *resultSet.add_rows();
                value.set_int64_value(i);
            }
            const auto [handler, event] = bootstrap.WriteResultData(TWriteResultDataBuilder{}.SetResultSet(resultSet).SetResultId(resId).SetDeadline(deadline).Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [result, issues] = bootstrap.GetResultData(TGetResultDataBuilder{}.SetQueryId(queryId).Build());
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Result removed by TTL, code: 1004");
        }

        {
            const auto result = bootstrap.GetTable(QUERIES_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            if (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(QUERY_ID_COLUMN_NAME).GetOptionalString(), queryId);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(RESULT_SETS_EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(RESULT_SETS_EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), deadline);
                if (parser.TryNextRow()) {
                    UNIT_ASSERT("Row count more than 1");
                }
            }
        }

        {
            const auto result = bootstrap.GetTable(RESULT_SETS_TABLE_NAME);
            UNIT_ASSERT_VALUES_EQUAL(result.Defined(), true);
            NYdb::TResultSetParser parser(*result);
            UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 5);
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(RESULT_ID_COLUMN_NAME).GetOptionalString(), resId);
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp().Defined(), true);
                UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(EXPIRE_AT_COLUMN_NAME).GetOptionalTimestamp(), deadline);
            }
        }
    }

    Y_UNIT_TEST(ShouldSaveTopicConsumers)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        FederatedQuery::Query query;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        // consumers
        Cerr << "Adding first batch of consumers" << Endl;
        {
            auto request = TPingTaskBuilder{}
                .SetScope(TestScope)
                .SetQueryId(queryId)
                .SetOwner(TGetTaskBuilder::DefaultOwner())
                .AddCreatedConsumer("db_id", "/root", "topic", "consumer", "endpoint", true)
                .AddCreatedConsumer("db_id", "/root", "topic", "consumer2", "endpoint", true)
                .SetDeadline(deadline)
                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        // moar consumers (storage is expected to merge them together)
        Cerr << "Adding second batch of consumers" << Endl;
        {
            auto request = TPingTaskBuilder{}
                .SetScope(TestScope)
                .SetQueryId(queryId)
                .SetOwner(TGetTaskBuilder::DefaultOwner())
                .AddCreatedConsumer("db_id", "/root", "topic", "consumer", "endpoint", true) // the same
                .AddCreatedConsumer("db_id", "/root", "other_topic", "other_consumer", "endpoint", true) // new
                .SetDeadline(deadline)
                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        // resign
        Cerr << "Fail query" << Endl;
        {
            auto request = TPingTaskBuilder{}
                .SetScope(TestScope)
                .SetQueryId(queryId)
                .SetOwner(TGetTaskBuilder::DefaultOwner())
                .SetResignQuery(true)
                .SetStatusCode(NYql::NDqProto::StatusIds::UNAVAILABLE)
                .SetDeadline(deadline)
                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        Cerr << "Getting task after resign" << Endl;
        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL_C(task.created_topic_consumers().size(), 3, "Consumers size");

            auto checkConsumer = [&](size_t index, const TString& topic, const TString& consumer) {
                UNIT_ASSERT_VALUES_EQUAL(task.created_topic_consumers()[index].database_id(), "db_id");
                UNIT_ASSERT_VALUES_EQUAL(task.created_topic_consumers()[index].database(), "/root");
                UNIT_ASSERT_VALUES_EQUAL(task.created_topic_consumers()[index].topic_path(), topic);
                UNIT_ASSERT_VALUES_EQUAL(task.created_topic_consumers()[index].consumer_name(), consumer);
                UNIT_ASSERT_VALUES_EQUAL(task.created_topic_consumers()[index].cluster_endpoint(), "endpoint");
                UNIT_ASSERT(task.created_topic_consumers()[index].use_ssl());
            };

            checkConsumer(0, "other_topic", "other_consumer");
            checkConsumer(1, "topic", "consumer");
            checkConsumer(2, "topic", "consumer2");
        }
    }

    Y_UNIT_TEST(ShouldSaveDqGraphs)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            auto request = TPingTaskBuilder{}
                .SetScope(TestScope)
                .SetQueryId(queryId)
                .SetOwner(TGetTaskBuilder::DefaultOwner())
                .AddDqGraph("Plan1")
                .AddDqGraph("Plan2")
                .SetDqGraphIndex(1)
                .SetResignQuery(true)
                .SetStatusCode(NYql::NDqProto::StatusIds::UNAVAILABLE)
                .SetDeadline(deadline)
                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL_C(task.dq_graph().size(), 2, task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.dq_graph(0), "Plan1", task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.dq_graph(1), "Plan2", task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.dq_graph_index(), 1, task);
        }
    }

    Y_UNIT_TEST(ShouldSaveResultSetMetas)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        TString queryId;
        TInstant deadline;
        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            FederatedQuery::ResultSetMeta meta1;
            meta1.add_column()->set_name("column1");
            FederatedQuery::ResultSetMeta meta2;
            meta2.add_column()->set_name("column21");
            meta2.add_column()->set_name("column22");

            TVector<FederatedQuery::ResultSetMeta> metas;
            metas.emplace_back(meta1);
            metas.emplace_back(meta2);

            auto request = TPingTaskBuilder{}
                .SetScope(TestScope)
                .SetQueryId(queryId)
                .SetOwner(TGetTaskBuilder::DefaultOwner())
                .SetResultSetMetas(metas)
                .SetResignQuery(true)
                .SetStatusCode(NYql::NDqProto::StatusIds::UNAVAILABLE)
                .SetDeadline(deadline)
                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL_C(task.result_set_meta().size(), 2, task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.result_set_meta(0).column_size(), 1, task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.result_set_meta(0).column(0).name(), "column1", task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.result_set_meta(1).column_size(), 2, task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.result_set_meta(1).column(0).name(), "column21", task);
            UNIT_ASSERT_VALUES_EQUAL_C(task.result_set_meta(1).column(1).name(), "column22", task);
        }
    }

    Y_UNIT_TEST(ShouldCheckDisableCurrentIamGetTask)
    {
        NConfig::TControlPlaneStorageConfig config;
        config.SetDisableCurrentIam(true);
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__, config};

        {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() > 0);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.user_token(), "");
        }
    }

    Y_UNIT_TEST(ShouldReturnPartialBatchForGetTask)
    {
        TTestBootstrap bootstrap{__PRETTY_FUNCTION__};

        constexpr int COUNT = 2;
        TVector<TString> queryIds(COUNT);
        TInstant deadline;

        for (auto& queryId: queryIds) {
            const auto [result, issues] = bootstrap.CreateQuery(TCreateQueryBuilder{}.Build());
            UNIT_ASSERT_C(!issues, issues.ToString());
            queryId = result.query_id();
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() == COUNT);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
            deadline = NProtoInterop::CastFromProto(task.deadline());
        }

        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT_C(tasks.size() == 0, "Task should not be returned");
        }

        for (auto& queryId: queryIds) {
            TVector<FederatedQuery::ResultSetMeta> resultSet;
            resultSet.emplace_back();

            TIntrusivePtr<NYql::TIssue> topIssue = MakeIntrusive<NYql::TIssue>("error: 0");
            if (queryId == queryIds.front()) {
                for (int i = 1; i < 101; i++) {
                    TIntrusivePtr<NYql::TIssue> newIssue = MakeIntrusive<NYql::TIssue>(TStringBuilder{} << "error: " << i);
                    newIssue->AddSubIssue(topIssue);
                    topIssue = newIssue;
                }
            }
            auto request = TPingTaskBuilder{}
                                .SetScope(TestScope)
                                .SetQueryId(queryId)
                                .SetOwner(TGetTaskBuilder::DefaultOwner())
                                .SetResignQuery(true)
                                .SetIssues(NYql::TIssues{{*topIssue}})
                                .SetStatusCode(NYql::NDqProto::StatusIds::UNAVAILABLE)
                                .SetDeadline(deadline)
                                .Build();
            const auto [handler, event] = bootstrap.PingTask(std::move(request));
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
        }


        {
            const auto [handler, event] = bootstrap.GetTask(TGetTaskBuilder{}.Build());
            UNIT_ASSERT_C(!event->Issues, event->Issues.ToString());
            const auto& tasks = event->Record.tasks();
            UNIT_ASSERT(tasks.size() == COUNT - 1);
            const auto& task = tasks[tasks.size() - 1];
            UNIT_ASSERT_VALUES_EQUAL(task.query_name(), "test_query_name_1");
            UNIT_ASSERT_VALUES_EQUAL(task.connection().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(task.binding().size(), 0);
        }
    }
}

} // NFq
