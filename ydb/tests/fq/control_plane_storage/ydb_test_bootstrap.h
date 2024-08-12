#pragma once

#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/message_builders.h>
#include <ydb/core/fq/libs/control_plane_storage/schema.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/init/init.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/fq/libs/init/init.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/rate_limiter/control_plane_service/rate_limiter_control_plane_service.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/retry/retry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/system/env.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

//////////////////////////////////////////////////////

using TRuntimePtr = std::unique_ptr<TTestActorRuntime>;

inline void AssertNoFullScan(TDebugInfoPtr debugInfo) {
    if (debugInfo) {
        for (const auto& item: *debugInfo) {
            UNIT_ASSERT_C(!item.Plan.Contains("FullScan"), item.ToString());
        }
    }
}

inline void AssertNoTopSort(TDebugInfoPtr debugInfo) {
    if (debugInfo) {
        for (const auto& item: *debugInfo) {
            UNIT_ASSERT_C(!item.Plan.Contains("TopSort"), item.ToString());
        }
    }
}

namespace {

TString Exec(const TString& cmd) {
    std::array<char, 128> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

}

struct TTestBootstrap {
    NConfig::TControlPlaneStorageConfig Config;
    NFq::TYqSharedResources::TPtr YqSharedResources;
    TRuntimePtr Runtime;
    TString TablePrefix;
    NFq::TYdbConnectionPtr Connection;

    const TDuration RequestTimeout = TDuration::Seconds(5);

    static const TString& GetDefaultTenantName() {
        static const TString name = "/root/tenant";
        return name;
    }

    TTestBootstrap(std::string tablePrefix, const NConfig::TControlPlaneStorageConfig& config = {})
        : Config(config)
    {
        Cerr << "Netstat: " << Exec("netstat --all --program") << Endl;
        Cerr << "Process stat: " << Exec("ps aux") << Endl;
        Cerr << "YDB receipt endpoint: " << GetEnv("YDB_ENDPOINT") << ", database: " << GetEnv("YDB_DATABASE") << Endl;
        tablePrefix.erase(std::remove_if(tablePrefix.begin(), tablePrefix.end(), isspace), tablePrefix.end());
        TablePrefix = tablePrefix;
        auto& storageConfig = *Config.MutableStorage();
        storageConfig.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        storageConfig.SetDatabase(GetEnv("YDB_DATABASE"));
        storageConfig.SetToken("");
        storageConfig.SetTablePrefix(TablePrefix);

        Config.AddSuperUsers("super_user@staff");
        Config.SetEnableDebugMode(true);
        Config.SetNumTasksProportion(1); //Get all tasks from pending

        if (Config.AvailableBindingSize() == 0) {
            Config.AddAvailableBinding("DATA_STREAMS");
            Config.AddAvailableBinding("OBJECT_STORAGE");
        }

        if (Config.AvailableConnectionSize() == 0) {
            Config.AddAvailableConnection("YDB_DATABASE");
            Config.AddAvailableConnection("CLICKHOUSE_CLUSTER");
            Config.AddAvailableConnection("DATA_STREAMS");
            Config.AddAvailableConnection("OBJECT_STORAGE");
            Config.AddAvailableConnection("MONITORING");
            Config.AddAvailableConnection("POSTGRESQL_CLUSTER");
        }

        NYdb::TDriver driver({});
        Connection = NewYdbConnection(storageConfig, NKikimr::CreateYdbCredentialsProviderFactory, driver);
        std::tie(Runtime, YqSharedResources) = Prepare();
    }

    ~TTestBootstrap() {
        Connection = nullptr;
        YqSharedResources->Stop();
        Runtime = nullptr;
    }

    TMaybe<NYdb::NTable::TTableDescription> WaitTable(const TString& name)
    {
        return DoWithRetry<NYdb::NTable::TTableDescription, yexception>([&]() {
            auto tablePath = ::NFq::JoinPath(Connection->TablePathPrefix, name);
            auto session = Connection->TableClient.GetSession().GetValue(RequestTimeout).GetSession();
            auto description = session.DescribeTable(tablePath).GetValue(RequestTimeout);
            if (description.IsSuccess()) {
                return description.GetTableDescription();
            }

            ythrow yexception() << "Table with name " << name << " does not exist";
        }, TRetryOptions(2000, TDuration::MilliSeconds(100), TDuration::MilliSeconds(50)), true);
    }

    TMaybe<NYdb::TResultSet> GetTable(const TString& tableName)
    {
        auto session = Connection->TableClient.GetSession().GetValue(RequestTimeout).GetSession();
        TSqlQueryBuilder queryBuilder(Connection->TablePathPrefix);
        queryBuilder.AddText(
            "SELECT * FROM " + tableName + "\n"
            "LIMIT 1000;\n"
        );

        auto paramsBuilder = session.GetParamsBuilder();
        auto result = session.ExecuteDataQuery(queryBuilder.Build().Sql, NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(), paramsBuilder.Build()).GetValueSync();

        if (!result.IsSuccess() || result.GetResultSets().size() != 1) {
            ythrow yexception() << "Results size != 1 but results size = " << result.GetResultSets().size();
        }

        return result.GetResultSet(0);
    }

    template <typename Request, typename Response>
    std::pair<typename Response::TProto, NYql::TIssues> SendPublicRequest(
        const typename Request::TProto& proto,
        TPermissions permissions = TPermissions{},
        const TString& scope = "yandexcloud://test_folder_id_1",
        const TString& user = "test_user@staff",
        const TString& token = "xxx")
    {
        TActorId sender = Runtime->AllocateEdgeActor();

        while (true) {
            auto request = std::make_unique<Request>(scope, proto, user, token, "mock_cloud", permissions, TQuotaMap{{QUOTA_CPU_PERCENT_LIMIT, TQuotaUsage{42000}}}, std::make_shared<TTenantInfo>(), FederatedQuery::Internal::ComputeDatabaseInternal{});
            Runtime->Send(new IEventHandle(ControlPlaneStorageServiceActorId(), sender, request.release()));

            TAutoPtr<IEventHandle> handle;
            Response* event = Runtime->GrabEdgeEvent<Response>(handle);
            AssertNoFullScan(event->DebugInfo);
            AssertNoTopSort(event->DebugInfo);

            bool isNotReady = false;
            for (auto issue : event->Issues) {
                if (issue.IssueCode == TIssuesIds::NOT_READY) {
                    isNotReady = true;
                    break;
                }
            }

            if (isNotReady) {
                continue;
            }

            return {event->Result, event->Issues};
        }
    }

    inline auto CreateQuery(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvCreateQueryRequest, TEvControlPlaneStorage::TEvCreateQueryResponse>(proto, permissions, scope, user, token);
    }

    inline auto ListQueries(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvListQueriesRequest, TEvControlPlaneStorage::TEvListQueriesResponse>(proto, permissions, scope, user, token);
    }

    inline auto DescribeQuery(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvDescribeQueryRequest, TEvControlPlaneStorage::TEvDescribeQueryResponse>(proto, permissions, scope, user, token);
    }

    inline auto GetQueryStatus(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvGetQueryStatusRequest, TEvControlPlaneStorage::TEvGetQueryStatusResponse>(proto, permissions, scope, user, token);
    }

    inline auto DeleteQuery(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvDeleteQueryRequest, TEvControlPlaneStorage::TEvDeleteQueryResponse>(proto, permissions, scope, user, token);
    }

    inline auto ModifyQuery(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvModifyQueryRequest, TEvControlPlaneStorage::TEvModifyQueryResponse>(proto, permissions, scope, user, token);
    }

    inline auto ControlQuery(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvControlQueryRequest, TEvControlPlaneStorage::TEvControlQueryResponse>(proto, permissions, scope, user, token);
    }

    inline auto ListJobs(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvListJobsRequest, TEvControlPlaneStorage::TEvListJobsResponse>(proto, permissions, scope, user, token);
    }

    inline auto DescribeJob(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvDescribeJobRequest, TEvControlPlaneStorage::TEvDescribeJobResponse>(proto, permissions, scope, user, token);
    }

    inline auto GetResultData(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvGetResultDataRequest, TEvControlPlaneStorage::TEvGetResultDataResponse>(proto, permissions, scope, user, token);
    }

    inline auto CreateConnection(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvCreateConnectionRequest, TEvControlPlaneStorage::TEvCreateConnectionResponse>(proto, permissions, scope, user, token);
    }

    inline auto ListConnections(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvListConnectionsRequest, TEvControlPlaneStorage::TEvListConnectionsResponse>(proto, permissions, scope, user, token);
    }

    inline auto DescribeConnection(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvDescribeConnectionRequest, TEvControlPlaneStorage::TEvDescribeConnectionResponse>(proto, permissions, scope, user, token);
    }

    inline auto DeleteConnection(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvDeleteConnectionRequest, TEvControlPlaneStorage::TEvDeleteConnectionResponse>(proto, permissions, scope, user, token);
    }

    inline auto ModifyConnection(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvModifyConnectionRequest, TEvControlPlaneStorage::TEvModifyConnectionResponse>(proto, permissions, scope, user, token);
    }

    inline auto CreateBinding(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvCreateBindingRequest, TEvControlPlaneStorage::TEvCreateBindingResponse>(proto, permissions, scope, user, token);
    }

    inline auto ListBindings(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvListBindingsRequest, TEvControlPlaneStorage::TEvListBindingsResponse>(proto, permissions, scope, user, token);
    }

    inline auto DescribeBinding(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvDescribeBindingRequest, TEvControlPlaneStorage::TEvDescribeBindingResponse>(proto, permissions, scope, user, token);
    }

    inline auto DeleteBinding(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvDeleteBindingRequest, TEvControlPlaneStorage::TEvDeleteBindingResponse>(proto, permissions, scope, user, token);
    }

    inline auto ModifyBinding(const auto& proto, TPermissions permissions = TPermissions{}, const TString& scope = "yandexcloud://test_folder_id_1", const TString& user = "test_user@staff", const TString& token = "xxx") {
        return SendPublicRequest<TEvControlPlaneStorage::TEvModifyBindingRequest, TEvControlPlaneStorage::TEvModifyBindingResponse>(proto, permissions, scope, user, token);
    }

    template <typename Request, typename Response>
    std::pair<TAutoPtr<IEventHandle>, Response*> SendInternalRequest(std::unique_ptr<Request> request)
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        Runtime->Send(new IEventHandle(ControlPlaneStorageServiceActorId(), sender, request.release()));

        TAutoPtr<IEventHandle> handle;
        Response* event = Runtime->GrabEdgeEvent<Response>(handle);
        AssertNoFullScan(event->DebugInfo);
        return {handle, event};
    }

    inline auto WriteResultData(auto request) {
        return SendInternalRequest<TEvControlPlaneStorage::TEvWriteResultDataRequest, TEvControlPlaneStorage::TEvWriteResultDataResponse>(std::move(request));
    }

    inline auto GetTask(auto request) {
        return SendInternalRequest<TEvControlPlaneStorage::TEvGetTaskRequest, TEvControlPlaneStorage::TEvGetTaskResponse>(std::move(request));
    }

    inline auto PingTask(auto request) {
        return SendInternalRequest<TEvControlPlaneStorage::TEvPingTaskRequest, TEvControlPlaneStorage::TEvPingTaskResponse>(std::move(request));
    }

    inline auto NodesHealthCheck(auto request) {
        return SendInternalRequest<TEvControlPlaneStorage::TEvNodesHealthCheckRequest, TEvControlPlaneStorage::TEvNodesHealthCheckResponse>(std::move(request));
    }

    inline auto CreateRateLimiterResource(auto request) {
        return SendInternalRequest<TEvControlPlaneStorage::TEvCreateRateLimiterResourceRequest, TEvControlPlaneStorage::TEvCreateRateLimiterResourceResponse>(std::move(request));
    }

    inline auto DeleteRateLimiterResource(auto request) {
        return SendInternalRequest<TEvControlPlaneStorage::TEvDeleteRateLimiterResourceRequest, TEvControlPlaneStorage::TEvDeleteRateLimiterResourceResponse>(std::move(request));
    }

private:
    std::pair<TRuntimePtr, NFq::TYqSharedResources::TPtr> Prepare() {
        TRuntimePtr runtime(new TTestBasicRuntime(1, true /* use real threads */));
        runtime->SetLogPriority(NKikimrServices::YQ_CONTROL_PLANE_STORAGE, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::FQ_QUOTA_SERVICE, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::YQ_RATE_LIMITER, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_EMERG);

        NYdb::TDriverConfig stub;
        NYdb::TDriver driver(stub);

        NFq::NConfig::TConfig yqConfig;
        auto& dbPool = *yqConfig.MutableDbPool();
        dbPool.SetEnabled(true);
        auto& storage = *dbPool.MutableStorage();
        storage.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        storage.SetDatabase(GetEnv("YDB_DATABASE"));
        storage.SetTablePrefix(TablePrefix);

        NFq::NConfig::TCommonConfig common;
        common.SetIdsPrefix("ut");
        NYql::TS3GatewayConfig s3Config;
        s3Config.SetGeneratorPathsLimit(50'000);

        const auto& credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        NFq::TYqSharedResources::TPtr yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResources(yqConfig, credFactory, counters));
        yqSharedResources->Init(runtime->GetAnyNodeActorSystem());
        auto controlPlaneService = CreateYdbControlPlaneStorageServiceActor(Config, s3Config, common, NConfig::TComputeConfig{}, MakeIntrusive<NMonitoring::TDynamicCounters>(), yqSharedResources, credFactory, GetDefaultTenantName());

        runtime->AddLocalService(
            ControlPlaneStorageServiceActorId(),
            TActorSetupCmd(controlPlaneService, TMailboxType::Simple, 0));

        {
            NConfig::TRateLimiterConfig rateLimiterConfig;
            rateLimiterConfig.SetEnabled(true);
            rateLimiterConfig.SetControlPlaneEnabled(true);
            auto& db = *rateLimiterConfig.MutableDatabase();
            db.SetEndpoint(GetEnv("YDB_ENDPOINT"));
            db.SetDatabase(GetEnv("YDB_DATABASE"));
            db.SetTablePrefix(TablePrefix + "_rate_limiter");
            rateLimiterConfig.AddLimiters()->SetCoordinationNodePath("alpha");
            auto* rateLimiterControlPlaneService = CreateRateLimiterControlPlaneService(rateLimiterConfig, yqSharedResources, credFactory);

            runtime->AddLocalService(
                RateLimiterControlPlaneServiceId(),
                TActorSetupCmd(rateLimiterControlPlaneService, TMailboxType::Simple, 0));
        }

        SetupTabletServices(*runtime);

        WaitTable("queries");
        WaitTable("pending_small");
        WaitTable("nodes");
        WaitTable("connections");
        WaitTable("bindings");
        WaitTable("idempotency_keys");
        WaitTable("result_sets");
        WaitTable("jobs");

        return {std::move(runtime), std::move(yqSharedResources)};
    }
};

} // namespace NFq
