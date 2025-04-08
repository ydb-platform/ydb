#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

namespace NKikimr::NKqp::NFederatedQueryTest {
    using namespace NKikimr::NKqp;

    TString GetSymbolsString(char start, char end, const TString& skip = "");

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(
        const NYdb::TOperation::TOperationId& operationId,
        const NYdb::TDriver& ydbDriver);

    void WaitResourcesPublish(ui32 nodeId, ui32 expectedNodeCount);
    void WaitResourcesPublish(const TKikimrRunner& kikimrRunner);

    struct TKikimrRunnerOptions {
        TString DomainRoot = "Root";
        ui32 NodeCount = 1;
    };

    std::shared_ptr<TKikimrRunner> MakeKikimrRunner(
        bool initializeHttpGateway = false,
        NYql::NConnector::IClient::TPtr connectorClient = nullptr,
        NYql::IDatabaseAsyncResolver::TPtr databaseAsyncResolver = nullptr,
        std::optional<NKikimrConfig::TAppConfig> appConfig = std::nullopt,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory = nullptr,
        const TKikimrRunnerOptions& options = {},
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory = nullptr);
} // namespace NKikimr::NKqp::NFederatedQueryTest
