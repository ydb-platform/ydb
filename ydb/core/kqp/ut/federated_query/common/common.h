#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

namespace NKikimr::NKqp::NFederatedQueryTest {
    using namespace NKikimr::NKqp;

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(
        const NYdb::TOperation::TOperationId& operationId,
        const NYdb::TDriver& ydbDriver);

    std::shared_ptr<TKikimrRunner> MakeKikimrRunner(
        bool initializeHttpGateway = false,
        NYql::NConnector::IClient::TPtr connectorClient = nullptr,
        NYql::IDatabaseAsyncResolver::TPtr databaseAsyncResolver = nullptr,
        std::optional<NKikimrConfig::TAppConfig> appConfig = std::nullopt,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory = nullptr,
        const TString& domainRoot = "Root");
}
