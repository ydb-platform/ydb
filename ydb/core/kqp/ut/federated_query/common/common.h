#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

namespace NKikimr::NKqp::NFederatedQueryTest {
    using namespace NKikimr::NKqp;

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(
        const NYdb::TOperation::TOperationId& operationId,
        const NYdb::TDriver& ydbDriver);

    std::shared_ptr<TKikimrRunner> MakeKikimrRunner(
        bool initializeHttpGateway = false,
        NYql::NConnector::IClient::TPtr connectorClient = nullptr,
        NYql::IDatabaseAsyncResolver::TPtr databaseAsyncResolver = nullptr,
        std::optional<NKikimrConfig::TAppConfig> appConfig = std::nullopt);
}
