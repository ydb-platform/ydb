#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/external_function/gateway/dq_function_gateway.h>
#include <ydb/library/yql/providers/external_function/common/dq_function_types.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <util/generic/string.h>
#include <util/generic/ptr.h>

namespace NYql::NDqFunction {

struct TDqFunctionState : public TThrRefBase {
    using TPtr = TIntrusivePtr<TDqFunctionState>;

    TString SessionId;
    TString ScopeFolderId;

    TDqFunctionResolver::TPtr FunctionsResolver = MakeIntrusive<TDqFunctionResolver>();
    TDqFunctionGatewayFactory::TPtr GatewayFactory;
    TDqFunctionsSet FunctionsDescription;
};

TIntrusivePtr<IDataProvider> CreateDqFunctionDataSource(TDqFunctionState::TPtr state);
//TIntrusivePtr<IDataProvider> CreateDqFunctionDataSink(TDqFunctionState::TPtr state);
}

namespace NYql {

TDataProviderInitializer GetDqFunctionDataProviderInitializer(
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        TDqFunctionGatewayFactory::TPtr gatewayFactory,
        const TString& scopeFolderId = {});

}