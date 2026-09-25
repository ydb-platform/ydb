#pragma once

#include "yql_solomon_gateway.h"
#include "yql_solomon_config.h"

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <yql/essentials/core/yql_data_provider.h>

namespace NYql {

constexpr i32 SolomonMaxLabelsCount = 16;
constexpr i32 SolomonMaxSensorsCount = 50;

struct TSolomonState : public TThrRefBase
{
    using TPtr = TIntrusivePtr<TSolomonState>;

public:
    bool IsRtmrMode() const {
       return SupportRtmrMode;
    }

public:
    bool SupportRtmrMode = true;
    bool WriteThroughDqIntegration = false;

    ISolomonGateway::TPtr Gateway;
    TTypeAnnotationContext* Types = nullptr;
    IStructuredTokenCredentialsFactory::TPtr CredentialsFactory;
    TSolomonConfiguration::TPtr Configuration = MakeIntrusive<TSolomonConfiguration>();
    std::unique_ptr<IDqIntegration> DqIntegration;
    std::unique_ptr<IYtflowIntegration> YtflowIntegration;
    std::unique_ptr<IYtflowOptimization> YtflowOptimization;
    ui32 ExecutorPoolId = 0;
};

TDataProviderInitializer GetSolomonDataProviderInitializer(ISolomonGateway::TPtr gateway, IStructuredTokenCredentialsFactory::TPtr credentialsFactory, bool supportRtmrMode = true, bool useYtflowEngine = false);

TIntrusivePtr<IDataProvider> CreateSolomonDataSource(TSolomonState::TPtr state);
TIntrusivePtr<IDataProvider> CreateSolomonDataSink(TSolomonState::TPtr state);

} // namespace NYql
