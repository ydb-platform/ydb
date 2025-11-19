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

    ISolomonGateway::TPtr Gateway;
    TTypeAnnotationContext* Types = nullptr;
    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    TSolomonConfiguration::TPtr Configuration = MakeIntrusive<TSolomonConfiguration>();
    THolder<IDqIntegration> DqIntegration;
    ui32 ExecutorPoolId = 0;
};

TDataProviderInitializer GetSolomonDataProviderInitializer(ISolomonGateway::TPtr gateway, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory = nullptr, bool supportRtmrMode = true);

TIntrusivePtr<IDataProvider> CreateSolomonDataSource(TSolomonState::TPtr state);
TIntrusivePtr<IDataProvider> CreateSolomonDataSink(TSolomonState::TPtr state);

} // namespace NYql
