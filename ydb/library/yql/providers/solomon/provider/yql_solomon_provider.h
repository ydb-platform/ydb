#pragma once

#include "yql_solomon_gateway.h"
#include "yql_solomon_config.h"

#include <ydb/library/yql/core/yql_data_provider.h>

namespace NYql {

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
    TSolomonConfiguration::TPtr Configuration = MakeIntrusive<TSolomonConfiguration>();
    THolder<IDqIntegration> DqIntegration;
};

TDataProviderInitializer GetSolomonDataProviderInitializer(ISolomonGateway::TPtr gateway, bool supportRtmrMode = true);

TIntrusivePtr<IDataProvider> CreateSolomonDataSource(TSolomonState::TPtr state);
TIntrusivePtr<IDataProvider> CreateSolomonDataSink(TSolomonState::TPtr state);

} // namespace NYql
