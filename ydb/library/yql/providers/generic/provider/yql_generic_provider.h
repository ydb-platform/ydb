#pragma once

#include "yql_generic_state.h"

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>

namespace NYql {
    TDataProviderInitializer GetGenericDataProviderInitializer(
        NConnector::IClient::TPtr genericClient,                                           // required
        const IDatabaseAsyncResolver::TPtr& dbResolver = nullptr,                          // can be missing in on-prem installations
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory = nullptr // can be missing in on-prem installations
    );

    TIntrusivePtr<IDataProvider> CreateGenericDataSource(TGenericState::TPtr state);

    TIntrusivePtr<IDataProvider> CreateGenericDataSink(TGenericState::TPtr state);

} // namespace NYql
