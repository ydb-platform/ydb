#pragma once

#include "yql_generic_state.h"

#include <sstream>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>

namespace NYql {
    TDataProviderInitializer
    GetGenericDataProviderInitializer(NConnector::IClient::TPtr genericClient,
                                      std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver = nullptr);

    TIntrusivePtr<IDataProvider> CreateGenericDataSource(TGenericState::TPtr state,
                                                         NConnector::IClient::TPtr genericClient);

    TIntrusivePtr<IDataProvider> CreateGenericDataSink(TGenericState::TPtr state);

} // namespace NYql
