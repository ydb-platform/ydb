#pragma once
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <yql/essentials/core/yql_data_provider.h>

namespace NYql {
TIntrusivePtr<IDataProvider> CreateQytDataSource(IStructuredTokenCredentialsFactory::TPtr credentialsFactory);
}   // namespace NYql
