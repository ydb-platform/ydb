#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <yql/essentials/core/yql_data_provider.h>

#include <util/datetime/base.h>

namespace NYql {

TDataProviderInfo CreateYdbRemoteDataProviders(
    TTypeAnnotationContext* types,
    // Independently constructed drivers isolate the baseline SDK channel caches.
    const NYdb::TDriver& driver,
    const NYdb::TDriver& tlsDriver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory = CreateStructuredTokenCredentialsFactory(),
    TInstant metadataDeadline = TInstant::Max());

} // namespace NYql
