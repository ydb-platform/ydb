#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <yql/essentials/core/yql_data_provider.h>

#include <util/datetime/base.h>
#include <memory>

namespace NYql {

namespace NNative {
class IAsyncMemoryQuota;
}

TDataProviderInfo CreateYdbRemoteDataProviders(
    TTypeAnnotationContext* types,
    const NYdb::TDriver& driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory = CreateStructuredTokenCredentialsFactory(),
    TInstant metadataDeadline = TInstant::Max(),
    std::shared_ptr<NNative::IAsyncMemoryQuota> metadataQuota = {});

} // namespace NYql
