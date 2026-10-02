#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <yql/essentials/core/yql_data_provider.h>

#include <util/datetime/base.h>

namespace NYql {

class IYdbRemoteMetadataClientCache {
public:
    virtual ~IYdbRemoteMetadataClientCache() = default;

    virtual std::shared_ptr<NYdb::NTable::TTableClient> GetClient(
        const TString& endpoint, const TString& database, bool useTls,
        const TString& structuredToken, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) = 0;
};

// Shares clients across compilations, with a bounded number of credential-isolated
// entries. Expired entries are discarded on access; destruction releases the cache.
std::shared_ptr<IYdbRemoteMetadataClientCache> CreateYdbRemoteMetadataClientCache(
    const NYdb::TDriver& driver, const NYdb::TDriver& tlsDriver,
    size_t maxEntries = 64, TDuration idleTimeout = TDuration::Minutes(10));

TDataProviderInfo CreateYdbRemoteDataProviders(
    TTypeAnnotationContext* types,
    // Independently constructed drivers isolate the baseline SDK channel caches.
    const NYdb::TDriver& driver,
    const NYdb::TDriver& tlsDriver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory = CreateStructuredTokenCredentialsFactory(),
    TInstant metadataDeadline = TInstant::Max(),
    std::shared_ptr<IYdbRemoteMetadataClientCache> metadataClientCache = {});

} // namespace NYql
