#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <yql/essentials/core/yql_data_provider.h>

#include <util/datetime/base.h>
#include <functional>

namespace NYql {

class IYdbExternalMetadataClientCache {
public:
    virtual ~IYdbExternalMetadataClientCache() = default;

    virtual std::shared_ptr<NYdb::NTable::TTableClient> GetClient(
        const TString& endpoint, const TString& database, bool useTls,
        const TString& structuredToken, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) = 0;
};

// Shares clients across compilations, with a bounded number of credential-isolated
// entries. Expired entries are discarded on access; destruction releases the cache.
std::shared_ptr<IYdbExternalMetadataClientCache> CreateYdbExternalMetadataClientCache(
    const NYdb::TDriver& driver, const NYdb::TDriver& tlsDriver,
    size_t maxEntries = 64, TDuration idleTimeout = TDuration::Minutes(10));

using TYdbExternalMetadataClientCacheFactory = std::function<std::shared_ptr<IYdbExternalMetadataClientCache>()>;

TDataProviderInfo CreateYdbExternalDataProviders(
    TTypeAnnotationContext* types,
    // Called only when metadata is needed, so registering the provider does not
    // create SDK drivers for queries that never read a YdbExternal source.
    TYdbExternalMetadataClientCacheFactory metadataClientCacheFactory,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory = CreateStructuredTokenCredentialsFactory(),
    TInstant metadataDeadline = TInstant::Max());

} // namespace NYql
