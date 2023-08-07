#pragma once

#include "db_async_resolver.h"

namespace NYql {
    // IMdbHostTransformer is responsible for transforming the managed database instance host name
    // into endpoint (`fqdn:port`) to establish network connection with data source.
    // The host names are obtained from MDB API, for example:
    // https://cloud.yandex.ru/docs/managed-clickhouse/api-ref/Cluster/listHosts
    class IMdbHostTransformer {
    public:
        using TPtr = std::shared_ptr<IMdbHostTransformer>;

        virtual TString ToEndpoint(const NYql::EDatabaseType databaseType, const TString& mdbHost) const = 0;
        virtual ~IMdbHostTransformer() = default;
    };
}
