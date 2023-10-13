#pragma once

#include "db_async_resolver.h"

namespace NYql {
    // IMdbEndpointGenerator is responsible for transforming the managed database instance hostname
    // into endpoint (`fqdn:port`) to establish network connection with data source.
    // The host names are obtained from MDB API, for example:
    // https://cloud.yandex.ru/docs/managed-clickhouse/api-ref/Cluster/listHosts
    class IMdbEndpointGenerator {
    public:
        using TPtr = std::shared_ptr<IMdbEndpointGenerator>;
        using TEndpoint = std::pair<TString, ui32>;

        // Returns FQDN and port of the database host
        virtual TEndpoint ToEndpoint(
            const NYql::EDatabaseType databaseType,
            const TString& mdbHost,
            bool useTls) const = 0;

        virtual ~IMdbEndpointGenerator() = default;
    };
}
