#include "mdb_host_transformer.h"

namespace NFq {
    // TMdbHostTransformerLegacy implements behavior required by YQL legacy ClickHouse provider
    class TMdbHostTransformerLegacy: public NYql::IMdbHostTransformer {
        TString ToEndpoint(const NYql::EDatabaseType databaseType, const TString& mdbHost) const override {
            // Inherited from here
            // https://a.yandex-team.ru/arcadia/ydb/core/fq/libs/actors/database_resolver.cpp?rev=r11819335#L27
            if (databaseType == NYql::EDatabaseType::ClickHouse) {
                return mdbHost.substr(0, mdbHost.find('.')) + ".db.yandex.net:8443";
            }

            ythrow yexception() << TStringBuilder() << "Unexpected database type: " << int(databaseType);
        }
    };

    NYql::IMdbHostTransformer::TPtr
    MakeTMdbHostTransformerLegacy() {
        return std::make_shared<TMdbHostTransformerLegacy>();
    }

    // TMdbHostTransformerGeneric implements behavior required by YQL Generic provider
    // that interacts with data sources through a separate Connector service
    class TMdbHostTransformerGeneric: public NYql::IMdbHostTransformer {
        TString ToEndpoint(const NYql::EDatabaseType databaseType, const TString& mdbHost) const override {
            switch (databaseType) {
                case NYql::EDatabaseType::ClickHouse:
                    // https://cloud.yandex.ru/docs/managed-clickhouse/operations/connect
                    // TODO: fix Native protocol + TLS https://st.yandex-team.ru/YQ-2286
                    return mdbHost + ":8443";
                case NYql::EDatabaseType::PostgreSQL:
                    // https://cloud.yandex.ru/docs/managed-postgresql/operations/connect
                    return mdbHost + ":6432";
                default:
                    ythrow yexception() << TStringBuilder() << "Unexpected database type: " << int(databaseType);
            };
        }
    };

    NYql::IMdbHostTransformer::TPtr
    MakeTMdbHostTransformerGeneric() {
        return std::make_shared<TMdbHostTransformerGeneric>();
    }

    // TMdbHostTransformerNoop just does nothing
    class TMdbHostTransformerNoop: public NYql::IMdbHostTransformer {
        TString ToEndpoint(const NYql::EDatabaseType, const TString& mdbHost) const override {
            return mdbHost;
        }
    };

    NYql::IMdbHostTransformer::TPtr
    MakeTMdbHostTransformerNoop() {
        return std::make_shared<TMdbHostTransformerNoop>();
    }
}