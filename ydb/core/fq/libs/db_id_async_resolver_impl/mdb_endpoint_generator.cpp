#include "mdb_endpoint_generator.h"

namespace NFq {

    TString ReplaceDomain(const TString& mdbHost) {
        return mdbHost.substr(0, mdbHost.find('.')) + ".db.yandex.net";
    }

    // Currently we're going to use only HTTP protocol for ClickHouse
    constexpr TStringBuf CLICKHOUSE_SECURE_PORT = "8443";
    constexpr TStringBuf CLICKHOUSE_INSECURE_PORT = "8123";

    // Managed PostgreSQL provides the only port both for secure and insecure connections
    constexpr TStringBuf POSTGRESQL_PORT = "6432";

    // TMdbEndpointGeneratorLegacy implements behavior required by YQL legacy ClickHouse provider
    class TMdbEndpointGeneratorLegacy: public NYql::IMdbEndpointGenerator {
        TString ToEndpoint(const NYql::EDatabaseType databaseType, const TString& mdbHost, bool useTls) const override {
            // Inherited from here
            // https://a.yandex-team.ru/arcadia/ydb/core/fq/libs/actors/database_resolver.cpp?rev=r11819335#L27
            if (databaseType == NYql::EDatabaseType::ClickHouse) {
                auto port = useTls ? CLICKHOUSE_SECURE_PORT : CLICKHOUSE_INSECURE_PORT;
                return ReplaceDomain(mdbHost) + ":" + port;
            }

            ythrow yexception() << TStringBuilder() << "Unexpected database type: " << int(databaseType);
        }
    };

    NYql::IMdbEndpointGenerator::TPtr
    MakeMdbEndpointGeneratorLegacy() {
        return std::make_shared<TMdbEndpointGeneratorLegacy>();
    }

    // TMdbEndpointGeneratorGeneric implements behavior required by YQL Generic provider
    // that interacts with data sources through a separate Connector service
    class TMdbEndpointGeneratorGeneric: public NYql::IMdbEndpointGenerator {
    public:
        TMdbEndpointGeneratorGeneric(bool transformHost)
            : TransformHost(transformHost)
        {
        }

        TString ToEndpoint(const NYql::EDatabaseType databaseType, const TString& mdbHost, bool useTls) const override {
            auto fixedHost = TransformHost ? ReplaceDomain(mdbHost) : mdbHost;

            switch (databaseType) {
                case NYql::EDatabaseType::ClickHouse: {
                    // https://cloud.yandex.ru/docs/managed-clickhouse/operations/connect
                    // TODO: fix Native protocol + TLS https://st.yandex-team.ru/YQ-2286
                    auto port = useTls ? CLICKHOUSE_SECURE_PORT : CLICKHOUSE_INSECURE_PORT;
                    return fixedHost + ":" + port;
                }
                case NYql::EDatabaseType::PostgreSQL:
                    // https://cloud.yandex.ru/docs/managed-postgresql/operations/connect
                    return fixedHost + ":" + POSTGRESQL_PORT;
                default:
                    ythrow yexception() << TStringBuilder() << "Unexpected database type: " << int(databaseType);
            };
        }

    private:
        bool TransformHost;
    };

    NYql::IMdbEndpointGenerator::TPtr
    MakeMdbEndpointGeneratorGeneric(bool transformHost) {
        return std::make_shared<TMdbEndpointGeneratorGeneric>(transformHost);
    }

    // TMdbEndpointGeneratorNoop just does nothing
    class TMdbEndpointGeneratorNoop: public NYql::IMdbEndpointGenerator {
        TString ToEndpoint(const NYql::EDatabaseType, const TString& mdbHost, bool) const override {
            return mdbHost;
        }
    };

    NYql::IMdbEndpointGenerator::TPtr
    MakeMdbEndpointGeneratorNoop() {
        return std::make_shared<TMdbEndpointGeneratorNoop>();
    }
}