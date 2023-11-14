#include "mdb_endpoint_generator.h"

namespace NFq {

    TString ReplaceDomain(const TString& mdbHost) {
        return mdbHost.substr(0, mdbHost.find('.')) + ".db.yandex.net";
    }

    using TEndpoint = NYql::IMdbEndpointGenerator::TEndpoint;

    constexpr ui32 CLICKHOUSE_NATIVE_SECURE_PORT = 9440;
    constexpr ui32 CLICKHOUSE_NATIVE_INSECURE_PORT = 9000;
    constexpr ui32 CLICKHOUSE_HTTP_SECURE_PORT = 8443;
    constexpr ui32 CLICKHOUSE_HTTP_INSECURE_PORT = 8123;

    // Managed PostgreSQL provides the only port both for secure and insecure connections
    constexpr ui32 POSTGRESQL_PORT = 6432;

    // TMdbEndpointGeneratorLegacy implements behavior required by YQL legacy ClickHouse provider
    class TMdbEndpointGeneratorLegacy: public NYql::IMdbEndpointGenerator {
        TEndpoint ToEndpoint(const NYql::EDatabaseType databaseType, const TString& mdbHost, bool useTls) const override {
            // Inherited from here
            // https://a.yandex-team.ru/arcadia/ydb/core/fq/libs/actors/database_resolver.cpp?rev=r11819335#L27
            if (databaseType == NYql::EDatabaseType::ClickHouse) {
                auto port = useTls ? CLICKHOUSE_HTTP_SECURE_PORT : CLICKHOUSE_HTTP_INSECURE_PORT;
                return TEndpoint(ReplaceDomain(mdbHost), port);
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
        TMdbEndpointGeneratorGeneric(bool transformHost, bool useNativeProtocolForClickHouse = false)
            : TransformHost(transformHost), UseNativeProtocolForClickHouse(useNativeProtocolForClickHouse)
        {
        }

        TEndpoint ToEndpoint(const NYql::EDatabaseType databaseType, const TString& mdbHost, bool useTls) const override {
            auto fixedHost = TransformHost ? ReplaceDomain(mdbHost) : mdbHost;

            switch (databaseType) {
                case NYql::EDatabaseType::ClickHouse: {
                    // https://cloud.yandex.ru/docs/managed-clickhouse/operations/connect
                    ui32 port;
                    if (UseNativeProtocolForClickHouse) {
                        port = useTls ? CLICKHOUSE_NATIVE_SECURE_PORT : CLICKHOUSE_NATIVE_INSECURE_PORT;
                    } else {
                        port = useTls ? CLICKHOUSE_HTTP_SECURE_PORT : CLICKHOUSE_HTTP_INSECURE_PORT;
                    }

                    return TEndpoint(fixedHost, port);
                }
                case NYql::EDatabaseType::PostgreSQL:
                    // https://cloud.yandex.ru/docs/managed-postgresql/operations/connect
                    return TEndpoint(fixedHost, POSTGRESQL_PORT);
                default:
                    ythrow yexception() << TStringBuilder() << "Unexpected database type: " << int(databaseType);
            };
        }

    private:
        bool TransformHost;
        bool UseNativeProtocolForClickHouse;
    };

    NYql::IMdbEndpointGenerator::TPtr
    MakeMdbEndpointGeneratorGeneric(bool transformHost, bool useNativeProtocolForClickHouse) {
        return std::make_shared<TMdbEndpointGeneratorGeneric>(transformHost, useNativeProtocolForClickHouse);
    }

    // TMdbEndpointGeneratorNoop just does nothing
    class TMdbEndpointGeneratorNoop: public NYql::IMdbEndpointGenerator {
        TEndpoint ToEndpoint(const NYql::EDatabaseType, const TString& mdbHost, bool) const override {
            return TEndpoint(mdbHost, 0);
        }
    };

    NYql::IMdbEndpointGenerator::TPtr
    MakeMdbEndpointGeneratorNoop() {
        return std::make_shared<TMdbEndpointGeneratorNoop>();
    }
}