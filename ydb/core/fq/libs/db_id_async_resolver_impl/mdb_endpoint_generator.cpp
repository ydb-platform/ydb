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

    // Managed PostgreSQL and Greenplum provide the only port both for secure and insecure connections
    constexpr ui32 POSTGRESQL_PORT = 6432;
    constexpr ui32 GREENPLUM_PORT = 6432;

    constexpr ui32 MYSQL_PORT = 3306;

    // TMdbEndpointGeneratorLegacy implements behavior required by YQL legacy ClickHouse provider
    class TMdbEndpointGeneratorLegacy: public NYql::IMdbEndpointGenerator {
        TEndpoint ToEndpoint(const NYql::IMdbEndpointGenerator::TParams& params) const override {
            // Inherited from here
            // https://a.yandex-team.ru/arcadia/ydb/core/fq/libs/actors/database_resolver.cpp?rev=r11819335#L27
            if (params.DatabaseType == NYql::EDatabaseType::ClickHouse) {
                auto port = params.UseTls ? CLICKHOUSE_HTTP_SECURE_PORT : CLICKHOUSE_HTTP_INSECURE_PORT;
                return TEndpoint(ReplaceDomain(params.MdbHost), port);
            }

            ythrow yexception() << "Unexpected database type: " << ToString(params.DatabaseType);
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

        TEndpoint ToEndpoint(const NYql::IMdbEndpointGenerator::TParams& params) const override {
            auto fixedHost = TransformHost ? ReplaceDomain(params.MdbHost) : params.MdbHost;

            switch (params.DatabaseType) {
                case NYql::EDatabaseType::ClickHouse: {
                    // https://cloud.yandex.ru/docs/managed-clickhouse/operations/connect
                    ui32 port;

                    switch (params.Protocol) {
                        case NYql::NConnector::NApi::EProtocol::NATIVE:
                            port = params.UseTls ? CLICKHOUSE_NATIVE_SECURE_PORT : CLICKHOUSE_NATIVE_INSECURE_PORT;
                            break;
                        case NYql::NConnector::NApi::EProtocol::HTTP:
                            port = params.UseTls ? CLICKHOUSE_HTTP_SECURE_PORT : CLICKHOUSE_HTTP_INSECURE_PORT;
                            break;
                        default:
                            ythrow yexception() << "Unexpected protocol for ClickHouse: " << NYql::NConnector::NApi::EProtocol_Name(params.Protocol);
                    }

                    return TEndpoint(fixedHost, port);
                }
                case NYql::EDatabaseType::PostgreSQL:
                    // https://cloud.yandex.ru/docs/managed-postgresql/operations/connect
                    switch (params.Protocol) {
                        case NYql::NConnector::NApi::EProtocol::NATIVE:
                            return TEndpoint(fixedHost, POSTGRESQL_PORT);
                        default:
                            ythrow yexception() << "Unexpected protocol for PostgreSQL " << NYql::NConnector::NApi::EProtocol_Name(params.Protocol);
                    }
                case NYql::EDatabaseType::Greenplum:
                    // https://cloud.yandex.ru/docs/managed-greenplum/operations/connect
                    switch (params.Protocol) {
                        case NYql::NConnector::NApi::EProtocol::NATIVE:
                            return TEndpoint(fixedHost, GREENPLUM_PORT);
                        default:
                            ythrow yexception() << "Unexpected protocol for Greenplum: " << NYql::NConnector::NApi::EProtocol_Name(params.Protocol);
                    }
                case NYql::EDatabaseType::MySQL:
                    // https://cloud.yandex.ru/docs/managed-mysql/operations/connect
                    switch (params.Protocol) {
                        case NYql::NConnector::NApi::EProtocol::NATIVE:
                            return TEndpoint(fixedHost, MYSQL_PORT);
                        default:
                            ythrow yexception() << "Unexpected protocol for MySQL: " << NYql::NConnector::NApi::EProtocol_Name(params.Protocol);
                    }
                default:
                    ythrow yexception() << "Unexpected database type: " << ToString(params.DatabaseType);
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
        TEndpoint ToEndpoint(const NYql::IMdbEndpointGenerator::TParams& params) const override {
            return TEndpoint(params.MdbHost, 0);
        }
    };

    NYql::IMdbEndpointGenerator::TPtr
    MakeMdbEndpointGeneratorNoop() {
        return std::make_shared<TMdbEndpointGeneratorNoop>();
    }
}
