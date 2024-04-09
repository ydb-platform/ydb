#pragma once
#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NYql::NConnector::NTest {
    extern const TString DEFAULT_DATABASE;
    extern const TString DEFAULT_LOGIN;
    extern const TString DEFAULT_PASSWORD;
    extern const TString DEFAULT_TABLE;
    extern const TString DEFAULT_DATA_SOURCE_NAME;

    extern const TString DEFAULT_PG_HOST;
    constexpr int DEFAULT_PG_PORT = 5432;
    constexpr bool DEFAULT_USE_TLS = true;
    extern const TString PG_SOURCE_TYPE;
    constexpr NApi::EProtocol DEFAULT_PG_PROTOCOL = NApi::EProtocol::NATIVE;
    extern const TString DEFAULT_PG_SCHEMA;

    extern const TString DEFAULT_CH_HOST;
    constexpr int DEFAULT_CH_PORT = 8443;
    extern const TString DEFAULT_CH_ENDPOINT;
    extern const TString DEFAULT_CH_CLUSTER_ID;
    constexpr NApi::EProtocol DEFAULT_CH_PROTOCOL = NApi::EProtocol::HTTP;
    extern const TString DEFAULT_CH_SERVICE_ACCOUNT_ID;
    extern const TString DEFAULT_CH_SERVICE_ACCOUNT_ID_SIGNATURE;

    extern const TString DEFAULT_YDB_HOST;
    constexpr int DEFAULT_YDB_PORT = 2136;
    extern const TString DEFAULT_YDB_ENDPOINT;
    constexpr NApi::EProtocol DEFAULT_YDB_PROTOCOL = NApi::EProtocol::NATIVE;
} // namespace NYql::NConnector::NTest
