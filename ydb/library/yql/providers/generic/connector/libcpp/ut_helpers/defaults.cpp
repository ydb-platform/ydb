#include "defaults.h"

namespace NYql::NConnector::NTest {
    extern const TString DEFAULT_DATABASE = "pgdb";
    extern const TString DEFAULT_LOGIN = "crab";
    extern const TString DEFAULT_PASSWORD = "qwerty12345";
    extern const TString DEFAULT_TABLE = "example_1";
    extern const TString DEFAULT_DATA_SOURCE_NAME = "external_data_source";

    extern const TString DEFAULT_PG_HOST = "localhost";
    extern const TString PG_SOURCE_TYPE = "PostgreSQL";
    extern const TString DEFAULT_PG_SCHEMA = "public";

    extern const TString DEFAULT_CH_HOST = "rc1a-d6dv17lv47v5mcop.db.yandex.net";
    extern const TString DEFAULT_CH_ENDPOINT = TStringBuilder() << DEFAULT_CH_HOST << ':' << DEFAULT_CH_PORT;
    extern const TString DEFAULT_CH_CLUSTER_ID = "ch-managed";
    extern const TString DEFAULT_CH_SERVICE_ACCOUNT_ID = "sa";
    extern const TString DEFAULT_CH_SERVICE_ACCOUNT_ID_SIGNATURE = "sa_signature";

    extern const TString DEFAULT_YDB_HOST = "localhost";
    extern const TString DEFAULT_YDB_ENDPOINT = TStringBuilder() << DEFAULT_YDB_HOST << ':' << DEFAULT_YDB_PORT;
} // namespace NYql::NConnector::NTest
