#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>

Y_UNIT_TEST_SUITE(MdbEndpoingGenerator) {
    using TEndpoint = NYql::IMdbEndpointGenerator::TEndpoint;

    Y_UNIT_TEST(Legacy) {
        auto transformer = NFq::MakeMdbEndpointGeneratorLegacy();

        auto params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::ClickHouse,
            .MdbHost = "rc1c-p5waby2y5y1kb5ue.db.yandex.net",
            .UseTls = true,
            .Protocol = NYql::NConnector::NApi::EProtocol::HTTP,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1c-p5waby2y5y1kb5ue.db.yandex.net", 8443));

        params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::ClickHouse,
            .MdbHost = "ya.ru",
            .UseTls = false,
            .Protocol = NYql::NConnector::NApi::EProtocol::HTTP,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("ya.db.yandex.net", 8123));
    }

    Y_UNIT_TEST(Generic_NoTransformHost) {
        auto transformer = NFq::MakeMdbEndpointGeneratorGeneric(false);

        auto params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::ClickHouse,
            .MdbHost = "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net",
            .UseTls = true,
            .Protocol = NYql::NConnector::NApi::EProtocol::HTTP,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net", 8443));

        params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::PostgreSQL,
            .MdbHost = "rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net",
            .UseTls = false,
            .Protocol = NYql::NConnector::NApi::EProtocol::NATIVE,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net", 6432));
    }

    Y_UNIT_TEST(Generic_WithTransformHost) {
        auto transformer = NFq::MakeMdbEndpointGeneratorGeneric(true);

        // ClickHouse

        auto params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::ClickHouse,
            .MdbHost = "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net",
            .UseTls = false,
            .Protocol = NYql::NConnector::NApi::EProtocol::HTTP,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1a-d6dv17lv47v5mcop.db.yandex.net", 8123));

        params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::ClickHouse,
            .MdbHost = "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net",
            .UseTls = false,
            .Protocol = NYql::NConnector::NApi::EProtocol::NATIVE,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1a-d6dv17lv47v5mcop.db.yandex.net", 9000));

        params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::ClickHouse,
            .MdbHost = "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net",
            .UseTls = true,
            .Protocol = NYql::NConnector::NApi::EProtocol::HTTP,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1a-d6dv17lv47v5mcop.db.yandex.net", 8443));

        params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::ClickHouse,
            .MdbHost = "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net",
            .UseTls = true,
            .Protocol = NYql::NConnector::NApi::EProtocol::NATIVE,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1a-d6dv17lv47v5mcop.db.yandex.net", 9440));

        // PostgreSQL

        params = NYql::IMdbEndpointGenerator::TParams{
            .DatabaseType = NYql::EDatabaseType::PostgreSQL,
            .MdbHost = "rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net",
            .UseTls = true,
            .Protocol = NYql::NConnector::NApi::EProtocol::NATIVE,
        };

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(params),
            TEndpoint("rc1b-eyt6dtobu96rwydq.db.yandex.net", 6432));
    }
}
