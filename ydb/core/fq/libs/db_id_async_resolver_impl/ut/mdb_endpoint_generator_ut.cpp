#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>

Y_UNIT_TEST_SUITE(MdbEndpoingGenerator) {
    using TEndpoint = NYql::IMdbEndpointGenerator::TEndpoint;

    Y_UNIT_TEST(Legacy) {
        auto transformer = NFq::MakeMdbEndpointGeneratorLegacy();

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(NYql::EDatabaseType::ClickHouse, "rc1c-p5waby2y5y1kb5ue.db.yandex.net", true),
            TEndpoint("rc1c-p5waby2y5y1kb5ue.db.yandex.net", 9440));

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(NYql::EDatabaseType::ClickHouse, "ya.ru", false),
            TEndpoint("ya.db.yandex.net", 9000));
    }

    Y_UNIT_TEST(Generic_NoTransformHost) {
        auto transformer = NFq::MakeMdbEndpointGeneratorGeneric(false);

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(NYql::EDatabaseType::ClickHouse, "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net", true),
            TEndpoint("rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net", 9440));

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(NYql::EDatabaseType::PostgreSQL, "rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net", false),
            TEndpoint("rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net", 6432));
    }

    Y_UNIT_TEST(Generic_WithTransformHost) {
        auto transformer = NFq::MakeMdbEndpointGeneratorGeneric(true);

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(NYql::EDatabaseType::ClickHouse, "rc1a-d6dv17lv47v5mcop.mdb.yandexcloud.net", false),
            TEndpoint("rc1a-d6dv17lv47v5mcop.db.yandex.net", 9000));

        UNIT_ASSERT_VALUES_EQUAL(
            transformer->ToEndpoint(NYql::EDatabaseType::PostgreSQL, "rc1b-eyt6dtobu96rwydq.mdb.yandexcloud.net", true),
            TEndpoint("rc1b-eyt6dtobu96rwydq.db.yandex.net", 6432));
    }
}
