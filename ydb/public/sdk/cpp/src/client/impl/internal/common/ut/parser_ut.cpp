#include "parser.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev {

Y_UNIT_TEST_SUITE(ConnectionStringParserTest) {
    Y_UNIT_TEST(ParseWithQueryParameter) {
        // Test: grpc://localhost:2135/?database=/local
        auto connInfo = ParseConnectionString("grpc://localhost:2135/?database=/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, false);
    }

    Y_UNIT_TEST(ParseWithQueryParameterNoSlash) {
        // Test: grpc://localhost:2135?database=/local
        auto connInfo = ParseConnectionString("grpc://localhost:2135?database=/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, false);
    }

    Y_UNIT_TEST(ParseWithPathAsDatabase) {
        // Test: grpc://localhost:2135/local
        auto connInfo = ParseConnectionString("grpc://localhost:2135/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, false);
    }

    Y_UNIT_TEST(ParseSecureConnection) {
        // Test: grpcs://localhost:2135/?database=/local
        auto connInfo = ParseConnectionString("grpcs://localhost:2135/?database=/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, true);
    }

    Y_UNIT_TEST(ParseSecureConnectionNoSlash) {
        // Test: grpcs://localhost:2135?database=/local
        auto connInfo = ParseConnectionString("grpcs://localhost:2135?database=/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, true);
    }

    Y_UNIT_TEST(ParseSecureConnectionWithPath) {
        // Test: grpcs://localhost:2135/local
        auto connInfo = ParseConnectionString("grpcs://localhost:2135/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, true);
    }

    Y_UNIT_TEST(ParseNoScheme) {
        // Test: localhost:2135/?database=/local
        auto connInfo = ParseConnectionString("localhost:2135/?database=/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, false);
    }

    Y_UNIT_TEST(ParseNoSchemeWithPath) {
        // Test: localhost:2135/local
        auto connInfo = ParseConnectionString("localhost:2135/local");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "/local");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, false);
    }

    Y_UNIT_TEST(ParseNoDatabaseInQuery) {
        // Test: grpc://localhost:2135
        auto connInfo = ParseConnectionString("grpc://localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Endpoint, "localhost:2135");
        UNIT_ASSERT_EQUAL(connInfo.Database, "");
        UNIT_ASSERT_EQUAL(connInfo.EnableSsl, false);
    }

    Y_UNIT_TEST(InvalidScheme) {
        // Test: http://localhost:2135/?database=/local
        UNIT_ASSERT_EXCEPTION(
            ParseConnectionString("http://localhost:2135/?database=/local"),
            TContractViolation
        );
    }

    Y_UNIT_TEST(EmptyConnectionString) {
        // Test: empty string
        UNIT_ASSERT_EXCEPTION(
            ParseConnectionString(""),
            TContractViolation
        );
    }
}

} // namespace NYdb
