#include <gtest/gtest.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/parser.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

using namespace NYdb;

TEST(ConnectionStringParser, ParseWithQueryParameter) {
    // Test: grpc://localhost:2135/?database=/local
    auto connInfo = ParseConnectionString("grpc://localhost:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseWithQueryParameterNoSlash) {
    // Test: grpc://localhost:2135?database=/local
    auto connInfo = ParseConnectionString("grpc://localhost:2135?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseWithPathAsDatabase) {
    // Test: grpc://localhost:2135/local
    auto connInfo = ParseConnectionString("grpc://localhost:2135/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseSecureConnection) {
    // Test: grpcs://localhost:2135/?database=/local
    auto connInfo = ParseConnectionString("grpcs://localhost:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseSecureConnectionNoSlash) {
    // Test: grpcs://localhost:2135?database=/local
    auto connInfo = ParseConnectionString("grpcs://localhost:2135?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseSecureConnectionWithPath) {
    // Test: grpcs://localhost:2135/local
    auto connInfo = ParseConnectionString("grpcs://localhost:2135/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseNoSchemeLocalhost) {
    // Test: localhost:2135/?database=/local
    auto connInfo = ParseConnectionString("localhost:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseNoSchemeNonLocalhost) {
    // Test: example.com:2135/?database=/local (should use SSL)
    auto connInfo = ParseConnectionString("example.com:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "example.com:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseNoSchemeWithPath) {
    // Test: localhost:2135/local
    auto connInfo = ParseConnectionString("localhost:2135/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseNoDatabaseInQuery) {
    // Test: grpc://localhost:2135
    auto connInfo = ParseConnectionString("grpc://localhost:2135");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, InvalidScheme) {
    // Test: http://localhost:2135/?database=/local
    EXPECT_THROW(
        ParseConnectionString("http://localhost:2135/?database=/local"),
        TContractViolation
    );
}

TEST(ConnectionStringParser, EmptyConnectionString) {
    // Test: empty string
    EXPECT_THROW(
        ParseConnectionString(""),
        TContractViolation
    );
}

TEST(ConnectionStringParser, DatabaseInBothPathAndQuery) {
    // Test: grpc://localhost:2135/local?database=/other (should throw)
    EXPECT_THROW(
        ParseConnectionString("grpc://localhost:2135/local?database=/other"),
        TContractViolation
    );
}
