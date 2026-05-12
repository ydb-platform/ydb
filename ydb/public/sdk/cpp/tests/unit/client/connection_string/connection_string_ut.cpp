#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/parser.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <gtest/gtest.h>

using namespace NYdb;

TEST(ConnectionStringParser, ParseWithQueryParameter) {
    auto connInfo = ParseConnectionString("grpc://localhost:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseWithQueryParameterNoSlash) {
    auto connInfo = ParseConnectionString("grpc://localhost:2135?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseWithPathAsDatabase) {
    auto connInfo = ParseConnectionString("grpc://localhost:2135/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseSecureConnection) {
    auto connInfo = ParseConnectionString("grpcs://localhost:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseSecureConnectionNoSlash) {
    auto connInfo = ParseConnectionString("grpcs://localhost:2135?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseSecureConnectionWithPath) {
    auto connInfo = ParseConnectionString("grpcs://localhost:2135/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseNoSchemeLocalhost) {
    auto connInfo = ParseConnectionString("localhost:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseNoSchemeNonLocalhost) {
    auto connInfo = ParseConnectionString("example.com:2135/?database=/local");
    EXPECT_EQ(connInfo.Endpoint, "example.com:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, true);
}

TEST(ConnectionStringParser, ParseNoSchemeWithPath) {
    auto connInfo = ParseConnectionString("localhost:2135/local");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "/local");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, ParseNoDatabaseInQuery) {
    auto connInfo = ParseConnectionString("grpc://localhost:2135");
    EXPECT_EQ(connInfo.Endpoint, "localhost:2135");
    EXPECT_EQ(connInfo.Database, "");
    EXPECT_EQ(connInfo.EnableSsl, false);
}

TEST(ConnectionStringParser, InvalidScheme) {
    EXPECT_THROW(
        ParseConnectionString("http://localhost:2135/?database=/local"),
        TContractViolation
    );
}

TEST(ConnectionStringParser, EmptyConnectionString) {
    EXPECT_THROW(
        ParseConnectionString(""),
        TContractViolation
    );
}

TEST(ConnectionStringParser, DatabaseInBothPathAndQuery) {
    EXPECT_THROW(
        ParseConnectionString("grpc://localhost:2135/local?database=/other"),
        TContractViolation
    );
}
