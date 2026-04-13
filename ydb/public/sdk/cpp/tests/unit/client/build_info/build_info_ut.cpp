#include <gtest/gtest.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

using namespace NYdb;

TEST(AppendBuildInfo, ValidSegments) {
    TDriverConfig config;
    EXPECT_NO_THROW(config.AppendBuildInfo("ydb-cli/2.31.0"));
    EXPECT_NO_THROW(config.AppendBuildInfo("ydb-cli-import-file-csv/2.31.0"));
    EXPECT_NO_THROW(config.AppendBuildInfo("ya-ydb/2026.02.09"));
}

TEST(AppendBuildInfo, EmptySegmentIsNoOp) {
    TDriverConfig config;
    EXPECT_NO_THROW(config.AppendBuildInfo(""));
}

TEST(AppendBuildInfo, RejectsNoSlash) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsNoDots) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli/dev"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsOneDot) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli/2.0"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsThreeDots) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli/1.2.3.4"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsDashInVersion) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli/2.31.0-rc1"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsUppercase) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("YDB/1.0.0"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsSemicolon) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("a;b/1.0.0"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsEmptyName) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("/1.0.0"), TContractViolation);
}

TEST(AppendBuildInfo, RejectsEmptyVersionPart) {
    TDriverConfig config;
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli/.1.0"), TContractViolation);
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli/1..0"), TContractViolation);
    EXPECT_THROW(config.AppendBuildInfo("ydb-cli/1.0."), TContractViolation);
}

TEST(AppendBuildInfo, LengthLimit) {
    TDriverConfig config;
    std::string longName(510, 'a');
    EXPECT_THROW(config.AppendBuildInfo(longName + "/1.0.0"), TContractViolation);
}

TEST(AppendBuildInfo, MultipleSegmentsAccumulate) {
    TDriverConfig config;
    EXPECT_NO_THROW(config.AppendBuildInfo("ydb-cli/2.31.0"));
    EXPECT_NO_THROW(config.AppendBuildInfo("ydb-cli-sql/2.31.0"));
}
