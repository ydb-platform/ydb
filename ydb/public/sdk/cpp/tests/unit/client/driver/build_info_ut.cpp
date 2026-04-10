#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(AppendBuildInfoValidation) {
    Y_UNIT_TEST(ValidSegments) {
        TDriverConfig config;
        UNIT_ASSERT_NO_EXCEPTION(config.AppendBuildInfo("ydb-cli/2.31.0"));
        UNIT_ASSERT_NO_EXCEPTION(config.AppendBuildInfo("cli-command-ydb-import-file-csv/2.31.0"));
        UNIT_ASSERT_NO_EXCEPTION(config.AppendBuildInfo("ya-ydb/2026.02.09"));
    }

    Y_UNIT_TEST(EmptySegmentIsNoOp) {
        TDriverConfig config;
        UNIT_ASSERT_NO_EXCEPTION(config.AppendBuildInfo(""));
    }

    Y_UNIT_TEST(RejectsNoSlash) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsNoDots) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli/dev"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsOneDot) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli/2.0"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsThreeDots) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli/1.2.3.4"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsDashInVersion) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli/2.31.0-rc1"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsUppercase) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("YDB/1.0.0"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsSemicolon) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("a;b/1.0.0"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsEmptyName) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("/1.0.0"),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(RejectsEmptyVersionPart) {
        TDriverConfig config;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli/.1.0"),
            yexception,
            "Invalid build info segment");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli/1..0"),
            yexception,
            "Invalid build info segment");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo("ydb-cli/1.0."),
            yexception,
            "Invalid build info segment");
    }

    Y_UNIT_TEST(LengthLimit) {
        TDriverConfig config;
        std::string longName(500, 'a');
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            config.AppendBuildInfo(longName + "/1.0.0"),
            yexception,
            "exceeds maximum length");
    }

    Y_UNIT_TEST(MultipleSegmentsAccumulate) {
        TDriverConfig config;
        config.AppendBuildInfo("ydb-cli/2.31.0");
        config.AppendBuildInfo("cli-command-ydb-sql/2.31.0");
    }
}
