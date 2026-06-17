#include "bulk_upsert.h"

#include <library/cpp/testing/gtest/gtest.h>

#include <util/string/cast.h>


TEST(BulkUpsert, BulkUpsert) {
    uint32_t correctSumApp = 0;
    uint32_t correctSumHost = 0;
    uint32_t correctRowCount = 0;

    auto [driver, path] = GetRunArgs();

    TTableClient client(driver);
    uint32_t count = 1000;
    TStatus statusCreate = CreateTable(client, path);
    if (!statusCreate.IsSuccess()) {
        FAIL() << "Create table failed with status: " << ToString(statusCreate) << std::endl;
    }

    TRetryOperationSettings writeRetrySettings;
    writeRetrySettings
            .Idempotent(true)
            .MaxRetries(20);

    std::vector<TLogMessage> logBatch;
    for (uint32_t offset = 0; offset < count; ++offset) {

        auto [batchSumApp, batchSumHost, batchRowCount] = GetLogBatch(offset, logBatch, correctRowCount);
        correctSumApp += batchSumApp;
        correctSumHost += batchSumHost;
        correctRowCount += batchRowCount;

        TStatus statusWrite = WriteLogBatch(client, path, logBatch, writeRetrySettings);
        if (!statusWrite.IsSuccess()) {
            FAIL() << "Write failed with status: " << ToString(statusWrite) << std::endl;
        }
    }

    try {
        auto [sumApp, sumHost, rowCount] = Select(client, path);
        EXPECT_EQ(rowCount, correctRowCount);
        EXPECT_EQ(sumApp, correctSumApp);
        EXPECT_EQ(sumHost, correctSumHost);
    } catch (const NYdb::NStatusHelpers::TYdbErrorException& e) {
        driver.Stop(true);
        FAIL() << "Execution failed due to fatal error:\n" << e.what() << std::endl;
    }

    DropTable(client, path);
    driver.Stop(true);
}

TEST(BulkUpsert, RetryOverheadOnHappyPath) {
    constexpr size_t kPerfBatchSize = 10000;
    constexpr size_t kWarmupIterations = 3;
    constexpr size_t kMeasuredIterations = 30;
    // Retry wrapper adds client-side bookkeeping; server RPC dominates, so allow modest variance.
    constexpr double kMaxSlowdownRatio = 1.1;

    auto [driver, basePath] = GetRunArgs();
    const std::string tableNoRetry = basePath + "_perf_noretry";
    const std::string tableWithRetry = basePath + "_perf_retry";

    TTableClient client(driver);

    const auto statusCreateNoRetry = CreateTable(client, tableNoRetry);
    ASSERT_TRUE(statusCreateNoRetry.IsSuccess()) << ToString(statusCreateNoRetry);
    const auto statusCreateWithRetry = CreateTable(client, tableWithRetry);
    ASSERT_TRUE(statusCreateWithRetry.IsSuccess()) << ToString(statusCreateWithRetry);

    const TBulkUpsertSettings noRetrySettings = [] {
        TBulkUpsertSettings settings;
        settings.RetrySettings(TRetryOperationSettings().MaxRetries(0));
        return settings;
    }();

    const TBulkUpsertSettings withRetrySettings = [] {
        TBulkUpsertSettings settings;
        settings.RetrySettings(TRetryOperationSettings().MaxRetries(10).Idempotent(true));
        return settings;
    }();

    auto runInterleavedIterations = [&](size_t iterations) {
        TDuration noRetryTotal;
        TDuration withRetryTotal;
        for (size_t i = 0; i < iterations; ++i) {
            const uint32_t offset = static_cast<uint32_t>(i * kPerfBatchSize);
            noRetryTotal += MeasureBulkUpsertWallTime(
                client, tableNoRetry, i, offset, noRetrySettings, kPerfBatchSize);
            withRetryTotal += MeasureBulkUpsertWallTime(
                client, tableWithRetry, i, offset, withRetrySettings, kPerfBatchSize);
        }
        return std::make_pair(noRetryTotal, withRetryTotal);
    };

    try {
        runInterleavedIterations(kWarmupIterations);

        const auto [noRetryTime, withRetryTime] = runInterleavedIterations(kMeasuredIterations);

        const double ratio = static_cast<double>(withRetryTime.MicroSeconds())
            / static_cast<double>(noRetryTime.MicroSeconds());

        EXPECT_LE(ratio, kMaxSlowdownRatio)
            << "BulkUpsert with retries should not be significantly slower on the happy path."
            << " noRetryTotalUs=" << noRetryTime.MicroSeconds()
            << " withRetryTotalUs=" << withRetryTime.MicroSeconds()
            << " ratio=" << ratio;
    } catch (const NYdb::NStatusHelpers::TYdbErrorException& e) {
        FAIL() << "BulkUpsert benchmark failed:\n" << e.what();
    }

    DropTable(client, tableNoRetry);
    DropTable(client, tableWithRetry);
    driver.Stop(true);
}
