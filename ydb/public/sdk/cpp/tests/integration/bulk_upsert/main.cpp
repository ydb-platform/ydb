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
