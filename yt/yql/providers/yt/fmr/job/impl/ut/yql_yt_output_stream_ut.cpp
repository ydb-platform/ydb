#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_output_stream.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(OutputStreamTests) {
    Y_UNIT_TEST(WriteOneChunk) {
        size_t chunkSize = 1024;

        TString originalTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrOutputStreamSettings settings{chunkSize};
        TFmrOutputStream outputStream("tableId", "partId", tableDataServicePtr, settings);

        outputStream.Write(originalTableContent.data(), originalTableContent.size());
        outputStream.Flush();

        auto tableKey = GetTableDataServiceKey("tableId", "partId", 0);
        auto uploadedTableContent = tableDataServicePtr->Get(tableKey).GetValueSync();

        auto expectedTableContent = originalTableContent;
        auto expectedChunks = expectedTableContent.size() / chunkSize + 1;
        auto expectedDataWeight = expectedTableContent.size();

        auto realChunks = outputStream.GetStats().Chunks;
        auto realDataWeight = outputStream.GetStats().DataWeight;

        UNIT_ASSERT_EQUAL_C(expectedChunks, realChunks, TString() << "expected_chunks: " << expectedChunks << " real_chunks: " << realChunks);
        UNIT_ASSERT_EQUAL_C(expectedDataWeight, realDataWeight, TString() << "expected_data_weight: " << expectedDataWeight << " real_data_weight: " << realDataWeight);

        UNIT_ASSERT_C(uploadedTableContent, "Uploaded table not found");
        UNIT_ASSERT_NO_DIFF(originalTableContent, *uploadedTableContent);
    }
    Y_UNIT_TEST(WriteOneChunkWithMultipleWrites) {
        size_t chunkSize = 1024;

        TString originalTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrOutputStreamSettings settings{chunkSize};
        TFmrOutputStream outputStream("tableId", "partId", tableDataServicePtr, settings);

        outputStream.Write(originalTableContent.data(), originalTableContent.size());
        outputStream.Write(originalTableContent.data(), originalTableContent.size());
        outputStream.Write(originalTableContent.data(), originalTableContent.size());
        outputStream.Flush();

        auto tableKey = GetTableDataServiceKey("tableId", "partId", 0);
        auto uploadedTableContent = tableDataServicePtr->Get(tableKey).GetValueSync();

        auto expectedTableContent = originalTableContent * 3;
        auto expectedChunks = expectedTableContent.size() / chunkSize + 1;
        auto expectedDataWeight = expectedTableContent.size();

        auto realChunks = outputStream.GetStats().Chunks;
        auto realDataWeight = outputStream.GetStats().DataWeight;

        UNIT_ASSERT_EQUAL_C(expectedChunks, realChunks, TString() << "expected_chunks: " << expectedChunks << " real_chunks: " << realChunks);
        UNIT_ASSERT_EQUAL_C(expectedDataWeight, realDataWeight, TString() << "expected_data_weight: " << expectedDataWeight << " real_data_weight: " << realDataWeight);

        UNIT_ASSERT_C(uploadedTableContent, "Uploaded table not found");
        UNIT_ASSERT_NO_DIFF(expectedTableContent, *uploadedTableContent);
    }
    Y_UNIT_TEST(WriteMultipleChunksWithOneWrite) {
        size_t chunkSize = 128;

        TString originalTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        originalTableContent *= 3;

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrOutputStreamSettings settings{chunkSize};
        TFmrOutputStream outputStream("tableId", "partId", tableDataServicePtr, settings);

        outputStream.Write(originalTableContent.data(), originalTableContent.size());
        outputStream.Flush();

        auto expectedTableContent = originalTableContent;
        auto expectedChunks = expectedTableContent.size() / chunkSize + 1;
        auto expectedDataWeight = expectedTableContent.size();

        auto realChunks = outputStream.GetStats().Chunks;
        auto realDataWeight = outputStream.GetStats().DataWeight;

        UNIT_ASSERT_EQUAL_C(expectedChunks, realChunks, TString() << "expected_chunks: " << expectedChunks << " real_chunks: " << realChunks);
        UNIT_ASSERT_EQUAL_C(expectedDataWeight, realDataWeight, TString() << "expected_data_weight: " << expectedDataWeight << " real_data_weight: " << realDataWeight);

        TString uploadedTableContent;
        for (size_t chunk = 0; chunk < expectedChunks; ++chunk) {
            auto tableKey = GetTableDataServiceKey("tableId", "partId", chunk);
            auto uploadedChunkContent = tableDataServicePtr->Get(tableKey).GetValueSync();

            UNIT_ASSERT_C(uploadedChunkContent, "Uploaded chunk not found");
            uploadedTableContent += *uploadedChunkContent;
        }
        UNIT_ASSERT_NO_DIFF(expectedTableContent, uploadedTableContent);
    }
    Y_UNIT_TEST(WriteMultipleDifferentChunksWithMultipleWrites) {
        size_t chunkSize = 1024;

        TString originalTableContent =
        "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
        "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
        "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
        "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

        auto originalSmallTableContent = originalTableContent;
        auto originalBigTableContent = originalTableContent * 10;

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrOutputStreamSettings settings{chunkSize};
        TFmrOutputStream outputStream("tableId", "partId", tableDataServicePtr, settings);

        // small + big + small + small + big
        outputStream.Write(originalSmallTableContent.data(), originalSmallTableContent.size());
        outputStream.Write(originalBigTableContent.data(), originalBigTableContent.size());
        outputStream.Write(originalSmallTableContent.data(), originalSmallTableContent.size());
        outputStream.Write(originalSmallTableContent.data(), originalSmallTableContent.size());
        outputStream.Write(originalBigTableContent.data(), originalBigTableContent.size());
        outputStream.Flush();

        auto expectedTableContent = originalSmallTableContent + originalBigTableContent + originalSmallTableContent + originalSmallTableContent + originalBigTableContent;
        auto expectedChunks = expectedTableContent.size() / chunkSize + 1;
        auto expectedDataWeight = expectedTableContent.size();

        auto realChunks = outputStream.GetStats().Chunks;
        auto realDataWeight = outputStream.GetStats().DataWeight;

        UNIT_ASSERT_EQUAL_C(expectedChunks, realChunks, TString() << "expected_chunks: " << expectedChunks << " real_chunks: " << realChunks);
        UNIT_ASSERT_EQUAL_C(expectedDataWeight, realDataWeight, TString() << "expected_data_weight: " << expectedDataWeight << " real_data_weight: " << realDataWeight);

        TString uploadedTableContent;
        for (size_t chunk = 0; chunk < expectedChunks; ++chunk) {
            auto tableKey = GetTableDataServiceKey("tableId", "partId", chunk);
            auto uploadedChunkContent = tableDataServicePtr->Get(tableKey).GetValueSync();

            UNIT_ASSERT_C(uploadedChunkContent, "Uploaded chunk not found");
            uploadedTableContent += *uploadedChunkContent;
        }
        UNIT_ASSERT_NO_DIFF(expectedTableContent, uploadedTableContent);
    }
}
} // namespace NYql::NFmr
