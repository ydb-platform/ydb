#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>

namespace NYql::NFmr {

Y_UNIT_TEST_SUITE(OutputStreamTests) {
    Y_UNIT_TEST(WriteYsonRows) {
        std::vector<TString> tableYsonRows = {
            "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};",
            "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};",
            "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};",
            "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};"
        };
        ui64 totalSize = 0;
        for (auto& row: tableYsonRows) {
            totalSize += row.size();
        }

        ui64 chunkSize = totalSize / 2;
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TFmrTableDataServiceWriterSettings settings{.ChunkSize = chunkSize};
        TFmrTableDataServiceWriter outputWriter("tableId", "partId", tableDataServicePtr, settings);

        for (auto& row: tableYsonRows) {
            outputWriter.Write(row.data(), row.size());
            outputWriter.NotifyRowEnd();
        }
        outputWriter.Flush();

        auto realChunks = outputWriter.GetStats().Chunks;
        auto realDataWeight = outputWriter.GetStats().DataWeight;
        UNIT_ASSERT_VALUES_EQUAL(realChunks, 2);
        UNIT_ASSERT_VALUES_EQUAL(realDataWeight, totalSize);

        TString expectedFirstChunkTableContent = JoinRange(TStringBuf(), tableYsonRows.begin(), tableYsonRows.begin() + 2);
        TString expectedSecondChunkTableContent = JoinRange(TStringBuf(), tableYsonRows.begin() + 2, tableYsonRows.end());

        auto firstChunkTableKey = GetTableDataServiceKey("tableId", "partId", 0);
        auto firstChunkTableContent = tableDataServicePtr->Get(firstChunkTableKey).GetValueSync();
        auto secondChunkTableKey = GetTableDataServiceKey("tableId", "partId", 1);
        auto secondChunkTableContent = tableDataServicePtr->Get(secondChunkTableKey).GetValueSync();

        UNIT_ASSERT_NO_DIFF(*firstChunkTableContent, expectedFirstChunkTableContent);
        UNIT_ASSERT_NO_DIFF(*secondChunkTableContent, expectedSecondChunkTableContent);
    }
}

} // namespace NYql::NFmr
