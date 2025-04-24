#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/yql_yt_table_data_service_local.h>

namespace NYql::NFmr {

const std::vector<TString> TableYsonRows = {
    "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};",
    "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};",
    "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};",
    "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};"
};

TTableStats WriteDataToTableDataSerice(
    ITableDataService::TPtr tableDataService,
    const std::vector<TString>& tableYsonRows,
    ui64 chunkSize,
    TMaybe<ui64> maxRowWeight = Nothing()
) {
    TFmrWriterSettings settings{.ChunkSize = chunkSize};
    if (maxRowWeight) {
        settings.MaxRowWeight = *maxRowWeight;
    }
    TFmrTableDataServiceWriter outputWriter("tableId", "partId", tableDataService, settings);

    for (auto& row: tableYsonRows) {
        outputWriter.Write(row.data(), row.size());
        outputWriter.NotifyRowEnd();
    }
    outputWriter.Flush();
    return outputWriter.GetStats();
}

Y_UNIT_TEST_SUITE(FmrWriterTests) {
    Y_UNIT_TEST(WriteYsonRows) {
        ui64 totalSize = 0;
        for (auto& row: TableYsonRows) {
            totalSize += row.size();
        }

        ui64 chunkSize = totalSize / 2;
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        auto stats = WriteDataToTableDataSerice(tableDataService, TableYsonRows, chunkSize);
        auto realChunks = stats.Chunks;
        auto realDataWeight =stats.DataWeight;
        UNIT_ASSERT_VALUES_EQUAL(realChunks, 2);
        UNIT_ASSERT_VALUES_EQUAL(realDataWeight, totalSize);

        TString expectedFirstChunkTableContent = JoinRange(TStringBuf(), TableYsonRows.begin(), TableYsonRows.begin() + 2);
        TString expectedSecondChunkTableContent = JoinRange(TStringBuf(), TableYsonRows.begin() + 2, TableYsonRows.end());

        auto firstChunkTableKey = GetTableDataServiceKey("tableId", "partId", 0);
        auto firstChunkTableContent = tableDataService->Get(firstChunkTableKey).GetValueSync();
        auto secondChunkTableKey = GetTableDataServiceKey("tableId", "partId", 1);
        auto secondChunkTableContent = tableDataService->Get(secondChunkTableKey).GetValueSync();

        UNIT_ASSERT_NO_DIFF(*firstChunkTableContent, expectedFirstChunkTableContent);
        UNIT_ASSERT_NO_DIFF(*secondChunkTableContent, expectedSecondChunkTableContent);
    }
    Y_UNIT_TEST(RecordIsLargerThanMaxRowWeight) {
        ui64 chunkSize = 1, maxRowWeight = 3;
        auto rowSize = TableYsonRows[0].size();
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));
        TString expectedErrorMessage = TStringBuilder() << rowSize << " is larger than max row weight: " << maxRowWeight;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            WriteDataToTableDataSerice(tableDataService, TableYsonRows, chunkSize, maxRowWeight),
            yexception,
            expectedErrorMessage
        );
    }
}

} // namespace NYql::NFmr
