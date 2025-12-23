#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>

namespace NYql::NFmr {

const std::vector<TString> TableYsonRows = {
    "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n",
    "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n",
    "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};\n",
    "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};\n"
};

TTableChunkStats WriteDataToTableDataSerice(
    ITableDataService::TPtr tableDataService,
    const std::vector<TString>& tableYsonRows,
    ui64 chunkSize,
    TMaybe<ui64> maxRowWeight = Nothing()
) {
    TFmrWriterSettings settings{.ChunkSize = chunkSize};
    if (maxRowWeight) {
        settings.MaxRowWeight = *maxRowWeight;
    }
    TFmrTableDataServiceWriter outputWriter("tableId", "partId", tableDataService, TString(), settings);

    for (auto& row: tableYsonRows) {
        outputWriter.Write(row.data(), row.size());
        outputWriter.NotifyRowEnd();
    }
    outputWriter.Flush();
    return outputWriter.GetStats();
}

Y_UNIT_TEST_SUITE(FmrWriterTests) {
    Y_UNIT_TEST(WriteYsonRows) {
        ui64 totalSize = 0, firstPartSize = 0, secPartSize = 0;
        for (ui64 i = 0; i < TableYsonRows.size(); ++i) {
            auto& row = TableYsonRows[i];
            totalSize += row.size();
            if (i < 2) {
                firstPartSize += row.size();
            } else {
                secPartSize += row.size();
            }
        }

        ui64 chunkSize = totalSize / 2;
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService();

        auto stats = WriteDataToTableDataSerice(tableDataService, TableYsonRows, chunkSize);
        UNIT_ASSERT_VALUES_EQUAL(stats.PartId, "partId");
        std::vector<TChunkStats> gottenPartIdChunkStats = stats.PartIdChunkStats;

        // Проверяем количество чанков
        UNIT_ASSERT_VALUES_EQUAL(gottenPartIdChunkStats.size(), 2);

        // Проверяем первый чанк
        UNIT_ASSERT_VALUES_EQUAL(gottenPartIdChunkStats[0].Rows, 2);
        UNIT_ASSERT_VALUES_EQUAL(gottenPartIdChunkStats[0].DataWeight, firstPartSize);
        UNIT_ASSERT_VALUES_EQUAL(gottenPartIdChunkStats[0].SortedChunkStats.IsSorted, false);

        // Проверяем второй чанк
        UNIT_ASSERT_VALUES_EQUAL(gottenPartIdChunkStats[1].Rows, 2);
        UNIT_ASSERT_VALUES_EQUAL(gottenPartIdChunkStats[1].DataWeight, secPartSize);
        UNIT_ASSERT_VALUES_EQUAL(gottenPartIdChunkStats[1].SortedChunkStats.IsSorted, false);

        TString expectedFirstChunkTableContent = JoinRange(TStringBuf(), TableYsonRows.begin(), TableYsonRows.begin() + 2);
        TString expectedSecondChunkTableContent = JoinRange(TStringBuf(), TableYsonRows.begin() + 2, TableYsonRows.end());

        TString group = GetTableDataServiceGroup("tableId", "partId");
        auto firstChunkTableContent = tableDataService->Get(group, "0").GetValueSync();
        auto secondChunkTableContent = tableDataService->Get(group, "1").GetValueSync();

        UNIT_ASSERT_NO_DIFF(GetTextYson(*firstChunkTableContent), expectedFirstChunkTableContent);
        UNIT_ASSERT_NO_DIFF(GetTextYson(*secondChunkTableContent), expectedSecondChunkTableContent);
    }
    Y_UNIT_TEST(RecordIsLargerThanMaxRowWeight) {
        ui64 chunkSize = 1, maxRowWeight = 3;
        auto rowSize = TableYsonRows[0].size();
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService();
        TString expectedErrorMessage = TStringBuilder() << rowSize << " is larger than max row weight: " << maxRowWeight;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            WriteDataToTableDataSerice(tableDataService, TableYsonRows, chunkSize, maxRowWeight),
            yexception,
            expectedErrorMessage
        );
    }

    Y_UNIT_TEST(SortingWriterBoundaryKeys) {
        std::vector<TString> textYsonRows = {
            "{\"key\"=20;\"subkey\"=\"a\";\"value\"=\"x\"}",
            "{\"key\"=50;\"subkey\"=\"b\";\"value\"=\"y\"}",
            "{\"key\"=75;\"subkey\"=\"c\";\"value\"=\"z\"}",
            "{\"key\"=150;\"subkey\"=\"d\";\"value\"=\"w\"}"
        };

        std::vector<TString> binaryYsonRows;
        for (const auto& row : textYsonRows) {
            TString binaryRow = GetBinaryYson(row);
            binaryYsonRows.push_back(std::move(binaryRow));
        }

        // Set chunk size to trigger split after 2nd row (before 3rd row)
        ui64 sizeOfFirstTwoRows = binaryYsonRows[0].size() + binaryYsonRows[1].size();
        ui64 chunkSize = sizeOfFirstTwoRows;
        ITableDataService::TPtr tableDataService = MakeLocalTableDataService();

        TFmrWriterSettings settings{.ChunkSize = chunkSize};
        TSortingColumns sortingColumns;
        sortingColumns.Columns = {"key", "subkey"};
        sortingColumns.SortOrders = {ESortOrder::Ascending, ESortOrder::Ascending};

        TFmrTableDataServiceSortedWriter outputWriter(
            "tableId",
            "partId",
            tableDataService,
            TString(),
            settings,
            sortingColumns
        );

        for (const auto& row : binaryYsonRows) {
            outputWriter.Write(row.data(), row.size());
            outputWriter.NotifyRowEnd();
        }
        outputWriter.Flush();

        auto stats = outputWriter.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(stats.PartIdChunkStats.size(), 2);

        auto& chunk1Stats = stats.PartIdChunkStats[0];
        UNIT_ASSERT_VALUES_EQUAL(chunk1Stats.Rows, 2);
        UNIT_ASSERT(chunk1Stats.SortedChunkStats.IsSorted);

        UNIT_ASSERT(chunk1Stats.SortedChunkStats.FirstRowKeys.IsMap());
        UNIT_ASSERT_VALUES_EQUAL(chunk1Stats.SortedChunkStats.FirstRowKeys["key"].AsInt64(), 20);

        auto& chunk2Stats = stats.PartIdChunkStats[1];
        UNIT_ASSERT_VALUES_EQUAL(chunk2Stats.Rows, 2);
        UNIT_ASSERT(chunk2Stats.SortedChunkStats.IsSorted);

        UNIT_ASSERT_VALUES_EQUAL(chunk2Stats.SortedChunkStats.FirstRowKeys["key"].AsInt64(), 75);
    }
}

} // namespace NYql::NFmr
