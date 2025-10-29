#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/impl/yql_yt_table_data_service_local.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>

namespace NYql::NFmr {

TString originalTableContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};\n"
                            "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};\n"
                            "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};\n"
                            "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};\n";

Y_UNIT_TEST_SUITE(FmrReaderTests) {
    Y_UNIT_TEST(ReadAllOneChunk) {
        size_t chunkSize = 1024;

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService();

        TFmrWriterSettings settings{.ChunkSize= chunkSize, .MaxInflightChunks = 2};
        TFmrTableDataServiceWriter outputStream("tableId", "partId", tableDataServicePtr, TString(), settings);

        outputStream.Write(originalTableContent.data(), originalTableContent.size());
        outputStream.Flush();

        TFmrReaderSettings readerSettings{1};
        std::vector<TTableRange> tableRanges = {{"partId", 0, 1}};
        TFmrTableDataServiceReader reader("tableId", tableRanges, tableDataServicePtr, {}, TString(), readerSettings);

        auto readTableContent = reader.ReadAll();
        UNIT_ASSERT_NO_DIFF(GetTextYson(readTableContent), originalTableContent);
    }

    Y_UNIT_TEST(ReadAllMultipleChunks) {
        size_t chunkSize = 32;
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService();

        TFmrWriterSettings settings{.ChunkSize= chunkSize, .MaxInflightChunks = 2};
        TFmrTableDataServiceWriter outputStream("tableId", "partId", tableDataServicePtr, TString(), settings);

        for (size_t i = 0; i < 3; ++i) {
            outputStream.Write(originalTableContent.data(), originalTableContent.size());
            outputStream.NotifyRowEnd();
        }
        outputStream.Flush();

        TFmrReaderSettings readerSettings{1};
        std::vector<TTableRange> tableRanges = {{"partId", 0, 3}};
        TFmrTableDataServiceReader reader("tableId", tableRanges, tableDataServicePtr, {}, TString(), readerSettings);

        auto readTableContent = reader.ReadAll();
        UNIT_ASSERT_NO_DIFF(GetTextYson(readTableContent), originalTableContent * 3);
    }

    Y_UNIT_TEST(ReadAllMultipleChunksBigReadAhead) {
        size_t chunkSize = 32;
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService();

        TFmrWriterSettings settings{chunkSize};
        TFmrTableDataServiceWriter outputStream("tableId", "partId", tableDataServicePtr, TString(), settings);

        for (size_t i = 0; i < 3; ++i) {
            outputStream.Write(originalTableContent.data(), originalTableContent.size());
            outputStream.NotifyRowEnd();
        }
        outputStream.Flush();

        TFmrReaderSettings readerSettings{5};
        std::vector<TTableRange> tableRanges = {{"partId", 0, 3}};
        TFmrTableDataServiceReader reader("tableId", tableRanges, tableDataServicePtr, {}, TString(), readerSettings);

        auto readTableContent = reader.ReadAll();
        UNIT_ASSERT_NO_DIFF(GetTextYson(readTableContent), originalTableContent * 3);
    }
}

} // namespace NYql::NFmr
