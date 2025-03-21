#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>

namespace NYql::NFmr {

TString originalTableContent = "{\"key\"=\"075\";\"subkey\"=\"1\";\"value\"=\"abc\"};"
                            "{\"key\"=\"800\";\"subkey\"=\"2\";\"value\"=\"ddd\"};"
                            "{\"key\"=\"020\";\"subkey\"=\"3\";\"value\"=\"q\"};"
                            "{\"key\"=\"150\";\"subkey\"=\"4\";\"value\"=\"qzz\"};";

Y_UNIT_TEST_SUITE(FmrRawTableReaderTests) {
    Y_UNIT_TEST(ReadOneChunkSmallPart) {
        size_t chunkSize = 1024;

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrTableDataServiceWriterSettings settings{chunkSize};
        TFmrTableDataServiceWriter outputWriter("tableId", "partId", tableDataServicePtr, settings);

        outputWriter.Write(originalTableContent.data(), originalTableContent.size());
        outputWriter.Flush();

        TFmrTableDataServiceReaderSettings readerSettings{1};
        std::vector<TTableRange> tableRanges = {{"partId", 0, 1}};
        TFmrTableDataServiceReader reader("tableId", tableRanges, tableDataServicePtr, readerSettings);

        char buffer[10];
        reader.Read(buffer, 10);
        TString readTableContentPart = {buffer, 10};
        auto originalTableContentPart = originalTableContent.substr(0, 10);
        UNIT_ASSERT_NO_DIFF(readTableContentPart, originalTableContentPart);
    }

    Y_UNIT_TEST(ReadAllOneChunk) {
        size_t chunkSize = 1024;

        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrTableDataServiceWriterSettings settings{chunkSize};
        TFmrTableDataServiceWriter outputStream("tableId", "partId", tableDataServicePtr, settings);

        outputStream.Write(originalTableContent.data(), originalTableContent.size());
        outputStream.Flush();

        TFmrTableDataServiceReaderSettings readerSettings{1};
        std::vector<TTableRange> tableRanges = {{"partId", 0, 1}};
        TFmrTableDataServiceReader reader("tableId", tableRanges, tableDataServicePtr, readerSettings);

        auto readTableContent = reader.ReadAll();
        UNIT_ASSERT_NO_DIFF(readTableContent, originalTableContent);
    }

    Y_UNIT_TEST(ReadAllMultipleChunks) {
        size_t chunkSize = 32;
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrTableDataServiceWriterSettings settings{chunkSize};
        TFmrTableDataServiceWriter outputStream("tableId", "partId", tableDataServicePtr, settings);

        for (size_t i = 0; i < 3; ++i) {
            outputStream.Write(originalTableContent.data(), originalTableContent.size());
            outputStream.NotifyRowEnd();
        }
        outputStream.Flush();

        TFmrTableDataServiceReaderSettings readerSettings{1};
        std::vector<TTableRange> tableRanges = {{"partId", 0, 3}};
        TFmrTableDataServiceReader reader("tableId", tableRanges, tableDataServicePtr, readerSettings);

        auto readTableContent = reader.ReadAll();
        UNIT_ASSERT_NO_DIFF(readTableContent, originalTableContent * 3);
    }

    Y_UNIT_TEST(ReadAllMultipleChunksBigReadAhead) {
        size_t chunkSize = 32;
        ITableDataService::TPtr tableDataServicePtr = MakeLocalTableDataService(TLocalTableDataServiceSettings(1));

        TFmrTableDataServiceWriterSettings settings{chunkSize};
        TFmrTableDataServiceWriter outputStream("tableId", "partId", tableDataServicePtr, settings);

        for (size_t i = 0; i < 3; ++i) {
            outputStream.Write(originalTableContent.data(), originalTableContent.size());
            outputStream.NotifyRowEnd();
        }
        outputStream.Flush();

        TFmrTableDataServiceReaderSettings readerSettings{5};
        std::vector<TTableRange> tableRanges = {{"partId", 0, 3}};
        TFmrTableDataServiceReader reader("tableId", tableRanges, tableDataServicePtr, readerSettings);

        auto readTableContent = reader.ReadAll();
        UNIT_ASSERT_NO_DIFF(readTableContent, originalTableContent * 3);
    }
}

} // namespace NYql::NFmr
