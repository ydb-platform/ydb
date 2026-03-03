#include "yql_yt_fmr_block_iterator.h"

namespace NYql::NFmr {

TTempFileHandle WriteTempFile(const TString& content) {
    TTempFileHandle file;
    TFileOutput out(file.Name());
    out.Write(content.data(), content.size());
    return file;
}

IBlockIterator::TPtr MakeYtFileIterator(
    const TString& filePath,
    const TVector<TString>& keyColumns,
    const TYtBlockIteratorSettings& settings,
    const TVector<ESortOrder>& sortOrders
) {
    NYT::TRichYPath richPath;
    TYtTableRef testFileYtTable(richPath, filePath);

    IYtJobService::TPtr jobService = MakeFileYtJobService();
    TVector<NYT::TRawTableReaderPtr> fileReaders{jobService->MakeReader(testFileYtTable)};
    return MakeIntrusive<TYtBlockIterator>(fileReaders, keyColumns, settings, sortOrders);
}

IBlockIterator::TPtr MakeTableDataServiceIterator(
    const TString& tableId,
    const TString& partId,
    const ITableDataService::TPtr& tds,
    const TVector<TString>& keyColumns,
    const TVector<TString>& neededColumns,
    const TVector<ESortOrder>& sortOrders
) {
    TVector<TTableRange> ranges = {{partId}};
    return MakeIntrusive<TTableDataServiceBlockIterator>(
        tableId,
        ranges,
        tds,
        keyColumns,
        sortOrders,
        neededColumns,
        TString()
    );
}

TVector<IBlockIterator::TPtr> MakeTestBlockIterators(
    TVector<TMergeTestTable> rawTestTables,
    TMaybe<ILocalTableDataService::TPtr> tableDataService,
    TYtBlockIteratorSettings settings,
    const TVector<ESortOrder>& sortOrders
) {
    TVector<IBlockIterator::TPtr> iterators;
    ui64 counter = 1;
    for (const auto& table: rawTestTables) {
        if (table.SourceType == EMergeReaderSourceType::YT) {
            const auto tempFile = WriteTempFile(table.RawTableBody);
            iterators.push_back(MakeYtFileIterator(tempFile.Name(), table.KeyColumns, std::move(settings), sortOrders));
        } else {
            const TString group = GetTableDataServiceGroup("table" + ToString(counter), "part");
            const TString chunk = GetTableDataServiceChunkId(0, TString());
            Y_ENSURE(tableDataService);
            (*tableDataService)->Put(group, chunk, GetBinaryYson(table.RawTableBody));
            iterators.push_back(MakeTableDataServiceIterator("table" + ToString(counter), "part", *tableDataService, table.KeyColumns, table.NeededColumns, sortOrders));
        }
        counter++;
    }
    return iterators;
};

} // namespace NYql::NFmr
