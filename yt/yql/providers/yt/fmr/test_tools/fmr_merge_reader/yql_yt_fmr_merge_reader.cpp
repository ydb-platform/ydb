#include "yql_yt_fmr_merge_reader.h"

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
    const TYtBlockIteratorSettings& settings
) {
    NYT::TRichYPath richPath;
    TYtTableRef testFileYtTable(richPath, filePath);

    IYtJobService::TPtr jobService = MakeFileYtJobService();
    TVector<NYT::TRawTableReaderPtr> fileReaders{jobService->MakeReader(testFileYtTable)};
    return MakeIntrusive<TYtBlockIterator>(fileReaders, keyColumns, settings);
}

IBlockIterator::TPtr MakeTdsIterator(
    const TString& tableId,
    const TString& partId,
    const ITableDataService::TPtr& tds,
    const TVector<TString>& keyColumns,
    const TVector<TString>& neededColumns
) {
    TVector<TTableRange> ranges = {{partId}};
    return MakeIntrusive<TTDSBlockIterator>(
        tableId,
        ranges,
        tds,
        keyColumns,
        TVector<ESortOrder>(keyColumns.size(), ESortOrder::Ascending),
        neededColumns,
        TString()
    );
}

TVector<IBlockIterator::TPtr> MakeTestIterators(TVector<TMergeTestTable> rawTestTables, TMaybe<ILocalTableDataService::TPtr> tds = {}, TYtBlockIteratorSettings settings = {}) {
    TVector<IBlockIterator::TPtr> iterators;
    ui64 counter = 1;
    for (const auto& table: rawTestTables) {
        if (table.SourceType == EMergeReaderSourceType::YT) {
            const auto tempFile = WriteTempFile(table.RawTableBody);
            iterators.push_back(MakeYtFileIterator(tempFile.Name(), table.KeyColumns, std::move(settings)));
        } else {
            const TString group = GetTableDataServiceGroup("table" + ToString(counter), "part");
            const TString chunk = GetTableDataServiceChunkId(0, TString());
            Y_ENSURE(tds);
            (*tds)->Put(group, chunk, GetBinaryYson(table.RawTableBody));
            iterators.push_back(MakeTdsIterator("table" + ToString(counter), "part", *tds, table.KeyColumns, table.NeededColumns));
        }
        counter++;
    }
    return iterators;
};

TString GetMergeResult(TVector<IBlockIterator::TPtr> inputs, TVector<ESortOrder> sortOrders) {
    auto reader = MakeIntrusive<TSortedMergeReader>(std::move(inputs), sortOrders);
    const TString mergedBinary = reader->ReadAll();
    return GetTextYson(mergedBinary);
}

} // namespace NYql::NFmr
