#include "yql_yt_table_data_service_sorted_writer.h"
#include <library/cpp/threading/future/wait/wait.h>
#include <util/string/join.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NFmr {

TFmrTableDataServiceSortedWriter::TFmrTableDataServiceSortedWriter(
    const TString& tableId,
    const TString& partId,
    ITableDataService::TPtr tableDataService,
    const TString& columnGroupSpec,
    const TFmrWriterSettings& settings,
    TSortingColumns keyColumns
)
    : TFmrTableDataServiceBaseWriter(tableId, partId, tableDataService, columnGroupSpec, settings),
    KeyColumns_(std::move(keyColumns))
{
}

void TFmrTableDataServiceSortedWriter::PutRows() {
    if (TableContent_.Size() == 0) {
        return;
    }
    std::unordered_map<TString, TString> splittedYsonByColumnGroups;
    auto currentYsonContent = TString(TableContent_.Data(), TableContent_.Size());

    auto parserKeyIndexes = TParserFragmentListIndex(currentYsonContent, KeyColumns_.Columns);
    parserKeyIndexes.Parse();
    const auto& chunkIndexes = parserKeyIndexes.GetRows();

    CheckIsSorted(currentYsonContent, chunkIndexes);

    TSortedRowMetadata metadata{chunkIndexes, KeyColumns_.Columns};
    TStringStream metadataStream;
    metadata.Save(&metadataStream);

    auto tableDataServiceGroup = GetTableDataServiceGroup(TableId_, PartId_);
    auto tableDataServiceMetaChunkId = GetTableDataServiceMetaChunkId(ChunkCount_);

    auto tdsService = this->TableDataService_;

    PutYsonByColumnGroups(currentYsonContent).Subscribe([tdsService, &tableDataServiceGroup, &tableDataServiceMetaChunkId, &metadataStream](const NThreading::TFuture<void>& future) {
        future.GetValue();
        return tdsService->Put(tableDataServiceGroup, tableDataServiceMetaChunkId,metadataStream.Str());
    });
    auto sortedChunkStats = GetSortedChunkStats(currentYsonContent, chunkIndexes);
    PartIdChunkStats_.emplace_back(TChunkStats{.Rows = CurrentChunkRows_, .DataWeight = TableContent_.Size(), .SortedChunkStats = sortedChunkStats});
    ClearTableData();
}

TString TFmrTableDataServiceSortedWriter::GetIndexValue(TStringBuf currentYsonContent, const TColumnOffsetRange& index) const {
    return TString(currentYsonContent.SubStr(index.StartOffset, index.EndOffset - index.StartOffset));
};

NYT::TNode TFmrTableDataServiceSortedWriter::GetKeyRowByIndexes(TStringBuf currentYsonContent, const TVector<TColumnOffsetRange>& indexes) const {
    NYT::TNode result = NYT::TNode::CreateMap();

    for (size_t i = 0; i < KeyColumns_.Columns.size(); ++i) {
        const auto& index = indexes[i];
        TString columnValue =  GetIndexValue(currentYsonContent, index);
        NYT::TNode columnValueNode = NYT::NodeFromYsonString(
            columnValue,
            NYT::NYson::EYsonType::Node
        );
        const TString& columnName = KeyColumns_.Columns[i];
        result[columnName] = std::move(columnValueNode);
    }
    return result;
};

TSortedChunkStats TFmrTableDataServiceSortedWriter::GetSortedChunkStats(TStringBuf currentYsonContent, const TVector<TRowIndexMarkup>& chunkIndexes) const {
    if (chunkIndexes.empty()) {
        return TSortedChunkStats{.IsSorted = true};
    }
    auto sortOrders = KeyColumns_.SortOrders;
    TBinaryYsonComparator comparator(currentYsonContent, sortOrders);

    auto upBoundIt = chunkIndexes[chunkIndexes.size() - 1];
    auto downBoundIt = chunkIndexes[0];

    NYT::TNode upBoundRow = GetKeyRowByIndexes(currentYsonContent, upBoundIt);
    NYT::TNode downBoundRow = GetKeyRowByIndexes(currentYsonContent, downBoundIt);
    return {.IsSorted = true, .FirstRowKeys = downBoundRow};
}

void TFmrTableDataServiceSortedWriter::CheckIsSorted(TStringBuf currentYsonContent, const TVector<TRowIndexMarkup>& chunkIndexes) const {
    TBinaryYsonComparator comparator(currentYsonContent, KeyColumns_.SortOrders);
    for (ui64 i = 0; i < chunkIndexes.size() - 1; ++i) {
        const auto& FirstRowKeys = chunkIndexes[i];
        const auto& secondRow = chunkIndexes[i + 1];
        if (comparator.CompareRows(FirstRowKeys, secondRow) > 0) {
            ythrow yexception() << "Data is not sorted";
        }
    }
}

} // namespace NYql::NFmr
