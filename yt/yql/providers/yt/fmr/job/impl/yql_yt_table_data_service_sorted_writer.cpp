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
    : TFmrTableDataServiceBaseWriter(tableId, partId, tableDataService, columnGroupSpec, settings)
    , KeyColumns_(std::move(keyColumns))
    , SkipSortedCheck_(settings.SkipSortedCheck)
{
}

void TFmrTableDataServiceSortedWriter::PutRows() {
    if (TableContent_.Size() == 0) {
        return;
    }
    auto currentYsonContent = TString(TableContent_.Data(), TableContent_.Size());

    auto parserKeyIndexes = TParserFragmentListIndex(currentYsonContent, KeyColumns_.Columns);
    parserKeyIndexes.Parse();
    const auto& chunkIndexes = parserKeyIndexes.GetRows();

    if (!SkipSortedCheck_) {
        CheckIsSorted(currentYsonContent, chunkIndexes);
    }

    auto sortedChunkStats = GetSortedChunkStats(currentYsonContent, chunkIndexes);

    PutYsonByColumnGroups(currentYsonContent);

    PartIdChunkStats_.emplace_back(TChunkStats{
        .Rows = CurrentChunkRows_,
        .DataWeight = TableContent_.Size(),
        .SortedChunkStats = sortedChunkStats,
    });
    ClearTableData();
}

TString TFmrTableDataServiceSortedWriter::GetIndexValue(TStringBuf currentYsonContent, const TColumnOffsetRange& index) const {
    return TString(currentYsonContent.SubStr(index.StartOffset, index.EndOffset - index.StartOffset));
};

NYT::TNode TFmrTableDataServiceSortedWriter::GetKeyRowByIndexes(TStringBuf currentYsonContent, const std::vector<TColumnOffsetRange>& indexes) const {
    NYT::TNode result = NYT::TNode::CreateMap();

    for (size_t i = 0; i < KeyColumns_.Columns.size(); ++i) {
        const auto& index = indexes[i];
        const TString& columnName = KeyColumns_.Columns[i];
        // A key column can be legitimately absent from the row's binary yson: rows with a NULL
        // value for a nullable key column (e.g. the direct map-bypass output of a FULL JOIN with
        // nullable join keys) are written without that key at all, rather than with an explicit
        // '#' entity. Parsing an empty blob as a YSON node would throw "Premature end of yson
        // stream", so treat a missing column the same way the row comparator does - as null.
        result[columnName] = index.IsValid()
            ? NYT::NodeFromYsonString(GetIndexValue(currentYsonContent, index), NYT::NYson::EYsonType::Node)
            : NYT::TNode::CreateEntity();
    }
    return result;
};

TSortedChunkStats TFmrTableDataServiceSortedWriter::GetSortedChunkStats(TStringBuf currentYsonContent, const std::vector<TRowIndexMarkup>& chunkIndexes) const {
    if (chunkIndexes.empty()) {
        return TSortedChunkStats{.IsSorted = true};
    }
    auto sortOrders = KeyColumns_.SortOrders;
    TBinaryYsonComparator comparator(currentYsonContent, sortOrders);

    auto upBoundaryIndexes = chunkIndexes.back();
    auto downBoundaryIndexes = chunkIndexes.front();

    NYT::TNode upBoundaryKeys = GetKeyRowByIndexes(currentYsonContent, upBoundaryIndexes);
    NYT::TNode downBoundaryKeys = GetKeyRowByIndexes(currentYsonContent, downBoundaryIndexes);
    return {.IsSorted = true, .FirstRowKeys = downBoundaryKeys, .LastRowKeys = upBoundaryKeys};
}

void TFmrTableDataServiceSortedWriter::CheckIsSorted(TStringBuf currentYsonContent, const std::vector<TRowIndexMarkup>& chunkIndexes) const {
    TBinaryYsonComparator comparator(currentYsonContent, KeyColumns_.SortOrders);
    for (ui64 i = 0; i + 1 < chunkIndexes.size(); ++i) {
        const auto& curRowKeys = chunkIndexes[i];
        const auto& nextRowKeys = chunkIndexes[i + 1];
        if (comparator.CompareRows(curRowKeys, nextRowKeys) > 0) {
            ythrow yexception() << "Data is not sorted";
        }
    }
}

} // namespace NYql::NFmr
