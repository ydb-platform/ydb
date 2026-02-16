#pragma once
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>

namespace NYql::NFmr {

class TTDSBlockIterator final: public IBlockIterator {
public:
    using TPtr = TIntrusivePtr<TTDSBlockIterator>;

    TTDSBlockIterator(
        TString tableId,
        std::vector<TTableRange> tableRanges,
        ITableDataService::TPtr tableDataService,
        std::vector<TString> keyColumns,
        std::vector<ESortOrder> sortOrders,
        std::vector<TString> neededColumns,
        TString serializedColumnGroupsSpec = {},
        TMaybe<bool> isFirstRowKeysInclusive = Nothing(),
        TMaybe<TString> firstRowKeys = Nothing(),
        TMaybe<TString> lastRowKeys = Nothing()

    );

    ~TTDSBlockIterator() final;

    bool NextBlock(TIndexedBlock& out) final;

private:
    void SetMinChunkInNewRange();
    bool RowInKeyBounds(const TString& blob, const TRowIndexMarkup& row) const;

private:
    const TString TableId_;
    std::vector<TTableRange> TableRanges_;
    const ITableDataService::TPtr TableDataService_;
    const std::vector<TString> KeyColumns_;
    std::vector<ESortOrder> SortOrders_;
    const std::vector<TString> NeededColumns_;
    const TString SerializedColumnGroupsSpec_;

    ui64 CurrentRange_ = 0;
    ui64 CurrentChunk_ = 0;

    TMaybe<TFmrTableKeysBoundary> FirstBound_;
    TMaybe<TFmrTableKeysBoundary> LastBound_;
    TMaybe<bool> IsFirstBoundInclusive_;
};

} // namespace NYql::NFmr
