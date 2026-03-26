#pragma once
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>

#include <library/cpp/threading/future/future.h>
#include <deque>

namespace NYql::NFmr {

class TTableDataServiceBlockIterator final: public IBlockIterator {
public:
    using TPtr = TIntrusivePtr<TTableDataServiceBlockIterator>;

    TTableDataServiceBlockIterator(
        TString tableId,
        std::vector<TTableRange> tableRanges,
        ITableDataService::TPtr tableDataService,
        std::vector<TString> keyColumns,
        std::vector<ESortOrder> sortOrders,
        std::vector<TString> neededColumns,
        TString serializedColumnGroupsSpec = {},
        TMaybe<bool> isFirstRowKeysInclusive = Nothing(),
        TMaybe<TString> firstRowKeys = Nothing(),
        TMaybe<TString> lastRowKeys = Nothing(),
        ui64 readAheadChunks = 4

    );

    ~TTableDataServiceBlockIterator() final;

    bool NextBlock(TIndexedBlock& out) final;

    std::vector<ESortOrder> GetSortOrder() final;

private:
    void SetMinChunkInNewRange();
    bool RowInKeyBounds(const TString& blob, const TRowIndexMarkup& row) const;

    struct TPrefetchEntry {
        std::vector<NThreading::TFuture<TMaybe<TString>>> Futures;
    };

    bool TrySchedulePrefetch();
    void FillPrefetchQueue();

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

    // Separate cursors for prefetch
    ui64 PrefetchRange_ = 0;
    ui64 PrefetchChunk_ = 0;

    TMaybe<TFmrTableKeysBoundary> FirstBoundary_;
    TMaybe<TFmrTableKeysBoundary> LastBoundary_;
    bool IsFirstBoundInclusive_ = true;

    std::vector<TString> GroupNamesToRead_;

    ui64 ReadAheadChunks_ = 4;
    std::deque<TPrefetchEntry> PrefetchQueue_;
};

} // namespace NYql::NFmr
