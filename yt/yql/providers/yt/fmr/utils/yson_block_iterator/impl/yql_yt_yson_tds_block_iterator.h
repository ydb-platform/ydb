#pragma once
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>

namespace NYql::NFmr {

class TTDSBlockIterator final: public IBlockIterator {
public:
    using TPtr = TIntrusivePtr<TTDSBlockIterator>;

    TTDSBlockIterator(
        TString tableId,
        TVector<TTableRange> tableRanges,
        ITableDataService::TPtr tableDataService,
        TVector<TString> keyColumns,
        TVector<TString> neededColumns,
        TString serializedColumnGroupsSpec = {}
    );

    ~TTDSBlockIterator() final;

    bool NextBlock(TIndexedBlock& out) final;

private:
    void SetMinChunkInNewRange();

private:
    const TString TableId_;
    TVector<TTableRange> TableRanges_;
    const ITableDataService::TPtr TableDataService_;
    const TVector<TString> KeyColumns_;
    const TVector<TString> NeededColumns_;
    const TString SerializedColumnGroupsSpec_;

    ui64 CurrentRange_ = 0;
    ui64 CurrentChunk_ = 0;
};

} // namespace NYql::NFmr
