#pragma once

#include <queue>
#include <util/generic/buffer.h>
#include <util/stream/input.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_column_group_helpers.h>

namespace NYql::NFmr {

struct TFmrReaderSettings {
    ui64 ReadAheadChunks = 1;
};

class TFmrTableDataServiceReader: public NYT::TRawTableReader {
public:
    TFmrTableDataServiceReader(
        const TString& tableId,
        const std::vector<TTableRange>& tableRanges,
        ITableDataService::TPtr tableDataService,
        const std::vector<TString>& neededColumns = {},
        const TString& columnGroupSpec = TString(),
        const TFmrReaderSettings& settings = TFmrReaderSettings{}
    );

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override;

    void ResetRetries() override;

    bool HasRangeIndices() const override;

private:
    struct TFmrChunkMeta {
        TString TableId;
        TString PartId;
        ui64 Chunk = 0;

        TString ToString() const;
    };

    struct TPendingFmrChunk {
        NThreading::TFuture<TMaybe<TString>> Data;
        TFmrChunkMeta Meta;
    };

    size_t DoRead(void* buf, size_t len) override;
    void ReadAhead();

    void SetMinChunkInNewRange();

    NThreading::TFuture<TMaybe<TString>> GetTableDataServiceValueFuture(const TString& partId, ui64 chunkNum);

    const TString TableId_;
    std::vector<TTableRange> TableRanges_;
    ITableDataService::TPtr TableDataService_;
    const ui64 ReadAheadChunks_;

    ui64 CurrentRange_ = 0;
    ui64 CurrentChunk_ = 0;
    ui64 CurrentPosition_ = 0;
    TBuffer DataBuffer_;
    std::queue<TPendingFmrChunk> PendingChunks_;
    const std::vector<TString> NeededColumns_;
    const TParsedColumnGroupSpec ColumnGroupSpec_;
};

} // namespace NYql::NFmr
