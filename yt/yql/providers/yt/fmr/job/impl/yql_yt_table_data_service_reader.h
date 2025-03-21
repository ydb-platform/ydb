#pragma once

#include <queue>
#include <util/generic/buffer.h>
#include <util/stream/input.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>

namespace NYql::NFmr {

struct TFmrTableDataServiceReaderSettings {
    ui64 ReadAheadChunks = 1;
};

struct TPendingFmrChunk {
    NThreading::TFuture<TMaybe<TString>> Data;
    TFmrChunkMeta Meta;
};

class TFmrTableDataServiceReader: public NYT::TRawTableReader {
public:
    TFmrTableDataServiceReader(
        const TString& tableId,
        const std::vector<TTableRange>& tableRanges,
        ITableDataService::TPtr tableDataService,
        const TFmrTableDataServiceReaderSettings& settings = TFmrTableDataServiceReaderSettings{}
    );

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override;

    void ResetRetries() override;

    bool HasRangeIndices() const override;

private:
    size_t DoRead(void* buf, size_t len) override;
    void ReadAhead();

    const TString TableId_;
    std::vector<TTableRange> TableRanges_;
    ITableDataService::TPtr TableDataService_;
    const ui64 ReadAheadChunks_;

    ui64 CurrentRange_ = 0;
    ui64 CurrentChunk_ = 0;
    ui64 CurrentPosition_ = 0;
    TBuffer DataBuffer_;
    std::queue<TPendingFmrChunk> PendingChunks_;
};

} // namespace NYql::NFmr
