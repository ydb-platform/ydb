#pragma once

#include <queue>
#include <util/generic/buffer.h>
#include <util/stream/input.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>

namespace NYql::NFmr {

struct TFmrRawTableReaderSettings {
    ui64 ReadAheadChunks = 1;
};

struct TPendingFmrChunk {
    NThreading::TFuture<TMaybe<TString>> Data;
    TFmrChunkMeta Meta;
};

class TFmrRawTableReader: public IInputStream {
    public:
        TFmrRawTableReader(
            const TString& tableId,
            const std::vector<TTableRange>& tableRanges,
            ITableDataService::TPtr tableDataService,
            const TFmrRawTableReaderSettings& settings
        );
    protected:
        size_t DoRead(void* buf, size_t len) override;
    private:
        void ReadAhead();
    private:
        const TString TableId_;
        const std::vector<TTableRange> TableRanges_;
        ITableDataService::TPtr TableDataService_;
        const TFmrRawTableReaderSettings Settings_;

        ui64 CurrentRange_ = 0;
        ui64 CurrentChunk_ = 0;
        ui64 CurrentPosition_ = 0;
        TBuffer DataBuffer_;
        std::queue<TPendingFmrChunk> PendingChunks_;
};

} // namespace NYql::NFmr
