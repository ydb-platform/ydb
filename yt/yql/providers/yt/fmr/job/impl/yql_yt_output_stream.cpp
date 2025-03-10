#include "yql_yt_output_stream.h"

namespace NYql::NFmr {

TFmrOutputStream::TFmrOutputStream(TString tableId, TString partId, ITableDataService::TPtr tableDataService, TFmrOutputStreamSettings settings)
    : TableId_(tableId)
    , PartId_(partId)
    , TableDataService_(tableDataService)
    , MaxChunkSize_(settings.MaxChunkSize)
    , ChunkBuf_(MaxChunkSize_)
{
}

void TFmrOutputStream::DoWrite(const void* buf, size_t len) {
    size_t bytesWritten = 0;
    while (bytesWritten < len) {
        size_t bytesToWrite = std::min(MaxChunkSize_ - CurrentChunkSize_, len - bytesWritten);
        ChunkBuf_.Append(static_cast<const char*>(buf) + bytesWritten, bytesToWrite);
        CurrentChunkSize_ += bytesToWrite;
        bytesWritten += bytesToWrite;
        if (CurrentChunkSize_ == MaxChunkSize_) {
            DoFlush();
        }
    }
}

void TFmrOutputStream::DoFlush() {
    TString chunkKey = GetTableDataServiceKey(TableId_, PartId_, ChunkCount_);
    TableDataService_->Put(chunkKey, TString(ChunkBuf_.Data(), CurrentChunkSize_)).Wait();
    ChunkCount_++;
    DataWeight_ += CurrentChunkSize_;
    ChunkBuf_.Clear();
    CurrentChunkSize_ = 0;
}

TTableStats TFmrOutputStream::GetStats() {
    return TTableStats{
        .Chunks = ChunkCount_,
        .DataWeight = DataWeight_,
    };
}
} // namespace NYql::NFmr
