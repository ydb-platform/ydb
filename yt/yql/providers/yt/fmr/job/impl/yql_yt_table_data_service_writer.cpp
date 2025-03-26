#include "yql_yt_table_data_service_writer.h"
#include <util/string/join.h>

namespace NYql::NFmr {

TFmrTableDataServiceWriter::TFmrTableDataServiceWriter(
    const TString& tableId,
    const TString& partId,
    ITableDataService::TPtr tableDataService,
    const TFmrTableDataServiceWriterSettings& settings
)
    : TableId_(tableId), PartId_(partId), TableDataService_(tableDataService), ChunkSize_(settings.ChunkSize) {}

void TFmrTableDataServiceWriter::DoWrite(const void* buf, size_t len) {
    TableContent_.Append(static_cast<const char*>(buf), len);
}

void TFmrTableDataServiceWriter::NotifyRowEnd()  {
    Rows_ += 1;
    if (TableContent_.size() >= ChunkSize_) {
        DoFlush();
    }
}

void TFmrTableDataServiceWriter::DoFlush() {
    TString chunkKey = GetTableDataServiceKey(TableId_, PartId_, ChunkCount_);
    TableDataService_->Put(chunkKey, TString(TableContent_.Data(), TableContent_.Size())).Wait();
    ChunkCount_++;
    DataWeight_ += TableContent_.Size();
    TableContent_.Clear();
}

TTableStats TFmrTableDataServiceWriter::GetStats() {
    return TTableStats{
        .Chunks = ChunkCount_,
        .Rows = Rows_,
        .DataWeight = DataWeight_,
    };
}

} // namespace NYql::NFmr
