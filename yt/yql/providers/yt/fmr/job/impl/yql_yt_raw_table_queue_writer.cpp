#include "yql_yt_raw_table_queue_writer.h"

namespace NYql::NFmr {

TFmrRawTableQueueWriter::TFmrRawTableQueueWriter(TFmrRawTableQueue::TPtr rawTableQueue,const TFmrRawTableQueueWriterSettings& settings)
    : RawTableQueue_(rawTableQueue), Settings_(settings)
{
}

void TFmrRawTableQueueWriter::DoWrite(const void* buf, size_t len)
{
    BlockContent_.Append(static_cast<const char*>(buf), len);
}

void TFmrRawTableQueueWriter::NotifyRowEnd() {
    if (BlockContent_.size() >= Settings_.ChunkSize) {
        DoFlush();
    }
}

void TFmrRawTableQueueWriter::DoFlush() {
    if (BlockContent_.Empty()) {
        return;
    }
    RawTableQueue_->AddRow(std::move(BlockContent_));
    BlockContent_.Clear();
}

} // namespace NYql::NFmr
