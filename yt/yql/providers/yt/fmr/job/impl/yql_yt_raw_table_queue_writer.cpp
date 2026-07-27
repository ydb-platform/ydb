#include "yql_yt_raw_table_queue_writer.h"

#include "yql_yt_table_index_marker.h"

namespace NYql::NFmr {

TFmrRawTableQueueWriter::TFmrRawTableQueueWriter(TFmrRawTableQueue::TPtr rawTableQueue, ui32 tableIndex, bool enableTableIndexMarking, const TFmrRawTableQueueWriterSettings& settings)
    : RawTableQueue_(rawTableQueue), TableIndex_(tableIndex), EnableTableIndexMarking_(enableTableIndexMarking), NeedsTableIndexMarker_(enableTableIndexMarking), Settings_(settings)
{
}

void TFmrRawTableQueueWriter::SetTableIndex(ui32 tableIndex) {
    if (EnableTableIndexMarking_ && tableIndex != TableIndex_) {
        NeedsTableIndexMarker_ = true;
    }
    TableIndex_ = tableIndex;
}

void TFmrRawTableQueueWriter::DoWrite(const void* buf, size_t len)
{
    if (NeedsTableIndexMarker_) {
        AppendTableIndexMarker(BlockContent_, TableIndex_);
        NeedsTableIndexMarker_ = false;
    }
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
    NeedsTableIndexMarker_ = EnableTableIndexMarking_;
}

} // namespace NYql::NFmr
