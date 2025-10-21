#include "yql_yt_raw_table_queue_reader.h"

namespace NYql::NFmr {

TFmrRawTableQueueReader::TFmrRawTableQueueReader(TFmrRawTableQueue::TPtr rawTableQueue)
    : RawTableQueue_(rawTableQueue)
{
}

size_t TFmrRawTableQueueReader::DoRead(void* buf, size_t len) {
    ui64 totalRead = 0;
    char* output = static_cast<char*>(buf);

    while (len > 0) {
        if (BlockContentPos_ == BlockContent_.size()) {
            auto row = RawTableQueue_->PopRow();
            if (!row) {
                return totalRead;
            }
            BlockContent_.Clear();
            BlockContent_.Append(row->Data(), row->size());
            BlockContentPos_ = 0;
        }
        if (!BlockContent_.Empty()) {
            ui64 available = BlockContent_.size() - BlockContentPos_;

            if (available > 0) {
                ui64 toCopy = std::min(available, len);
                std::copy(
                    BlockContent_.Begin() + BlockContentPos_,
                    BlockContent_.Begin() + BlockContentPos_ + toCopy,
                    output
                );
                output += toCopy;
                len -= toCopy;
                BlockContentPos_ += toCopy;
                totalRead += toCopy;
            }
        }
    }
    return totalRead;
}

bool TFmrRawTableQueueReader::Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) {
    return false;
}

void TFmrRawTableQueueReader::ResetRetries() {
}

bool TFmrRawTableQueueReader::HasRangeIndices() const {
    return false;
}

} // namespace NYql::NFmr
