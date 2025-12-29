#include "yql_yt_table_queue_writer_with_lock.h"
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

TFmrRawTableQueueWriterWithLock::TFmrRawTableQueueWriterWithLock(
    TFmrRawTableQueue::TPtr rawTableQueue,
    ui64 tableId,
    std::shared_ptr<TOrderedWriteState> orderedWriteState,
    const TTableWriterSettings& settings
)
    : RawTableQueue_(rawTableQueue)
    , OrderedWriteState_(std::move(orderedWriteState))
    , TableId_(tableId)
    , Settings_(settings)
{
}

void TFmrRawTableQueueWriterWithLock::DoWrite(const void* buf, ui64 len) {
    BlockContent_.Append(static_cast<const char*>(buf), len);
}

void TFmrRawTableQueueWriterWithLock::NotifyRowEnd() {
    if (BlockContent_.size() >= Settings_.ChunkSize) {
        DoFlush();
    }
}

void TFmrRawTableQueueWriterWithLock::DoFlush() {
    if (BlockContent_.Empty()) {
        return;
    }

    with_lock(OrderedWriteState_->Mutex) {
        OrderedWriteState_->CondVar.Wait(OrderedWriteState_->Mutex, [&] {
            return OrderedWriteState_->NextToEmit == TableId_;
        });

        RawTableQueue_->AddRow(std::move(BlockContent_));
    }
    BlockContent_.Clear();
}

} // namespace NYql::NFmr

