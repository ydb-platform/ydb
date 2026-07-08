#include "yql_yt_table_queue_writer_with_lock.h"
#include "yql_yt_table_index_marker.h"
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

TFmrRawTableQueueWriterWithLock::TFmrRawTableQueueWriterWithLock(
    TFmrRawTableQueue::TPtr rawTableQueue,
    ui64 tableId,
    std::shared_ptr<TOrderedWriteState> orderedWriteState,
    bool enableSectionIndexMarking,
    const TTableWriterSettings& settings
)
    : RawTableQueue_(rawTableQueue)
    , OrderedWriteState_(std::move(orderedWriteState))
    , TableId_(tableId)
    , EnableSectionIndexMarking_(enableSectionIndexMarking)
    , NeedsTableIndexMarker_(enableSectionIndexMarking)
    , Settings_(settings)
{
}

void TFmrRawTableQueueWriterWithLock::SetSectionIndex(ui32 sectionIndex) {
    if (EnableSectionIndexMarking_ && sectionIndex != SectionIndex_) {
        NeedsTableIndexMarker_ = true;
    }
    SectionIndex_ = sectionIndex;
}

void TFmrRawTableQueueWriterWithLock::DoWrite(const void* buf, ui64 len) {
    if (NeedsTableIndexMarker_) {
        AppendTableIndexMarker(BlockContent_, SectionIndex_);
        NeedsTableIndexMarker_ = false;
    }
    BlockContent_.Append(static_cast<const char*>(buf), len);
}

void TFmrRawTableQueueWriterWithLock::NotifyRowEnd() {
    if (BlockContent_.size() >= Settings_.ChunkSize) {
        DoFlush();
    }
}

void TFmrRawTableQueueWriterWithLock::DoFlush() {
    // Always wait for this table's turn, even with an empty buffer: the caller
    // advances NextToEmit right after Flush() returns, so skipping the wait here
    // would let an empty/already-drained table jump the queue and desync ordering
    // for tables that haven't taken their turn yet.
    with_lock(OrderedWriteState_->Mutex) {
        OrderedWriteState_->CondVar.Wait(OrderedWriteState_->Mutex, [&] {
            return OrderedWriteState_->NextToEmit == TableId_;
        });

        if (!BlockContent_.Empty()) {
            RawTableQueue_->AddRow(std::move(BlockContent_));
        }
    }
    BlockContent_.Clear();
    // Deliberately NOT resetting NeedsTableIndexMarker_ here: NextToEmit only advances after this
    // writer's entire turn (every internal flush plus the caller's final Flush()) completes, so no
    // other writer's chunk can be interleaved between two flushes of the same turn — the marker
    // written at turn/section start still covers this next chunk. Re-marking is only needed when
    // SetSectionIndex() actually changes the active section mid-turn.
}

} // namespace NYql::NFmr

