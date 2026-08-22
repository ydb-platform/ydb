#pragma once

#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <memory>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_raw_table_queue.h>

namespace NYql::NFmr {

struct TOrderedWriteState {
    ui64 NextToEmit = 0;
    TMutex Mutex;
    TCondVar CondVar;
};

struct TTableWriterSettings {
    ui64 ChunkSize = 1024 * 1024;
    ui64 MaxTaskInflightBytes = 256 * 1024 * 1024;
};

class TFmrRawTableQueueWriterWithLock: public NYT::TRawTableWriter {
public:
    // enableTableIndexMarking should be false whenever the whole job only ever reads a single
    // physical table (the common case): that always decodes fine against the decoder's default
    // TableIndex_ (0), so marking would be pure overhead with no correctness benefit.
    TFmrRawTableQueueWriterWithLock(
        TFmrRawTableQueue::TPtr rawTableQueue,
        ui64 tableId,
        std::shared_ptr<TOrderedWriteState> orderedWriteState,
        bool enableTableIndexMarking = false,
        const TTableWriterSettings& settings = TTableWriterSettings()
    );

    void NotifyRowEnd() override;

    // Changes which table_index gets written before subsequent rows. Independent of TableId_
    // (which only controls write-turn ordering): call this between ParseRecords() calls when
    // this writer relays rows from several distinct physical inputs.
    void SetTableIndex(ui32 tableIndex);

protected:
    void DoWrite(const void* buf, ui64 len) override;

    void DoFlush() override;

private:
    TFmrRawTableQueue::TPtr RawTableQueue_;
    std::shared_ptr<TOrderedWriteState> OrderedWriteState_;
    ui64 TableId_;
    ui32 TableIndex_ = 0;
    bool EnableTableIndexMarking_;
    TBuffer BlockContent_;
    // Marks each flushed chunk with TableIndex_ so the decoder can tell which schema
    // to use once chunks from different input tables are read back as one stream. Only
    // relevant when EnableTableIndexMarking_ is set.
    bool NeedsTableIndexMarker_ = true;
    TTableWriterSettings Settings_;
};

} // namespace NYql::NFmr

