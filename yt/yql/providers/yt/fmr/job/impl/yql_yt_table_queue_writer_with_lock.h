#pragma once

#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <memory>
#include "yql_yt_raw_table_queue.h"

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
    TFmrRawTableQueueWriterWithLock(
        TFmrRawTableQueue::TPtr rawTableQueue,
        ui64 tableId,
        std::shared_ptr<TOrderedWriteState> orderedWriteState,
        const TTableWriterSettings& settings = TTableWriterSettings()
    );

    void NotifyRowEnd() override;

protected:
    void DoWrite(const void* buf, ui64 len) override;

    void DoFlush() override;

private:
    TFmrRawTableQueue::TPtr RawTableQueue_;
    std::shared_ptr<TOrderedWriteState> OrderedWriteState_;
    ui64 TableId_;
    TBuffer BlockContent_;
    TTableWriterSettings Settings_;
};

} // namespace NYql::NFmr

