#include "yql_yt_raw_table_queue.h"

namespace NYql::NFmr {

struct TFmrRawTableQueueWriterSettings {
    ui64 ChunkSize = 1024 * 1024;
};

class TFmrRawTableQueueWriter: public NYT::TRawTableWriter {
public:
    TFmrRawTableQueueWriter(
        TFmrRawTableQueue::TPtr rawTableQueue,
        const TFmrRawTableQueueWriterSettings& settings = TFmrRawTableQueueWriterSettings()
    );

    void NotifyRowEnd() override;

protected:
    void DoWrite(const void* buf, size_t len) override;

    void DoFlush() override;

private:
    TFmrRawTableQueue::TPtr RawTableQueue_;

    TBuffer BlockContent_;

    TFmrRawTableQueueWriterSettings Settings_ = {};
};

} // namespace NYql::NFmr
