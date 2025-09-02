#include "yql_yt_raw_table_queue.h"

namespace NYql::NFmr {

class TFmrRawTableQueueReader: public NYT::TRawTableReader {
public:
    using TPtr = TIntrusivePtr<TFmrRawTableQueueReader>;

    TFmrRawTableQueueReader(TFmrRawTableQueue::TPtr rawTableQueue);

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override;

    void ResetRetries() override;

    bool HasRangeIndices() const override;

private:
    size_t DoRead(void* buf, size_t len) override;

private:
    TFmrRawTableQueue::TPtr RawTableQueue_;
    TBuffer BlockContent_; // Represents row currently read from queue
    ui64 BlockContentPos_ = 0;
};

} // namespace NYql::NFmr
