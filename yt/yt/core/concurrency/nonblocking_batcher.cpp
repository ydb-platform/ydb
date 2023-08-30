#include "nonblocking_batcher.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TBatchSizeLimiter::TBatchSizeLimiter(int maxBatchSize)
    : MaxBatchSize_(maxBatchSize)
{ }

bool TBatchSizeLimiter::IsFull() const
{
    return CurrentBatchSize_ >= MaxBatchSize_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
