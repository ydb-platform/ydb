#include "fair_share_hierarchical_queue.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// The earlier the log arrived, the more expensive it is.
std::strong_ordering TFairShareLogKey::operator<=>(const TFairShareLogKey& other) const
{
    if (CreatedAt > other.CreatedAt) {
        return std::strong_ordering::less;
    } else if (CreatedAt < other.CreatedAt) {
        return std::strong_ordering::greater;
    }
    return RequestId <=> other.RequestId;
}

TFairShareLogKey::operator size_t() const
{
    return MultiHash(RequestId, CreatedAt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
