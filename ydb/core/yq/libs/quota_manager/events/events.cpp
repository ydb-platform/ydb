#include "events.h"

namespace NYq {

void TQuotaUsage::Merge(const TQuotaUsage& other) {
    if (other.Limit.UpdatedAt > Limit.UpdatedAt) {
        Limit = other.Limit;
    }
    if (!Usage || (other.Usage && other.Usage->UpdatedAt > Usage->UpdatedAt)) {
        Usage = other.Usage;
    }
}

} /* NYq */