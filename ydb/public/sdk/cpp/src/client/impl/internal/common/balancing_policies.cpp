#define INCLUDE_YDB_INTERNAL_H
#include "balancing_policies.h"

namespace NYdb::inline Dev {

std::unique_ptr<TBalancingPolicy::TImpl> TBalancingPolicy::TImpl::UseAllNodes() {
    return std::make_unique<TImpl>();
}

std::unique_ptr<TBalancingPolicy::TImpl> TBalancingPolicy::TImpl::UsePreferableLocation(const std::string& location) {
    return std::make_unique<TImpl>(location);
}

std::unique_ptr<TBalancingPolicy::TImpl> TBalancingPolicy::TImpl::UsePreferablePileState(EPileState pileState) {
    return std::make_unique<TImpl>(pileState);
}

}
