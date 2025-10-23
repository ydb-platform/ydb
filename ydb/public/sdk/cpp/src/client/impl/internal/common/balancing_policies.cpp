#define INCLUDE_YDB_INTERNAL_H
#include "balancing_policies.h"

namespace NYdb::inline Dev {

TBalancingPolicy::TImpl TBalancingPolicy::TImpl::UseAllNodes() {
    TBalancingPolicy::TImpl impl;
    impl.PolicyType = EPolicyType::UseAllNodes;
    return impl;
}

TBalancingPolicy::TImpl TBalancingPolicy::TImpl::UsePreferableLocation(const std::optional<std::string>& location) {
    TBalancingPolicy::TImpl impl;
    impl.PolicyType = EPolicyType::UsePreferableLocation;
    impl.Location = location;
    return impl;
}

TBalancingPolicy::TImpl TBalancingPolicy::TImpl::UsePreferablePileState(EPileState pileState) {
    TBalancingPolicy::TImpl impl;
    impl.PolicyType = EPolicyType::UsePreferablePileState;
    impl.PileState = pileState;
    return impl;
}

}
