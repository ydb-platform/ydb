#define INCLUDE_YDB_INTERNAL_H
#include "balancing_policies.h"

namespace NYdb::inline Dev {

TBalancingPolicy::TImpl TBalancingPolicy::TImpl::UseAllNodes() {
    return {EPolicyType::UseAllNodes, std::nullopt, EPileState::UNSPECIFIED};
}

TBalancingPolicy::TImpl TBalancingPolicy::TImpl::UsePreferableLocation(const std::optional<std::string>& location) {
    return {EPolicyType::UsePreferableLocation, location, EPileState::UNSPECIFIED};
}

TBalancingPolicy::TImpl TBalancingPolicy::TImpl::UsePreferablePileState(EPileState pileState) {
    return {EPolicyType::UsePreferablePileState, std::nullopt, pileState};
}

}
