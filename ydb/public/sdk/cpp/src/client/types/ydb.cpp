#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/ydb.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/balancing_policies.h>
#undef INCLUDE_YDB_INTERNAL_H


namespace NYdb::inline Dev {

TBalancingPolicy::TBalancingPolicy(EBalancingPolicy policy, const std::string& params) {
    switch (policy) {
        case EBalancingPolicy::UsePreferableLocation:
            Impl_ = TImpl::UsePreferableLocation(params);
            break;
        case EBalancingPolicy::UseAllNodes:
            Impl_ = TImpl::UseAllNodes();
            break;
    }
}

TBalancingPolicy TBalancingPolicy::UsePreferableLocation(const std::string& location) {
    return TBalancingPolicy(TImpl::UsePreferableLocation(location));
}

TBalancingPolicy TBalancingPolicy::UseAllNodes() {
    return TBalancingPolicy(TImpl::UseAllNodes());
}

TBalancingPolicy TBalancingPolicy::UsePreferablePileState(EPileState pileState) {
    return TBalancingPolicy(TImpl::UsePreferablePileState(pileState));
}

TBalancingPolicy::TBalancingPolicy(std::unique_ptr<TImpl>&& impl)
    : Impl_(std::move(impl))
{}

}
