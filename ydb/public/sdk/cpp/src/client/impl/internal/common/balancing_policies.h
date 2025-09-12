#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/ydb.h>

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <memory>
#include <unordered_map>

namespace NYdb::inline Dev {

class TBalancingPolicy::TImpl {
public:
    enum class EPolicyType {
        UseAllNodes,
        UsePreferableLocation,
        UsePreferablePileState
    };

    static std::unique_ptr<TImpl> UseAllNodes();

    static std::unique_ptr<TImpl> UsePreferableLocation(const std::string& location);

    static std::unique_ptr<TImpl> UsePreferablePileState(EPileState pileState);

    TImpl()
        : PolicyType(EPolicyType::UseAllNodes)
    {}

    TImpl(const std::string& location)
        : PolicyType(EPolicyType::UsePreferableLocation)
        , Location(location)
    {}

    TImpl(EPileState pileState)
        : PolicyType(EPolicyType::UsePreferablePileState)
        , PileState(pileState)
    {}

    EPolicyType PolicyType;

    // UsePreferableLocation
    std::string Location;

    // UsePreferablePileState
    EPileState PileState;
};

}
