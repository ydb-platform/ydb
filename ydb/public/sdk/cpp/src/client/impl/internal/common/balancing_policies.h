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

    static TImpl UseAllNodes();

    static TImpl UsePreferableLocation(const std::optional<std::string>& location);

    static TImpl UsePreferablePileState(EPileState pileState);

    EPolicyType PolicyType;

    // UsePreferableLocation
    std::optional<std::string> Location;

    // UsePreferablePileState
    EPileState PileState;
};

}
