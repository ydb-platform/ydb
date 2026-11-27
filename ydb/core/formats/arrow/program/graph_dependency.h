#pragma once
#include "abstract.h"

#include <optional>

namespace NKikimr::NArrow::NSSA::NGraph {

class TDependency {
private:
    // No resource means an execution dependency; Data(0) is not a sentinel.
    std::optional<TResourceId> ResourceId;

    explicit TDependency(const std::optional<TResourceId> resourceId)
        : ResourceId(resourceId) {
    }

public:
    static TDependency Data(const TResourceId resourceId) {
        return TDependency(resourceId);
    }

    static TDependency Execution() {
        return TDependency(std::nullopt);
    }

    bool IsData() const {
        return ResourceId.has_value();
    }

    bool IsExecution() const {
        return !IsData();
    }

    TResourceId GetResourceId() const {
        YDB_LOG_VERIFY(IsData());
        return *ResourceId;
    }

    bool operator<(const TDependency& other) const {
        return ResourceId < other.ResourceId;
    }

    bool operator==(const TDependency& other) const = default;
};

} // namespace NKikimr::NArrow::NSSA::NGraph
