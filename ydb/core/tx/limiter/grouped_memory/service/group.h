#pragma once
#include "allocation.h"

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TProcessMemoryScope;

class TGrouppedAllocations: public NColumnShard::TMonitoringObjectsCounter<TGrouppedAllocations> {
private:
    THashMap<ui64, std::shared_ptr<TAllocationInfo>> Allocations;

public:
    std::vector<std::shared_ptr<TAllocationInfo>> ExtractAllocationsToVector() {
        std::vector<std::shared_ptr<TAllocationInfo>> result;
        result.reserve(Allocations.size());
        for (auto&& i : Allocations) {
            result.emplace_back(std::move(i.second));
        }
        Allocations.clear();
        return result;
    }

    const THashMap<ui64, std::shared_ptr<TAllocationInfo>>& GetAllocations() const {
        return Allocations;
    }

    bool IsEmpty() const {
        return Allocations.empty();
    }

    void AddAllocation(const std::shared_ptr<TAllocationInfo>& allocation) {
        AFL_VERIFY(Allocations.emplace(allocation->GetIdentifier(), allocation).second);
    }

    [[nodiscard]] bool Remove(const std::shared_ptr<TAllocationInfo>& allocation) {
        return Allocations.erase(allocation->GetIdentifier());
    }

    std::vector<std::shared_ptr<TAllocationInfo>> AllocatePossible(const ui32 allocationsLimit);

    template <typename TPred>
    std::shared_ptr<TAllocationInfo> TakeOne(TPred&& pred) {
        for (auto it = Allocations.begin(); it != Allocations.end(); ++it) {
            if (pred(*it->second)) {
                auto result = std::move(it->second);
                Allocations.erase(it);
                return result;
            }
        }
        return nullptr;
    }

    TString DebugString() const;
};

class TAllocationGroups {
private:
    std::map<ui64, TGrouppedAllocations> Groups;

public:
    bool IsEmpty() const {
        return Groups.empty();
    }

    [[nodiscard]] bool Allocate(const bool isPriorityProcess, TProcessMemoryScope& process, const ui32 allocationsLimit);

    [[nodiscard]] std::vector<std::shared_ptr<TAllocationInfo>> ExtractGroupExt(const ui64 id) {
        auto it = Groups.find(id);
        if (it == Groups.end()) {
            return {};
        }
        auto result = it->second.ExtractAllocationsToVector();
        Groups.erase(it);
        return result;
    }

    std::optional<ui64> GetMinExternalGroupId() const {
        if (Groups.size()) {
            return Groups.begin()->first;
        } else {
            return std::nullopt;
        }
    }

    [[nodiscard]] bool RemoveAllocationExt(const ui64 externalGroupId, const std::shared_ptr<TAllocationInfo>& allocation) {
        auto groupIt = Groups.find(externalGroupId);
        if (groupIt == Groups.end()) {
            return false;
        }
        if (!groupIt->second.Remove(allocation)) {
            return false;
        }
        if (groupIt->second.IsEmpty()) {
            Groups.erase(groupIt);
        }
        return true;
    }

    void AddAllocationExt(const ui64 externalGroupId, const std::shared_ptr<TAllocationInfo>& allocation) {
        Groups[externalGroupId].AddAllocation(allocation);
    }

    template <typename TPred>
    bool ContainsIf(const ui64 externalGroupId, TPred&& pred) const {
        auto groupIt = Groups.find(externalGroupId);
        if (groupIt == Groups.end()) {
            return false;
        }
        for (const auto& [_, allocation] : groupIt->second.GetAllocations()) {
            if (pred(*allocation)) {
                return true;
            }
        }
        return false;
    }

    template <typename TPred>
    std::shared_ptr<TAllocationInfo> TakeOne(const ui64 externalGroupId, TPred&& pred) {
        auto groupIt = Groups.find(externalGroupId);
        if (groupIt == Groups.end()) {
            return nullptr;
        }
        auto allocation = groupIt->second.TakeOne(std::forward<TPred>(pred));
        if (groupIt->second.IsEmpty()) {
            Groups.erase(groupIt);
        }
        return allocation;
    }

    TString DebugString() const;
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
