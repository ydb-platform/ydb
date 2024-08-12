#pragma once
#include <ydb/library/accessor/accessor.h>
#include <map>
#include <set>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TIdsControl {
private:
    std::map<ui64, ui64> ExternalIdIntoInternalId;
    YDB_READONLY_DEF(std::set<ui64>, InternalIds);
    ui64 CurrentInternalId = 0;

public:
    void Clear() {
        ExternalIdIntoInternalId.clear();
        InternalIds.clear();
    }

    [[nodiscard]] ui64 ExtractInternalIdVerified(const ui64 externalId);

    ui64 GetMinInternalIdVerified() const;

    std::optional<ui64> GetInternalIdOptional(const ui64 externalId) const;

    ui64 GetInternalIdVerified(const ui64 externalId) const;

    [[nodiscard]] ui64 RegisterExternalId(const ui64 externalId);
    [[nodiscard]] ui64 RegisterExternalIdOrGet(const ui64 externalId);

    [[nodiscard]] bool UnregisterExternalId(const ui64 externalId);

    std::optional<ui64> GetMinInternalIdOptional() const {
        if (InternalIds.size()) {
            return *InternalIds.begin();
        } else {
            return std::nullopt;
        }
    }

    ui64 GetMinInternalIdDef(const ui64 def) const {
        if (InternalIds.size()) {
            return *InternalIds.begin();
        } else {
            return def;
        }
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
