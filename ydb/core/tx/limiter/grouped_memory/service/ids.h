#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>

#include <map>
#include <set>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TIdsControl {
private:
    THashMap<ui64, ui64> ExternalIdIntoInternalId;
    std::map<ui64, ui64> InternalIdIntoExternalId;
    ui64 CurrentInternalId = 0;

public:
    void Clear() {
        ExternalIdIntoInternalId.clear();
        InternalIdIntoExternalId.clear();
    }

    const std::map<ui64, ui64>& GetInternalIdToExternalIds() const {
        return InternalIdIntoExternalId;
    }

    ui64 GetSize() const {
        return InternalIdIntoExternalId.size();
    }

    [[nodiscard]] ui64 ExtractInternalIdVerified(const ui64 externalId);

    ui64 GetMinInternalIdVerified() const;
    ui64 GetExternalIdVerified(const ui64 internalId) const;

    std::optional<ui64> GetInternalIdOptional(const ui64 externalId) const;

    ui64 GetInternalIdVerified(const ui64 externalId) const;

    [[nodiscard]] ui64 RegisterExternalId(const ui64 externalId);
    [[nodiscard]] ui64 RegisterExternalIdOrGet(const ui64 externalId);

    [[nodiscard]] bool UnregisterExternalId(const ui64 externalId);

    std::optional<ui64> GetMinInternalIdOptional() const {
        if (InternalIdIntoExternalId.size()) {
            return InternalIdIntoExternalId.begin()->first;
        } else {
            return std::nullopt;
        }
    }

    std::optional<ui64> GetMinExternalIdOptional() const {
        if (InternalIdIntoExternalId.size()) {
            return InternalIdIntoExternalId.begin()->second;
        } else {
            return std::nullopt;
        }
    }

    ui64 GetMinInternalIdDef(const ui64 def) const {
        if (InternalIdIntoExternalId.size()) {
            return InternalIdIntoExternalId.begin()->first;
        } else {
            return def;
        }
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
