#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>

#include <map>
#include <set>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TExternalIdsControl {
private:
    std::set<ui64> ExternalIds;

public:
    void Clear() {
        ExternalIds.clear();
    }

    bool HasExternalId(const ui64 idExt) const {
        return ExternalIds.contains(idExt);
    }

    ui64 GetSize() const {
        return ExternalIds.size();
    }

    const std::set<ui64> GetExternalIds() const {
        return ExternalIds;
    }

    std::optional<ui64> GetMinExternalIdOptional() const {
        if (ExternalIds.empty()) {
            return std::nullopt;
        }
        return *ExternalIds.begin();
    }

    ui64 GetMinExternalIdVerified() const;

    ui64 GetMinExternalIdDef(const ui64 val) const {
        return GetMinExternalIdOptional().value_or(val);
    }

    void RegisterExternalId(const ui64 id);

    [[nodiscard]] bool UnregisterExternalId(const ui64 id) {
        return ExternalIds.erase(id);
    }
};

class TIdsControl {
private:
    std::map<ui64, ui64> ExternalIdIntoInternalId;
    std::map<ui64, ui64> InternalIdIntoExternalId;
    ui64 CurrentInternalId = 0;

public:
    void Clear() {
        ExternalIdIntoInternalId.clear();
        InternalIdIntoExternalId.clear();
    }

    bool HasExternalId(const ui64 idExt) const {
        return ExternalIdIntoInternalId.contains(idExt);
    }

    const std::map<ui64, ui64>& GetInternalIdToExternalIds() const {
        return InternalIdIntoExternalId;
    }

    const std::map<ui64, ui64>& GetExternalIdToInternalIds() const {
        return ExternalIdIntoInternalId;
    }

    ui64 GetSize() const {
        return InternalIdIntoExternalId.size();
    }

    [[nodiscard]] ui64 ExtractInternalIdVerified(const ui64 externalId);
    [[nodiscard]] std::optional<ui64> ExtractInternalIdOptional(const ui64 externalId);

    ui64 GetMinInternalIdVerified() const;
    ui64 GetMinExternalIdVerified() const;
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

    ui64 GetMinExternalIdDef(const ui64 def) const {
        if (ExternalIdIntoInternalId.size()) {
            return ExternalIdIntoInternalId.begin()->first;
        } else {
            return def;
        }
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
