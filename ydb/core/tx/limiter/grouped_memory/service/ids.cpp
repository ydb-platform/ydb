#include "ids.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

ui64 TIdsControl::ExtractInternalIdVerified(const ui64 externalId) {
    auto it = ExternalIdIntoInternalId.find(externalId);
    AFL_VERIFY(it != ExternalIdIntoInternalId.end())("external_id", externalId);
    const ui64 result = it->second;
    InternalIds.erase(result);
    ExternalIdIntoInternalId.erase(it);
    return result;
}

std::optional<ui64> TIdsControl::GetInternalIdOptional(const ui64 externalId) const {
    auto it = ExternalIdIntoInternalId.find(externalId);
    if (it != ExternalIdIntoInternalId.end()) {
        return it->second;
    }
    return std::nullopt;
}

ui64 TIdsControl::GetMinInternalIdVerified() const {
    AFL_VERIFY(InternalIds.size());
    return *InternalIds.begin();
}

ui64 TIdsControl::GetInternalIdVerified(const ui64 externalId) const {
    auto it = ExternalIdIntoInternalId.find(externalId);
    AFL_VERIFY(it != ExternalIdIntoInternalId.end())("external_id", externalId);
    return it->second;
}

ui64 TIdsControl::RegisterExternalId(const ui64 externalId) {
    AFL_VERIFY(ExternalIdIntoInternalId.emplace(externalId, ++CurrentInternalId).second);
    InternalIds.emplace(CurrentInternalId);
    return CurrentInternalId;
}

ui64 TIdsControl::RegisterExternalIdOrGet(const ui64 externalId) {
    auto it = ExternalIdIntoInternalId.find(externalId);
    if (it != ExternalIdIntoInternalId.end()) {
        return it->second;
    }
    AFL_VERIFY(ExternalIdIntoInternalId.emplace(externalId, ++CurrentInternalId).second);
    InternalIds.emplace(CurrentInternalId);
    return CurrentInternalId;
}

bool TIdsControl::UnregisterExternalId(const ui64 externalId) {
    auto it = ExternalIdIntoInternalId.find(externalId);
    if (it == ExternalIdIntoInternalId.end()) {
        return false;
    }
    AFL_VERIFY(InternalIds.erase(it->second));
    ExternalIdIntoInternalId.erase(it);
    return true;
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
