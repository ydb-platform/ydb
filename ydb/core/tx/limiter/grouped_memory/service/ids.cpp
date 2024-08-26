#include "ids.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

ui64 TIdsControl::ExtractInternalIdVerified(const ui64 externalId) {
    auto it = ExternalIdIntoInternalId.find(externalId);
    AFL_VERIFY(it != ExternalIdIntoInternalId.end())("external_id", externalId);
    const ui64 result = it->second;
    InternalIdIntoExternalId.erase(result);
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
    AFL_VERIFY(InternalIdIntoExternalId.size());
    return InternalIdIntoExternalId.begin()->first;
}

ui64 TIdsControl::GetInternalIdVerified(const ui64 externalId) const {
    auto it = ExternalIdIntoInternalId.find(externalId);
    AFL_VERIFY(it != ExternalIdIntoInternalId.end())("external_id", externalId);
    return it->second;
}

ui64 TIdsControl::RegisterExternalId(const ui64 externalId) {
    AFL_VERIFY(ExternalIdIntoInternalId.emplace(externalId, ++CurrentInternalId).second);
    InternalIdIntoExternalId.emplace(CurrentInternalId, externalId);
    return CurrentInternalId;
}

ui64 TIdsControl::RegisterExternalIdOrGet(const ui64 externalId) {
    auto it = ExternalIdIntoInternalId.find(externalId);
    if (it != ExternalIdIntoInternalId.end()) {
        return it->second;
    }
    AFL_VERIFY(ExternalIdIntoInternalId.emplace(externalId, ++CurrentInternalId).second);
    InternalIdIntoExternalId.emplace(CurrentInternalId, externalId);
    return CurrentInternalId;
}

bool TIdsControl::UnregisterExternalId(const ui64 externalId) {
    auto it = ExternalIdIntoInternalId.find(externalId);
    if (it == ExternalIdIntoInternalId.end()) {
        return false;
    }
    AFL_VERIFY(InternalIdIntoExternalId.erase(it->second));
    ExternalIdIntoInternalId.erase(it);
    return true;
}

ui64 TIdsControl::GetExternalIdVerified(const ui64 internalId) const {
    auto it = InternalIdIntoExternalId.find(internalId);
    AFL_VERIFY(it != InternalIdIntoExternalId.end());
    return it->second;
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
