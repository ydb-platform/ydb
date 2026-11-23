#include "upload_memory_reservation.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NTxProxy {

void TUploadMemoryReservation::AccountNavigateRequest(NSchemeCache::TSchemeCacheNavigate& request) const {
    if (!Attempt) {
        return;
    }
    if (!Retain) {
        auto owner = std::make_shared<TRequestReservation>(Attempt, Batch, nullptr);
        request.RetainSchemaMemory = [owner = std::move(owner)](ui64) { return owner; };
        return;
    }

    ui64 bytes = sizeof(request) + sizeof(TEvTxProxySchemeCache::TEvNavigateKeySet) + request.DatabaseName.capacity()
        + request.ResultSet.capacity() * sizeof(NSchemeCache::TSchemeCacheNavigate::TEntry);
    for (const auto& entry : request.ResultSet) {
        bytes += entry.Path.capacity() * sizeof(TString);
        for (const auto& component : entry.Path) {
            bytes += component.capacity();
        }
    }
    request.RetainSchemaMemory = [attempt = Attempt, retain = Retain, requestMemory = Retain(bytes)](ui64 schemaBytes) {
        (void)requestMemory;
        return std::make_shared<TRequestReservation>(attempt, retain(schemaBytes), nullptr);
    };
}

} // namespace NKikimr::NTxProxy
