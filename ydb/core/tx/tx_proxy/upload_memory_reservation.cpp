#include "upload_memory_reservation.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NTxProxy {

void TUploadMemoryReservation::AccountNavigateRequest(NSchemeCache::TSchemeCacheNavigate& request) const {
    if (!Attempt) {
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
    request.RetainSchemaMemory = MakeRequestRetainer(bytes);
}

void TUploadMemoryReservation::AccountResolveRequest(NSchemeCache::TSchemeCacheRequest& request) const {
    if (!Attempt) {
        return;
    }
    ui64 bytes = sizeof(request) + sizeof(TEvTxProxySchemeCache::TEvResolveKeySet) + request.DatabaseName.capacity()
        + request.ResultSet.capacity() * sizeof(NSchemeCache::TSchemeCacheRequest::TEntry);
    for (const auto& entry : request.ResultSet) {
        const auto& key = *entry.KeyDescription;
        bytes += sizeof(key) + key.TableId.SysViewInfo.capacity()
            + key.KeyColumnTypes.capacity() * sizeof(NScheme::TTypeInfo)
            + key.Columns.capacity() * sizeof(TKeyDesc::TColumnOp)
            + key.Columns.size() * sizeof(TKeyDesc::TColumnInfo)
            + key.Range.GetOwnedFrom().DataSize();
        if (key.Range.From.data() != key.Range.To.data()) {
            bytes += key.Range.GetOwnedTo().DataSize();
        }
    }
    request.RetainPartitioningMemory = MakeRequestRetainer(bytes);
}

TUploadMemoryReservation::TRetainer TUploadMemoryReservation::MakeRequestRetainer(ui64 requestBytes) const {
    if (!Retain) {
        auto owner = std::make_shared<TRequestReservation>(Attempt, Batch, nullptr);
        return [owner = std::move(owner)](ui64) { return owner; };
    }
    return [attempt = Attempt, retain = Retain, requestMemory = Retain(requestBytes)](ui64 replyBytes) {
        (void)requestMemory;
        return std::make_shared<TRequestReservation>(attempt, retain(replyBytes), nullptr);
    };
}

} // namespace NKikimr::NTxProxy
