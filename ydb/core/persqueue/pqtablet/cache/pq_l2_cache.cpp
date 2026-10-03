#include "pq_l2_cache.h"
#include <ydb/core/mon/mon.h>

namespace NKikimr {
namespace NPQ {

IActor* CreateNodePersQueueL2Cache(const TCacheL2Parameters& params, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
{
    return new TPersQueueCacheL2(params, counters);
}

void TPersQueueCacheL2::Bootstrap(const TActorContext& ctx)
{
    TAppData * appData = AppData(ctx);
    AFL_ENSURE(appData);

    auto mon = appData->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage * page = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(page, "pql2", "PersQueue Node Cache", false, ctx.ActorSystem(), ctx.SelfID);
    }

    Become(&TThis::StateFunc);
}

void TPersQueueCacheL2::Handle(TEvPqCache::TEvCacheL2Request::TPtr& ev, const TActorContext& ctx)
{
    THolder<TCacheL2Request> request(ev->Get()->Data.Release());
    ui64 tabletId = request->TabletId;

    AFL_ENSURE(tabletId != 0)("d", "PQ L2. Empty tabletID in L2");

    TouchBlobs(ctx, tabletId, request->RequestedBlobs);
    TouchBlobs(ctx, tabletId, request->ExpectedBlobs, false);
    RemoveBlobs(ctx, tabletId, request->RemovedBlobs);
    RegretBlobs(ctx, tabletId, request->MissedBlobs);
    RenameBlobs(ctx, tabletId, request->RenamedBlobs);

    THashMap<TKey, TCacheValue::TPtr> evicted;
    AddBlobs(ctx, tabletId, request->StoredBlobs, evicted);

    SendResponses(ctx, evicted);
}

void TPersQueueCacheL2::SendResponses(const TActorContext& ctx, const THashMap<TKey, TCacheValue::TPtr>& evictedBlobs)
{
    TInstant now = TAppData::TimeProvider->Now();
    THashMap<TActorId, THolder<TCacheL2Response>> responses;

    for (const auto& rm : evictedBlobs) {
        const TKey& key = rm.first;
        TCacheValue::TPtr evicted = rm.second;

        THolder<TCacheL2Response>& resp = responses[evicted->GetOwner()];
        if (!resp) {
            resp = MakeHolder<TCacheL2Response>();
            resp->TabletId = key.TabletId;
        }

        AFL_ENSURE(key.TabletId == resp->TabletId)("d", "PQ L2. Multiple topics in one PQ tablet.");
        resp->Removed.emplace_back(key.Partition, key.Offset, key.PartNo, key.Count, key.InternalPartsCount, key.Suffix, evicted);

        RetentionTime = now - evicted->GetAccessTime();
        if (RetentionTime < KeepTime)
            resp->Overload = true;
    }

    for (auto& resp : responses)
        ctx.Send(resp.first, new TEvPqCache::TEvCacheL2Response(resp.second.Release()));

    { // counters
        (*Counters.Retention) = RetentionTime.Seconds();
    }
}

void TPersQueueCacheL2::Handle(TEvPqCache::TEvCacheKeysRequest::TPtr& ev, const TActorContext& ctx)
{
    auto response = MakeHolder<TEvPqCache::TEvCacheKeysResponse>();
    response->RenamedKeys = RenamedKeys;
    ctx.Send(ev->Sender, response.Release());
}

void TPersQueueCacheL2::RepairCurrentSize(const TKey& key)
{
    const ui64 limit = static_cast<ui64>(Cache.Size()) * static_cast<ui64>(MAX_BLOB_SIZE);
    if (CurrentSize <= limit) {
        return;
    }

    ui64 summed = 0;
    ui64 maxBlob = 0;
    for (auto it = Cache.Begin(); it != Cache.End(); ++it) {
        const ui64 size = it.Value()->GetDataSize();
        summed += size;
        if (size > maxBlob) {
            maxBlob = size;
        }
    }
    if (summed == CurrentSize) {
        return;
    }

    LOG_E("PQ Cache (L2). CurrentSize exceeds count * MAX_BLOB_SIZE",
        {"key", key},
        {"CurrentSize", CurrentSize},
        {"SummedSize", summed},
        {"CacheSize", Cache.Size()},
        {"MaxBlob", maxBlob},
        {"Limit", limit},
        {"MaxBlobSizeConst", static_cast<ui64>(MAX_BLOB_SIZE)});
    CurrentSize = summed;
}

/// @return outRemoved - map of evicted items. L1 should be noticed about them
void TPersQueueCacheL2::AddBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs,
                                 THashMap<TKey, TCacheValue::TPtr>& outEvicted)
{
    Y_UNUSED(ctx);
    ui32 numUnused = 0;
    for (const TCacheBlobL2& blob : blobs) {
        const ui64 blobSize = blob.Value->GetDataSize();
        AFL_ENSURE(blobSize)("d", "Trying to place empty blob into L2 cache");

        TKey key(tabletId, blob);
        // PQ tablet could send some data twice (if it's restored after die)
        if (Cache.FindWithoutPromote(key) != Cache.End()) {
            LOG_W("PQ Cache (L2). Same blob insertion. size",
                {"key", key},
                {"valueDataSize", blobSize});
            continue;
        }

        RepairCurrentSize(key);

        // Evict until the new blob fits. Its bytes are counted only after Insert
        // keeps it, so one blob larger than MaxSize stays as the only entry.
        while (CurrentSize + blobSize > MaxSize) {
            auto oldest = Cache.FindOldest();
            if (oldest == Cache.End()) {
                break;
            }

            TCacheValue::TPtr value = oldest.Value();
            outEvicted.emplace(oldest.Key(), value);
            if (value->GetAccessCount() == 0)
                ++numUnused;

            LOG_D("PQ Cache (L2). Evicting blob. size",
                {"key", oldest.Key()},
                {"dataSize", value->GetDataSize()});

            CurrentSize -= value->GetDataSize();
            Cache.Erase(oldest);
        }

        TMaybe<TKey> overflowKey;
        TCacheValue::TPtr overflowValue;
        if (!Cache.Empty() && Cache.Size() + 1 > Cache.GetMaxSize()) {
            auto oldest = Cache.FindOldest();
            overflowKey = oldest.Key();
            overflowValue = oldest.Value();
        }

        if (!Cache.Insert(key, blob.Value)) {
            LOG_W("PQ Cache (L2). Blob was not stored",
                {"key", key},
                {"valueDataSize", blobSize});
            continue;
        }
        CurrentSize += blobSize;

        if (overflowKey && Cache.FindWithoutPromote(*overflowKey) == Cache.End()) {
            outEvicted.emplace(*overflowKey, overflowValue);
            if (overflowValue->GetAccessCount() == 0)
                ++numUnused;
            LOG_D("PQ Cache (L2). Evicting blob. size",
                {"key", *overflowKey},
                {"dataSize", overflowValue->GetDataSize()});
            CurrentSize -= overflowValue->GetDataSize();
        }

        LOG_D("PQ Cache (L2). Adding blob. size",
            {"key", key},
            {"valueDataSize", blobSize});
    }

    { // counters
        (*Counters.TotalSize) = CurrentSize;
        (*Counters.TotalCount) = Cache.Size();
        (*Counters.Evictions) += outEvicted.size();
        (*Counters.Unused) += numUnused;
        (*Counters.Used) += outEvicted.size() - numUnused;
    }
}

void TPersQueueCacheL2::RemoveBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs)
{
    Y_UNUSED(ctx);
    ui32 numEvicted = 0;
    ui32 numUnused = 0;
    for (const TCacheBlobL2& blob : blobs) {
        TKey key(tabletId, blob);
        auto it = Cache.FindWithoutPromote(key);
        if (it != Cache.End()) {
            CurrentSize -= (*it)->GetDataSize();
            numEvicted++;
            if ((*it)->GetAccessCount() == 0)
                ++numUnused;
            LOG_D("PQ Cache (L2). Removed. size",
                {"key", key},
                {"dataSize", (*it)->GetDataSize()});
            Cache.Erase(it);
        } else {
            LOG_D("PQ Cache (L2). Miss in remove",
                {"key", key});
        }
    }

    { // counters
        (*Counters.TotalSize) = CurrentSize;
        (*Counters.TotalCount) = Cache.Size();
        (*Counters.Evictions) += numEvicted;
        (*Counters.Unused) += numUnused;
        (*Counters.Used) += numEvicted - numUnused;
    }
}

void TPersQueueCacheL2::RenameBlobs(const TActorContext& ctx, ui64 tabletId,
                                    const TVector<std::pair<TCacheBlobL2, TCacheBlobL2>>& blobs)
{
    Y_UNUSED(ctx);
    RenamedKeys += blobs.size();

    for (const auto& [oldBlob, newBlob] : blobs) {
        TKey oldKey(tabletId, oldBlob);

        auto it = Cache.FindWithoutPromote(oldKey);
        if (it == Cache.End()) {
            continue;
        }

        TKey newKey(tabletId, newBlob);
        if (oldKey == newKey) {
            continue;
        }

        TCacheValue::TPtr value = *it;
        const ui64 oldSize = value->GetDataSize();
        if (Cache.FindWithoutPromote(newKey) != Cache.End()) {
            // Destination is already counted. Dropping the source must drop its bytes.
            CurrentSize -= oldSize;
            Cache.Erase(it);
            LOG_D("PQ Cache (L2). Renamed. old new",
                {"oldKey", oldKey},
                {"newKey", newKey});
            continue;
        }

        TMaybe<TKey> overflowKey;
        TCacheValue::TPtr overflowValue;
        if (Cache.Size() + 1 > Cache.GetMaxSize()) {
            auto oldest = Cache.FindOldest();
            if (oldest != Cache.End()) {
                overflowKey = oldest.Key();
                overflowValue = oldest.Value();
            }
        }

        const bool inserted = Cache.Insert(newKey, value);
        if (overflowKey && !(*overflowKey == oldKey) && Cache.FindWithoutPromote(*overflowKey) == Cache.End()) {
            CurrentSize -= overflowValue->GetDataSize();
        }

        auto oldIt = Cache.FindWithoutPromote(oldKey);
        if (oldIt != Cache.End()) {
            Cache.Erase(oldIt);
        }
        if (!inserted) {
            CurrentSize -= oldSize;
        }

        LOG_D("PQ Cache (L2). Renamed. old new",
            {"oldKey", oldKey},
            {"newKey", newKey});
    }

    (*Counters.TotalSize) = CurrentSize;
    (*Counters.TotalCount) = Cache.Size();
}

void TPersQueueCacheL2::TouchBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs, bool isHit)
{
    Y_UNUSED(ctx);
    TInstant now = TAppData::TimeProvider->Now();

    for (const TCacheBlobL2& blob : blobs) {
        TKey key(tabletId, blob);
        auto it = Cache.Find(key);
        if (it != Cache.End()) {
            (*it)->Touch(now);
            LOG_D("PQ Cache (L2). Touched",
                {"key", key});
        } else {
            LOG_D("PQ Cache (L2). Miss in touch",
                {"key", key});
        }
    }

    { // counters
        (*Counters.Touches) += blobs.size();
        if (isHit)
            (*Counters.Hits) += blobs.size();

        auto oldest = Cache.FindOldest();
        if (oldest != Cache.End())
            RetentionTime = now - oldest.Value()->GetAccessTime();
    }
}

void TPersQueueCacheL2::RegretBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs)
{
    Y_UNUSED(ctx);
    for (const TCacheBlobL2& blob : blobs) {
        LOG_D("PQ Cache (L2). Missed blob. tabletId partition offset partno count parts_count",
            {"tabletId", tabletId},
            {"blobPartition", blob.Partition},
            {"blobOffset", blob.Offset},
            {"partNo", blob.PartNo},
            {"blobCount", blob.Count},
            {"internalPartsCount", blob.InternalPartsCount});
    }

    { // counters
        (*Counters.Misses) += blobs.size();
    }
}

void TPersQueueCacheL2::Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx)
{
    const auto& params = ev->Get()->Request.GetParams();
    if (params.Has("submit")) {
        TString strParam = params.Get("newCacheLimit");
        if (strParam.size()) {
            ui32 valueMb = atoll(strParam.data());
            MaxSize = ClampMinSize(valueMb * 1_MB); // will be applyed at next AddBlobs
        }
    }

    TString html = HttpForm();
    ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(html));
}

TString TPersQueueCacheL2::HttpForm() const
{
    TStringStream str;
    HTML(str) {
        FORM_CLASS("form-horizontal") {
            DIV_CLASS("row") {
                PRE() {
                        str << "CacheLimit (MB): " << (MaxSize >> 20) << Endl;
                        str << "CacheSize (MB): " << (CurrentSize >> 20) << Endl;
                        str << "Count of blobs: " << Cache.Size() << Endl;
                        str << "Min RetentionTime: " << KeepTime << Endl;
                        str << "RetentionTime: " << RetentionTime << Endl;
                }
            }
            DIV_CLASS("control-group") {
                LABEL_CLASS_FOR("control-label", "inputTo") {str << "New Chache Limit";}
                DIV_CLASS("controls") {
                    str << "<input type=\"number\" id=\"inputTo\" placeholder=\"CacheLimit (MB)\" name=\"newCacheLimit\">";
                }
            }
            DIV_CLASS("control-group") {
                DIV_CLASS("controls") {
                    str << "<button type=\"submit\" name=\"submit\" class=\"btn btn-primary\">Change</button>";
                }
            }
        }
    }
    return str.Str();
}

} // NPQ
} // NKikimr
