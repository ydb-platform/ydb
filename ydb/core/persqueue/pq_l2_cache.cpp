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
    Y_ABORT_UNLESS(appData);

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

    Y_ABORT_UNLESS(tabletId != 0, "PQ L2. Empty tabletID in L2");

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

        Y_ABORT_UNLESS(key.TabletId == resp->TabletId, "PQ L2. Multiple topics in one PQ tablet.");
        resp->Removed.emplace_back(key.Partition, key.Offset, key.PartNo, key.Count, key.InternalPartsCount, evicted);

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

/// @return outRemoved - map of evicted items. L1 should be noticed about them
void TPersQueueCacheL2::AddBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs,
                                 THashMap<TKey, TCacheValue::TPtr>& outEvicted)
{
    ui32 numUnused = 0;
    for (const TCacheBlobL2& blob : blobs) {
        Y_ABORT_UNLESS(blob.Value->DataSize(), "Trying to place empty blob into L2 cache");

        TKey key(tabletId, blob);
        // PQ tablet could send some data twice (if it's restored after die)
        if (Cache.FindWithoutPromote(key) != Cache.End()) {
            LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Same blob insertion. " << key.ToString() << " size " << blob.Value->DataSize());
            continue;
        }

        Y_ABORT_UNLESS(CurrentSize <= Cache.Size() * MAX_BLOB_SIZE);

        CurrentSize += blob.Value->DataSize();

        // manualy manage LRU size
        while (CurrentSize > MaxSize) {
            auto oldest = Cache.FindOldest();
            Y_ABORT_UNLESS(oldest != Cache.End(), "Tablet %" PRIu64" count %" PRIu64 " size %" PRIu64
                " maxSize %" PRIu64 " blobSize %" PRIu64 " blobs %" PRIu64 " evicted %" PRIu64,
                tabletId, Cache.Size(), CurrentSize, MaxSize, blob.Value->DataSize(), blobs.size(), outEvicted.size());

            TCacheValue::TPtr value = oldest.Value();
            outEvicted.emplace(oldest.Key(), value);
            if (value->GetAccessCount() == 0)
                ++numUnused;

            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Evicting blob. " << oldest.Key().ToString() << " size " << value->DataSize());

            CurrentSize -= value->DataSize();
            Cache.Erase(oldest);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Adding blob. " << key.ToString() << " size " << blob.Value->DataSize());

        Cache.Insert(key, blob.Value);
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
    ui32 numEvicted = 0;
    ui32 numUnused = 0;
    for (const TCacheBlobL2& blob : blobs) {
        TKey key(tabletId, blob);
        auto it = Cache.FindWithoutPromote(key);
        if (it != Cache.End()) {
            CurrentSize -= (*it)->DataSize();
            numEvicted++;
            if ((*it)->GetAccessCount() == 0)
                ++numUnused;
            Cache.Erase(it);
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Removed. " << key.ToString());
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Miss in remove. " << key.ToString());
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
    RenamedKeys += blobs.size();

    for (const auto& [oldBlob, newBlob] : blobs) {
        TKey oldKey(tabletId, oldBlob);

        auto it = Cache.FindWithoutPromote(oldKey);
        if (it == Cache.End()) {
            continue;
        }

        TKey newKey(tabletId, newBlob);
        Cache.Insert(newKey, *it);
        Cache.Erase(it);

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Renamed. old " << oldKey.ToString() << ", new " << newKey.ToString());
    }
}

void TPersQueueCacheL2::TouchBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs, bool isHit)
{
    TInstant now = TAppData::TimeProvider->Now();

    for (const TCacheBlobL2& blob : blobs) {
        TKey key(tabletId, blob);
        auto it = Cache.Find(key);
        if (it != Cache.End()) {
            (*it)->Touch(now);
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Touched. " << key.ToString());
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Miss in touch. " << key.ToString());
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
    for (const TCacheBlobL2& blob : blobs) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "PQ Cache (L2). Missed blob. tabletId '" << tabletId
            << "' partition " << blob.Partition << " offset " << blob.Offset);
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
