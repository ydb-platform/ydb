#pragma once

#include "partition.h"
#include "pq_l2_service.h"
#include "cache_eviction.h"

#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr {
namespace NPQ {

    inline TString ToStringLocalTimeUpToSeconds(const TInstant &time) {
        return time.GetValue() ? time.ToStringLocalUpToSeconds() : "0";
    }

    /// Intablet cache proxy: Partition <-> CacheProxy <-> KV
    class TPQCacheProxy : public TActorBootstrapped<TPQCacheProxy> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::PERSQUEUE_CACHE_ACTOR;
        }

        TPQCacheProxy(const TActorId& tablet, ui64 tabletId)
        : Tablet(tablet)
        , TabletId(tabletId)
        , Cookie(0)
        , Cache(tabletId)
        , CountersUpdateTime(TAppData::TimeProvider->Now())
        {
        }

        void Bootstrap(const TActorContext& ctx)
        {
            Y_UNUSED(ctx);
            Become(&TThis::StateFunc);
        }

    private:
        ui64 SaveKvRequest(TKvRequest&& kvRequest)
        {
            ui64 cookie = Cookie++;
            auto savedRequest = KvRequests.insert({cookie, std::move(kvRequest)});
            Y_ABORT_UNLESS(savedRequest.second);
            return cookie;
        }

        void SaveInProgress(const TKvRequest& kvRequest)
        {
            for (const TRequestedBlob& reqBlob : kvRequest.Blobs) {
                TBlobId blob(kvRequest.Partition, reqBlob.Offset, reqBlob.PartNo, reqBlob.Count, reqBlob.InternalPartsCount);
                ReadsInProgress.insert(blob);
            }
        }

        bool CheckInProgress(const TActorContext& ctx, TKvRequest& kvRequest)
        {
            for (const TRequestedBlob& reqBlob : kvRequest.Blobs) {
                TBlobId blob(kvRequest.Partition, reqBlob.Offset, reqBlob.PartNo, reqBlob.Count, reqBlob.InternalPartsCount);
                auto it = ReadsInProgress.find(blob);
                if (it != ReadsInProgress.end()) {
                    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Read request is blocked. Partition "
                        << blob.Partition << " offset " << blob.Offset);
                    // Save only first occured blocker, others (if any) would be checked later
                    BlockedReads[blob].emplace_back(std::move(kvRequest));
                    return true;
                }
            }
            return false;
        }

        TVector<TKvRequest> RemoveFromProgress(const TActorContext& ctx, const TKvRequest& blocker)
        {
            TVector<TKvRequest> unblocked;
            for (const TRequestedBlob& reqBlob : blocker.Blobs) {
                TBlobId blob(blocker.Partition, reqBlob.Offset, reqBlob.PartNo, reqBlob.Count, reqBlob.InternalPartsCount);
                ReadsInProgress.erase(blob);

                auto it = BlockedReads.find(blob);
                if (it != BlockedReads.end()) {
                    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Read requests are unblocked. Partition "
                        << blob.Partition << " offset " << blob.Offset << " num unblocked " << it->second.size());
                    for (TKvRequest& kvReq : it->second)
                        unblocked.emplace_back(std::move(kvReq));
                    BlockedReads.erase(it);
                }
            }
            return unblocked;
        }

        void Handle(TEvPQ::TEvChangeCacheConfig::TPtr& ev, const TActorContext& ctx)
        {
            if (ev->Get()->TopicName) {
                TopicName = ev->Get()->TopicName;
            }
            Cache.Touch(ctx);
        }

        void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx)
        {
            Y_ABORT_UNLESS(ev->Sender == Tablet);
            Die(ctx);
        }

        void Handle(TEvPQ::TEvBlobRequest::TPtr& ev, const TActorContext& ctx)
        {
            const TPartitionId& partition = ev->Get()->Partition;
            Cache.SetUserOffset(ctx, ev->Get()->User, partition, ev->Get()->ReadOffset);

            TKvRequest kvReq(TKvRequest::TypeRead, ev->Sender, ev->Get()->Cookie, partition);
            kvReq.Blobs = std::move(ev->Get()->Blobs);

            ReadBlobs(ctx, std::move(kvReq));
        }

        void ReadBlobs(const TActorContext& ctx, TKvRequest&& kvReq)
        {
            ui32 fromCache = Cache.RequestBlobs(ctx, kvReq);

            if (fromCache == kvReq.Blobs.size()) { // all from cache
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reading cookie " << kvReq.CookiePQ
                    << ". All " << fromCache << " blobs are from cache.");

                THolder<TEvPQ::TEvBlobResponse> response = kvReq.MakePQResponse(ctx);
                response->Check();

                ctx.Send(kvReq.Sender, response.Release()); // -> Partition
                return;
            }

            if (CheckInProgress(ctx, kvReq)) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reading cookie " << kvReq.CookiePQ
                    << ". There's another reading of the same blob. Waiting");
                return;
            }

            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reading cookie " << kvReq.CookiePQ
                << ". Have to read " << kvReq.Blobs.size() - fromCache << " of " << kvReq.Blobs.size() << " from KV");

            SaveInProgress(kvReq);
            THolder<TEvKeyValue::TEvRequest> request = kvReq.MakeKvRequest(); // before save
            ui64 cookie = SaveKvRequest(std::move(kvReq));
            request->Record.SetCookie(cookie);
            ctx.Send(Tablet, request.Release()); // -> KV
        }

        void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx)
        {
            auto resp = ev->Get()->Record;
            Y_ABORT_UNLESS(resp.HasCookie());
            auto it = KvRequests.find(resp.GetCookie());
            Y_ABORT_UNLESS(it != KvRequests.end());

            TErrorInfo error;
            if (it->second.Type == TKvRequest::TypeRead) {
                OnKvReadResult(ev, ctx, it->second);
            } else if (it->second.Type == TKvRequest::TypeWrite) {
                OnKvWriteResult(ev, ctx, it->second);
            }

            KvRequests.erase(it);
        }

        void OnKvReadResult(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx, TKvRequest& kvReq)
        {
            auto resp = ev->Get()->Record;
            TVector<TRequestedBlob>& outBlobs = kvReq.Blobs;

            ui32 cachedCount = std::accumulate(outBlobs.begin(), outBlobs.end(), 0u, [](ui32 sum, const TRequestedBlob& blob) {
                                                                                    return sum + (blob.Value.empty() ? 0 : 1);
                                                                                });
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Got results. "
                << resp.ReadResultSize() << " of " << outBlobs.size() << " from KV. Status " << resp.GetStatus());

            TErrorInfo error;
            if (resp.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Got Error response for whole request status "
                    << resp.GetStatus() << " cookie " << kvReq.CookiePQ);
                error = TErrorInfo(NPersQueue::NErrorCode::ERROR, Sprintf("Got bad response: %s", resp.DebugString().c_str()));
            } else {

                Y_ABORT_UNLESS(resp.ReadResultSize() && resp.ReadResultSize() + cachedCount == outBlobs.size(),
                    "Unexpected KV read result size %" PRIu64 " for cached %" PRIu32 "/%" PRIu64 " blobs, proto %s",
                    resp.ReadResultSize(), cachedCount, outBlobs.size(), ev->Get()->ToString().data());

                TVector<bool> kvBlobs(outBlobs.size(), false);
                ui32 pos = 0;
                for (ui32 i = 0; i < resp.ReadResultSize(); ++i, ++pos) {
                    auto r = resp.MutableReadResult(i);
                    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Got results. result " << i << " from KV. Status " << r->GetStatus());
                    if (r->GetStatus() == NKikimrProto::OVERRUN) { //this blob and next are not readed at all. Return as answer only previous blobs
                        Y_ABORT_UNLESS(i > 0, "OVERRUN in first read request");
                        break;
                    } else if (r->GetStatus() == NKikimrProto::OK) {
                        Y_ABORT_UNLESS(r->HasValue() && r->GetValue().size());

                        // skip cached blobs, find position for the next value
                        while (pos < outBlobs.size() && outBlobs[pos].Value) {
                            ++pos;
                        }

                        Y_ABORT_UNLESS(pos < outBlobs.size(), "Got resulting blob with no place for it");
                        kvBlobs[pos] = true;

                        Y_ABORT_UNLESS(outBlobs[pos].Value.empty());
                        outBlobs[pos].Value = r->GetValue();
                    } else {
                        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Got Error response " << r->GetStatus()
                                        << " for " << i << "'s blob from " << resp.ReadResultSize() << " blobs");
                        error = TErrorInfo(r->GetStatus() == NKikimrProto::NODATA ? NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET
                                                                              : NPersQueue::NErrorCode::ERROR,
                                              Sprintf("Got bad response: %s", r->DebugString().c_str()));
                        break;
                    }
                }

                Cache.SavePrefetchBlobs(ctx, kvReq, kvBlobs);
            }

            TVector<TKvRequest> unblockedReads = RemoveFromProgress(ctx, kvReq); // before kvReq.MakePQResponse()

            THolder<TEvPQ::TEvBlobResponse> response = kvReq.MakePQResponse(ctx, error);
            response->Check();
            ctx.Send(kvReq.Sender, response.Release()); // -> Partition

            // Retry previously blocked reads. Should be called after saving blobs in cache
            for (TKvRequest& ubr : unblockedReads)
                ReadBlobs(ctx, std::move(ubr));

            UpdateCounters(ctx);
        }

        void OnKvWriteResult(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx, TKvRequest& kvReq)
        {
            auto resp = ev->Get()->Record;
            if (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK) {
                Y_ABORT_UNLESS(resp.WriteResultSize() == (kvReq.Blobs.size() + kvReq.MetadataWritesCount),
                    "Mismatch write result size: %" PRIu64 " vs expected blobs %" PRIu64 " and metadata %" PRIu32,
                    resp.WriteResultSize(), kvReq.Blobs.size(), kvReq.MetadataWritesCount);

                for (ui32 i = 0; i < resp.WriteResultSize(); ++i) {
                    auto status = resp.GetWriteResult(i).GetStatus();
                    Y_ABORT_UNLESS(status == NKikimrProto::OK, "Not OK from KV blob: %s", ev->Get()->ToString().data());
                }

                Cache.SaveHeadBlobs(ctx, kvReq);
            }

            THolder<TEvKeyValue::TEvResponse> response = MakeHolder<TEvKeyValue::TEvResponse>();
            response->Record = std::move(ev->Get()->Record);

            response->Record.ClearCookie(); //cookie must not leak to Partition - it uses cookie for SetOffset requests

            ctx.Send(kvReq.Sender, response.Release()); // -> Partition

            UpdateCounters(ctx);
        }

        // Passthrough request to KV
        void Handle(TEvKeyValue::TEvRequest::TPtr& ev, const TActorContext& ctx)
        {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "CacheProxy. Passthrough write request to KV");

            auto srcRequest = ev->Get()->Record;

            TKvRequest kvReq(TKvRequest::TypeWrite, ev->Sender, Max<ui64>(), TPartitionId(Max<ui32>()));
            kvReq.Blobs.reserve(srcRequest.CmdWriteSize());

            for (ui32 i = 0; i < srcRequest.CmdWriteSize(); ++i) {
                auto cmd = srcRequest.GetCmdWrite(i);
                if (cmd.HasKeyToCache()) {
                    TString strKey = cmd.GetKeyToCache();
                    TKey key = TKey(strKey);
                    Y_ABORT_UNLESS(!key.IsHead());

                    Y_ABORT_UNLESS(strKey.size() == TKey::KeySize(), "Unexpected key size: %" PRIu64, strKey.size());
                    TString value = cmd.GetValue();
                    kvReq.Partition = key.GetPartition();
                    TRequestedBlob blob(key.GetOffset(), key.GetPartNo(), key.GetCount(), key.GetInternalPartsCount(), value.size(), value, key);
                    kvReq.Blobs.push_back(blob);

                    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "CacheProxy. Passthrough blob. Partition "
                        << kvReq.Partition << " offset " << blob.Offset << " partNo " << blob.PartNo << " count " << blob.Count << " size " << value.size());
                } else {
                    kvReq.MetadataWritesCount++;
                }
            }

            ui64 cookie = SaveKvRequest(std::move(kvReq));

            THolder<TEvKeyValue::TEvRequest> request = MakeHolder<TEvKeyValue::TEvRequest>();
            request->Record = std::move(ev->Get()->Record);
            request->Record.SetCookie(cookie);
            ctx.Send(Tablet, request.Release()); // -> KV
        }

        void Handle(TEvPqCache::TEvCacheL2Response::TPtr& ev, const TActorContext& ctx)
        {
            THolder<TCacheL2Response> resp(ev->Get()->Data.Release());
            Y_ABORT_UNLESS(resp->TabletId == TabletId);

            for (TCacheBlobL2& blob : resp->Removed)
                Cache.RemoveEvictedBlob(ctx, TBlobId(blob.Partition, blob.Offset, blob.PartNo, 0, 0), blob.Value);

            if (resp->Overload) {
                LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE,
                    "Have to remove new data from cache. Topic " << TopicName << ", tablet id" << TabletId
                                                                 << ", cookie " << resp->Cookie);
            }

            UpdateCounters(ctx);
        }

        void HandleMonitoring(TEvPQ::TEvMonRequest::TPtr& ev, const TActorContext& ctx)
        {
            TStringStream out;
            HTML(out)
            {
                DIV_CLASS_ID("tab-pane fade", "cache") {
                    TABLE_SORTABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {out << "Partition";}
                                TABLEH() {out << "Offset";}
                                TABLEH() {out << "Count";}
                                TABLEH() {out << "Size";}
                                TABLEH() {out << "Time";}
                            }
                        }
                        TABLEBODY() {
                            for (const auto& c: Cache.CachedMap()) {
                                const TCacheValue::TPtr data = c.second.GetBlob();
                                if (!data)
                                    continue;
                                TABLER() {
                                    TABLED() {out << c.first.Partition;}
                                    TABLED() {out << c.first.Offset;}
                                    TABLED() {out << c.first.Count;}
                                    TABLED() {out << data->GetValue().size();}
                                    TABLED() {out << ToStringLocalTimeUpToSeconds(data->GetAccessTime());}
                                }
                            }

                        }
                    }
                }
            }
            ctx.Send(ev->Sender, new TEvPQ::TEvMonResponse(TVector<TString>(), out.Str()));
        }

        void UpdateCounters(const TActorContext& ctx)
        {
            static const ui32 UPDATE_TIMEOUT_S = 5;

            TInstant now = TAppData::TimeProvider->Now();
            if (now < CountersUpdateTime + TDuration::Seconds(UPDATE_TIMEOUT_S))
                return;

            THolder<TEvPQ::TEvTabletCacheCounters> event = MakeHolder<TEvPQ::TEvTabletCacheCounters>();
            event->Counters.CacheSizeBytes = Cache.GetCounters().SizeBytes;
            event->Counters.CacheSizeBlobs = Cache.GetSize();
            event->Counters.CachedOnRead = Cache.GetCounters().CachedOnRead;
            event->Counters.CachedOnWrite = Cache.GetCounters().CachedOnWrite;

            ctx.Send(Tablet, event.Release());
            CountersUpdateTime = now;
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvPQ::TEvBlobRequest, Handle);       // read requests
                HFunc(TEvents::TEvPoisonPill, Handle);
                HFunc(TEvPQ::TEvMonRequest, HandleMonitoring);
                HFunc(TEvKeyValue::TEvRequest, Handle);     // write requests
                HFunc(TEvKeyValue::TEvResponse, Handle);    // read & write responses
                HFunc(TEvPQ::TEvChangeCacheConfig, Handle);
                HFunc(TEvPqCache::TEvCacheL2Response, Handle);
            default:
                break;
            };
        }

        TActorId Tablet;
        TString TopicName;
        ui64 TabletId;
        ui64 Cookie;
        // any TKvRequest would be placed into KvRequests or into BlockedReads depending on ReadsInProgress content
        THashMap<ui64, TKvRequest> KvRequests;
        THashMap<TBlobId, TVector<TKvRequest>> BlockedReads;
        THashSet<TBlobId> ReadsInProgress;
        TIntabletCache Cache;
        TInstant CountersUpdateTime;
    };

} //NPQ
} //NKikimr
