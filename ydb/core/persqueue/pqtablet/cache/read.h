#pragma once

#include "pq_l2_service.h"
#include "cache_eviction.h"

#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr {
namespace NPQ {

    inline TString ToStringLocalTimeUpToSeconds(const TInstant &time) {
        return time.GetValue() ? time.ToStringLocalUpToSeconds() : "0";
    }

    /// Intablet cache proxy: Partition <-> CacheProxy <-> KV
    class TPQCacheProxy : public TBaseTabletActor<TPQCacheProxy> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::PERSQUEUE_CACHE_ACTOR;
        }

        TPQCacheProxy(const TActorId& tablet, ui64 tabletId)
        : TBaseTabletActor(tabletId, tablet, NKikimrServices::PERSQUEUE)
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

        const TString& GetLogPrefix() const
        {
            static const TString LogPrefix = "[PQCacheProxy]";
            return LogPrefix;
        }

    private:
        ui64 SaveKvRequest(TKvRequest&& kvRequest)
        {
            ui64 cookie = Cookie++;
            auto savedRequest = KvRequests.emplace(cookie, std::move(kvRequest));
            AFL_ENSURE(savedRequest.second);
            return cookie;
        }

        void SaveInProgress(const TKvRequest& kvRequest)
        {
            for (const TRequestedBlob& reqBlob : kvRequest.Blobs) {
                auto blobId = MakeBlobId(kvRequest.Partition, reqBlob);
                ReadsInProgress.insert(blobId);
            }
        }

        bool CheckInProgress(const TActorContext&, TKvRequest& kvRequest)
        {
            for (const TRequestedBlob& reqBlob : kvRequest.Blobs) {
                TBlobId blob = MakeBlobId(kvRequest.Partition, reqBlob);
                auto it = ReadsInProgress.find(blob);
                if (it != ReadsInProgress.end()) {
                    LOG_D("Read request is blocked. Partition "
                        << blob.Partition << " offset " << blob.Offset);
                    // Save only first occured blocker, others (if any) would be checked later
                    BlockedReads[blob].emplace_back(std::move(kvRequest));
                    return true;
                }
            }
            return false;
        }

        TVector<TKvRequest> RemoveFromProgress(const TActorContext&, const TKvRequest& blocker)
        {
            TVector<TKvRequest> unblocked;
            for (const TRequestedBlob& reqBlob : blocker.Blobs) {
                TBlobId blob = MakeBlobId(blocker.Partition, reqBlob);
                ReadsInProgress.erase(blob);

                auto it = BlockedReads.find(blob);
                if (it != BlockedReads.end()) {
                    LOG_D("Read requests are unblocked. Partition "
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
            AFL_ENSURE(ev->Sender == TabletActorId);
            Die(ctx);
        }

        void Handle(TEvPQ::TEvBlobRequest::TPtr& ev, const TActorContext& ctx)
        {
            const TPartitionId& partition = ev->Get()->Partition;

            TKvRequest kvReq(TKvRequest::TypeRead, ev->Sender, ev->Get()->Cookie, partition);
            kvReq.Blobs = std::move(ev->Get()->Blobs);

            ReadBlobs(ctx, std::move(kvReq));
        }

        void ReadBlobs(const TActorContext& ctx, TKvRequest&& kvReq)
        {
            ui32 fromCache = Cache.RequestBlobs(ctx, kvReq);

            if (fromCache == kvReq.Blobs.size()) { // all from cache
                LOG_D("Reading cookie " << kvReq.CookiePQ
                    << ". All " << fromCache << " blobs are from cache.");

                THolder<TEvPQ::TEvBlobResponse> response = kvReq.MakePQResponse(ctx);
                response->Check();

                ctx.Send(kvReq.Sender, response.Release()); // -> Partition
                return;
            }

            if (CheckInProgress(ctx, kvReq)) {
                LOG_D("Reading cookie " << kvReq.CookiePQ
                    << ". There's another reading of the same blob. Waiting");
                return;
            }

            LOG_D("Reading cookie " << kvReq.CookiePQ
                << ". Have to read " << kvReq.Blobs.size() - fromCache << " of " << kvReq.Blobs.size() << " from KV");

            SaveInProgress(kvReq);
            THolder<TEvKeyValue::TEvRequest> request = kvReq.MakeKvRequest(); // before save
            ui64 cookie = SaveKvRequest(std::move(kvReq));
            request->Record.SetCookie(cookie);
            ctx.Send(TabletActorId, request.Release()); // -> KV
        }

        void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx)
        {
            const auto& resp = ev->Get()->Record;
            AFL_ENSURE(resp.HasCookie());
            auto it = KvRequests.find(resp.GetCookie());
            AFL_ENSURE(it != KvRequests.end());

            switch (it->second.Type) {
            case TKvRequest::TypeRead:
                OnKvReadResult(ev, ctx, it->second);
                break;
            case TKvRequest::TypeWrite:
                OnKvWriteResult(ev, ctx, it->second);
                break;
            }

            KvRequests.erase(it);
        }

        void OnKvReadResult(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx, TKvRequest& kvReq)
        {
            auto& resp = ev->Get()->Record;
            TVector<TRequestedBlob>& outBlobs = kvReq.Blobs;

            ui32 cachedCount = std::accumulate(outBlobs.begin(), outBlobs.end(), 0u, [](ui32 sum, const TRequestedBlob& blob) {
                                                                                    return sum + (blob.Value.empty() ? 0 : 1);
                                                                                });
            LOG_D("Got results. " << resp.ReadResultSize() << " of " << outBlobs.size() << " from KV. Status " << resp.GetStatus());

            TErrorInfo error;
            if (resp.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                LOG_E("Got Error response for whole request status "
                    << resp.GetStatus() << " cookie " << kvReq.CookiePQ);
                error = TErrorInfo(NPersQueue::NErrorCode::ERROR, Sprintf("Got bad response: %s", resp.DebugString().c_str()));
            } else {

                AFL_ENSURE(resp.ReadResultSize() && resp.ReadResultSize() + cachedCount == outBlobs.size())
                    ("resultSize", resp.ReadResultSize())("cachedCount", cachedCount)
                    ("outBlobs.size()", outBlobs.size())("proto", ev->Get()->ToString());

                TVector<bool> kvBlobs(outBlobs.size(), false);
                ui32 pos = 0;
                for (ui32 i = 0; i < resp.ReadResultSize(); ++i, ++pos) {
                    auto* r = resp.MutableReadResult(i);
                    LOG_D("Got results. result " << i << " from KV. Status " << r->GetStatus());
                    if (r->GetStatus() == NKikimrProto::OVERRUN) { //this blob and next are not readed at all. Return as answer only previous blobs
                        AFL_ENSURE(i > 0)("description", "OVERRUN in first read request")("i", i);
                        break;
                    } else if (r->GetStatus() == NKikimrProto::OK) {
                        AFL_ENSURE(r->HasValue() && r->GetValue().size());

                        // skip cached blobs, find position for the next value
                        while (pos < outBlobs.size() && outBlobs[pos].Value) {
                            ++pos;
                        }

                        AFL_ENSURE(pos < outBlobs.size())("description", "Got resulting blob with no place for it")
                            ("pos", pos)("outBlobs.size()", outBlobs.size());
                        kvBlobs[pos] = true;

                        AFL_ENSURE(outBlobs[pos].Value.empty());
                        outBlobs[pos].Value = r->GetValue();
                        outBlobs[pos].CreationUnixTime = r->GetCreationUnixTime();
                    } else {
                        LOG_E("Got Error response " << r->GetStatus()
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

        void OnKvWriteResult(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx, const TKvRequest& kvReq)
        {
            auto& resp = ev->Get()->Record;
            if (resp.GetStatus() == NMsgBusProxy::MSTATUS_OK) {
                AFL_ENSURE(resp.WriteResultSize() == (kvReq.Blobs.size() + kvReq.MetadataWritesCount))
                    ("description", "Mismatch write result size")("size", resp.WriteResultSize())
                    ("kvReq.Blobs.size()", kvReq.Blobs.size())("kvReq.MetadataWritesCount", kvReq.MetadataWritesCount);

                for (ui32 i = 0; i < resp.WriteResultSize(); ++i) {
                    auto status = resp.GetWriteResult(i).GetStatus();
                    AFL_ENSURE(status == NKikimrProto::OK)("Not OK from KV blob", ev->Get()->ToString());
                }

                Cache.SaveHeadBlobs(ctx, kvReq);
            }

            auto response = MakeHolder<TEvKeyValue::TEvResponse>();
            response->Record = std::move(resp);
            if (kvReq.CookiePQ == Max<ui64>()) {
                response->Record.ClearCookie(); //cookie must not leak to Partition - it uses cookie for SetOffset requests
            } else {
                response->Record.SetCookie(kvReq.CookiePQ);
            }
            ctx.Send(kvReq.Sender, response.Release()); // -> Partition

            UpdateCounters(ctx);
        }

        static ui64 GetKeyValueRequestCookie(const NKikimrClient::TKeyValueRequest& request) {
            return request.HasCookie() ? request.GetCookie() : Max<ui64>();
        }

        // Passthrough request to KV
        void Handle(TEvKeyValue::TEvRequest::TPtr& ev, const TActorContext& ctx)
        {
            LOG_D("CacheProxy. Passthrough write request to KV");

            auto& srcRequest = ev->Get()->Record;

            TKvRequest kvReq(TKvRequest::TypeWrite, ev->Sender, GetKeyValueRequestCookie(srcRequest), TPartitionId(Max<ui32>()));

            SaveCmdWrite(srcRequest, kvReq, ctx);
            SaveCmdRename(srcRequest, kvReq, ctx);
            SaveCmdDelete(srcRequest, kvReq, ctx);

            ui64 cookie = SaveKvRequest(std::move(kvReq));

            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
            request->Record = std::move(srcRequest);
            request->Record.SetCookie(cookie);

            ctx.Send(TabletActorId, request.Release(), 0, 0, std::move(ev->TraceId)); // -> KV
        }

        void SaveCmdWrite(const NKikimrClient::TKeyValueRequest& srcRequest, TKvRequest& kvReq, const TActorContext&)
        {
            kvReq.Blobs.reserve(srcRequest.CmdWriteSize());

            for (ui32 i = 0; i < srcRequest.CmdWriteSize(); ++i) {
                const auto& cmd = srcRequest.GetCmdWrite(i);
                const TString& strKey = cmd.GetKey();
                if (IsDataKey(strKey)) {
                    AFL_ENSURE((strKey.size() >= TKey::KeySize()) && (strKey.size() - TKey::KeySize() <= 1))
                        ("Unexpected key size", strKey.size())("key", strKey);
                    auto key = TKey::FromString(strKey);

                    const TString& value = cmd.GetValue();
                    TRequestedBlob reqBlob(key.GetOffset(), key.GetPartNo(), key.GetCount(), key.GetInternalPartsCount(), value.size(), value, key, cmd.GetCreationUnixTime());
                    kvReq.Partition = key.GetPartition();
                    kvReq.Blobs.push_back(std::move(reqBlob));
                    const TRequestedBlob& blob = kvReq.Blobs.back();

                    LOG_D("CacheProxy. Passthrough blob. Partition "
                        << kvReq.Partition << " offset " << blob.Offset << " partNo " << blob.PartNo << " count " << blob.Count << " size " << value.size());
                } else {
                    kvReq.MetadataWritesCount++;
                }
            }
        }

        void SaveCmdRename(const NKikimrClient::TKeyValueRequest& srcRequest, TKvRequest& kvReq, const TActorContext&)
        {
            kvReq.RenamedBlobs.reserve(srcRequest.CmdRenameSize());

            for (ui32 i = 0; i < srcRequest.CmdRenameSize(); ++i) {
                const auto& cmd = srcRequest.GetCmdRename(i);
                if (!IsDataKey(cmd.GetOldKey()) || !IsDataKey(cmd.GetNewKey())) {
                    continue;
                }
                kvReq.RenamedBlobs.emplace_back(cmd.GetOldKey(), cmd.GetNewKey());

                LOG_D("CacheProxy. Rename blob from " << cmd.GetOldKey() << " to " << cmd.GetNewKey());
            }
        }

        void SaveCmdDelete(const NKikimrClient::TKeyValueRequest& srcRequest, TKvRequest& kvReq, const TActorContext&)
        {
            kvReq.DeletedBlobs.reserve(srcRequest.CmdDeleteRangeSize());

            for (ui32 i = 0; i < srcRequest.CmdDeleteRangeSize(); ++i) {
                const auto& cmd = srcRequest.GetCmdDeleteRange(i);
                const auto& range = cmd.GetRange();
                if (!IsDataKey(range.GetFrom()) || !IsDataKey(range.GetTo())) {
                    continue;
                }
                kvReq.DeletedBlobs.emplace_back(range.GetFrom(), range.GetIncludeFrom(),
                                                range.GetTo(), range.GetIncludeTo());

                LOG_D("CacheProxy. Delete blobs from " <<
                            range.GetFrom() << "(" << (range.GetIncludeFrom() ? '+' : '-') << ") to " <<
                            range.GetTo() << "(" << (range.GetIncludeTo() ? '+' : '-') << ")");
            }
        }

        bool IsDataKey(const TString& key) const {
            if (key.empty()) {
                return false;
            }
            const char type = std::tolower(key.front());
            return type == TKeyPrefix::TypeData; // TypeData || ServiceTypeData
        }

        void Handle(TEvPqCache::TEvCacheL2Response::TPtr& ev, const TActorContext& ctx)
        {
            THolder<TCacheL2Response> resp(ev->Get()->Data.Release());
            AFL_ENSURE(resp->TabletId == TabletId);

            for (const TCacheBlobL2& blob : resp->Removed)
                Cache.RemoveEvictedBlob(ctx, TBlobId(blob.Partition, blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount, blob.Suffix), blob.Value);

            if (resp->Overload) {
                LOG_N("Have to remove new data from cache. Topic " << TopicName << ", tablet id" << TabletId
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
                    TABLE_CLASS("table") {
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
            ctx.Send(ev->Sender, new TEvPQ::TEvMonResponse(out.Str()));
        }

        void UpdateCounters(const TActorContext& ctx)
        {
            static const ui32 UPDATE_TIMEOUT_S = 5;

            TInstant now = TAppData::TimeProvider->Now();
            if (now < CountersUpdateTime + TDuration::Seconds(UPDATE_TIMEOUT_S))
                return;

            auto event = MakeHolder<TEvPQ::TEvTabletCacheCounters>();
            event->Counters.CacheSizeBytes = Cache.GetCounters().SizeBytes;
            event->Counters.CacheSizeBlobs = Cache.GetSize();
            event->Counters.CachedOnRead = Cache.GetCounters().CachedOnRead;
            event->Counters.CachedOnWrite = Cache.GetCounters().CachedOnWrite;

            ctx.Send(TabletActorId, event.Release());
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

        TString TopicName;
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
