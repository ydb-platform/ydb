#include "vdisk_mock.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <util/generic/hash_set.h>
#include <util/system/guard.h>

namespace NKikimr {

using namespace NActors;

class TVDiskMockActor : public TActorBootstrapped<TVDiskMockActor> {
    const TVDiskID VDiskId;
    const TIntrusivePtr<TVDiskMockSharedState> Shared;
    const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
    TVDiskContextPtr VCtx;
    TMap<TLogoBlobID, std::optional<TString>> LogoBlobs;
    TMap<std::tuple<ui64, ui8, ui32, ui32, bool>, std::tuple<ui32, ui32>> Barriers;
    TMap<ui64, ui32> Blocks;
    bool ErrorMode = false;
    bool LostMode = false;

public:
    TVDiskMockActor(const TVDiskID& vdiskId,
            TIntrusivePtr<TVDiskMockSharedState> shared,
            std::shared_ptr<TBlobStorageGroupInfo::TTopology> top)
        : VDiskId(vdiskId)
        , Shared(std::move(shared))
        , Top(std::move(top))
    {
    }

    template<typename T>
    static void FinalizeAndSend(std::unique_ptr<T> ptr, const TActorContext& ctx, const TActorId& recipient) {
        T* const p = ptr.get();
        p->FinalizeAndSend(ctx, std::make_unique<IEventHandle>(recipient, ctx.SelfID, ptr.release()));
    }

    void Bootstrap(const TActorContext& ctx) {
        VCtx.Reset(new TVDiskContext(ctx.SelfID, Top, new ::NMonitoring::TDynamicCounters, VDiskId,
                ctx.ExecutorThread.ActorSystem, NPDisk::DEVICE_TYPE_UNKNOWN));
        Become(&TVDiskMockActor::StateFunc);
    }

    bool IsBlocked(ui64 tabletId, ui32 generation) const {
        auto it = Blocks.find(tabletId);
        return it != Blocks.end() && generation <= it->second;
    }

    bool IsBlocked(const TLogoBlobID& id) const {
        return IsBlocked(id.TabletID(), id.Generation());
    }

    template<typename TMsg>
    void PutBlob(const TLogoBlobID& id, std::optional<TString> data, TMsg& msg, ui32 payloadIdx) {
        // get data
        if (!data) {
            const TRope& rope = msg.GetPayload(payloadIdx);
            data = TString::Uninitialized(rope.GetSize());
            rope.Begin().ExtractPlainDataAndAdvance(data->Detach(), data->size());
        }

        Y_ABORT_UNLESS(data->size() == Shared->GroupInfo->Type.PartSize(id));

        // write record
        if (auto it = LogoBlobs.find(id); it != LogoBlobs.end()) {
            // ensure that new record is the same as the existing one with same blob id
            auto makeErrorString = [&] {
                TStringBuilder s;
                s << "id# " << id.ToString();
                if (it->second) {
                    s << " " << it->second->size() << "b";
                } else {
                    s << " NODATA";
                }
                s << " data# " << data->size() << "b";
                return s;
            };
            Y_ABORT_UNLESS(!it->second || *it->second == *data, "%s", makeErrorString().data());
        }
        LogoBlobs[id] = std::move(data);

        // report this blob to shared state (emulate syncer)
        with_lock (Shared->Mutex) {
            TLogoBlobID fullId(id.FullID());
            TIngress ingress(*TIngress::CreateIngressWithLocal(&Shared->GroupInfo->GetTopology(), VDiskId, id));
            TIngress& sharedIngress = Shared->BlobToIngressMap[fullId];
            sharedIngress.Merge(ingress.CopyWithoutLocal(Shared->GroupInfo->Type));
        }
    }

    void Handle(TEvBlobStorage::TEvVPut::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId,
                "record.VDiskId# %s VDiskId# %s",
                VDiskIDFromVDiskID(record.GetVDiskID()).ToString().data(), VDiskId.ToString().data());
        TLogoBlobID id{LogoBlobIDFromLogoBlobID(record.GetBlobID())};

        LOG_DEBUG(ctx, NActorsServices::TEST, "TEvVPut# %s", ev->Get()->ToString().data());

        auto sendResponse = [&](NKikimrProto::EReplyStatus status, const TString& errorReason) {
            ui64 cookie = record.GetCookie();
            auto response = std::make_unique<TEvBlobStorage::TEvVPutResult>(status, id,
                    VDiskIDFromVDiskID(record.GetVDiskID()), record.HasCookie() ? &cookie : nullptr,
                    TOutOfSpaceStatus(0u, 0.0), TAppData::TimeProvider->Now(), (ui32)ev->Get()->GetCachedByteSize(),
                    &record, nullptr, nullptr, nullptr, 0, 0, errorReason);
            FinalizeAndSend(std::move(response), ctx, ev->Sender);
        };

        if (ErrorMode) {
            return sendResponse(NKikimrProto::ERROR, "error mode");
        }

        // check for blocks
        if (!record.GetIgnoreBlock()) {
            if (IsBlocked(id)) {
                return sendResponse(NKikimrProto::BLOCKED, "blocked");
            }
            for (const auto& extra : record.GetExtraBlockChecks()) {
                if (IsBlocked(extra.GetTabletId(), extra.GetGeneration())) {
                    return sendResponse(NKikimrProto::BLOCKED, "blocked");
                }
            }
        }

        // put the blob in place
        PutBlob(id, record.HasBuffer() ? std::make_optional(record.GetBuffer()) : std::nullopt, *ev->Get(), 0);

        // report success
        return sendResponse(NKikimrProto::OK, TString());
    }

    void Handle(TEvBlobStorage::TEvVMultiPut::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId,
                "record.VDiskId# %s VDiskId# %s",
                VDiskIDFromVDiskID(record.GetVDiskID()).ToString().data(), VDiskId.ToString().data());

        LOG_DEBUG(ctx, NActorsServices::TEST, "TEvVMultiPut# %s", ev->Get()->ToString().data());

        ui64 cookie = record.GetCookie();
        auto response = std::make_unique<TEvBlobStorage::TEvVMultiPutResult>(NKikimrProto::OK,
                VDiskIDFromVDiskID(record.GetVDiskID()), record.HasCookie() ? &cookie : nullptr,
                TAppData::TimeProvider->Now(), (ui32)ev->Get()->GetCachedByteSize(),
                &record, nullptr, nullptr, nullptr, 0, 0, TString());
        if (ErrorMode) {
            response->MakeError(NKikimrProto::ERROR, "error mode", record);
            LOG_DEBUG(ctx, NActorsServices::TEST, "TEvVMultiPut %s -> %s", ev->Get()->ToString().data(),
                    response->ToString().data());
            FinalizeAndSend(std::move(response), ctx, ev->Sender);
            return;
        }

        for (ui64 i = 0; i < ev->Get()->Record.ItemsSize(); ++i) {
            auto &item = ev->Get()->Record.GetItems(i);
            TLogoBlobID id{LogoBlobIDFromLogoBlobID(item.GetBlobID())};
            // check for blocks
            if (!record.GetIgnoreBlock() && IsBlocked(id)) {
                response->AddVPutResult(NKikimrProto::BLOCKED, "blocked", id, &i);
                continue;
            }

            // put the blob in place
            PutBlob(id, item.HasBuffer() ? std::make_optional(item.GetBuffer()) : std::nullopt, *ev->Get(), i);

            // report success
            response->AddVPutResult(NKikimrProto::OK, TString(), id, &i);
        }
        FinalizeAndSend(std::move(response), ctx, ev->Sender);
    }

    void Handle(TEvBlobStorage::TEvVGet::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId);

        TMaybe<ui64> cookie;
        if (record.HasCookie()) {
            cookie = record.GetCookie();
        }

        auto response = std::make_unique<TEvBlobStorage::TEvVGetResult>(NKikimrProto::OK,
            VDiskIDFromVDiskID(record.GetVDiskID()), TAppData::TimeProvider->Now(), (ui32)ev->Get()->GetCachedByteSize(),
            &record, nullptr, nullptr, nullptr, cookie, 0U, 0U);

        if (ErrorMode) {
            response->MakeError(NKikimrProto::ERROR, "error mode", record);
            LOG_DEBUG(ctx, NActorsServices::TEST, "TEvVGet# %s -> %s", ev->Get()->ToString().data(), response->ToString().data());
            FinalizeAndSend(std::move(response), ctx, ev->Sender);
            return;
        }

        TMaybe<TLogoBlobID> prevGenericId;

        auto addResult = [&](const TLogoBlobID& id, const std::optional<TString>& buffer, ui32 offset, ui32 size, const ui64 *cookie) {
            const TLogoBlobID fullId(id.FullID());

            if (record.GetIndexOnly()) {
                // do not allow duplicates when executing index-only query
                if (prevGenericId && fullId == *prevGenericId) {
                    return;
                } else {
                    prevGenericId = fullId;
                }
            }

            // get the ingress for this blob
            ui64 ingress = 0;
            const ui64 *pingr = nullptr;

            // find common ingress entry for this item
            TIngress ingr;
            with_lock (Shared->Mutex) {
                auto iter = Shared->BlobToIngressMap.find(fullId);
                Y_ABORT_UNLESS(iter != Shared->BlobToIngressMap.end(), "fullId# %s", fullId.ToString().data());
                ingr = iter->second;
            }

            // find all local replicas for this blob and merge 'em into ingress
            for (auto iter = LogoBlobs.lower_bound(fullId); iter != LogoBlobs.end() && iter->first.FullID() == fullId; ++iter) {
                if (iter->second) {
                    ingr.Merge(*TIngress::CreateIngressWithLocal(&Shared->GroupInfo->GetTopology(), VDiskId, iter->first));
                }
            }

            if (record.GetShowInternals()) {
                ingress = ingr.Raw();
                pingr = &ingress;
            }

            if (buffer) {
                offset = Min<size_t>(offset, buffer->size());
                const ui32 maxSize = buffer->size() - offset;
                size = size ? Min(size, maxSize) : maxSize;
            }

            if (LostMode) {
                // answer like in VDisk query code
                if (record.GetIndexOnly()) {
                    response->AddResult(NKikimrProto::NOT_YET, fullId, cookie, pingr);
                } else {
                    response->AddResult(NKikimrProto::NOT_YET, id, offset, size, cookie, pingr);
                }
            } else {
                // check the status; we reply with OK on all index queries (we have found the blob); also we reply OK on
                // data queries where we have the data
                NKikimrProto::EReplyStatus status = record.GetIndexOnly() || buffer
                    ? NKikimrProto::OK
                    : NKikimrProto::NODATA;

                if (record.GetIndexOnly() || !buffer) {
                    NMatrix::TVectorType local = ingr.LocalParts(Shared->GroupInfo->Type);
                    response->AddResult(status, record.GetIndexOnly() ? fullId : id, cookie, pingr, &local);
                } else {
                    auto data = TRcBuf::Copy(buffer->data() + offset, size);
                    response->AddResult(status, id, offset, TRope(std::move(data)), cookie, pingr);
                }
            }
        };

        // process ranged queries
        if (record.HasRangeQuery()) {
            const auto& rangeQuery = record.GetRangeQuery();
            ui64 rangeCookie = rangeQuery.GetCookie();

            TLogoBlobID from{LogoBlobIDFromLogoBlobID(rangeQuery.GetFrom())};
            TLogoBlobID to{LogoBlobIDFromLogoBlobID(rangeQuery.GetTo())};

            const bool forward = from <= to;
            auto it = forward ? LogoBlobs.lower_bound(from) : LogoBlobs.upper_bound(from);
            ui32 numResults = 0;
            for (;;) {
                // move one step back when going down
                if (forward) {
                    if (it == LogoBlobs.end()) {
                        break;
                    }
                } else {
                    if (it == LogoBlobs.begin()) {
                        break;
                    }
                    --it;
                    if (it->first < to) {
                        break;
                    }
                }

                // check for results count
                if (rangeQuery.HasMaxResults() && numResults == rangeQuery.GetMaxResults()) {
                    // FIXME: result?
                    break;
                }

                addResult(it->first, it->second, 0, 0, record.HasCookie() ? &rangeCookie : nullptr);
                ++numResults;

                // move one step forward
                if (forward) {
                    if (++it == LogoBlobs.end()) {
                        break;
                    }
                    if (it->first > to) {
                        break;
                    }
                }
            }
        }

        // process extreme queries
        for (const auto& query : record.GetExtremeQueries()) {
            const TLogoBlobID id{LogoBlobIDFromLogoBlobID(query.GetId())};
            ui64 queryCookie = query.GetCookie();
            bool found = false;
            prevGenericId.Clear();
            for (auto it = LogoBlobs.lower_bound(id); it != LogoBlobs.end() && it->first.FullID() == id.FullID(); ++it) {
                if (id.PartId() && id.PartId() != it->first.PartId()) {
                    break;
                }
                addResult(it->first, it->second, query.GetShift(), query.GetSize(), query.HasCookie() ? &queryCookie : nullptr);
                found = true;
            }
            if (!found) {
                response->AddResult(NKikimrProto::NODATA, id, query.HasCookie() ? &queryCookie : nullptr);
            }
        }

        // send final response
        LOG_DEBUG(ctx, NActorsServices::TEST, "TEvVGet# %s -> %s", ev->Get()->ToString().data(), response->ToString().data());
        FinalizeAndSend(std::move(response), ctx, ev->Sender);
    }

    void Handle(TEvBlobStorage::TEvVBlock::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId);

        ui32& gen = Blocks[record.GetTabletId()];
        gen = Max(gen, record.GetGeneration());
        TEvBlobStorage::TEvVBlockResult::TTabletActGen actual(record.GetTabletId(), record.GetGeneration());
        auto response = std::make_unique<TEvBlobStorage::TEvVBlockResult>(NKikimrProto::OK, &actual,
                VDiskIDFromVDiskID(record.GetVDiskID()), TAppData::TimeProvider->Now(),
                (ui32)ev->Get()->GetCachedByteSize(), &record, nullptr, nullptr, nullptr, 0);
        FinalizeAndSend(std::move(response), ctx, ev->Sender);
    }

    void Handle(TEvBlobStorage::TEvVGetBlock::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId);

        std::unique_ptr<TEvBlobStorage::TEvVGetBlockResult> response;
        auto it = Blocks.find(record.GetTabletId());
        if (it != Blocks.end()) {
            response.reset(new TEvBlobStorage::TEvVGetBlockResult(NKikimrProto::OK, it->first, it->second,
                    VDiskIDFromVDiskID(record.GetVDiskID()), TAppData::TimeProvider->Now(),
                    ev->Get()->GetCachedByteSize(), &record, nullptr, nullptr, nullptr));
        } else {
            response.reset(new TEvBlobStorage::TEvVGetBlockResult(NKikimrProto::NODATA, record.GetTabletId(),
                    VDiskIDFromVDiskID(record.GetVDiskID()), TAppData::TimeProvider->Now(),
                    ev->Get()->GetCachedByteSize(), &record, nullptr, nullptr, nullptr));
        }

        FinalizeAndSend(std::move(response), ctx, ev->Sender);
    }

    void Handle(TEvBlobStorage::TEvVCollectGarbage::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId);

        if (IsBlocked(record.GetTabletId(), record.GetRecordGeneration())) {
            auto response = std::make_unique<TEvBlobStorage::TEvVCollectGarbageResult>(NKikimrProto::BLOCKED,
                    record.GetTabletId(), record.GetRecordGeneration(), record.GetChannel(),
                    VDiskIDFromVDiskID(record.GetVDiskID()), TAppData::TimeProvider->Now(),
                    (ui32)ev->Get()->GetCachedByteSize(), &record, nullptr, nullptr, nullptr, 0);
            FinalizeAndSend(std::move(response), ctx, ev->Sender);
            return;
        }

        auto key = std::make_tuple(record.GetTabletId(), record.GetChannel(), record.GetRecordGeneration(),
                record.GetPerGenerationCounter(), record.GetHard());
        auto value = std::make_tuple(record.GetCollectGeneration(), record.GetCollectStep());

        auto it = Barriers.find(key);
        if (it != Barriers.end()) {
            Y_ABORT_UNLESS(it->second == value);
        } else {
            Barriers[key] = value;
        }

        // FIXME: collect garbage

        auto response = std::make_unique<TEvBlobStorage::TEvVCollectGarbageResult>(NKikimrProto::OK, record.GetTabletId(),
                record.GetRecordGeneration(), record.GetChannel(), VDiskIDFromVDiskID(record.GetVDiskID()),
                TAppData::TimeProvider->Now(), (ui32)ev->Get()->GetCachedByteSize(), &record, nullptr, nullptr, nullptr, 0);
        FinalizeAndSend(std::move(response), ctx, ev->Sender);
    }

    void Handle(TEvBlobStorage::TEvVGetBarrier::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId);

        const auto& from = record.GetFrom();
        auto first = std::make_tuple(from.GetTabletId(), from.GetChannel(), from.GetRecordGeneration(),
                from.GetPerGenerationCounter(), from.GetHard());

        const auto& to = record.GetTo();
        auto last = std::make_tuple(to.GetTabletId(), to.GetChannel(), to.GetRecordGeneration(),
                to.GetPerGenerationCounter(), to.GetHard());

        auto response = std::make_unique<TEvBlobStorage::TEvVGetBarrierResult>(NKikimrProto::OK,
            VDiskIDFromVDiskID(record.GetVDiskID()), TAppData::TimeProvider->Now(), (ui32)ev->Get()->GetCachedByteSize(),
            &record, nullptr, nullptr, nullptr);

        auto it = Barriers.lower_bound(first);
        while (it != Barriers.end() && it->first <= last) {
            const auto& key = it->first;
            const auto& value = it->second;
            response->AddResult({std::get<0>(key), std::get<1>(key), std::get<2>(key), std::get<3>(key), std::get<4>(key)},
                    {std::get<0>(value), std::get<1>(value), {}}, false);
            ++it;
        }

        FinalizeAndSend(std::move(response), ctx, ev->Sender);
    }

    void Handle(TEvBlobStorage::TEvVCheckReadiness::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Sender, new TEvBlobStorage::TEvVCheckReadinessResult(NKikimrProto::OK), 0, ev->Cookie);
    }

    void Handle(TEvVMockCtlRequest::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        switch (msg->Action) {
            case TEvVMockCtlRequest::EAction::DeleteBlobs: {
                auto first = LogoBlobs.lower_bound(msg->First);
                auto last = LogoBlobs.upper_bound(msg->Last);
                for (auto it = first; it != last; ++it) {
                    it->second.reset();
                }
                break;
            }

            // erase blobs as they have never been written to this disk
            case TEvVMockCtlRequest::EAction::WipeOutBlobs: {
                auto first = LogoBlobs.lower_bound(msg->First);
                auto last = LogoBlobs.upper_bound(msg->Last);
                with_lock (Shared->Mutex) {
                    for (auto it = first; it != last; ++it) {
                        const TLogoBlobID& id = it->first;
                        TLogoBlobID fullId(id.FullID());
                        TIngress ingress(*TIngress::CreateIngressWithLocal(&Shared->GroupInfo->GetTopology(), VDiskId, id));
                        TIngress& sharedIngress = Shared->BlobToIngressMap[fullId];
                        ui64 newSharedIngress = sharedIngress.Raw() & ~ingress.Raw();
                        if (!newSharedIngress) {
                            Shared->BlobToIngressMap.erase(fullId);
                        } else {
                            sharedIngress = TIngress(newSharedIngress);
                        }
                    }
                }
                LogoBlobs.erase(first, last);
                break;
            }

            case TEvVMockCtlRequest::EAction::SetErrorMode:
                ErrorMode = msg->ErrorMode;
                LostMode = msg->LostMode;
                break;
        }
        ctx.Send(ev->Sender, new TEvVMockCtlResponse);
    }

    STRICT_STFUNC(StateFunc,
            HFunc(TEvBlobStorage::TEvVPut, Handle);
            HFunc(TEvBlobStorage::TEvVMultiPut, Handle);
            HFunc(TEvBlobStorage::TEvVGet, Handle);
            HFunc(TEvBlobStorage::TEvVBlock, Handle);
            HFunc(TEvBlobStorage::TEvVGetBlock, Handle);
            HFunc(TEvBlobStorage::TEvVCollectGarbage, Handle);
            HFunc(TEvBlobStorage::TEvVGetBarrier, Handle);
            HFunc(TEvBlobStorage::TEvVCheckReadiness, Handle);
            HFunc(TEvVMockCtlRequest, Handle);
    )
};

IActor *CreateVDiskMockActor(const TVDiskID& vdiskId,
        TIntrusivePtr<TVDiskMockSharedState> shared,
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> top) {
    return new TVDiskMockActor(vdiskId, std::move(shared), std::move(top));
}

} // NKikimr
