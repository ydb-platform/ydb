#include "skeleton_loggedrec.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/common/circlebufresize.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // ILoggedRec -- interface for a record that has been written to the
    // recovery log.
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    ILoggedRec::ILoggedRec(TLsnSeg seg, bool confirmSyncLogAlso)
        : Seg(seg)
        , ConfirmSyncLogAlso(confirmSyncLogAlso)
    {}

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVPut -- incapsulates TEvVPut replay action (for small blobs)
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecVPut::TLoggedRecVPut(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            const TLogoBlobID &id,
            const TIngress &ingress,
            TRope &&buffer,
            std::unique_ptr<TEvBlobStorage::TEvVPutResult> result,
            const TActorId &recipient,
            ui64 recipientCookie,
            NWilson::TTraceId traceId)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , Id(id)
        , Ingress(ingress)
        , Buffer(std::move(buffer))
        , Result(std::move(result))
        , Recipient(recipient)
        , RecipientCookie(recipientCookie)
        , Span(TWilson::VDiskInternals, std::move(traceId), "VDisk.Log.Put")
    {
        if (Span) {
            Span.Attribute("blob_id", id.ToString());
        }
    }

    void TLoggedRecVPut::Replay(THull &hull, const TActorContext &ctx) {
        TLogoBlobID genId(Id, 0);
        hull.AddLogoBlob(ctx, genId, Id.PartId(), Ingress, Buffer, Seg.Point());

        LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_PUT, hull.GetHullCtx()->VCtx->VDiskLogPrefix << "TEvVPut: reply;"
                << " id# " << Id
                << " msg# " << Result->ToString()
                << " Marker# BSVSLR01");

        Span.EndOk();
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        SendVDiskResponse(ctx, Recipient, Result.release(), RecipientCookie, vCtx);
    }

    NWilson::TTraceId TLoggedRecVPut::GetTraceId() const {
        return Span.GetTraceId();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVPut -- incapsulates TEvVPut replay action (for small blobs)
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecVMultiPutItem::TLoggedRecVMultiPutItem(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            const TLogoBlobID &id,
            const TIngress &ingress,
            TRope &&buffer,
            std::unique_ptr<TEvVMultiPutItemResult> result,
            const TActorId &recipient,
            ui64 recipientCookie,
            NWilson::TTraceId traceId)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , Id(id)
        , Ingress(ingress)
        , Buffer(std::move(buffer))
        , Result(std::move(result))
        , Recipient(recipient)
        , RecipientCookie(recipientCookie)
        , Span(TWilson::VDiskInternals, std::move(traceId), "VDisk.Log.MultiPutItem")
    {
        if (Span) {
            Span.Attribute("blob_id", Id.ToString());
        }
    }

    void TLoggedRecVMultiPutItem::Replay(THull &hull, const TActorContext &ctx) {
        TLogoBlobID genId(Id, 0);
        hull.AddLogoBlob(ctx, genId, Id.PartId(), Ingress, Buffer, Seg.Point());

        LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_PUT, hull.GetHullCtx()->VCtx->VDiskLogPrefix
                << "TEvVMultiPut: item reply;"
                << " id# " << Id
                << " msg# " << Result->ToString()
                << " Marker# BSVSLR02");

        Span.EndOk();
        ctx.Send(Recipient, Result.release(), RecipientCookie);
    }

    NWilson::TTraceId TLoggedRecVMultiPutItem::GetTraceId() const {
        return Span.GetTraceId();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVPut -- incapsulates TEvVPut replay action (for huge blobs)
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecVPutHuge::TLoggedRecVPutHuge(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            const TActorId &hugeKeeperId,
            TEvHullLogHugeBlob::TPtr ev)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , HugeKeeperId(hugeKeeperId)
        , Ev(ev)
        , Span(TWilson::VDiskInternals, std::move(Ev->TraceId), "VDisk.Log.PutHuge")
    {
        if (Span) {
            Span.Attribute("blob_id", Ev->Get()->LogoBlobID.ToString());
        }
    }

    void TLoggedRecVPutHuge::Replay(THull &hull, const TActorContext &ctx) {
        TEvHullLogHugeBlob *msg = Ev->Get();

        TLogoBlobID genId(msg->LogoBlobID, 0);
        hull.AddHugeLogoBlob(ctx, genId, msg->Ingress, msg->HugeBlob, Seg.Point());
        // notify huge keeper
        if (msg->HugeBlob != TDiskPart()) {
            ctx.Send(HugeKeeperId, new TEvHullHugeBlobLogged(msg->WriteId, msg->HugeBlob, Seg.Point(), true));
        }

        LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_PUT, hull.GetHullCtx()->VCtx->VDiskLogPrefix
                << "TEvVPut: realtime# false result# " << msg->Result->ToString()
                << " Marker# BSVSLR03");
        Span.EndOk();
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        SendVDiskResponse(ctx, msg->OrigClient, msg->Result.release(), msg->OrigCookie, vCtx);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVBlock -- incapsulates TEvVBlock replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecVBlock::TLoggedRecVBlock(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            ui64 tabletId,
            ui32 gen,
            ui64 issuerGuid,
            std::unique_ptr<TEvBlobStorage::TEvVBlockResult> result,
            const TActorId &recipient,
            ui64 recipientCookie)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , TabletId(tabletId)
        , Gen(gen)
        , IssuerGuid(issuerGuid)
        , Result(std::move(result))
        , Recipient(recipient)
        , RecipientCookie(recipientCookie)
    {}

    void TLoggedRecVBlock::Replay(THull &hull, const TActorContext &ctx) {
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        auto replySender = [&ctx, &vCtx] (const TActorId &id, ui64 cookie, NWilson::TTraceId, IEventBase *msg) {
            SendVDiskResponse(ctx, id, msg, cookie, vCtx);
        };

        hull.AddBlockCmd(ctx, TabletId, Gen, IssuerGuid, Seg.Point(), replySender);

        LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_BLOCK, hull.GetHullCtx()->VCtx->VDiskLogPrefix
                << "TEvVBlock: result# " << Result->ToString()
                << " Marker# BSVSLR04");
        SendVDiskResponse(ctx, Recipient, Result.release(), RecipientCookie, vCtx);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVCollectGarbage -- incapsulates TEvVCollectGarbage replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecVCollectGarbage::TLoggedRecVCollectGarbage(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            TBarrierIngress ingress,
            std::unique_ptr<TEvBlobStorage::TEvVCollectGarbageResult> result,
            TEvBlobStorage::TEvVCollectGarbage::TPtr origEv)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , Ingress(ingress)
        , Result(std::move(result))
        , OrigEv(origEv)
        , Span(TWilson::VDiskInternals, std::move(OrigEv->TraceId), "VDisk.LoggedRecVCollectGarbage")
    {}

    void TLoggedRecVCollectGarbage::Replay(THull &hull, const TActorContext &ctx) {
        NKikimrBlobStorage::TEvVCollectGarbage &record = OrigEv->Get()->Record;
        hull.AddGCCmd(ctx, record, Ingress, Seg);

        LOG_DEBUG_S(ctx, NKikimrServices::BS_VDISK_GC, hull.GetHullCtx()->VCtx->VDiskLogPrefix
                << "TEvVCollectGarbage: result# " << Result->ToString()
                << " Marker# BSVSLR05");
        Span.EndOk();
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        SendVDiskResponse(ctx, OrigEv->Sender, Result.release(), OrigEv->Cookie, vCtx);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecLocalSyncData -- incapsulates TEvLocalSyncData replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecLocalSyncData::TLoggedRecLocalSyncData(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            std::unique_ptr<TEvLocalSyncDataResult> result,
            TEvLocalSyncData::TPtr origEv)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , Result(std::move(result))
        , OrigEv(origEv)
        , Span(TWilson::VDiskInternals, std::move(OrigEv->TraceId), "VDisk.LoggedRecLocalSyncData")
    {}

    void TLoggedRecLocalSyncData::Replay(THull &hull, const TActorContext &ctx) {
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        auto replySender = [&ctx, &vCtx] (const TActorId &id, ui64 cookie, NWilson::TTraceId, IEventBase *msg) {
            SendVDiskResponse(ctx, id, msg, cookie, vCtx);
        };

#ifdef UNPACK_LOCALSYNCDATA
        hull.AddSyncDataCmd(ctx, std::move(OrigEv->Get()->Extracted), Seg, replySender);
#else
        hull.AddSyncDataCmd(ctx, OrigEv->Get()->Data, Seg, replySender);
#endif
        Span.EndOk();        
        SendVDiskResponse(ctx, OrigEv->Sender, Result.release(), OrigEv->Cookie, vCtx);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecAnubisOsirisPut -- incapsulates TEvAnubisOsirisPut replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecAnubisOsirisPut::TLoggedRecAnubisOsirisPut(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            const THullDbInsert &insert,
            std::unique_ptr<TEvAnubisOsirisPutResult> result,
            TEvAnubisOsirisPut::TPtr origEv)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , Insert(insert)
        , Result(std::move(result))
        , OrigEv(origEv)
    {}

    void TLoggedRecAnubisOsirisPut::Replay(THull &hull, const TActorContext &ctx) {
        hull.AddLogoBlob(ctx, Insert.Id, Insert.Ingress, Seg);
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        SendVDiskResponse(ctx, OrigEv->Sender, Result.release(), OrigEv->Cookie, vCtx);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecPhantoms -- incapsulates TEvDetectedPhantomBlob replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecPhantoms::TLoggedRecPhantoms(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            TEvDetectedPhantomBlob::TPtr origEv)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , OrigEv(origEv)
    {}

    void TLoggedRecPhantoms::Replay(THull &hull, const TActorContext &ctx) {
        TEvDetectedPhantomBlob *msg = OrigEv->Get();
        hull.CollectPhantoms(ctx, msg->Phantoms, Seg);
        TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvDetectedPhantomBlobCommitted, 0, OrigEv->Sender, {},
            nullptr, OrigEv->Cookie));
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecDelLogoBlobDataSyncLog -- incapsulates TEvDelLogoBlobDataSyncLog replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecDelLogoBlobDataSyncLog::TLoggedRecDelLogoBlobDataSyncLog(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            const THullDbInsert &insert,
            std::unique_ptr<TEvDelLogoBlobDataSyncLogResult> result,
            const TActorId &recipient,
            ui64 recipientCookie)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , Insert(insert)
        , Result(std::move(result))
        , Recipient(recipient)
        , RecipientCookie(recipientCookie)
    {}

    void TLoggedRecDelLogoBlobDataSyncLog::Replay(THull &hull, const TActorContext &ctx) {
        hull.AddLogoBlob(ctx, Insert.Id, Insert.Ingress, Seg);
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        SendVDiskResponse(ctx, Recipient, Result.release(), RecipientCookie, vCtx);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecAddBulkSst -- incapsulates TEvAddBulkSst replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    TLoggedRecAddBulkSst::TLoggedRecAddBulkSst(
            TLsnSeg seg,
            bool confirmSyncLogAlso,
            TEvAddBulkSst::TPtr ev)
        : ILoggedRec(seg, confirmSyncLogAlso)
        , OrigEv(ev)
    {}

    void TLoggedRecAddBulkSst::Replay(THull &hull, const TActorContext &ctx) {
        hull.AddBulkSst(ctx, OrigEv->Get()->Essence, Seg);
        const auto& vCtx = hull.GetHullCtx()->VCtx;
        SendVDiskResponse(ctx, OrigEv->Sender, new TEvAddBulkSstResult, OrigEv->Cookie, vCtx);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecsVault -- cookie manager for LoggedRecs that are in flight
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecsVault::TImpl {
    public:
        TImpl()
            : Queue(64)
        {}

        ~TImpl() {
            while (!Queue.Empty()) {
                delete Queue.Top().second;
                Queue.Top().second = nullptr;
                Queue.Pop();
            }
        }

        intptr_t Put(ILoggedRec *rec) {
            Y_DEBUG_ABORT_UNLESS(rec);
            intptr_t id = ++Counter;
            Queue.Push(TItem(id, rec));
            return id;
        }

        ILoggedRec *Extract(intptr_t id) {
            Y_ABORT_UNLESS((id == Extracted + 1) && !Queue.Empty(), "id# %" PRIu64 " Extracted# %" PRIu64, id, Extracted);
            Extracted = id;

            TItem item = Queue.Top();
            Queue.Pop();
            Y_ABORT_UNLESS(item.first == id);
            return item.second;
        }

        std::optional<ui64> GetLastLsnInFlight() const {
            return Queue.Empty() ? std::optional<ui64>() : std::optional<ui64>(Queue.Back().second->Seg.Last);
        }

    private:
        using TItem = std::pair<intptr_t, ILoggedRec*>;
        using TQueue = TAllocFreeQueue<TItem>;
        TQueue Queue;
        intptr_t Counter = 0;
        intptr_t Extracted = 0;
    };

    TLoggedRecsVault::TLoggedRecsVault()
        : Impl(new TImpl)
    {}

    TLoggedRecsVault::~TLoggedRecsVault() = default;

    intptr_t TLoggedRecsVault::Put(ILoggedRec *rec) {
        return Impl->Put(rec);
    }

    ILoggedRec *TLoggedRecsVault::Extract(intptr_t id) {
        return Impl->Extract(id);
    }

    std::optional<ui64> TLoggedRecsVault::GetLastLsnInFlight() const {
        return Impl->GetLastLsnInFlight();
    }

} // NKikimr

