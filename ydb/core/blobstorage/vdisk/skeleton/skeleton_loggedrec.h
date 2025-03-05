#pragma once
#include "defs.h"
#include "skeleton_vmultiput_actor.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_localwriter.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubis_osiris.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr {

    class THull;

    // FIXME: make debug logging consistent via all handlers
    // FIXME: check how we update SyncLog on local recovery for AnubisOsirisPut and PhantomBlobs
    // FIXME: test on BLOCKED record that is blocked by block in-flight
    //
    //
    // Plan:
    // 1. Support TDelayedResponses in actual state (switch to UpdateInFlight/CommitInFight)
    //   a) UpdateInFlight -- Block command                                                                 DONE
    //   b) UpdateInFlight -- SyncLocalData command                                                         DONE
    //   c) CommitInFlight                                                                                  DONE
    // 2. Add support for TDelayedResponses for every messages that could be blocked
    //   a) LogoBlobs                                                                                       DONE
    //   b) Blocks                                                                                          DONE
    //   c) GC                                                                                              DONE
    // 3. Make recovery log replaying unconditional                                                         DONE

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // ILoggedRec -- interface for a record that has been written to the
    // recovery log.
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class ILoggedRec {
    public:
        ILoggedRec(TLsnSeg seg, bool confirmSyncLogAlso);
        virtual ~ILoggedRec() = default;
        // a method that replays changes that has been written to the recovery log
        virtual void Replay(THull &hull, const TActorContext &ctx) = 0;

        const TLsnSeg Seg;
        const bool ConfirmSyncLogAlso;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVPut -- incapsulates TEvVPut replay action (for small blobs)
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecVPut : public ILoggedRec {
    public:
        TLoggedRecVPut(TLsnSeg seg, bool confirmSyncLogAlso, const TLogoBlobID &id, const TIngress &ingress,
                TRope &&buffer, std::unique_ptr<TEvBlobStorage::TEvVPutResult> result, const TActorId &recipient,
                ui64 recipientCookie, NWilson::TTraceId traceId, NKikimrBlobStorage::EPutHandleClass handleClass);
        void Replay(THull &hull, const TActorContext &ctx) override;

        NWilson::TTraceId GetTraceId() const;

    private:
        TLogoBlobID Id;
        TIngress Ingress;
        TRope Buffer;
        std::unique_ptr<TEvBlobStorage::TEvVPutResult> Result;
        TActorId Recipient;
        ui64 RecipientCookie;
        NWilson::TSpan Span;
        NKikimrBlobStorage::EPutHandleClass HandleClass;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVMultiPutItem -- incapsulates TEvVMultiPut item replay action (for small blobs)
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecVMultiPutItem : public ILoggedRec {
    public:
        TLoggedRecVMultiPutItem(TLsnSeg seg, bool confirmSyncLogAlso, const TLogoBlobID &id, const TIngress &ingress,
                TRope &&buffer, std::unique_ptr<TEvVMultiPutItemResult> result, const TActorId &recipient,
                ui64 recipientCookie, NWilson::TTraceId traceId, NKikimrBlobStorage::EPutHandleClass);
        void Replay(THull &hull, const TActorContext &ctx) override;

        NWilson::TTraceId GetTraceId() const;

    private:
        TLogoBlobID Id;
        TIngress Ingress;
        TRope Buffer;
        std::unique_ptr<TEvVMultiPutItemResult> Result;
        TActorId Recipient;
        ui64 RecipientCookie;
        NWilson::TSpan Span;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVPut -- incapsulates TEvVPut replay action (for huge blobs)
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecVPutHuge : public ILoggedRec {
    public:
        TLoggedRecVPutHuge(TLsnSeg seg, bool confirmSyncLogAlso, const TActorId &hugeKeeperId,
                TEvHullLogHugeBlob::TPtr ev);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        const TActorId HugeKeeperId;
        TEvHullLogHugeBlob::TPtr Ev;
        NWilson::TSpan Span;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVBlock -- incapsulates TEvVBlock replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecVBlock : public ILoggedRec {
    public:
        TLoggedRecVBlock(TLsnSeg seg, bool confirmSyncLogAlso, ui64 tabletId, ui32 gen, ui64 issuerGuid,
                std::unique_ptr<TEvBlobStorage::TEvVBlockResult> result, const TActorId &recipient, ui64 recipientCookie);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        ui64 TabletId;
        ui32 Gen;
        ui64 IssuerGuid;
        std::unique_ptr<TEvBlobStorage::TEvVBlockResult> Result;
        TActorId Recipient;
        ui64 RecipientCookie;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecVCollectGarbage -- incapsulates TEvVCollectGarbage replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecVCollectGarbage : public ILoggedRec {
    public:
        TLoggedRecVCollectGarbage(TLsnSeg seg, bool confirmSyncLogAlso, TBarrierIngress ingress,
                std::unique_ptr<TEvBlobStorage::TEvVCollectGarbageResult> result,
                TEvBlobStorage::TEvVCollectGarbage::TPtr origEv);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        TBarrierIngress Ingress;
        std::unique_ptr<TEvBlobStorage::TEvVCollectGarbageResult> Result;
        TEvBlobStorage::TEvVCollectGarbage::TPtr OrigEv;
        NWilson::TSpan Span;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecLocalSyncData -- incapsulates TEvLocalSyncData replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecLocalSyncData : public ILoggedRec {
    public:
        TLoggedRecLocalSyncData(TLsnSeg seg, bool confirmSyncLogAlso, std::unique_ptr<TEvLocalSyncDataResult> result,
                TEvLocalSyncData::TPtr origEv);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        std::unique_ptr<TEvLocalSyncDataResult> Result;
        TEvLocalSyncData::TPtr OrigEv;
        NWilson::TSpan Span;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecAnubisOsirisPut -- incapsulates TEvAnubisOsirisPut replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecAnubisOsirisPut : public ILoggedRec {
    public:
        TLoggedRecAnubisOsirisPut(TLsnSeg seg, bool confirmSyncLogAlso,
                const THullDbInsert &insert, std::unique_ptr<TEvAnubisOsirisPutResult> result,
                TEvAnubisOsirisPut::TPtr origEv);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        THullDbInsert Insert;
        std::unique_ptr<TEvAnubisOsirisPutResult> Result;
        TEvAnubisOsirisPut::TPtr OrigEv;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecPhantoms -- incapsulates TEvDetectedPhantomBlob replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecPhantoms : public ILoggedRec {
    public:
        TLoggedRecPhantoms(TLsnSeg seg, bool confirmSyncLogAlso, TEvDetectedPhantomBlob::TPtr origEv);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        TEvDetectedPhantomBlob::TPtr OrigEv;
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecDelLogoBlobDataSyncLog -- incapsulates TEvDelLogoBlobDataSyncLog replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecDelLogoBlobDataSyncLog : public ILoggedRec {
    public:
        TLoggedRecDelLogoBlobDataSyncLog(TLsnSeg seg, bool confirmSyncLogAlso,
                const THullDbInsert &insert,
                std::unique_ptr<TEvDelLogoBlobDataSyncLogResult> result, const TActorId &recipient,
                ui64 recipientCookie);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        THullDbInsert Insert;
        std::unique_ptr<TEvDelLogoBlobDataSyncLogResult> Result;
        TActorId Recipient;
        ui64 RecipientCookie;
    };


    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecAddBulkSst -- incapsulates TEvAddBulkSst replay action
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecAddBulkSst : public ILoggedRec {
    public:
        TLoggedRecAddBulkSst(TLsnSeg seg, bool confirmSyncLogAlso, TEvAddBulkSst::TPtr ev);
        void Replay(THull &hull, const TActorContext &ctx) override;

    private:
        TEvAddBulkSst::TPtr OrigEv;
    };


    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // TLoggedRecsVault -- cookie manager for LoggedRecs that are in flight
    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    class TLoggedRecsVault {
    public:
        TLoggedRecsVault();
        ~TLoggedRecsVault();
        intptr_t Put(ILoggedRec *rec);
        ILoggedRec *Extract(intptr_t id);
        // returns last lsn of last record in flight if any
        std::optional<ui64> GetLastLsnInFlight() const;

    private:
        class TImpl;
        std::unique_ptr<TImpl> Impl;
    };

} // NKikimr

