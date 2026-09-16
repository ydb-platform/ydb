#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_anubis_osiris.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_localwriter.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>

namespace NKikimr {

    class TVDiskContext;
    class THull;

    ////////////////////////////////////////////////////////////////////////////
    // TOutOfSpaceState -- logic of whether we accept writes or not
    ////////////////////////////////////////////////////////////////////////////
    class TOutOfSpaceLogic {
    public:
        TOutOfSpaceLogic(TIntrusivePtr<TVDiskContext> vctx, std::shared_ptr<THull> hull);
        ~TOutOfSpaceLogic();

        // TEvVPut / TEvVMultiPut: local color is the one this disk would be in after
        // compacting Fresh (this blob included); neighbors contribute only their
        // current color. Blocks, GC and sync are judged by the color right now.
        bool AllowVPutLikeWrite(const TActorContext& ctx, bool ignoreBlock, bool isZeroEntry, ui32 size,
            NKikimrBlobStorage::TDataKind::E dataKind, ui64 freshChunks) const;
        bool Allow(const TActorContext &ctx, TEvBlobStorage::TEvVPut::TPtr &ev, ui64 freshChunks) const;
        bool Allow(const TActorContext &ctx, TEvLocalSyncData::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvAnubisOsirisPut::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvRecoveredHugeBlob::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvDetectedPhantomBlob::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvBlobStorage::TEvVBlock::TPtr &ev, bool hasExistingEntry) const;
        bool Allow(const TActorContext &ctx, TEvBlobStorage::TEvVCollectGarbage::TPtr &ev) const;

        // output details about allows/rejects
        void RenderHtml(IOutputStream &str) const;

    private:
        TIntrusivePtr<TVDiskContext> VCtx;
        std::shared_ptr<THull> Hull;
        class TStat;
        mutable std::unique_ptr<TStat> Stat;

        bool DefaultAllow(ESpaceColor color) const;

        template <typename TEvPtr>
        friend bool AllowPut(const TOutOfSpaceLogic &logic, ESpaceColor color, TEvPtr &ev);

        static bool AllowByLocalColor(ESpaceColor color, bool system, bool unavoidable);
        static bool AllowByGlobalColor(ESpaceColor color, bool system, bool unavoidable);

        ESpaceColor GetSpaceColor() const;
    };

} // NKikimr
