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

        // Check if we allow this write
        bool Allow(const TActorContext &ctx, TEvBlobStorage::TEvVPut::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvBlobStorage::TEvVMultiPut::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvBlobStorage::TEvVBlock::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvBlobStorage::TEvVCollectGarbage::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvLocalSyncData::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvAnubisOsirisPut::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvRecoveredHugeBlob::TPtr &ev) const;
        bool Allow(const TActorContext &ctx, TEvDetectedPhantomBlob::TPtr &ev) const;
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

        ESpaceColor GetSpaceColor() const;
    };

} // NKikimr
