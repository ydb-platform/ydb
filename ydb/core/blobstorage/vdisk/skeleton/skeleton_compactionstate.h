#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_dbtype.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>

namespace NKikimr {

    class TVDiskCompactionState {
    public:
        struct TCompactionReq {
            bool CompactLogoBlobs = false;
            bool CompactBlocks = false;
            bool CompactBarriers = false;
            TEvCompactVDisk::EMode Mode;
            TActorId ClientId;
            ui64 ClientCookie = 0;
            std::unique_ptr<IEventBase> Reply;
            THashSet<ui64> TablesToCompact;

            bool AllDone() const { return !(CompactLogoBlobs || CompactBlocks || CompactBarriers); }
        };

        TVDiskCompactionState(TActorId logoBlobsActorId, TActorId blocksActorId, TActorId barriersActorId);
        // setup input compaction request
        void Setup(const TActorContext &ctx, std::optional<ui64> lsn, TCompactionReq cState);
        // when hull db reports compaction finish we change state by calling this function
        void Compacted(const TActorContext &ctx, i64 reqId, EHullDbType dbType, const TIntrusivePtr<TVDiskContext>& vCtx);
        // when data is flushed to recovery log run compaction
        void Logged(const TActorContext &ctx, ui64 lsn) {
            for (; !WaitQueue.empty(); WaitQueue.pop_front()) {
                auto& [waitingLsn, req] = WaitQueue.front();
                if (waitingLsn <= lsn) {
                    SendLocalCompactCmd(ctx, std::move(req));
                } else {
                    break;
                }
            }
        }
        void RenderHtml(IOutputStream &str, TDbMon::ESubRequestID subId) const;

    private:
        // Actor ids for Hull Dbs
        TActorId LogoBlobsActorId;
        TActorId BlocksActorId;
        TActorId BarriersActorId;
        // requests sent to execution (id to compaction request mapping)
        std::unordered_map<ui64, TCompactionReq> Requests;
        // requests waiting until commit to recovery log
        std::deque<std::tuple<ui64, TCompactionReq>> WaitQueue;
        // wait for lsn to commit
        ui64 RequestIdCounter = 0;

        void SendLocalCompactCmd(const TActorContext &ctx, TCompactionReq cState);
    };

} // NKikimr
