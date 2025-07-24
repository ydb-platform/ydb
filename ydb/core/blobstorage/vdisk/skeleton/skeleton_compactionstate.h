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
            bool Force = true;
            TActorId ClientId;
            ui64 ClientCookie = 0;
            std::unique_ptr<TEvCompactVDiskResult> Reply;

            bool AllDone() const { return !(CompactLogoBlobs || CompactBlocks || CompactBarriers); }
        };

        TVDiskCompactionState(TActorId logoBlobsActorId, TActorId blocksActorId, TActorId barriersActorId);
        // setup input compaction request
        void Setup(const TActorContext &ctx, std::optional<ui64> lsn, TCompactionReq cState);
        // when hull db reports compaction finish we change state by calling this function
        void Compacted(const TActorContext &ctx, i64 reqId, EHullDbType dbType, const TIntrusivePtr<TVDiskContext>& vCtx);
        // when data is flushed to recovery log run compaction
        void Logged(const TActorContext &ctx, ui64 lsn) {
            if (Triggered && lsn >= LsnToCommit) {
                Triggered = false;
                for (auto &req : WaitQueue) {
                    SendLocalCompactCmd(ctx, std::move(req));
                }
                WaitQueue.clear();
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
        std::deque<TCompactionReq> WaitQueue;
        // true if we are waiting for all prev records to commit
        bool Triggered = false;
        // wait for lsn to commit
        ui64 LsnToCommit = 0;
        ui64 RequestIdCounter = 0;

        void SendLocalCompactCmd(const TActorContext &ctx, TCompactionReq cState);
    };

} // NKikimr
