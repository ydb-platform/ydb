#pragma once

#include "defs.h"
#include "blobstorage_hulldefs.h"
#include "hullds_settings.h"

namespace NKikimr {

    class TTestContexts {
    public:
        TTestContexts(ui32 chunkSize = 135249920, ui64 compWorthReadSize = (2ul << 20ul))
            : ChunkSize(chunkSize)
            , CompWorthReadSize(compWorthReadSize)
            , GroupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4)
            , VCfg(TVDiskConfig::TBaseInfo::SampleForTests())
            , VCtx(new TVDiskContext(TActorId(), GroupInfo.PickTopology(), new ::NMonitoring::TDynamicCounters(),
                        TVDiskID(), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN))
            , HullCtx(
                    new THullCtx(
                        VCtx,
                        MakeIntrusive<TVDiskConfig>(VCfg),
                        ChunkSize,
                        CompWorthReadSize,
                        true,
                        true,
                        true,
                        true,
                        1,      // HullSstSizeInChunksFresh
                        1,      // HullSstSizeInChunksLevel
                        2.0,
                        0.5,
                        TDuration::Minutes(5),
                        TDuration::Seconds(1),
                        true))// AddHeader
            , LevelIndexSettings(
                HullCtx,
                8u,                         // Level0MaxSstsAtOnce
                64u << 20u,                 // BufSize
                0,                          // CompThreshold
                TDuration::Minutes(10),     // HistoryWindow
                10u,                        // HistoryBuckets
                false,
                false)
        {}

        inline TVDiskContextPtr GetVCtx() {
            return VCtx;
        }

        inline THullCtxPtr GetHullCtx() {
            return HullCtx;
        }

        const TLevelIndexSettings &GetLevelIndexSettings() const {
            return LevelIndexSettings;
        }

    private:
        const ui32 ChunkSize;
        const ui64 CompWorthReadSize;
        TBlobStorageGroupInfo GroupInfo;
        TVDiskConfig VCfg;
        TVDiskContextPtr VCtx;
        THullCtxPtr HullCtx;
        TLevelIndexSettings LevelIndexSettings;
    };

} // NKikimr

