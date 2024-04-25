#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>
#include <ydb/core/base/blobstorage.h>


namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvLocalSyncData
    // Apply data to hull.
    ////////////////////////////////////////////////////////////////////////////
    struct TEvLocalSyncData :
        public TEventLocal<TEvLocalSyncData, TEvBlobStorage::EvLocalSyncData>
    {
        // NOTES on Extracted field:
        // The Data field of TEvLocalSyncDatafield contains sync data that we got from another VDisk.
        // TExtracted has this data prepared for being applied to the Hull database. Preparation
        // takes time and CPU, so it's better to this async

        TVDiskID VDiskID;           // data obtained from this vdisk
        TSyncState SyncState;       // current sync state
        TString Data;               // data from other node to apply
        TFreshBatch Extracted;      // upacked Data that is ready to be applied to Hull

        TEvLocalSyncData(const TVDiskID &vdisk, const TSyncState &syncState, const TString &data);
        TEvLocalSyncData() = default;
        TString Serialize() const;
        void Serialize(IOutputStream &s) const;
        bool Deserialize(IInputStream &s);
        // prepare data from the field Data for inserting into Hull database
        void UnpackData(const TIntrusivePtr<TVDiskContext> &vctx);
        size_t ByteSize() const {
            return sizeof(TVDiskID) + sizeof(TSyncState) + sizeof(ui32) + Data.size();
        }

    protected:
        // for every key leave only one merged memRec
        static void Squeeze(TFreshAppendixLogoBlobs::TVec &vec);
        static void Squeeze(TFreshAppendixBlocks::TVec &vec);
        static void Squeeze(TFreshAppendixBarriers::TVec &vec);
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvLocalSyncDataResult
    // Hull replies with this message.
    ////////////////////////////////////////////////////////////////////////////
    struct TEvLocalSyncDataResult :
        public TEventLocal<TEvLocalSyncDataResult, TEvBlobStorage::EvLocalSyncDataResult>,
        public TEvVResultBase
    {
        NKikimrProto::EReplyStatus Status;

        TEvLocalSyncDataResult(NKikimrProto::EReplyStatus status, const TInstant &now,
                               ::NMonitoring::TDynamicCounters::TCounterPtr counterPtr,
                               NVDiskMon::TLtcHistoPtr histoPtr)
            : TEvVResultBase(now, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, counterPtr, histoPtr)
            , Status(status)
        {}
    };

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // CreateLocalSyncDataExtractor
    ///////////////////////////////////////////////////////////////////////////////////////////////
    IActor *CreateLocalSyncDataExtractor(const TIntrusivePtr<TVDiskContext> &vctx, const TActorId &skeletonId,
        const TActorId &parentId, std::unique_ptr<TEvLocalSyncData> ev);

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // CreateLocalSyncDataCutter
    ///////////////////////////////////////////////////////////////////////////////////////////////
    IActor* CreateLocalSyncDataCutter(const TIntrusivePtr<TVDiskConfig>& vconfig, const TIntrusivePtr<TVDiskContext>& vctx,
        const TActorId& skeletonId, const TActorId& parentId, std::unique_ptr<TEvLocalSyncData> ev);

} // NKikimr
