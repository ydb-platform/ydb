#include "vdisk_events.h"
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>

namespace NKikimr {

    TEvBlobStorage::TEvVPutResult::TEvVPutResult() = default;

    TEvBlobStorage::TEvVPutResult::TEvVPutResult(const NKikimrProto::EReplyStatus status,
            const TLogoBlobID &logoBlobId, const TVDiskID &vdisk, const ui64 *cookie, TOutOfSpaceStatus oosStatus,
            const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVPut *record,
            const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
            const NVDiskMon::TLtcHistoPtr &histoPtr, const ui64 bufferSizeBytes,
            ui64 incarnationGuid, const TString& errorReason)
        : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                recByteSize, record, skeletonFrontIDPtr)
    {
        IncrementSize(bufferSizeBytes);
        Record.SetStatus(status);
        LogoBlobIDFromLogoBlobID(logoBlobId, Record.MutableBlobID());
        VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        if (cookie) {
            Record.SetCookie(*cookie);
        }
        Record.SetStatusFlags(oosStatus.Flags);
        Record.SetApproximateFreeSpaceShare(oosStatus.ApproximateFreeSpaceShare);
        if (record && record->HasTimestamps()) {
            Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
        }
        if (status == NKikimrProto::OK) {
            Record.SetIncarnationGuid(incarnationGuid);
        }
        if (errorReason && status != NKikimrProto::OK) {
            Record.SetErrorReason(errorReason);
        }
    }

    void TEvBlobStorage::TEvVPut::StorePayload(TRope&& buffer) {
        AddPayload(std::move(buffer));
    }

    void TEvBlobStorage::TEvVMultiPut::StorePayload(const TRcBuf& buffer) {
        AddPayload(TRope(buffer));
        Y_DEBUG_ABORT_UNLESS(Record.ItemsSize() == GetPayloadCount());
    }

    TRope TEvBlobStorage::TEvVMultiPut::GetItemBuffer(ui64 itemIdx) const {
        auto &item = Record.GetItems(itemIdx);
        if (item.HasBuffer()) {
            return TRope(item.GetBuffer());
        } else {
            return GetPayload(itemIdx);
        }
    }


    TEvBlobStorage::TEvVGetBarrier::TEvVGetBarrier(const TVDiskID &vdisk, const TKeyBarrier &from, const TKeyBarrier &to, ui32 *maxResults,
            bool showInternals)
    {
        VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        from.Serialize(*Record.MutableFrom());
        to.Serialize(*Record.MutableTo());
        if (maxResults)
            Record.SetMaxResults(*maxResults);
        if (showInternals)
            Record.SetShowInternals(true);
        Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::GetFastRead);
    }

    TString TEvBlobStorage::TEvVGetBarrier::ToString() const {
        TStringStream str;
        str << "{From# " << TKeyBarrier(Record.GetFrom()).ToString()
            << " To# " << TKeyBarrier(Record.GetTo()).ToString();
        if (Record.HasMsgQoS()) {
            TEvBlobStorage::TEvVPut::OutMsgQos(Record.GetMsgQoS(), str);
        }
        if (Record.HasShowInternals() && Record.GetShowInternals())
            str << " ShowInternals";
        str << "}";
        return str.Str();
    }

    void TEvBlobStorage::TEvVGetBarrierResult::AddResult(const TKeyBarrier &key, const TMemRecBarrier &memRec, bool showInternals) {
        auto k = Record.AddKeys();
        key.Serialize(*k);
        auto v = Record.AddValues();
        memRec.Serialize(*v, showInternals);
    }

    TEvBlobStorage::TEvVSyncFull::TEvVSyncFull(const TSyncState &syncState, const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk,
            ui64 cookie, NKikimrBlobStorage::ESyncFullStage stage, const TLogoBlobID &logoBlobFrom,
            ui64 blockTabletFrom, const TKeyBarrier &barrierFrom)
    {
        SyncStateFromSyncState(syncState, Record.MutableSyncState());
        VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
        VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
        Record.SetCookie(cookie);
        Record.SetStage(stage);
        LogoBlobIDFromLogoBlobID(logoBlobFrom, Record.MutableLogoBlobFrom());
        Record.SetBlockTabletFrom(blockTabletFrom);
        barrierFrom.Serialize(*Record.MutableBarrierFrom());
    }
} // NKikimr
