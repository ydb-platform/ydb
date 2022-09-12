#include "vdisk_events.h"
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>

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
        Y_VERIFY(KIKIMR_USE_PROTOBUF_WITH_PAYLOAD);
        AddPayload(std::move(buffer));
    }

    void TEvBlobStorage::TEvVMultiPut::StorePayload(NKikimrBlobStorage::TVMultiPutItem &item, const TString& buffer) {
        if (KIKIMR_USE_PROTOBUF_WITH_PAYLOAD) {
            AddPayload(TRope(buffer));
            Y_VERIFY_DEBUG(Record.ItemsSize() == GetPayloadCount());
        } else {
            item.SetBuffer(buffer);
        }
    }

    TRope TEvBlobStorage::TEvVMultiPut::GetItemBuffer(ui64 itemIdx) const {
        auto &item = Record.GetItems(itemIdx);
        if (item.HasBuffer()) {
            return TRope(item.GetBuffer());
        } else {
            return GetPayload(itemIdx);
        }
    }
} // NKikimr
