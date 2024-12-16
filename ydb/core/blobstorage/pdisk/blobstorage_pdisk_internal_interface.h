#pragma once
#include "defs.h"
#include "blobstorage_pdisk_defs.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_blockdevice.h>

#include <ydb/library/pdisk_io/buffers.h>
#include <ydb/library/actors/core/mon.h>


namespace NKikimr {
namespace NPDisk {
////////////////////////////////////////////////////////////////////////////
// Whiteboard report
//
////////////////////////////////////////////////////////////////////////////

struct TEvWhiteboardReportResult :
                public TEventLocal<TEvWhiteboardReportResult, TEvBlobStorage::EvWhiteboardReportResult> {
    THolder<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate> PDiskState;
    TVector<std::tuple<TActorId, NKikimrWhiteboard::TVDiskStateInfo>> VDiskStateVect;
    THolder<TEvBlobStorage::TEvControllerUpdateDiskStatus> DiskMetrics;

    ~TEvWhiteboardReportResult();

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvWhiteboardReportResult &record);
};

////////////////////////////////////////////////////////////////////////////
// Http result
//
////////////////////////////////////////////////////////////////////////////

struct TEvHttpInfoResult : public TEventLocal<TEvHttpInfoResult, TEvBlobStorage::EvHttpInfoResult> {
    TAutoPtr<NMon::TEvHttpInfoRes> HttpInfoRes;
    const TActorId EndCustomer;

    TEvHttpInfoResult(const TActorId &endCustomer)
        : HttpInfoRes(nullptr)
        , EndCustomer(endCustomer)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvHttpInfoResult &record) {
        TStringStream str;
        str << "{";
        record.HttpInfoRes->Output(str);
        str << "}";
        return str.Str();
    }
};

struct TEvPDiskFormattingFinished : public TEventLocal<TEvPDiskFormattingFinished, TEvBlobStorage::EvPDiskFormattingFinished> {
    bool IsSucceed;
    TString ErrorStr;

    TEvPDiskFormattingFinished(bool isSucceed, const TString &errorStr)
        : IsSucceed(isSucceed)
        , ErrorStr(errorStr)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvPDiskFormattingFinished &record) {
        Y_UNUSED(record);
        TStringStream str;
        str << "{";
        str << "EvFormattingFinished";
        str << "}";
        return str.Str();
    }
};

struct TEvPDiskMetadataLoaded : public TEventLocal<TEvPDiskMetadataLoaded, TEvBlobStorage::EvPDiskMetadataLoaded> {
    std::optional<TRcBuf> Metadata;

    TEvPDiskMetadataLoaded(std::optional<TRcBuf> metadata)
        : Metadata(std::move(metadata))
    {}
};

////////////////////////////////////////////////////////////////////////////
// This event is used for continuing log reading if it is not possible
// to read in one IO device operation
////////////////////////////////////////////////////////////////////////////

struct TEvReadLogContinue : public TEventLocal<TEvReadLogContinue, TEvBlobStorage::EvReadLogContinue> {
    void *Data;
    ui32 Size;
    ui64 Offset;
    std::weak_ptr<TCompletionAction> CompletionAction;
    TReqId ReqId;

    TEvReadLogContinue(void *data, ui32 size, ui64 offset, std::weak_ptr<TCompletionAction> completionAction, TReqId reqId)
        : Data(data)
        , Size(size)
        , Offset(offset)
        , CompletionAction(completionAction)
        , ReqId(reqId)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvReadLogContinue &record) {
        TStringStream str;
        str << "{";
        str << "EvReadLogContinue ";
        str << "Size# " << record.Size << " ";
        str << "Offset# " << record.Offset << " ";
        str << "ReqId# " << record.ReqId << " ";
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// This event is used for restoring broken sectors found while reading log
//
////////////////////////////////////////////////////////////////////////////

struct TEvLogSectorRestore : public TEventLocal<TEvLogSectorRestore, TEvBlobStorage::EvLogSectorRestore> {
    void *Data;
    ui32 Size;
    ui64 Offset;
    TCompletionAction *CompletionAction;

    TEvLogSectorRestore(void *data, ui32 size, ui64 offset, TCompletionAction *completionAction)
        : Data(data)
        , Size(size)
        , Offset(offset)
        , CompletionAction(completionAction)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvLogSectorRestore &record) {
        TStringStream str;
        str << "{";
        str << "EvLogSectorRestore ";
        str << "Size# " << record.Size << " ";
        str << "Offset# " << record.Offset << " ";
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// Event is used for returning result of processing previously read log
//
////////////////////////////////////////////////////////////////////////////

struct TEvLogInitResult : public TEventLocal<TEvLogInitResult, TEvBlobStorage::EvLogInitResult> {
    bool IsInitializedGood = false;
    TString ErrorStr;

    TEvLogInitResult(bool isInitializedGood, TString error)
        : IsInitializedGood(isInitializedGood)
        , ErrorStr(error)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvLogInitResult &record) {
        Y_UNUSED(record);
        TStringStream str;
        str << "{";
        str << "EvLogInitResult";
        str << "}";
        return str.Str();
    }
};

struct TEvReadFormatResult : public TEventLocal<TEvReadFormatResult, TEvBlobStorage::EvReadFormatResult> {
    TAlignedData FormatSectors;
    ui32 FormatSectorsSize;
    TEvReadFormatResult(ui32 formatSectorsSize, bool useHugePages)
        : FormatSectors(formatSectorsSize, useHugePages)
        , FormatSectorsSize(formatSectorsSize)
    {}
};

struct TEvDeviceError : public TEventLocal<TEvDeviceError, TEvBlobStorage::EvDeviceError> {
    TString Info;

    TEvDeviceError(const TString& info)
        : Info(info)
    {}
};

struct TEvFormatReencryptionFinish : public TEventLocal<TEvFormatReencryptionFinish, TEvBlobStorage::EvFormatReencryptionFinish> {
    bool Success;
    TString ErrorReason;

    TEvFormatReencryptionFinish(bool success, TString errorReason)
        : Success(success)
        , ErrorReason(errorReason)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvFormatReencryptionFinish& record) {
        TStringStream str;
        str << "{";
        str << "EvFormatReencryptionFinished ";
        str << " Success# " << record.Success;
        if (record.ErrorReason) {
            str << " ErrorReason# " << record.ErrorReason;
        }
        str << "}";
        return str.Str();
    }
};

} // NPDisk
} // NKikimr
