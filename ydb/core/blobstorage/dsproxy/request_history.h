#pragma once
#include "defs.h"

#include <variant>

namespace NKikimr {
class THistory {
private:
    struct TBaseEntry {
        ui32 TimeUs;
        TBaseEntry(TInstant startTime)
            : TimeUs((TActivationContext::Now() - startTime).MicroSeconds())
        {}
    };

    struct TVRequestEntry : public TBaseEntry {
        ui8 PartIdx;
        ui32 QueryCount;
        ui8 OrderNumber;
        
        TVRequestEntry(TInstant startTime, ui8 partIdx, ui32 queryCount, ui8 orderNumber)
            : TBaseEntry(startTime)
            , PartIdx(partIdx)
            , QueryCount(queryCount)
            , OrderNumber(orderNumber)
        {}

        void Output(IOutputStream& str, const char* typeName, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            str << typeName         << "{"
                << " TimestampMs# " << 0.001 * TimeUs;
            if (blobId && PartIdx != InvalidPartId) {
                str << " sample PartId# "  << TLogoBlobID(*blobId, PartIdx).ToString();
            }
            str << " QueryCount# "  << QueryCount
                << " VDiskId# "     << info->GetVDiskId(OrderNumber).ToString()
                << " NodeId# "      << info->GetActorId(OrderNumber).NodeId()
                << " }";
        }
    };

    struct TVResponseEntry : public TBaseEntry {
        ui8 OrderNumber;
        ui8 Status;
        TString ErrorReason;
        
        TVResponseEntry(TInstant startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TBaseEntry(startTime)
            , OrderNumber(orderNumber)
            , Status(status)
            , ErrorReason(errorReason)
        {}

        void Output(IOutputStream& str, const char* typeName, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << typeName         << "{"
                << " TimestampMs# " << 0.001 * TimeUs
                << " VDiskId# "     << info->GetVDiskId(OrderNumber).ToString()
                << " NodeId# "      << info->GetActorId(OrderNumber).NodeId()
                << " Status# "      << NKikimrProto::EReplyStatus_Name(Status);
            if (ErrorReason) {
                str << "ErrorReason# \"" << ErrorReason << "\"";
            }
            str << " }";
        }
    };

    struct TVPutEntry : public TVRequestEntry {
        TVPutEntry(TInstant startTime, ui8 partIdx, ui32 queryCount, i8 orderNumber)
            : TVRequestEntry(startTime, partIdx, queryCount, orderNumber)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            Y_UNUSED(blobId);
            TVRequestEntry::Output(str, "TEvVPut", info, blobId);
        }
    };

    struct TVGetEntry : public TVRequestEntry {
        TVGetEntry(TInstant startTime, ui8 partIdx, ui32 queryCount, ui8 orderNumber)
            : TVRequestEntry(startTime, partIdx, queryCount,orderNumber)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            TVRequestEntry::Output(str, "TEvVGet", info, blobId);
        }
    };

    struct TVPutResultEntry : public TVResponseEntry {
        TVPutResultEntry(TInstant startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TVResponseEntry(startTime, orderNumber, status, errorReason)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            Y_UNUSED(blobId);
            TVResponseEntry::Output(str, "TEvVPutResult", info);
        }
    };

    struct TVGetResultEntry : public TVResponseEntry {
        TVGetResultEntry(TInstant startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TVResponseEntry(startTime, orderNumber, status, errorReason)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            Y_UNUSED(blobId);
            TVResponseEntry::Output(str, "TEvVGetResult", info);
        }
    };

    struct TAccelerationEntry : public TBaseEntry {
        TAccelerationEntry(TInstant startTime)
            : TBaseEntry(startTime)
        {}

        void Output(IOutputStream& str, const char* typeName) const {
            str << typeName << "{ TimestampMs# " << 0.001 * TimeUs << " }";
        }
    };

    struct TGetAccelerationEntry : public TAccelerationEntry {
        TGetAccelerationEntry(TInstant startTime)
            : TAccelerationEntry(startTime)
        {}
        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            Y_UNUSED(info);
            Y_UNUSED(blobId);
            TAccelerationEntry::Output(str, "GetAcceleration");
        }
    };

    struct TPutAccelerationEntry : public TAccelerationEntry {
        TPutAccelerationEntry(TInstant startTime)
            : TAccelerationEntry(startTime)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            Y_UNUSED(info);
            Y_UNUSED(blobId);
            TAccelerationEntry::Output(str, "PutAcceleration");
        }
    };


    using TEntry = std::variant<TVPutEntry, TVGetEntry, TVPutResultEntry, TVGetResultEntry, TPutAccelerationEntry,
            TGetAccelerationEntry>;

private:

    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TInstant StartTime;
    
    constexpr static ui32 TypicalHistorySize = TypicalPartsInBlob * 2;
    TStackVec<TEntry, TypicalHistorySize> Entries;

public:
    constexpr static ui32 InvalidPartId = 255;

public:
    THistory(const TIntrusivePtr<TBlobStorageGroupInfo>& info)
        : Info(info)
        , StartTime(TActivationContext::Now())
    {}

    void AddVPut(ui8 partId, ui32 queryCount, ui32 orderNumber) {
        Entries.emplace_back(TVPutEntry(StartTime, partId, queryCount, orderNumber));
    }

    void AddVGet(ui8 partId, ui32 queryCount, ui32 orderNumber) {
        Entries.emplace_back(TVGetEntry(StartTime, partId, queryCount, orderNumber));
    }

    void AddVPutResult(ui32 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        Entries.emplace_back(TVPutResultEntry(StartTime, orderNumber, status, errorReason));
    }

    void AddVGetResult(ui32 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        Entries.emplace_back(TVGetResultEntry(StartTime, orderNumber, status, errorReason));
    }

    void AddPutAcceleration() {
        Entries.emplace_back(TPutAccelerationEntry(StartTime));
    }

    void AddGetAcceleration() {
        Entries.emplace_back(TGetAccelerationEntry(StartTime));
    }

    TString Print(const TLogoBlobID* blobId = nullptr) const {
        TStringStream str("THistory { Entries# [");
        for (const TEntry& entry : Entries) {
            str << " ";
            std::visit([&](const auto& e) {
                e.Output(str, Info, blobId);
            }, entry);
        }
        str << " ] }";
        return str.Str();
    }
};

} // namespace NKikimr