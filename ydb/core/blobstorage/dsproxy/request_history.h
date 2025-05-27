#pragma once
#include "defs.h"

#include <variant>

namespace NKikimr {
class THistory {
private:
    enum AccelerationType : ui8 {
        PUT = 0,
        GET,
    };

    static TString AccelerationTypeName(AccelerationType type) {
        switch (type) {
        case AccelerationType::PUT:
            return "PutAcceleration";
        case AccelerationType::GET:
            return "GetAcceleration";
        }
    }

public:
    struct TBaseEntry {
        TDuration Timestamp;
        TBaseEntry(TMonotonic startTime)
            : Timestamp(TMonotonic::Now() - startTime)
        {}
    };

    struct TVRequestEntry : public TBaseEntry {
        ui8 PartIdx;
        ui8 OrderNumber;
        ui32 QueryCount;

        TVRequestEntry(TMonotonic startTime, ui8 partIdx, ui32 queryCount, ui8 orderNumber)
            : TBaseEntry(startTime)
            , PartIdx(partIdx)
            , OrderNumber(orderNumber)
            , QueryCount(queryCount)
        {}

        void Output(IOutputStream& str, const char* typeName, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            str << typeName         << "{"
                << " TimestampMs# " << Timestamp.MillisecondsFloat();
            if (blobId && PartIdx != InvalidPartId) {
                str << " sample PartId# " << TLogoBlobID(*blobId, PartIdx).ToString();
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
        
        TVResponseEntry(TMonotonic startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TBaseEntry(startTime)
            , OrderNumber(orderNumber)
            , Status(status)
            , ErrorReason(errorReason)
        {}

        void Output(IOutputStream& str, const char* typeName, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << typeName         << "{"
                << " TimestampMs# " << Timestamp.MillisecondsFloat()
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
        TVPutEntry(TMonotonic startTime, ui8 partIdx, ui32 queryCount, ui8 orderNumber)
            : TVRequestEntry(startTime, partIdx, queryCount, orderNumber)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            TVRequestEntry::Output(str, "TEvVPut", info, blobId);
        }
    };

    struct TVGetEntry : public TVRequestEntry {
        TVGetEntry(TMonotonic startTime, ui8 partIdx, ui32 queryCount, ui8 orderNumber)
            : TVRequestEntry(startTime, partIdx, queryCount, orderNumber)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            TVRequestEntry::Output(str, "TEvVGet", info, blobId);
        }
    };

    struct TVPutResultEntry : public TVResponseEntry {
        TVPutResultEntry(TMonotonic startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TVResponseEntry(startTime, orderNumber, status, errorReason)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            Y_UNUSED(blobId);
            TVResponseEntry::Output(str, "TEvVPutResult", info);
        }
    };

    struct TVGetResultEntry : public TVResponseEntry {
        TVGetResultEntry(TMonotonic startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TVResponseEntry(startTime, orderNumber, status, errorReason)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
                const TLogoBlobID* blobId) const {
            Y_UNUSED(blobId);
            TVResponseEntry::Output(str, "TEvVGetResult", info);
        }
    };

    struct TAccelerationEntry : public TBaseEntry {
        TAccelerationEntry(TMonotonic startTime, AccelerationType type)
            : TBaseEntry(startTime)
            , Type(type)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info, const TLogoBlobID* blobId) const {
            Y_UNUSED(info);
            Y_UNUSED(blobId);
            str << AccelerationTypeName(Type) << "{ TimestampMs# " << Timestamp.MillisecondsFloat() << " }";
        }

        AccelerationType Type;
    };

    using TEntry = std::variant<TVPutEntry, TVGetEntry, TVPutResultEntry, TVGetResultEntry, TAccelerationEntry>;

private:

    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TMonotonic StartTime;
    
    constexpr static ui32 TypicalHistorySize = TypicalPartsInBlob * 2;
    TStackVec<TEntry, TypicalHistorySize> Entries;
    TStackVec<TEntry, 2> WaitingEntries;

public:
    constexpr static ui32 InvalidPartId = 255;

public:
    THistory(const TIntrusivePtr<TBlobStorageGroupInfo>& info)
        : Info(info)
        , StartTime(TMonotonic::Now())
    {}

    void AddVPutToWaitingList(ui8 partId, ui32 queryCount, ui32 orderNumber) {
        WaitingEntries.emplace_back(TVPutEntry(StartTime, partId, queryCount, orderNumber));
    }
    void AddVGetToWaitingList(ui8 partId, ui32 queryCount, ui32 orderNumber) {
        WaitingEntries.emplace_back(TVGetEntry(StartTime, partId, queryCount, orderNumber));
    }

    void AddVPutResult(ui32 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        Entries.emplace_back(TVPutResultEntry(StartTime, orderNumber, status, errorReason));
    }

    void AddVGetResult(ui32 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        Entries.emplace_back(TVGetResultEntry(StartTime, orderNumber, status, errorReason));
    }

    void AddAcceleration(bool isPut) {
        AccelerationType type = isPut ? AccelerationType::PUT : AccelerationType::GET;
        Entries.emplace_back(TAccelerationEntry(StartTime, type));
    }

    void AddAllWaiting() {
        for (TEntry& entry : WaitingEntries) {
            std::visit([&](auto& e) {
                e.Timestamp = TMonotonic::Now() - StartTime;
                Entries.push_back(std::move(e));
            }, entry);
        }
        WaitingEntries.clear();
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
