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
        ui8 OrderNumber;
        ui32 QueryCount;

        TVRequestEntry(TMonotonic startTime, ui32 queryCount, ui8 orderNumber)
            : TBaseEntry(startTime)
            , OrderNumber(orderNumber)
            , QueryCount(queryCount)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << " TimestampMs# " << Timestamp.MillisecondsFloat()
                << " QueryCount# "  << QueryCount
                << " VDiskId# "     << info->GetVDiskId(OrderNumber).ToString()
                << " NodeId# "      << info->GetActorId(OrderNumber).NodeId();
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

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << " TimestampMs# " << Timestamp.MillisecondsFloat()
                << " VDiskId# "     << info->GetVDiskId(OrderNumber).ToString()
                << " NodeId# "      << info->GetActorId(OrderNumber).NodeId()
                << " Status# "      << NKikimrProto::EReplyStatus_Name(Status);
            if (ErrorReason) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
        }
    };

    struct TVPutEntry : public TVRequestEntry {
        struct TSubrequest {
            TLogoBlobID BlobId;

            TSubrequest(const TLogoBlobID& blobId)
                : BlobId(blobId)
            {}

            void Output(IOutputStream& str) const {
                str << "{ BlobId# " << BlobId.ToString()
                    << " }";
            }
        };

        TVector<TSubrequest> Subrequests;

        TVPutEntry(TMonotonic startTime, ui32 queryCount, ui8 orderNumber)
            : TVRequestEntry(startTime, queryCount, orderNumber)
        {}

        void AddSubrequest(const TLogoBlobID& blobId) {
            Subrequests.emplace_back(blobId);
        }

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << "TEvVPut{";
            TVRequestEntry::Output(str, info);
            if (!Subrequests.empty()) {
                str << " Subrequests# [";
                for (size_t i = 0; i < Subrequests.size(); ++i) {
                    if (i > 0) str << ", ";
                    Subrequests[i].Output(str);
                }
                str << "]";
            }
            str << " }";
        }
    };

    struct TVGetEntry : public TVRequestEntry {
        struct TSubrequest {
            TLogoBlobID BlobId;
            ui32 Shift;
            ui32 Size;

            TSubrequest(const TLogoBlobID& blobId, ui32 shift, ui32 size)
                : BlobId(blobId)
                , Shift(shift)
                , Size(size)
            {}

            void Output(IOutputStream& str) const {
                str << "{ BlobId# " << BlobId.ToString()
                    << " Shift# " << Shift
                    << " Size# " << Size
                    << " }";
            }
        };

        TVector<TSubrequest> Subrequests;

        TVGetEntry(TMonotonic startTime, ui32 queryCount, ui8 orderNumber)
            : TVRequestEntry(startTime, queryCount, orderNumber)
        {}

        void AddSubrequest(const TLogoBlobID& blobId, ui32 shift, ui32 size) {
            Subrequests.emplace_back(blobId, shift, size);
        }

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << "TEvVGet{";
            TVRequestEntry::Output(str, info);
            if (!Subrequests.empty()) {
                str << " Subrequests# [";
                for (size_t i = 0; i < Subrequests.size(); ++i) {
                    if (i > 0) str << ", ";
                    Subrequests[i].Output(str);
                }
                str << "]";
            }
            str << " }";
        }
    };

    struct TVPutResultEntry : public TVResponseEntry {
        struct TSubrequestResult {
            TLogoBlobID BlobId;
            ui8 Status;

            TSubrequestResult(const TLogoBlobID& blobId, NKikimrProto::EReplyStatus status)
                : BlobId(blobId)
                , Status(status)
            {}

            void Output(IOutputStream& str) const {
                str << "{ BlobId# " << BlobId.ToString()
                    << " Status# " << NKikimrProto::EReplyStatus_Name(Status)
                    << " }";
            }
        };

        TVector<TSubrequestResult> SubrequestResults;

        TVPutResultEntry(TMonotonic startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TVResponseEntry(startTime, orderNumber, status, errorReason)
        {}

        void AddSubrequestResult(const TLogoBlobID& blobId, NKikimrProto::EReplyStatus status) {
            SubrequestResults.emplace_back(blobId, status);
        }

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << "TEvVPutResult{";
            TVResponseEntry::Output(str, info);
            if (!SubrequestResults.empty()) {
                str << " SubrequestResults# [";
                for (size_t i = 0; i < SubrequestResults.size(); ++i) {
                    if (i > 0) str << ", ";
                    SubrequestResults[i].Output(str);
                }
                str << "]";
            }
            str << " }";
        }
    };

    struct TVGetResultEntry : public TVResponseEntry {
        struct TSubrequestResult {
            TLogoBlobID BlobId;
            ui8 Status;
            ui32 Shift;
            ui32 Size;

            TSubrequestResult(const TLogoBlobID& blobId, NKikimrProto::EReplyStatus status, ui32 shift, ui32 size)
                : BlobId(blobId)
                , Status(status)
                , Shift(shift)
                , Size(size)
            {}

            void Output(IOutputStream& str) const {
                str << "{ BlobId# " << BlobId.ToString()
                    << " Status# " << NKikimrProto::EReplyStatus_Name(Status)
                    << " Shift# " << Shift
                    << " Size# " << Size
                    << " }";
            }
        };

        TVector<TSubrequestResult> SubrequestResults;

        TVGetResultEntry(TMonotonic startTime, ui8 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason)
            : TVResponseEntry(startTime, orderNumber, status, errorReason)
        {}
        
        void AddSubrequestResult(const TLogoBlobID& blobId, NKikimrProto::EReplyStatus status, ui32 shift, ui32 size) {
            SubrequestResults.emplace_back(blobId, status, shift, size);
        }

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            str << "TEvVGetResult{";
            TVResponseEntry::Output(str, info);
            if (!SubrequestResults.empty()) {
                str << " SubrequestResults# [";
                for (size_t i = 0; i < SubrequestResults.size(); ++i) {
                    if (i > 0) str << ", ";
                    SubrequestResults[i].Output(str);
                }
                str << "]";
            }
            str << " }";
        }
    };

    struct TAccelerationEntry : public TBaseEntry {
        TAccelerationEntry(TMonotonic startTime, AccelerationType type)
            : TBaseEntry(startTime)
            , Type(type)
        {}

        void Output(IOutputStream& str, const TIntrusivePtr<TBlobStorageGroupInfo>& info) const {
            Y_UNUSED(info);
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

    TVPutEntry CreateVPut(ui32 queryCount, ui32 orderNumber) {
        return TVPutEntry(StartTime, queryCount, orderNumber);
    }
    void AddVPutToWaitingList(TVPutEntry&& entry) {
        WaitingEntries.emplace_back(std::move(entry));
    }
    void AddVPutToWaitingList(const TLogoBlobID& blobId, ui8 orderNumber) {
        auto vput = CreateVPut(1, orderNumber);
        vput.AddSubrequest(blobId);
        WaitingEntries.emplace_back(std::move(vput));
    }

    TVGetEntry CreateVGet(ui32 queryCount, ui32 orderNumber) {
        return TVGetEntry(StartTime, queryCount, orderNumber);
    }
    void AddVGetToWaitingList(TVGetEntry&& entry) {
        WaitingEntries.emplace_back(std::move(entry));
    }
    void AddVGetToWaitingList(ui32 queryCount, ui32 orderNumber) {
        WaitingEntries.emplace_back(CreateVGet(queryCount, orderNumber));
    }

    TVPutResultEntry CreateVPutResult(ui32 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        return TVPutResultEntry(StartTime, orderNumber, status, errorReason);
    }
    void AddVPutResult(TVPutResultEntry&& entry) {
        entry.Timestamp = TMonotonic::Now() - StartTime;
        Entries.emplace_back(std::move(entry));
    }
    void AddVPutResult(ui32 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        TVPutResultEntry entry = CreateVPutResult(orderNumber, status, errorReason);
        AddVPutResult(std::move(entry));
    }

    TVGetResultEntry CreateVGetResult(ui32 orderNumber, NKikimrProto::EReplyStatus status, const TString& errorReason) {
        return TVGetResultEntry(StartTime, orderNumber, status, errorReason);
    }
    void AddVGetResult(TVGetResultEntry&& entry) {
        entry.Timestamp = TMonotonic::Now() - StartTime;
        Entries.emplace_back(std::move(entry));
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

    TString Print() const {
        TStringStream str("THistory { Entries# [");
        for (const TEntry& entry : Entries) {
            str << " ";
            std::visit([&](const auto& e) {
                e.Output(str, Info);
            }, entry);
        }
        str << " ] }";
        return str.Str();
    }
};

} // namespace NKikimr
