#pragma once
#include "defs.h"

#include "blobstorage_pdisk_defs.h"
#include "blobstorage_pdisk_params.h"
#include "blobstorage_pdisk_config.h"

#include <ydb/core/blobstorage/base/vdisk_lsn.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/blobstorage/base/bufferwithgaps.h>
#include <ydb/core/blobstorage/base/transparent.h>
#include <ydb/core/blobstorage/base/batched_vec.h>
#include <ydb/core/util/stlog.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/map.h>
#include <util/system/file.h>
#include <util/system/fhandle.h>


namespace NKikimr {

IActor* CreatePDisk(const TIntrusivePtr<TPDiskConfig> &cfg, const NPDisk::TMainKey &mainKey,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

struct TPDiskMon;
namespace NPDisk {

struct TDiskFormat;
struct TPersistentBufferFormat;

using TDiskFormatPtr = std::unique_ptr<TDiskFormat, void(*)(TDiskFormat*)>;
using TPersistentBufferFormatPtr = std::unique_ptr<TPersistentBufferFormat, void(*)(TPersistentBufferFormat*)>;

struct TCommitRecord {
    ui64 FirstLsnToKeep = 0; // 0 == not set
    TVector<TChunkIdx> CommitChunks;
    TVector<TChunkIdx> DeleteChunks;
    TVector<TChunkIdx> DirtyChunks;
    bool IsStartingPoint = false;
    bool DeleteToDecommitted = false; // 1 == set chunks to Decommitted state that requires a ChunkForget event or a restart
    // the value of DeleteToDecommitted is not stored as a part of the commit record.

    void ValidateChunks(TVector<TChunkIdx> &chunks) {
        if (chunks.size()) {
            Sort(chunks.begin(), chunks.end());
            TChunkIdx lastIdx = chunks[0];
            for (size_t i = 1; i < chunks.size(); ) {
                if (chunks[i] == lastIdx) {
                    //Y_VERIFY(false);
                    chunks.erase(chunks.begin() + i);
                } else {
                    lastIdx = chunks[i];
                    ++i;
                }
            }
        }
    }

    void Validate() {
        ValidateChunks(CommitChunks);
        ValidateChunks(DeleteChunks);
    }

    void Verify() const {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&FirstLsnToKeep, sizeof(FirstLsnToKeep));
        if (CommitChunks.size()) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&CommitChunks[0], sizeof(CommitChunks[0]) * CommitChunks.size());
        }
        if (DeleteChunks.size()) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&DeleteChunks[0], sizeof(DeleteChunks[0]) * DeleteChunks.size());
        }
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&IsStartingPoint, sizeof(IsStartingPoint));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&DeleteToDecommitted, sizeof(DeleteToDecommitted));
    }

    TString ToString() const {
        TStringStream str;
        str << "{CommitRecord";
        str << " FirstLsnToKeep# " << FirstLsnToKeep;
        str << " IsStartingPoint# " << IsStartingPoint;
        str << " DeleteToDecommitted# " << DeleteToDecommitted;
        str << " CommitChunks# ";
        PrintChunks(str, CommitChunks);
        str << " DeleteChunks# ";
        PrintChunks(str, DeleteChunks);
        str << " DirtyChunks# ";
        PrintChunks(str, DirtyChunks);
        str << "}";
        return str.Str();
    }

    size_t ApproximateSize() const {
        return sizeof(TCommitRecord) + (CommitChunks.size() + DeleteChunks.size()) * sizeof(ui32);
    }

    static void PrintChunks(IOutputStream &str, const TVector<TChunkIdx> &vec) {
        str << "[";
        for (ui32 i = 0; i < vec.size(); i++) {
            if (i)
                str << ", ";
            str << vec[i];
        }
        str << "]";
    }
};

class TLogRecord {
public:
    TLogSignature Signature;
    TRcBuf Data;
    ui64 Lsn;

    TLogRecord(TLogSignature signature, const TRcBuf &data, ui64 lsn)
        : Signature(signature)
        , Data(data)
        , Lsn(lsn)
    {}

    TLogRecord()
        : Signature(0)
        , Lsn(0)
    {}

    void Verify() const {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Signature, sizeof(Signature));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(Data.Data(), Data.size());
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Lsn, sizeof(Lsn));
    }

    TString ToString() const {
        TStringStream str;
        str << "{TLogRecord Signature# " << Signature.ToString();
        str << " Data.Size()# " << Data.size();
        str << " Lsn# " << Lsn;
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// INIT
////////////////////////////////////////////////////////////////////////////
struct TEvYardInit : TEventLocal<TEvYardInit, TEvBlobStorage::EvYardInit> {
    TOwnerRound OwnerRound;
    TVDiskID VDisk;
    ui64 PDiskGuid;
    TActorId CutLogID; // ask this actor about log cut
    TActorId WhiteboardProxyId;
    ui32 SlotId;
    ui32 GroupSizeInUnits;
    bool GetDiskFd = false; // if true, response will contain a duplicated file descriptor for direct disk access

    TEvYardInit(
            TOwnerRound ownerRound,
            const TVDiskID &vdisk,
            ui64 pDiskGuid,
            const TActorId &cutLogID = TActorId(),
            const TActorId& whiteboardProxyId = {},
            ui32 slotId = Max<ui32>(),
            ui32 groupSizeInUnits = 0,
            bool getDiskFd = false
        )
        : OwnerRound(ownerRound)
        , VDisk(vdisk)
        , PDiskGuid(pDiskGuid)
        , CutLogID(cutLogID)
        , WhiteboardProxyId(whiteboardProxyId)
        , SlotId(slotId)
        , GroupSizeInUnits(groupSizeInUnits)
        , GetDiskFd(getDiskFd)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvYardInit &record) {
        TStringStream str;
        str << "{EvYardInit ownerRound# " << record.OwnerRound;
        str << " VDisk# " << record.VDisk.ToString();
        str << " PDiskGuid# " << record.PDiskGuid;
        str << " CutLogID# " << record.CutLogID;
        str << " WhiteboardProxyId# " << record.WhiteboardProxyId;
        str << " SlotId# " << record.SlotId;
        str << " GroupSizeInUnits# " << record.GroupSizeInUnits;
        str << " GetDiskFd# " << record.GetDiskFd;
        str << "}";
        return str.Str();
    }
};

struct TEvYardInitResult : TEventLocal<TEvYardInitResult, TEvBlobStorage::EvYardInitResult> {
    NKikimrProto::EReplyStatus Status;
    TMap<TLogSignature, TLogRecord> StartingPoints;
    TStatusFlags StatusFlags;
    TIntrusivePtr<TPDiskParams> PDiskParams;
    TVector<TChunkIdx> OwnedChunks;  // Sorted vector of owned chunk identifiers.
    TString ErrorReason;
    TFileHandle DiskFd; // A duplicated fd for direct disk access
    TDiskFormatPtr DiskFormat{nullptr, nullptr}; // On-device format for direct disk access offset calculations
    TPersistentBufferFormatPtr PersistentBufferFormat{nullptr, nullptr};

    TEvYardInitResult(const NKikimrProto::EReplyStatus status, TString errorReason)
        : Status(status)
        , StatusFlags(0)
        , PDiskParams(new TPDiskParams(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, DEVICE_TYPE_ROT, false, 0))
        , ErrorReason(std::move(errorReason))
    {
        Y_VERIFY(status != NKikimrProto::OK, "Single-parameter constructor is for error responses only");
    }

    TEvYardInitResult(NKikimrProto::EReplyStatus status, ui64 seekTimeUs, ui64 readSpeedBps,
            ui64 writeSpeedBps, ui64 readBlockSize, ui64 writeBlockSize,
            ui64 bulkWriteBlockSize, ui32 chunkSize, ui32 appendBlockSize,
            TOwner owner, TOwnerRound ownerRound, ui32 slotSizeInUnits,
            TStatusFlags statusFlags, TVector<TChunkIdx> ownedChunks,
            EDeviceType trueMediaType, bool isTinyDisk, ui32 rawSectorSize,
            TString errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , PDiskParams(new TPDiskParams(
                    owner,
                    ownerRound,
                    slotSizeInUnits,
                    chunkSize,
                    appendBlockSize,
                    seekTimeUs,
                    readSpeedBps,
                    writeSpeedBps,
                    readBlockSize,
                    writeBlockSize,
                    bulkWriteBlockSize,
                    trueMediaType,
                    isTinyDisk,
                    rawSectorSize))
        , OwnedChunks(std::move(ownedChunks))
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvYardInitResult &record) {
        TStringStream str;
        str << "{EvYardInitResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        for (auto it = record.StartingPoints.begin(); it != record.StartingPoints.end(); ++it) {
            str << "{StartingPoint Signature# " << (ui32)it->first;
            str << " Lsn# " << it->second.Lsn;
            str << "}";
        }
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << " PDiskParams# {";
        if (record.PDiskParams) {
            str << record.PDiskParams->ToString();
        }
        str << "}";
        str << " OwnedChunks# {";
        for (ui64 i = 0; i < record.OwnedChunks.size(); ++i) {
            if (i) {
                str << ", ";
            }
            str << record.OwnedChunks[i];
        }
        str << "}";
        str << " DiskFd# " << static_cast<FHANDLE>(record.DiskFd);
        str << " DiskFormat# " << (record.DiskFormat ? "set" : "null");
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CHANGE GroupSizeInUnits
////////////////////////////////////////////////////////////////////////////
struct TEvYardResize : TEventLocal<TEvYardResize, TEvBlobStorage::EvYardResize> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui32 GroupSizeInUnits;

    TEvYardResize(TOwner owner, TOwnerRound ownerRound, ui32 groupSizeInUnits)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , GroupSizeInUnits(groupSizeInUnits)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvYardResize &record) {
        TStringStream str;
        str << "{EvYardResize Owner# " << record.Owner;
        str << " OwnerRound# " << record.OwnerRound;
        str << " GroupSizeInUnits# " << record.GroupSizeInUnits;
        str << "}";
        return str.Str();
    }
};

struct TEvYardResizeResult : TEventLocal<TEvYardResizeResult, TEvBlobStorage::EvYardResizeResult> {
    NKikimrProto::EReplyStatus Status;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    TEvYardResizeResult(
            NKikimrProto::EReplyStatus status,
            TStatusFlags statusFlags,
            TString errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvYardResizeResult &record) {
        TStringStream str;
        str << "{TEvYardResizeResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << "}";
        return str.Str();
    }
};

struct TEvChangeExpectedSlotCount : TEventLocal<TEvChangeExpectedSlotCount, TEvBlobStorage::EvChangeExpectedSlotCount> {
    ui32 ExpectedSlotCount;
    ui32 SlotSizeInUnits;

    TEvChangeExpectedSlotCount(ui32 expectedSlotCount, ui32 slotSizeInUnits)
        : ExpectedSlotCount(expectedSlotCount)
        , SlotSizeInUnits(slotSizeInUnits)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChangeExpectedSlotCount& record) {
        TStringStream str;
        str << "{";
        str << "EvChangeExpectedSlotCount ";
        str << " ExpectedSlotCount# " << record.ExpectedSlotCount;
        str << " SlotSizeInUnits# " << record.SlotSizeInUnits;
        str << "}";
        return str.Str();
    }
};

struct TEvChangeExpectedSlotCountResult : TEventLocal<TEvChangeExpectedSlotCountResult, TEvBlobStorage::EvChangeExpectedSlotCountResult> {
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvChangeExpectedSlotCountResult(NKikimrProto::EReplyStatus status, TString errorReason)
        : Status(status)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChangeExpectedSlotCountResult& record) {
        TStringStream str;
        str << "{";
        str << "EvChangeExpectedSlotCountResult Status#" << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// LOG
////////////////////////////////////////////////////////////////////////////
struct TEvLogResult;

struct TEvLog : TEventLocal<TEvLog, TEvBlobStorage::EvLog> {
    struct ICallback {
        virtual ~ICallback() = default;
        virtual void operator ()(TActorSystem *actorSystem, const TEvLogResult &ev) = 0;
    };

    using TCallback = std::unique_ptr<ICallback>;

    explicit TEvLog(TOwner owner, TOwnerRound ownerRound, TLogSignature signature,
                    const TRcBuf &data, TLsnSeg seg, void *cookie, TCallback &&cb = TCallback())
        : Owner(owner)
        , OwnerRound(ownerRound)
        , Signature(signature)
        , Data(data)
        , LsnSegmentStart(seg.First)
        , Lsn(seg.Last)
        , Cookie(cookie)
        , LogCallback(std::move(cb))
    {
        Y_VERIFY(Owner);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&owner, sizeof(owner));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ownerRound, sizeof(ownerRound));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&signature, sizeof(signature));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(data.Data(), data.size());
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&seg, sizeof(seg));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cookie, sizeof(cookie));
    }

    explicit TEvLog(TOwner owner, TOwnerRound ownerRound, TLogSignature signature,
                    const TCommitRecord &commitRecord,
                    const TRcBuf &data, TLsnSeg seg, void *cookie, TCallback &&cb = TCallback())
        : Owner(owner)
        , OwnerRound(ownerRound)
        , Signature(signature, /*commitRecord*/ true)
        , Data(data)
        , LsnSegmentStart(seg.First)
        , Lsn(seg.Last)
        , Cookie(cookie)
        , LogCallback(std::move(cb))
        , CommitRecord(commitRecord)
    {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&owner, sizeof(owner));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ownerRound, sizeof(ownerRound));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&signature, sizeof(signature));
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(data.Data(), data.size());
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&seg, sizeof(seg));
        CommitRecord.Verify();
    }

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvLog &record) {
        TStringStream str;
        str << "{EvLog ownerId# " << (ui32)record.Owner;
        str << " ownerRound# " << record.OwnerRound;
        str << " Signature# " << (ui32)record.Signature;
        str << " DataSize# " << record.Data.size();
        str << " Lsn# " << (ui64)record.Lsn;
        str << " LsnSegmentStart# " << (ui32)record.LsnSegmentStart;
        str << " Cookie# " << (ui64)record.Cookie;
        if (record.Signature.HasCommitRecord()) {
            str << record.CommitRecord.ToString();
        }
        str << "}";
        return str.Str();
    }

    size_t ApproximateSize() const {
        return (sizeof(TEvLog) - sizeof(TCommitRecord)) + Data.size() + CommitRecord.ApproximateSize();
    }

    TOwner Owner;
    TOwnerRound OwnerRound;
    TLogSignature Signature;
    TRcBuf Data;
    ui64 LsnSegmentStart;   // we may write a log record for diapason of lsns [LsnSegmentStart, Lsn];
                            // usually LsnSegmentStart=Lsn and this diapason is a single point
    ui64 Lsn;
    void *Cookie;
    TCallback LogCallback;
    TCommitRecord CommitRecord;

    mutable NLWTrace::TOrbit Orbit;
};

struct TEvMultiLog : TEventLocal<TEvMultiLog, TEvBlobStorage::EvMultiLog> {
    void AddLog(THolder<TEvLog> &&ev, NWilson::TTraceId traceId = {}) {
        Logs.emplace_back(std::move(ev), std::move(traceId));
        auto &log = *Logs.back().Event;
        if (Logs.size() == 1) {
            LsnSeg = TLsnSeg(log.LsnSegmentStart, log.Lsn);
        } else {
            Y_VERIFY_S(LsnSeg.Last + 1 == log.LsnSegmentStart,
                    "LastLsn# " << LsnSeg.Last << " NewLsnStart# " << log.LsnSegmentStart);
            LsnSeg.Last = log.Lsn;
        }
    }

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvMultiLog &record) {
        TStringBuilder str;
        str << '{';
        for (ui64 idx = 0; idx < record.Logs.size(); ++idx) {
            str << (idx ? ", " : "") << idx << "# " << record.Logs[idx].Event->ToString();
        }
        str << '}';
        return str;
    }

    struct TItem {
        TItem(THolder<TEvLog>&& event, NWilson::TTraceId&& traceId)
            : Event(std::move(event))
            , TraceId(std::move(traceId))
        {}

        THolder<TEvLog> Event;
        NWilson::TTraceId TraceId;
    };

    TBatchedVec<TItem> Logs;
    TLsnSeg LsnSeg;
};

struct TEvLogResult : TEventLocal<TEvLogResult, TEvBlobStorage::EvLogResult> {
    struct TRecord {
        ui64 Lsn;
        void *Cookie;
        mutable NLWTrace::TOrbit Orbit;

        TRecord(ui64 lsn, void *cookie)
            : Lsn(lsn)
            , Cookie(cookie)
        {}

        TString ToString() const {
            return Sprintf("[Lsn# %" PRIu64 " Cookie# %p]", (ui64)Lsn, Cookie);
        }
    };

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvLogResult &record) {
        TStringStream str;
        str << "{EvLogResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << " LogChunkCount# " << record.LogChunkCount;
        for (auto it = record.Results.begin(); it != record.Results.end(); ++it) {
            str << "{Lsn# " << it->Lsn << " Cookie# " << (ui64)it->Cookie << "}";
        }
        str << "}";
        return str.Str();
    }

    // batch several log result records into one message
    typedef TVector<TRecord> TResults;
    TResults Results;
    NKikimrProto::EReplyStatus Status;
    TStatusFlags StatusFlags;
    TString ErrorReason;
    i64 LogChunkCount = 0;

    TEvLogResult(NKikimrProto::EReplyStatus status,
            TStatusFlags statusFlags,
            TString errorReason,
            i64 logChunkCount)
        : Status(status)
        , StatusFlags(statusFlags)
        , ErrorReason(std::move(errorReason))
        , LogChunkCount(logChunkCount)
    {}
};

////////////////////////////////////////////////////////////////////////////
// READ LOG
////////////////////////////////////////////////////////////////////////////
struct TEvReadLog : TEventLocal<TEvReadLog, TEvBlobStorage::EvReadLog> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    TLogPosition Position;
    ui64 SizeLimit;

    TEvReadLog(TOwner owner, TOwnerRound ownerRound, TLogPosition position = TLogPosition{0, 0}, ui64 sizeLimit = 16 << 20)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , Position(position)
        , SizeLimit(sizeLimit)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvReadLog &record) {
        TStringStream str;
        str << "{EvReadLog ownerId# " << (ui32)record.Owner;
        str << " ownerRound# " << (ui64)record.OwnerRound;
        str << " position# " << record.Position;
        str << " SizeLimit# " << record.SizeLimit;
        str << "}";
        return str.Str();
    }
};

struct TEvReadLogResult : TEventLocal<TEvReadLogResult, TEvBlobStorage::EvReadLogResult> {
    typedef TVector<TLogRecord> TResults;
    TResults Results;
    NKikimrProto::EReplyStatus Status;
    TLogPosition Position;
    TLogPosition NextPosition;
    bool IsEndOfLog;
    ui32 LastGoodChunkIdx = 0;
    ui64 LastGoodSectorIdx = 0;

    TStatusFlags StatusFlags;
    TString ErrorReason;
    TOwner Owner;

    TEvReadLogResult(NKikimrProto::EReplyStatus status, TLogPosition position, TLogPosition nextPosition,
            bool isEndOfLog, TStatusFlags statusFlags, TString errorReason, TOwner owner)
        : Status(status)
        , Position(position)
        , NextPosition(nextPosition)
        , IsEndOfLog(isEndOfLog)
        , StatusFlags(statusFlags)
        , ErrorReason(std::move(errorReason))
        , Owner(owner)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvReadLogResult &record) {
        TStringStream str;
        str << "{EvReadLogResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " position# " << record.Position;
        str << " nextPosition# " << record.NextPosition;
        str << " isEndOfLog# " << (record.IsEndOfLog ? "true" : "false");
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << " Results.size# " << record.Results.size();
        str << "}";
        return str.Str();
    }
};
////////////////////////////////////////////////////////////////////////////////
// Lock chucks specified by color or by count
////////////////////////////////////////////////////////////////////////////////
struct TEvChunkLock : TEventLocal<TEvChunkLock, TEvBlobStorage::EvChunkLock> {
    enum ELockFrom {
        LOG,
        PERSONAL_QUOTA,
        COUNT
    };

    static TString ELockFrom_Name(const ELockFrom& from) {
        switch (from) {
            case LOG: return "log";
            case PERSONAL_QUOTA: return "personal_quota";
            default: return "unknown";
        }
    }

    static ELockFrom LockFromByName(const TString& name) {
        for (ELockFrom from : {ELockFrom::LOG, ELockFrom::PERSONAL_QUOTA}) {
            if (name == ELockFrom_Name(from)) {
                return from;
            }
        }
        return ELockFrom::COUNT; // Wrong name
    }

    ELockFrom LockFrom;
    bool ByVDiskId = true;
    TOwner Owner = 0;
    TVDiskID VDiskId = TVDiskID::InvalidId;
    bool IsGenerationSet = true;
    ui32 Count;
    NKikimrBlobStorage::TPDiskSpaceColor::E Color;

    TEvChunkLock(ELockFrom from, ui32 count, NKikimrBlobStorage::TPDiskSpaceColor::E color)
        : LockFrom(from)
        , Count(count)
        , Color(color)
    {
        Y_VERIFY_DEBUG(from != ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkLock(ELockFrom from, TOwner owner, ui32 count, NKikimrBlobStorage::TPDiskSpaceColor::E color)
        : LockFrom(from)
        , ByVDiskId(false)
        , Owner(owner)
        , Count(count)
        , Color(color)
    {
        Y_VERIFY_DEBUG(from == ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkLock(ELockFrom from, TVDiskID vdiskId, bool isGenerationSet, ui32 count, NKikimrBlobStorage::TPDiskSpaceColor::E color)
        : LockFrom(from)
        , ByVDiskId(true)
        , VDiskId(vdiskId)
        , IsGenerationSet(isGenerationSet)
        , Count(count)
        , Color(color)
    {
        Y_VERIFY_DEBUG(from == ELockFrom::PERSONAL_QUOTA);
    }

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkLock &record) {
        TStringStream str;

        str << "{EvChunkLock LockFrom# " << ELockFrom_Name(record.LockFrom);

        if (record.LockFrom == ELockFrom::PERSONAL_QUOTA) {
            if (record.ByVDiskId) {
                if (record.IsGenerationSet) {
                    str << " VDiskId# " << record.VDiskId.ToString();
                } else {
                    str << " VDiskId# " << record.VDiskId.ToStringWOGeneration();
                }
            } else {
                str << " Owner# " << record.Owner;
            }
        }
        str << " Count# " << record.Count;
        str << "}";
        return str.Str();
    }
};

struct TEvChunkLockResult : TEventLocal<TEvChunkLockResult, TEvBlobStorage::EvChunkLockResult> {
    NKikimrProto::EReplyStatus Status;
    TVector<TChunkIdx> LockedChunks;
    ui32 AvailableChunksCount;
    TString ErrorReason;

    TEvChunkLockResult(NKikimrProto::EReplyStatus status, TVector<TChunkIdx> locked, ui32 availableChunksCount,
        TString errorReason = TString())
        : Status(status)
        , LockedChunks(locked)
        , AvailableChunksCount(availableChunksCount)
        , ErrorReason(errorReason)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkLockResult &record) {
        TStringStream str;
        str << "{EvChunkLockResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " LockedChunks# {";
        for (ui64 i = 0; i < record.LockedChunks.size(); ++i) {
            if (i) {
                str << ", ";
            }
            str << record.LockedChunks[i];
        }
        str << "}";
        if (!record.ErrorReason.empty()) {
            str << " ErrorReason# " << record.ErrorReason;
        }
        str << "}";
        return str.Str();
    }
};
////////////////////////////////////////////////////////////////////////////////
// Unlock all previously locked chunks
////////////////////////////////////////////////////////////////////////////////
struct TEvChunkUnlock : TEventLocal<TEvChunkUnlock, TEvBlobStorage::EvChunkUnlock> {
    TEvChunkLock::ELockFrom LockFrom;
    bool ByVDiskId = true;
    TOwner Owner = 0;
    TVDiskID VDiskId = TVDiskID::InvalidId;
    bool IsGenerationSet = true;

    TEvChunkUnlock(TEvChunkLock::ELockFrom lockFrom)
        : LockFrom(lockFrom)
    {
        Y_VERIFY_DEBUG(LockFrom != TEvChunkLock::ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkUnlock(TEvChunkLock::ELockFrom lockFrom, TOwner owner)
        : LockFrom(lockFrom)
        , ByVDiskId(false)
        , Owner(owner)
    {
        Y_VERIFY_DEBUG(LockFrom == TEvChunkLock::ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkUnlock(TEvChunkLock::ELockFrom lockFrom, TVDiskID vdiskId, bool isGenerationSet)
        : LockFrom(lockFrom)
        , ByVDiskId(true)
        , VDiskId(vdiskId)
        , IsGenerationSet(isGenerationSet)
    {
        Y_VERIFY_DEBUG(LockFrom == TEvChunkLock::ELockFrom::PERSONAL_QUOTA);
    }

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkUnlock &record) {
        Y_UNUSED(record);
        TStringStream str;
        str << "{EvChunkUnlock";
        str << "{EvChunkLock LockFrom# " << TEvChunkLock::ELockFrom_Name(record.LockFrom);
        if (record.Owner) {
            str << " Owner# " << record.Owner;
        }
        if (record.ByVDiskId) {
            if (record.IsGenerationSet) {
                str << " VDiskId# " << record.VDiskId.ToString();
            } else {
                str << " VDiskId# " << record.VDiskId.ToStringWOGeneration();
            }
        }
        str << "}";
        return str.Str();
    }
};

struct TEvChunkUnlockResult : TEventLocal<TEvChunkUnlockResult, TEvBlobStorage::EvChunkUnlockResult> {
    NKikimrProto::EReplyStatus Status;
    ui32 UnlockedChunks;
    TString ErrorReason;

    TEvChunkUnlockResult(NKikimrProto::EReplyStatus status, ui32 unlocked, TString errorReason = "")
        : Status(status)
        , UnlockedChunks(unlocked)
        , ErrorReason(errorReason)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkUnlockResult &record) {
        TStringStream str;
        str << "{EvChunkUnlockResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " UnlockedChunks# " << record.UnlockedChunks;
        if (!record.ErrorReason.empty()) {
            str << " ErrorReason# " << record.ErrorReason;
        }
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CHUNK RESERVATION
////////////////////////////////////////////////////////////////////////////
struct TEvChunkReserve : TEventLocal<TEvChunkReserve, TEvBlobStorage::EvChunkReserve> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui32 SizeChunks;

    TEvChunkReserve(TOwner owner, TOwnerRound ownerRound, ui32 sizeChunks)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , SizeChunks(sizeChunks)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkReserve &record) {
        TStringStream str;
        str << "{EvChunkReserve ownerId# " << (ui32)record.Owner;
        str << " ownerRound# " << record.OwnerRound;
        str << " SizeChunks# " << record.SizeChunks;
        str << "}";
        return str.Str();
    }
};

struct TEvChunkReserveResult : TEventLocal<TEvChunkReserveResult, TEvBlobStorage::EvChunkReserveResult> {
    NKikimrProto::EReplyStatus Status;
    TVector<TChunkIdx> ChunkIds;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    TEvChunkReserveResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags)
        : Status(status)
        , StatusFlags(statusFlags)
    {}

    TEvChunkReserveResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags, TString &errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkReserveResult &record) {
        TStringStream str;
        str << "{EvChunkReserveResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CHUNK FORGET
////////////////////////////////////////////////////////////////////////////
struct TEvChunkForget : TEventLocal<TEvChunkForget, TEvBlobStorage::EvChunkForget> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    TVector<TChunkIdx> ForgetChunks;

    TEvChunkForget(TOwner owner, TOwnerRound ownerRound)
        : Owner(owner)
        , OwnerRound(ownerRound)
    {}

    TEvChunkForget(TOwner owner, TOwnerRound ownerRound, TVector<TChunkIdx> forgetChunks)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , ForgetChunks(std::move(forgetChunks))
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkForget &record) {
        TStringStream str;
        str << "{EvChunkForget ownerId# " << (ui32)record.Owner;
        str << " ownerRound# " << record.OwnerRound;
        str << " ForgetChunks# ";
        TCommitRecord::PrintChunks(str, record.ForgetChunks);
        str << "}";
        return str.Str();
    }
};

struct TEvChunkForgetResult : TEventLocal<TEvChunkForgetResult, TEvBlobStorage::EvChunkForgetResult> {
    NKikimrProto::EReplyStatus Status;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    TEvChunkForgetResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags)
        : Status(status)
        , StatusFlags(statusFlags)
    {}

    TEvChunkForgetResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags, TString &errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkForgetResult &record) {
        TStringStream str;
        str << "{EvChunkForgetResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CHUNK READ
////////////////////////////////////////////////////////////////////////////
struct TEvChunkRead : TEventLocal<TEvChunkRead, TEvBlobStorage::EvChunkRead> {
    TChunkIdx ChunkIdx;
    ui32 Offset;
    ui32 Size;
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui8 PriorityClass;
    void *Cookie;

    TEvChunkRead(TOwner owner, TOwnerRound ownerRound, TChunkIdx chunkIdx, ui32 offset, ui32 size,
            ui8 priorityClass, void *cookie)
        : ChunkIdx(chunkIdx)
        , Offset(offset)
        , Size(size)
        , Owner(owner)
        , OwnerRound(ownerRound)
        , PriorityClass(priorityClass)
        , Cookie(cookie)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkRead &record) {
        TStringStream str;
        str << "{EvChunkRead chunkIdx# " << record.ChunkIdx;
        str << " Offset# " << record.Offset;
        str << " Size# " << record.Size;
        str << " ownerId# " << (i32)record.Owner;
        str << " ownerRound# " << record.OwnerRound;
        str << " PriorityClass# " << (i32)record.PriorityClass;
        str << " Cookie# " << (ui64)record.Cookie;
        str << "}";
        return str.Str();
    }
};

struct TEvChunkReadResult : TEventLocal<TEvChunkReadResult, TEvBlobStorage::EvChunkReadResult> {
    NKikimrProto::EReplyStatus Status;
    TChunkIdx ChunkIdx;
    ui32 Offset;
    TBufferWithGaps Data;
    void *Cookie;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    TEvChunkReadResult(NKikimrProto::EReplyStatus status, TChunkIdx chunkIdx, ui32 offset, void *cookie,
                        TStatusFlags statusFlags, TString errorReason)
        : Status(status)
        , ChunkIdx(chunkIdx)
        , Offset(offset)
        , Data(offset)
        , Cookie(cookie)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkReadResult &record) {
        TStringStream str;
        str << "{EvChunkReadres Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " chunkIdx# " << record.ChunkIdx;
        str << " Offset# " << record.Offset;
        str << " DataSize# " << record.Data.Size();
        str << " Cookie# " << (ui64)record.Cookie;
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CHUNK WRITE
////////////////////////////////////////////////////////////////////////////
struct TEvChunkWrite : TEventLocal<TEvChunkWrite, TEvBlobStorage::EvChunkWrite> {
    // this interface allows us to pass several memory parts for writing them continuously into chunk
    class IParts : public TThrRefBase {
    public:
        // TDataRef is a reference for continuous memory region to be written;
        // A special case is a pointer equal to nullptr, which means padding, i.e. write X zero bytes
        // A very special case is nullptr with Size = 0, which means nothing, just ignore such entries
        typedef std::pair<const void *, ui32> TDataRef;
        virtual ~IParts() {}
        virtual TDataRef operator[] (ui32) const = 0;
        virtual ui32 Size() const = 0; // returns number of elements in 'vector' addressable via [] operator
        virtual ui64 ByteSize() const {
            ui32 size = Size();
            ui64 byteSize = 0;
            for (ui32 i = 0; i < size; i++)
                byteSize += (operator [](i)).second;
            return byteSize;
        }
    };
    typedef TIntrusivePtr<IParts> TPartsPtr;

    struct TPart {
        const void *Data;
        ui32 Size;

        TPart(const void *data = nullptr, ui32 size = 0)
            : Data(data)
            , Size(size)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TPart Size# " << Size << " Data# " << (ui64)Data << "}";
            return str.Str();
        }
    };
    TChunkIdx ChunkIdx;
    ui32 Offset;
    TPartsPtr PartsPtr;
    void *Cookie;
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui8 PriorityClass;
    bool DoFlush;
    bool IsSeqWrite; // sequential write to this chunk (normally, it is 'true', for huge blobs -- 'false')

    mutable NLWTrace::TOrbit Orbit;

    ///////////////////// TNonOwningParts ///////////////////////////////
    class TNonOwningParts : public IParts {
        const TPart *Parts;
        ui32 PartsNum;
    public:
        TNonOwningParts(const TPart *parts, ui32 partsNum)
            : Parts(parts)
            , PartsNum(partsNum)
        {}

        virtual ~TNonOwningParts() {
        }

        TDataRef operator[] (ui32 index) const override {
            Y_VERIFY(index < PartsNum);
            const TPart &part = Parts[index];
            return TDataRef(part.Data, part.Size);
        }

        ui32 Size() const override {
            return PartsNum;
        }
    };

    ///////////////////// TBufBackedUpParts //////////////////////////////
    class TBufBackedUpParts : public IParts {
    public:
        TBufBackedUpParts(TTrackableBuffer &&buf)
            : Buffer(std::move(buf))
        {}

        virtual TDataRef operator[] (ui32 i) const override {
            Y_VERIFY_DEBUG(i == 0);
            return TDataRef(Buffer.Data(), Buffer.Size());
        }

        virtual ui32 Size() const override {
            return 1;
        }

    private:
        TTrackableBuffer Buffer;
    };

    ///////////////////// TAlignedParts //////////////////////////////
    class TAlignedParts : public IParts {
        TString Data;
        size_t FullSize;

    public:
        TAlignedParts(TString&& data)
            : Data(std::move(data))
            , FullSize(Data.size())
        {}

        TAlignedParts(TString&& data, size_t fullSize)
            : Data(std::move(data))
            , FullSize(fullSize)
        {
            Y_VERIFY_DEBUG(Data.size() <= FullSize);
        }

        virtual ui32 Size() const override {
            return Data.size() == FullSize ? 1 : 2;
        }

        virtual TDataRef operator [](ui32 index) const override {
            if (!index) {
                return std::make_pair(Data.data(), Data.size());
            } else {
                ui32 padding = FullSize - Data.size();
                Y_VERIFY_DEBUG(padding);
                return std::make_pair(nullptr, padding);
            }
        }
    };

    ///////////////////// TRopeAlignedParts //////////////////////////////
    class TRopeAlignedParts : public IParts {
        TRope Data; // we shall keep the rope here to prevent it from being freed
        TVector<TDataRef> Refs;

    public:
        TRopeAlignedParts(TRope&& data, size_t fullSize)
            : Data(std::move(data))
        {
            for (auto iter = Data.Begin(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
                Refs.emplace_back(iter.ContiguousData(), iter.ContiguousSize());
            }
            if (const size_t padding = fullSize - Data.GetSize()) {
                Refs.emplace_back(nullptr, padding);
            }
        }

        virtual ui32 Size() const override {
            return Refs.size();
        }

        virtual TDataRef operator [](ui32 index) const override {
            return Refs[index];
        }
    };


    TEvChunkWrite(TOwner owner, TOwnerRound ownerRound, TChunkIdx chunkIdx, ui32 offset, TPartsPtr partsPtr,
            void *cookie, bool doFlush, ui8 priorityClass, bool isSeqWrite = true)
        : ChunkIdx(chunkIdx)
        , Offset(offset)
        , PartsPtr(partsPtr)
        , Cookie(cookie)
        , Owner(owner)
        , OwnerRound(ownerRound)
        , PriorityClass(priorityClass)
        , DoFlush(doFlush)
        , IsSeqWrite(isSeqWrite)
    {
        Validate();
    }

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkWrite &record) {
        TStringStream str;
        str << "{EvChunkWrite chunkIdx# " << record.ChunkIdx;
        str << " Offset# " << record.Offset;
        if (record.PartsPtr) {
            str << " PartsPtr->Size()# " << record.PartsPtr->Size();
            if (record.PartsPtr->Size()) {
                for (ui32 partIdx = 0; partIdx < record.PartsPtr->Size(); ++partIdx) {
                    const IParts::TDataRef part = (*record.PartsPtr)[partIdx];
                    str << " {Size# " << part.second << " Data# " << (ui64)part.first << "}";
                }
            }
        }
        str << " Cookie# " << (ui64)record.Cookie;
        str << " ownerId# " << (ui32)record.Owner;
        str << " ownerRound# " << record.OwnerRound;
        str << " PriorityClass# " << (i32)record.PriorityClass;
        str << " DoFlush# " << (record.DoFlush ? "true" : "false");
        str << "}";
        return str.Str();
    }

    void Validate() const {
        const ui32 count = PartsPtr ? PartsPtr->Size() : 0;
        for (ui32 idx = 0; idx < count; ++idx) {
            Y_VERIFY((*PartsPtr)[idx].second);
            if ((*PartsPtr)[idx].first) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED((*PartsPtr)[idx].first, (*PartsPtr)[idx].second);
            }
        }
    }
};

struct TEvChunkWriteResult : TEventLocal<TEvChunkWriteResult, TEvBlobStorage::EvChunkWriteResult> {
    NKikimrProto::EReplyStatus Status;
    TChunkIdx ChunkIdx;
    TEvChunkWrite::TPartsPtr PartsPtr;
    void *Cookie;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    mutable NLWTrace::TOrbit Orbit;

    TEvChunkWriteResult(NKikimrProto::EReplyStatus status, TChunkIdx chunkIdx, void *cookie,
            TStatusFlags statusFlags, TString errorReason)
        : Status(status)
        , ChunkIdx(chunkIdx)
        , PartsPtr()
        , Cookie(cookie)
        , StatusFlags(statusFlags)
        , ErrorReason(std::move(errorReason))
    {}

    TEvChunkWriteResult(NKikimrProto::EReplyStatus status, TChunkIdx chunkIdx, TEvChunkWrite::TPartsPtr partsPtr,
                        void *cookie, TStatusFlags statusFlags, TString errorReason)
        : Status(status)
        , ChunkIdx(chunkIdx)
        , PartsPtr(partsPtr)
        , Cookie(cookie)
        , StatusFlags(statusFlags)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvChunkWriteResult &record) {
        TStringStream str;
        str << "{EvChunkWrite Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " chunkIdx# " << record.ChunkIdx;
        str << " Cookie# " << (ui64)record.Cookie;
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << "}";
        return str.Str();
    }
};

struct TEvChunkReadRaw : TEventLocal<TEvChunkReadRaw, TEvBlobStorage::EvChunkReadRaw> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    TChunkIdx ChunkIdx;
    ui32 Offset;
    ui32 Size;

    TEvChunkReadRaw(TOwner owner, TOwnerRound ownerRound, TChunkIdx chunkIdx, ui32 offset, ui32 size)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , ChunkIdx(chunkIdx)
        , Offset(offset)
        , Size(size)
    {}

    TString ToString() const {
        return TStringBuilder() << "{TEvChunkReadRaw Owner# " << Owner
            << " OwnerRound# " << OwnerRound
            << " ChunkIdx# " << ChunkIdx
            << " Offset# " << Offset
            << " Size# " << Size << "}";
    }
};

struct TEvChunkReadRawResult : TEventLocal<TEvChunkReadRawResult, TEvBlobStorage::EvChunkReadRawResult> {
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;
    TRope Data;

    TEvChunkReadRawResult(NKikimrProto::EReplyStatus status, TString errorReason)
        : Status(status)
        , ErrorReason(std::move(errorReason))
    {}

    TEvChunkReadRawResult(TRope&& data)
        : Status(NKikimrProto::OK)
        , Data(std::move(data))
    {}

    TString ToString() const {
        return TStringBuilder() << "{TEvChunkReadRawResult Status# " << NKikimrProto::EReplyStatus_Name(Status)
            << " ErrorReason# '" << ErrorReason << "'"
            << " Data.size# " << Data.size() << "}";
    }
};

struct TEvChunkWriteRaw : TEventLocal<TEvChunkWriteRaw, TEvBlobStorage::EvChunkWriteRaw> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    TChunkIdx ChunkIdx;
    ui32 Offset;
    TRope Data;

    TEvChunkWriteRaw(TOwner owner, TOwnerRound ownerRound, TChunkIdx chunkIdx, ui32 offset, TRope&& data)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , ChunkIdx(chunkIdx)
        , Offset(offset)
        , Data(std::move(data))
    {}

    TString ToString() const {
        return TStringBuilder() << "{TEvChunkWriteRaw Owner# " << Owner
            << " OwnerRound# " << OwnerRound
            << " ChunkIdx# " << ChunkIdx
            << " Offset# " << Offset
            << " Data.size# " << Data.size() << "}";
    }
};

struct TEvChunkWriteRawResult : TEventLocal<TEvChunkWriteRawResult, TEvBlobStorage::EvChunkWriteRawResult> {
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvChunkWriteRawResult(NKikimrProto::EReplyStatus status, TString errorReason)
        : Status(status)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        return TStringBuilder() << "{TEvChunkWriteRawResult Status# " << NKikimrProto::EReplyStatus_Name(Status)
            << " ErrorReason# '" << ErrorReason << "'" << "}";
    }
};

////////////////////////////////////////////////////////////////////////////
// DELETE OWNER AND ALL HIS DATA
////////////////////////////////////////////////////////////////////////////
struct TEvHarakiri : TEventLocal<TEvHarakiri, TEvBlobStorage::EvHarakiri> {
    TOwner Owner;
    TOwnerRound OwnerRound;

    TEvHarakiri(TOwner owner, TOwnerRound ownerRound)
        : Owner(owner)
        , OwnerRound(ownerRound)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvHarakiri &record) {
        TStringStream str;
        str << "{EvHarakiri ownerId# " << (ui32)record.Owner;
        str << " ownerRound# " << record.OwnerRound;
        str << "}";
        return str.Str();
    }
};

struct TEvHarakiriResult : TEventLocal<TEvHarakiriResult, TEvBlobStorage::EvHarakiriResult> {
    NKikimrProto::EReplyStatus Status;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    TEvHarakiriResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags, TString errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvHarakiriResult &record) {
        TStringStream str;
        str << "{EvHarakiriResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << "}";
        return str.Str();
    }
};

struct TEvSlay : TEventLocal<TEvSlay, TEvBlobStorage::EvSlay> {
    TVDiskID VDiskId;
    TOwnerRound SlayOwnerRound;
    ui32 PDiskId;
    ui32 VSlotId;

    TEvSlay(TVDiskID vDiskId, TOwnerRound slayOwnerRound, ui32 pDiskId, ui32 vSlotId)
        : VDiskId(vDiskId)
        , SlayOwnerRound(slayOwnerRound)
        , PDiskId(pDiskId)
        , VSlotId(vSlotId)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvSlay &record) {
        TStringStream str;
        str << "{EvSlay VDiskId# " << record.VDiskId.ToString();
        str << " Slay ownerRound# " << record.SlayOwnerRound;
        str << " PDiskId " << record.PDiskId;
        str << " VSlotId " << record.VSlotId;
        str << "}";
        return str.Str();
    }
};

struct TEvSlayResult : TEventLocal<TEvSlayResult, TEvBlobStorage::EvSlayResult> {
    NKikimrProto::EReplyStatus Status;  // OK => VDisk is no more
                                        // ERROR => Something bad has happened, see ErrorReason
                                        // NOTREADY => PDisk is still initializing, see ErrorReason
                                        // ALREADY => VDisk is not registered at all
                                        // RACE => SlayOwnerRound is not large enough
    TStatusFlags StatusFlags;
    TVDiskID VDiskId;
    TOwnerRound SlayOwnerRound;
    ui32 PDiskId;
    ui32 VSlotId;
    TString ErrorReason;

    TEvSlayResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags, TVDiskID vDiskId,
            TOwnerRound slayOwnerRound, ui32 pDiskId, ui32 vSlotId, TString errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , VDiskId(vDiskId)
        , SlayOwnerRound(slayOwnerRound)
        , PDiskId(pDiskId)
        , VSlotId(vSlotId)
        , ErrorReason(errorReason)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvSlayResult &record) {
        TStringStream str;
        str << "{EvSlayResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " StatusFlags# " << StatusFlagsToString(record.StatusFlags);
        str << " VDiskId# " << record.VDiskId.ToString();
        str << " Slay ownerRound# " << record.SlayOwnerRound;
        str << " PDiskId " << record.PDiskId;
        str << " VSlotId " << record.VSlotId;
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CHECK DISK SPACE
////////////////////////////////////////////////////////////////////////////
struct TEvCheckSpace : TEventLocal<TEvCheckSpace, TEvBlobStorage::EvCheckSpace> {
    TOwner Owner;
    TOwnerRound OwnerRound;

    TEvCheckSpace(TOwner owner, TOwnerRound ownerRound)
        : Owner(owner)
        , OwnerRound(ownerRound)
    {}

    TString ToString() const {
        TStringStream str;
        str << "{TEvCheckSpace ownerId# " << (ui32)Owner;
        str << " ownerRound# " << OwnerRound;
        str << "}";
        return str.Str();
    }
};

struct TEvCheckSpaceResult : TEventLocal<TEvCheckSpaceResult, TEvBlobStorage::EvCheckSpaceResult> {
    NKikimrProto::EReplyStatus Status;
    TStatusFlags StatusFlags;
    ui32 FreeChunks; // contains SharedQuota.Free
    ui32 TotalChunks; // contains OwnerQuota.HardLimit(owner), Total != Free + Used
    ui32 UsedChunks; // equals OwnerQuota.Used(owner) - a number of chunks allocated by requesting owner
    ui32 NumSlots; // number of VDisks over PDisk, not their weight
    ui32 NumActiveSlots; // sum of VDisks weights - $ \sum_i{ceil(VSlot[i].GroupSizeInUnits / PDisk.SlotSizeInUnits)} $
    double NormalizedOccupancy = 0;
    double VDiskSlotUsage = 0;  // 100.0 * Owner.Used / Owner.LightYellowLimit
    double VDiskRawUsage = 0;  // 100.0 * Owner.Used / Owner.HardLimit
    double PDiskUsage = 0;  // 100.0 * SharedQuota.Used / SharedQuota.HardLimit
    TString ErrorReason;
    TStatusFlags LogStatusFlags;

    TEvCheckSpaceResult(
            NKikimrProto::EReplyStatus status,
            TStatusFlags statusFlags,
            ui32 freeChunks,
            ui32 totalChunks,
            ui32 usedChunks,
            ui32 numSlots,
            ui32 numActiveSlots,
            TString errorReason,
            TStatusFlags logStatusFlags = {})
        : Status(status)
        , StatusFlags(statusFlags)
        , FreeChunks(freeChunks)
        , TotalChunks(totalChunks)
        , UsedChunks(usedChunks)
        , NumSlots(numSlots)
        , NumActiveSlots(numActiveSlots)
        , ErrorReason(std::move(errorReason))
        , LogStatusFlags(logStatusFlags)
    {}

    TString ToString() const {
        TStringStream str;
        str << "{TEvCheckSpaceResult Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
        str << " StatusFlags# " << StatusFlagsToString(StatusFlags);
        str << " FreeChunks# " << FreeChunks;
        str << " TotalChunks# " << TotalChunks;
        str << " UsedChunks# " << UsedChunks;
        str << " NumSlots# " << NumSlots;
        str << " NumActiveSlots# " << NumActiveSlots;
        str << " ErrorReason# \"" << ErrorReason << "\"";
        str << " LogStatusFlags# " << StatusFlagsToString(LogStatusFlags);
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CONFIGURE SCHEDULER
////////////////////////////////////////////////////////////////////////////
struct TEvConfigureScheduler : TEventLocal<TEvConfigureScheduler, TEvBlobStorage::EvConfigureScheduler> {
    TOwner Owner;
    TOwnerRound OwnerRound;

    TPDiskSchedulerConfig SchedulerCfg;

    TEvConfigureScheduler(TOwner owner, TOwnerRound ownerRound)
        : Owner(owner)
        , OwnerRound(ownerRound)
    {}

    TString ToString() const {
        TStringStream str;
        str << "{TEvConfigureScheduler ownerId# " << (ui32)Owner;
        str << " ownerRound# " << OwnerRound;
        str << " SchedulerCfg# " << SchedulerCfg.ToString(false);
        str << "}";
        return str.Str();
    }
};

struct TEvConfigureSchedulerResult :
        TEventLocal<TEvConfigureSchedulerResult, TEvBlobStorage::EvConfigureSchedulerResult> {
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvConfigureSchedulerResult(NKikimrProto::EReplyStatus status, TString errorReason)
        : Status(status)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        TStringStream str;
        str << "{TEvConfigureSchedulerResult Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
        str << " ErrorReason# \"" << ErrorReason << "\"";
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CONTROL
////////////////////////////////////////////////////////////////////////////
struct TEvYardControl : TEventLocal<TEvYardControl, TEvBlobStorage::EvYardControl> {
    enum EAction {
        ActionPause = 0,
        ActionStep = 1,
        ActionResume = 2,
        Brake = 3,
        PDiskStop = 4,
        // If pdisk is working now successfull responce will be sent immediately
        // Else responce will be sent only when PDisk is fully initialized or come in error state
        PDiskStart = 5,
        // Return pointer to TPDisk instance in TEvYardControlResult::Cookie
        GetPDiskPointer = 6,
    };

    ui32 Action;
    void *Cookie;

    TEvYardControl(EAction action, void* cookie)
        : Action(action)
        , Cookie(cookie)
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvYardControl &record) {
        TStringStream str;
        str << "{EvYardControl Action# " << record.Action;
        str << " Cookie# " << (ui64)record.Cookie;
        str << "}";
        return str.Str();
    }
};

struct TEvYardControlResult : TEventLocal<TEvYardControlResult, TEvBlobStorage::EvYardControlResult> {
    NKikimrProto::EReplyStatus Status;
    void *Cookie;
    TString ErrorReason;

    TEvYardControlResult(NKikimrProto::EReplyStatus status, void *cookie, TString errorReason)
        : Status(status)
        , Cookie(cookie)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        return ToString(*this);
    }

    static TString ToString(const TEvYardControlResult &record) {
        TStringStream str;
        str << "{EvYardControlResult Status# " << NKikimrProto::EReplyStatus_Name(record.Status).data();
        str << " ErrorReason# \"" << record.ErrorReason << "\"";
        str << " Cookie# " << (ui64)record.Cookie;
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CUTLOG
// Yard (PDisk) sends this message to VDisk to ask for log cut
////////////////////////////////////////////////////////////////////////////
struct TEvCutLog : TEventLocal<TEvCutLog, TEvBlobStorage::EvCutLog> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui64 FreeUpToLsn; // excluding this lsn

    // The following values are all set to 0 at ydb/core/blobstorage/vdisk/common/vdisk_recoverylogwriter.cpp : 141
    i64 LogSizeChunks; // Total number of chunks used for logs
    i64 OwnedLogChunks; // Number of log chunks held by the owner
    i64 YellowZoneChunks; // Stop logging for non-compaction purposes (even if it is not your fault), cut the log
    i64 RedZoneChunks; // PDisk will kill the process at this threshold

    TEvCutLog(TOwner owner, TOwnerRound ownerRound, ui64 lsn,
            i64 logSizeChunks, i64 ownedLogChunks, i64 yellowZoneChunks, i64 redZoneChunks)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , FreeUpToLsn(lsn)
        , LogSizeChunks(logSizeChunks)
        , OwnedLogChunks(ownedLogChunks)
        , YellowZoneChunks(yellowZoneChunks)
        , RedZoneChunks(redZoneChunks)
    {}

    TEvCutLog *Clone() const {
        return new TEvCutLog(Owner, OwnerRound, FreeUpToLsn,
                LogSizeChunks, OwnedLogChunks, YellowZoneChunks, RedZoneChunks);
    }

    TString ToString() const {
        TStringStream str;
        str << "{EvCutLogFromPDisk ownerId# " << (ui32)Owner
            << " ownerRound# " << OwnerRound
            << " FreeUpToLsn# " << (ui64)FreeUpToLsn
            << " LogSizeChunks# " << LogSizeChunks
            << " OwnedLogChunks# " << OwnedLogChunks
            << " YellowZoneChunks# " << YellowZoneChunks
            << " RedZoneChunks# " << RedZoneChunks
            << "}";
        return str.Str();
    }
};

struct TEvAskForCutLog : TEventLocal<TEvAskForCutLog, TEvBlobStorage::EvAskForCutLog> {
    TOwner Owner;
    TOwnerRound OwnerRound;

    TEvAskForCutLog(TOwner owner, TOwnerRound ownerRound)
        : Owner(owner)
        , OwnerRound(ownerRound)
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvAskForCutLog ownerId# " << (ui32)Owner;
        str << " ownerRound# " << OwnerRound;
        str << "}";
        return str.Str();
    }
};

struct TEvReadMetadata : TEventLocal<TEvReadMetadata, TEvBlobStorage::EvReadMetadata> {};

struct TEvReadMetadataResult : TEventLocal<TEvReadMetadataResult, TEvBlobStorage::EvReadMetadataResult> {
    EPDiskMetadataOutcome Outcome;
    TRcBuf Metadata;
    std::optional<ui64> PDiskGuid;

    TEvReadMetadataResult(EPDiskMetadataOutcome outcome, std::optional<ui64> pdiskGuid)
        : Outcome(outcome)
        , PDiskGuid(pdiskGuid)
    {}

    TEvReadMetadataResult(TRcBuf&& metadata, std::optional<ui64> pdiskGuid)
        : Outcome(EPDiskMetadataOutcome::OK)
        , Metadata(std::move(metadata))
        , PDiskGuid(pdiskGuid)
    {}
};

struct TEvWriteMetadata : TEventLocal<TEvWriteMetadata, TEvBlobStorage::EvWriteMetadata> {
    TRcBuf Metadata;

    TEvWriteMetadata(TRcBuf&& metadata)
        : Metadata(std::move(metadata))
    {}
};

struct TEvWriteMetadataResult : TEventLocal<TEvWriteMetadataResult, TEvBlobStorage::EvWriteMetadataResult> {
    EPDiskMetadataOutcome Outcome;
    std::optional<ui64> PDiskGuid;

    TEvWriteMetadataResult(EPDiskMetadataOutcome outcome, std::optional<ui64> pdiskGuid)
        : Outcome(outcome)
        , PDiskGuid(pdiskGuid)
    {}
};

// NodeWarden sends this message to PDisk to ask for shredding
struct TEvShredPDisk : TEventLocal<TEvShredPDisk, TEvBlobStorage::EvShredPDisk> {
    ui64 ShredGeneration;

    TEvShredPDisk(ui64 shredGeneration)
        : ShredGeneration(shredGeneration)
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvShredPDisk ShredGeneration# " << ShredGeneration << "}";
        return str.Str();
    }
};

// PDisk sends this message to NodeWarden when shredding is complete
struct TEvShredPDiskResult : TEventLocal<TEvShredPDiskResult, TEvBlobStorage::EvShredPDiskResult> {
    NKikimrProto::EReplyStatus Status;
    ui64 ShredGeneration;
    TString ErrorReason;

    TEvShredPDiskResult(NKikimrProto::EReplyStatus status, ui64 shredGeneration, TString errorReason)
        : Status(status)
        , ShredGeneration(shredGeneration)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvShredPDiskResult ShredGeneration# " << ShredGeneration
            << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data()
            << " ErrorReason# \"" << ErrorReason << "\"}";
        return str.Str();
    }
};

// PDisk sends this message to VDisk to ask for full compaction before shredding
struct TEvPreShredCompactVDisk : TEventLocal<TEvPreShredCompactVDisk, TEvBlobStorage::EvPreShredCompactVDisk> {
    ui64 ShredGeneration;

    TEvPreShredCompactVDisk(ui64 shredGeneration)
        : ShredGeneration(shredGeneration)
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvPreShredCompactVDisk ShredGeneration# " << ShredGeneration << "}";
        return str.Str();
    }
};

// VDisk sends this message to PDisk when pre-shred compaction is complete
struct TEvPreShredCompactVDiskResult : TEventLocal<TEvPreShredCompactVDiskResult, TEvBlobStorage::EvPreShredCompactVDiskResult> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui64 ShredGeneration;
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvPreShredCompactVDiskResult(TOwner owner, TOwnerRound ownerRound, ui64 shredGeneration, NKikimrProto::EReplyStatus status, TString errorReason)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , ShredGeneration(shredGeneration)
        , Status(status)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvPreShredCompactVDiskResult OwnerId# " << (ui32)Owner
            << " OwnerRound# " << OwnerRound
            << " ShredGeneration# " << ShredGeneration
            << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data()
            << " ErrorReason# \"" << ErrorReason << "\"}";
        return str.Str();
    }
};

// PDisk sends this message to VDisk to ask it to move all the data needed away from the chunks that are being shredded
struct TEvShredVDisk : TEventLocal<TEvShredVDisk, TEvBlobStorage::EvShredVDisk> {
    ui64 ShredGeneration;
    std::vector<TChunkIdx> ChunksToShred;

    TEvShredVDisk(ui64 shredGeneration, std::vector<TChunkIdx> chunksToShred)
        : ShredGeneration(shredGeneration)
        , ChunksToShred(std::move(chunksToShred))
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvShredVDisk ShredGeneration# " << ShredGeneration << "}";
        str << " ChunksToShred# ";
        FormatList(str, ChunksToShred);
        str << "}";
        return str.Str();
    }
};

// VDisk sends this message to PDisk when the data has been moved away from the chunks that are being shredded
struct TEvShredVDiskResult : TEventLocal<TEvShredVDiskResult, TEvBlobStorage::EvShredVDiskResult> {
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui64 ShredGeneration;
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvShredVDiskResult(TOwner owner, TOwnerRound ownerRound, ui64 shredGeneration, NKikimrProto::EReplyStatus status, TString errorReason)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , ShredGeneration(shredGeneration)
        , Status(status)
        , ErrorReason(std::move(errorReason))
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvShredVDiskResult OwnerId# " << (ui32)Owner
            << " OwnerRound# " << OwnerRound
            << " ShredGeneration# " << ShredGeneration
            << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data()
            << " ErrorReason# \"" << ErrorReason << "\"}";
        return str.Str();
    }
};

// PDisk sends this message to itself
struct TEvContinueShred : TEventLocal<TEvContinueShred, TEvBlobStorage::EvContinueShred> {
    TEvContinueShred()
    {}

    TString ToString() const {
        TStringStream str;
        str << "{EvContinueShred ";
        str << "}";
        return str.Str();
    }
};


/*
 * One common context in the PDisk's world.
 * It should only contain things that are used in each of the PDisk's component.
 * Since it is used from multiple threads it's build once
 * in TPDiskActor::Boorstrap and never changed
 */
struct TPDiskCtx {
    TActorSystem * const ActorSystem = nullptr;
    const ui32 PDiskId = 0;
    const TActorId PDiskActor;
    const TString PDiskLogPrefix;
    // TPDiskMon * const Mon = nullptr; TODO implement it

    TPDiskCtx() = default;

    TPDiskCtx(TActorSystem *actorSystem)
        : ActorSystem(actorSystem)
    {}

    TPDiskCtx(TActorSystem *actorSystem, ui32 pdiskId, TActorId pdiskActor)
        : ActorSystem(actorSystem)
        , PDiskId(pdiskId)
        , PDiskActor(pdiskActor)
        , PDiskLogPrefix(Sprintf("PDiskId# %" PRIu32 " ", PDiskId))
    {}
};

#define P_LOG(LEVEL, MARKER, ...) \
    do { \
        if (PCtx && PCtx->ActorSystem) { \
            STLOGX(*PCtx->ActorSystem, LEVEL, BS_PDISK, MARKER, __VA_ARGS__, (PDiskId, PCtx->PDiskId)); \
        } \
    } while (false)

#define S_LOG(LEVEL, MARKER, ...) \
    do { \
        if (PCtx && PCtx->ActorSystem) { \
            STLOGX(*PCtx->ActorSystem, LEVEL, BS_PDISK_SHRED, MARKER, __VA_ARGS__, (PDiskId, PCtx->PDiskId)); \
        } \
    } while (false)

} // NPDisk
} // NKikimr
