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
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/map.h>


namespace NKikimr {

IActor* CreatePDisk(const TIntrusivePtr<TPDiskConfig> &cfg, const NPDisk::TMainKey &mainKey,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

namespace NPDisk {

struct TCommitRecord {
    ui64 FirstLsnToKeep = 0; // 0 == not set
    TVector<TChunkIdx> CommitChunks;
    TVector<TChunkIdx> DeleteChunks;
    bool IsStartingPoint = false;
    bool DeleteToDecommitted = false; // 1 == set chunks to Decommitted state that requires a ChunkForget event or a restart
    // the value of DeleteToDecommitted is not stored as a part of the commit record.

    void ValidateChunks(TVector<TChunkIdx> &chunks) {
        if (chunks.size()) {
            Sort(chunks.begin(), chunks.end());
            TChunkIdx lastIdx = chunks[0];
            for (size_t i = 1; i < chunks.size(); ) {
                if (chunks[i] == lastIdx) {
                    //Y_ABORT_UNLESS(false);
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
struct TEvYardInit : public TEventLocal<TEvYardInit, TEvBlobStorage::EvYardInit> {
    TOwnerRound OwnerRound;
    TVDiskID VDisk;
    ui64 PDiskGuid;
    TActorId CutLogID; // ask this actor about log cut
    TActorId WhiteboardProxyId;
    ui32 SlotId;

    TEvYardInit(TOwnerRound ownerRound, const TVDiskID &vdisk, ui64 pDiskGuid,
            const TActorId &cutLogID = TActorId(), const TActorId& whiteboardProxyId = {},
            ui32 slotId = Max<ui32>())
        : OwnerRound(ownerRound)
        , VDisk(vdisk)
        , PDiskGuid(pDiskGuid)
        , CutLogID(cutLogID)
        , WhiteboardProxyId(whiteboardProxyId)
        , SlotId(slotId)
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
        str << "}";
        return str.Str();
    }
};

struct TEvYardInitResult : public TEventLocal<TEvYardInitResult, TEvBlobStorage::EvYardInitResult> {
    NKikimrProto::EReplyStatus Status;
    TMap<TLogSignature, TLogRecord> StartingPoints;
    TStatusFlags StatusFlags;
    TIntrusivePtr<TPDiskParams> PDiskParams;
    TVector<TChunkIdx> OwnedChunks;  // Sorted vector of owned chunk identifiers.
    TString ErrorReason;

    TEvYardInitResult(const NKikimrProto::EReplyStatus status, const TString &errorReason)
        : Status(status)
        , StatusFlags(0)
        , PDiskParams(new TPDiskParams(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, DEVICE_TYPE_ROT))
        , ErrorReason(errorReason)
    {
        Y_ABORT_UNLESS(status != NKikimrProto::OK, "Single-parameter constructor is for error responses only");
    }

    TEvYardInitResult(NKikimrProto::EReplyStatus status, ui64 seekTimeUs, ui64 readSpeedBps,
            ui64 writeSpeedBps, ui64 readBlockSize, ui64 writeBlockSize,
            ui64 bulkWriteBlockSize, ui32 chunkSize, ui32 appendBlockSize,
            TOwner owner, TOwnerRound ownerRound, TStatusFlags statusFlags, TVector<TChunkIdx> ownedChunks,
            EDeviceType trueMediaType, const TString &errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , PDiskParams(new TPDiskParams(
                    owner,
                    ownerRound,
                    chunkSize,
                    appendBlockSize,
                    seekTimeUs,
                    readSpeedBps,
                    writeSpeedBps,
                    readBlockSize,
                    writeBlockSize,
                    bulkWriteBlockSize,
                    trueMediaType))
        , OwnedChunks(std::move(ownedChunks))
        , ErrorReason(errorReason)
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
        str << "}";
        return str.Str();
    }
};

struct TEvLogResult;

////////////////////////////////////////////////////////////////////////////
// LOG
////////////////////////////////////////////////////////////////////////////
struct TEvLog : public TEventLocal<TEvLog, TEvBlobStorage::EvLog> {
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
        Y_ABORT_UNLESS(Owner);
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

struct TEvMultiLog : public TEventLocal<TEvMultiLog, TEvBlobStorage::EvMultiLog> {
    void AddLog(THolder<TEvLog> &&ev, NWilson::TTraceId traceId = {}) {
        Logs.emplace_back(std::move(ev), std::move(traceId));
        auto &log = *Logs.back().Event;
        if (Logs.size() == 1) {
            LsnSeg = TLsnSeg(log.LsnSegmentStart, log.Lsn);
        } else {
            Y_VERIFY_S(LsnSeg.Last + 1 == log.LsnSegmentStart, "LastLsn# " << LsnSeg.Last <<
                                                               " NewLsnStart# " << log.LsnSegmentStart);
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

struct TEvLogResult : public TEventLocal<TEvLogResult, TEvBlobStorage::EvLogResult> {
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

    TEvLogResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags, const TString &errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
    {}
};

////////////////////////////////////////////////////////////////////////////
// READ LOG
////////////////////////////////////////////////////////////////////////////
struct TEvReadLog : public TEventLocal<TEvReadLog, TEvBlobStorage::EvReadLog> {
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

struct TEvReadLogResult : public TEventLocal<TEvReadLogResult, TEvBlobStorage::EvReadLogResult> {
    typedef TVector<TLogRecord> TResults;
    TResults Results;
    NKikimrProto::EReplyStatus Status;
    TLogPosition Position;
    TLogPosition NextPosition;
    bool IsEndOfLog;
    TStatusFlags StatusFlags;
    TString ErrorReason;
    TOwner Owner;

    TEvReadLogResult(NKikimrProto::EReplyStatus status, TLogPosition position, TLogPosition nextPosition,
            bool isEndOfLog, TStatusFlags statusFlags, const TString &errorReason, TOwner owner)
        : Status(status)
        , Position(position)
        , NextPosition(nextPosition)
        , IsEndOfLog(isEndOfLog)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
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
struct TEvChunkLock : public TEventLocal<TEvChunkLock, TEvBlobStorage::EvChunkLock> {
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
        Y_DEBUG_ABORT_UNLESS(from != ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkLock(ELockFrom from, TOwner owner, ui32 count, NKikimrBlobStorage::TPDiskSpaceColor::E color)
        : LockFrom(from)
        , ByVDiskId(false)
        , Owner(owner)
        , Count(count)
        , Color(color)
    {
        Y_DEBUG_ABORT_UNLESS(from == ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkLock(ELockFrom from, TVDiskID vdiskId, bool isGenerationSet, ui32 count, NKikimrBlobStorage::TPDiskSpaceColor::E color)
        : LockFrom(from)
        , ByVDiskId(true)
        , VDiskId(vdiskId)
        , IsGenerationSet(isGenerationSet)
        , Count(count)
        , Color(color)
    {
        Y_DEBUG_ABORT_UNLESS(from == ELockFrom::PERSONAL_QUOTA);
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

struct TEvChunkLockResult : public TEventLocal<TEvChunkLockResult, TEvBlobStorage::EvChunkLockResult> {
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
struct TEvChunkUnlock : public TEventLocal<TEvChunkUnlock, TEvBlobStorage::EvChunkUnlock> {
    TEvChunkLock::ELockFrom LockFrom;
    bool ByVDiskId = true;
    TOwner Owner = 0;
    TVDiskID VDiskId = TVDiskID::InvalidId;
    bool IsGenerationSet = true;

    TEvChunkUnlock(TEvChunkLock::ELockFrom lockFrom)
        : LockFrom(lockFrom)
    {
        Y_DEBUG_ABORT_UNLESS(LockFrom != TEvChunkLock::ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkUnlock(TEvChunkLock::ELockFrom lockFrom, TOwner owner)
        : LockFrom(lockFrom)
        , ByVDiskId(false)
        , Owner(owner)
    {
        Y_DEBUG_ABORT_UNLESS(LockFrom == TEvChunkLock::ELockFrom::PERSONAL_QUOTA);
    }

    TEvChunkUnlock(TEvChunkLock::ELockFrom lockFrom, TVDiskID vdiskId, bool isGenerationSet)
        : LockFrom(lockFrom)
        , ByVDiskId(true)
        , VDiskId(vdiskId)
        , IsGenerationSet(isGenerationSet)
    {
        Y_DEBUG_ABORT_UNLESS(LockFrom == TEvChunkLock::ELockFrom::PERSONAL_QUOTA);
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

struct TEvChunkUnlockResult : public TEventLocal<TEvChunkUnlockResult, TEvBlobStorage::EvChunkUnlockResult> {
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
struct TEvChunkReserve : public TEventLocal<TEvChunkReserve, TEvBlobStorage::EvChunkReserve> {
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

struct TEvChunkReserveResult : public TEventLocal<TEvChunkReserveResult, TEvBlobStorage::EvChunkReserveResult> {
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
struct TEvChunkForget : public TEventLocal<TEvChunkForget, TEvBlobStorage::EvChunkForget> {
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

struct TEvChunkForgetResult : public TEventLocal<TEvChunkForgetResult, TEvBlobStorage::EvChunkForgetResult> {
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
struct TEvChunkRead : public TEventLocal<TEvChunkRead, TEvBlobStorage::EvChunkRead> {
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

struct TEvChunkReadResult : public TEventLocal<TEvChunkReadResult, TEvBlobStorage::EvChunkReadResult> {
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
struct TEvChunkWrite : public TEventLocal<TEvChunkWrite, TEvBlobStorage::EvChunkWrite> {
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
            Y_ABORT_UNLESS(index < PartsNum);
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
            : Buffers({std::move(buf)})
        {}

        virtual TDataRef operator[] (ui32 i) const override {
            Y_DEBUG_ABORT_UNLESS(i < Buffers.size());
            return TDataRef(Buffers[i].Data(), Buffers[i].Size());
        }

        virtual ui32 Size() const override {
            return Buffers.size();
        }

        void AppendBuffer(TTrackableBuffer&& buffer) {
            Buffers.push_back(std::move(buffer));
        }

    private:
        TVector<TTrackableBuffer> Buffers;
    };

    ///////////////////// TStrokaBackedUpParts //////////////////////////////
    class TStrokaBackedUpParts : public IParts {
    public:
        TStrokaBackedUpParts(TString &buf)
            : Buf()
        {
            Buf.swap(buf);
        }

        virtual TDataRef operator[] (ui32 i) const override {
            Y_DEBUG_ABORT_UNLESS(i == 0);
            return TDataRef(Buf.data(), (ui32)Buf.size());
        }

        virtual ui32 Size() const override {
            return 1;
        }

    private:
        TString Buf;
    };

    ///////////////////// TAlignedParts //////////////////////////////
    class TAlignedParts : public IParts {
        TString Data;
        size_t FullSize;

    public:
        TAlignedParts(TString&& data, size_t fullSize)
            : Data(std::move(data))
            , FullSize(fullSize)
        {
            Y_DEBUG_ABORT_UNLESS(Data.size() <= FullSize);
        }

        virtual ui32 Size() const override {
            return Data.size() == FullSize ? 1 : 2;
        }

        virtual TDataRef operator [](ui32 index) const override {
            if (!index) {
                return std::make_pair(Data.data(), Data.size());
            } else {
                ui32 padding = FullSize - Data.size();
                Y_DEBUG_ABORT_UNLESS(padding);
                return std::make_pair(nullptr, padding);
            }
        }
    };

    ///////////////////// TAlignedParts //////////////////////////////
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
            Y_ABORT_UNLESS((*PartsPtr)[idx].second);
            if ((*PartsPtr)[idx].first) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED((*PartsPtr)[idx].first, (*PartsPtr)[idx].second);
            }
        }
    }
};

struct TEvChunkWriteResult : public TEventLocal<TEvChunkWriteResult, TEvBlobStorage::EvChunkWriteResult> {
    NKikimrProto::EReplyStatus Status;
    TChunkIdx ChunkIdx;
    TEvChunkWrite::TPartsPtr PartsPtr;
    void *Cookie;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    mutable NLWTrace::TOrbit Orbit;

    TEvChunkWriteResult(NKikimrProto::EReplyStatus status, TChunkIdx chunkIdx, void *cookie,
            TStatusFlags statusFlags, const TString &errorReason)
        : Status(status)
        , ChunkIdx(chunkIdx)
        , PartsPtr()
        , Cookie(cookie)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
    {}

    TEvChunkWriteResult(NKikimrProto::EReplyStatus status, TChunkIdx chunkIdx, TEvChunkWrite::TPartsPtr partsPtr,
                        void *cookie, TStatusFlags statusFlags, const TString &errorReason)
        : Status(status)
        , ChunkIdx(chunkIdx)
        , PartsPtr(partsPtr)
        , Cookie(cookie)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
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

////////////////////////////////////////////////////////////////////////////
// DELETE OWNER AND ALL HIS DATA
////////////////////////////////////////////////////////////////////////////
struct TEvHarakiri : public TEventLocal<TEvHarakiri, TEvBlobStorage::EvHarakiri> {
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

struct TEvHarakiriResult : public TEventLocal<TEvHarakiriResult, TEvBlobStorage::EvHarakiriResult> {
    NKikimrProto::EReplyStatus Status;
    TStatusFlags StatusFlags;
    TString ErrorReason;

    TEvHarakiriResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags, const TString &errorReason)
        : Status(status)
        , StatusFlags(statusFlags)
        , ErrorReason(errorReason)
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

struct TEvSlay : public TEventLocal<TEvSlay, TEvBlobStorage::EvSlay> {
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

struct TEvSlayResult : public TEventLocal<TEvSlayResult, TEvBlobStorage::EvSlayResult> {
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
struct TEvCheckSpace : public TEventLocal<TEvCheckSpace, TEvBlobStorage::EvCheckSpace> {
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

struct TEvCheckSpaceResult : public TEventLocal<TEvCheckSpaceResult, TEvBlobStorage::EvCheckSpaceResult> {
    NKikimrProto::EReplyStatus Status;
    TStatusFlags StatusFlags;
    ui32 FreeChunks;
    ui32 TotalChunks; // contains common limit in shared free space mode, Total != Free + Used
    ui32 UsedChunks; // number of chunks allocated by requesting owner
    ui32 NumSlots; // number of VSlots over PDisk
    double Occupancy = 0;
    TString ErrorReason;
    TStatusFlags LogStatusFlags;

    TEvCheckSpaceResult(NKikimrProto::EReplyStatus status, TStatusFlags statusFlags, ui32 freeChunks,
            ui32 totalChunks, ui32 usedChunks, ui32 numSlots, const TString &errorReason,
            TStatusFlags logStatusFlags = {})
        : Status(status)
        , StatusFlags(statusFlags)
        , FreeChunks(freeChunks)
        , TotalChunks(totalChunks)
        , UsedChunks(usedChunks)
        , NumSlots(numSlots)
        , ErrorReason(errorReason)
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
        str << " ErrorReason# \"" << ErrorReason << "\"";
        str << " LogStatusFlags# " << StatusFlagsToString(LogStatusFlags);
        str << "}";
        return str.Str();
    }
};

////////////////////////////////////////////////////////////////////////////
// CONFIGURE SCHEDULER
////////////////////////////////////////////////////////////////////////////
struct TEvConfigureScheduler : public TEventLocal<TEvConfigureScheduler, TEvBlobStorage::EvConfigureScheduler> {
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
        public TEventLocal<TEvConfigureSchedulerResult, TEvBlobStorage::EvConfigureSchedulerResult> {
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvConfigureSchedulerResult(NKikimrProto::EReplyStatus status, const TString &errorReason)
        : Status(status)
        , ErrorReason(errorReason)
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
struct TEvYardControl : public TEventLocal<TEvYardControl, TEvBlobStorage::EvYardControl> {
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

struct TEvYardControlResult : public TEventLocal<TEvYardControlResult, TEvBlobStorage::EvYardControlResult> {
    NKikimrProto::EReplyStatus Status;
    void *Cookie;
    TString ErrorReason;

    TEvYardControlResult(NKikimrProto::EReplyStatus status, void *cookie, const TString &errorReason)
        : Status(status)
        , Cookie(cookie)
        , ErrorReason(errorReason)
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
struct TEvCutLog : public TEventLocal<TEvCutLog, TEvBlobStorage::EvCutLog> {
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

struct TEvAskForCutLog : public TEventLocal<TEvAskForCutLog, TEvBlobStorage::EvAskForCutLog> {
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

} // NPDisk
} // NKikimr

