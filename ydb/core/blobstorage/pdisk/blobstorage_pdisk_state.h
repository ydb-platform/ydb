#pragma once
#include "defs.h"

#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_logreader_base.h"
#include "blobstorage_pdisk_tools.h"

#include <ydb/core/util/metrics.h>
#include <ydb/core/debug_tools/operation_log.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////
// PDisk In-memory structures
////////////////////////////////////////////////////////////////////////////

enum class EInitPhase {
    Uninitialized,
    ReadingSysLog,
    ReadingLog,
    Initialized,
};

enum EOwner {
    OwnerSystem = 0, // Chunk0, SysLog chunks and CommonLog + just common log tracking, mens "for dynamic" in requests
    OwnerUnallocated = 1, // Unallocated chunks, Trim scheduling, Slay commands
    OwnerBeginUser = 2,
    OwnerEndUser = 241,
    OwnerMetadata = 250, // Metadata chunks, the real owner
    OwnerSystemLog = 251, // Not used to actually mark chunks, just for space tracking
    OwnerSystemReserve = 252, // Not used to actually mark chunks, just for space tracking, means "for static" in requests
    OwnerCommonStaticLog = 253, // Not used to actually mark chunks, just for space tracking
    OwnerUnallocatedTrimmed = 254, // Because of forward compatibility may not be written to disk
    OwnerLocked = 255,
    OwnerCount = 256
};

inline bool IsOwnerAllocated(TOwner owner) {
    return owner != OwnerUnallocated && owner != OwnerUnallocatedTrimmed;
}

inline bool IsOwnerUser(TOwner owner) {
    return OwnerBeginUser <= owner && owner < OwnerEndUser;
}

struct TOwnerInflight : TThrRefBase {
    std::atomic<i64> ChunkWrites = 0;
    std::atomic<i64> ChunkReads = 0;
    std::atomic<i64> LogWrites = 0;
};

struct TOwnerData {
    enum EVDiskStatus {
        VDISK_STATUS_DEFAULT = 0,
        VDISK_STATUS_HASNT_COME = 1,
        VDISK_STATUS_SENT_INIT = 2,
        VDISK_STATUS_READING_LOG = 3,
        VDISK_STATUS_LOGGED = 4,
    };
    struct TLogEndPosition {
        ui32 ChunkIdx;
        ui32 SectorIdx;

        explicit TLogEndPosition(ui32 chunkIdx, ui32 sectorIdx) : ChunkIdx(chunkIdx), SectorIdx(sectorIdx) {}
    };
    TMap<TLogSignature, NPDisk::TLogRecord> StartingPoints;
    TVDiskID VDiskId = TVDiskID::InvalidId;
    EVDiskStatus Status = VDISK_STATUS_DEFAULT;
    ui64 CurrentFirstLsnToKeep = 0;
    ui64 LastWrittenCommitLsn = 0;
    TActorId CutLogId;
    TLogEndPosition LogEndPosition {0, 0};
    TActorId WhiteboardProxyId;
    ui64 LogRecordsInitiallyRead = 0;
    ui64 LogRecordsConsequentlyRead = 0;
    TOwnerRound OwnerRound = 0;
    TInstant AskedToCutLogAt;
    ui64 AskedFreeUpToLsn = 0;
    size_t LogChunkCountBeforeCut = 0;
    size_t AskedLogChunkToCut = 0;
    TInstant CutLogAt;
    ui64 LastSeenLsn = 0;
    bool HasAlreadyLoggedThisIncarnation = false;
    bool HasReadTheWholeLog = false;
    TLogPosition LogStartPosition{0, 0};
    NMetrics::TDecayingAverageValue<ui64, NMetrics::DurationPerMinute, NMetrics::DurationPerSecond> ReadThroughput;
    NMetrics::TDecayingAverageValue<ui64, NMetrics::DurationPerMinute, NMetrics::DurationPerSecond> WriteThroughput;
    ui32 VDiskSlotId = 0;

    TIntrusivePtr<TLogReaderBase> LogReader;
    TIntrusivePtr<TOwnerInflight> InFlight;

    bool OnQuarantine = false;

    TOperationLog<8> OperationLog;

    TOwnerData()
      : InFlight(new TOwnerInflight)
    {}

    bool IsStaticGroupOwner() const {
        if (VDiskId == TVDiskID::InvalidId) {
            return false;
        }
        return TGroupID(VDiskId.GroupID).ConfigurationType() == EGroupConfigurationType::Static;
    }

    bool IsNextLsnOk(const ui64 lsn) const {
        if (lsn == LastSeenLsn + 1) {
            // The most common case, linear increment.
            return true;
        }
        if (lsn <= LastSeenLsn) {
            // Lsn reversal or duplication.
            return false;
        }
        // Forward jump.
        if (HasAlreadyLoggedThisIncarnation) {
            return false;
        }
        return true;
    }

    void SetLastSeenLsn(const ui64 lsn) {
        LastSeenLsn = lsn;
        if (!HasAlreadyLoggedThisIncarnation) {
            HasAlreadyLoggedThisIncarnation = true;
        }
    }

    const char* GetStringStatus() const {
        return RenderStatus(Status);
    }

    static const char* RenderStatus(const EVDiskStatus status) {
        switch(status) {
        case VDISK_STATUS_DEFAULT:
            return "Error in status, status is unknown";
        case VDISK_STATUS_HASNT_COME:
            // VDisk is known, but didn't sent TEvYardInit yet
            return "seen before, but not initialized yet";
        case VDISK_STATUS_SENT_INIT:
            // VDisk sent TEvYardInit, but didn't sent TEvReadLog
            return "alive, but didn't start to read log";
        case VDISK_STATUS_READING_LOG:
            // VDisk sent TEvReadLog, but didn't sent TEvLog
            return "reading log";
        case VDISK_STATUS_LOGGED:
            // VDisk sent TEvLog at least once
            return "logged";
        }
        return "Unexpected enum value";
    }

    bool HaveRequestsInFlight() const {
        return LogReader || InFlight->ChunkWrites.load() || InFlight->ChunkReads.load() || InFlight->LogWrites.load();
    }

    TString ToString() const {
        TStringStream str;
        str << "TOwnerData {";
        str << "VDiskId# " << VDiskId.ToString();
        str << " Status# " << RenderStatus(Status);
        str << " CurrentFirstLsnToKeep# " << CurrentFirstLsnToKeep;
        str << " LastWrittenCommitLsn# " << LastWrittenCommitLsn;
        str << " LogRecordsInitiallyRead# " << LogRecordsInitiallyRead;
        str << " LogRecordsConsequentlyRead# " << LogRecordsConsequentlyRead;
        str << " OwnerRound# " << OwnerRound;
        str << " AskedToCutLogAt# " << AskedToCutLogAt;
        str << " AskedFreeUpToLsn# " << AskedFreeUpToLsn;
        str << " CutLogAt# " << CutLogAt;
        str << " LastSeenLsn# " << LastSeenLsn;
        if (HasAlreadyLoggedThisIncarnation) {
            str << " HasAlreadyLoggedThisIncarnation";
        }
        if (HasReadTheWholeLog) {
            str << " HasReadTheWholeLog";
        }
        str << " VDiskSlotId# " << VDiskSlotId;
        if (InFlight) {
            str << " Inflight {";
            str << " ChunkWrites# " << InFlight->ChunkWrites.load();
            str << " ChunkReads# " << InFlight->ChunkReads.load();
            str << " LogWrites# " << InFlight->LogWrites.load();
            str << " }";
        }
        str << "}";

        return str.Str();
    }

    void Reset(bool quarantine = false) {
        StartingPoints.clear();
        VDiskId = TVDiskID::InvalidId;
        Status = EVDiskStatus::VDISK_STATUS_DEFAULT;
        CurrentFirstLsnToKeep = 0;
        LastWrittenCommitLsn = 0;
        CutLogId = TActorId();
        LogEndPosition = TLogEndPosition(0, 0);
        WhiteboardProxyId = TActorId();
        LogRecordsInitiallyRead = 0;
        LogRecordsConsequentlyRead = 0;
        OwnerRound = 0;
        AskedToCutLogAt = TInstant();
        CutLogAt = TInstant();
        LastSeenLsn = 0;
        HasAlreadyLoggedThisIncarnation = false;
        HasReadTheWholeLog = false;
        LogStartPosition = TLogPosition{0, 0};
        ReadThroughput = NMetrics::TDecayingAverageValue<ui64, NMetrics::DurationPerMinute, NMetrics::DurationPerSecond>();
        WriteThroughput = NMetrics::TDecayingAverageValue<ui64, NMetrics::DurationPerMinute, NMetrics::DurationPerSecond>();
        VDiskSlotId = 0;

        if (!quarantine) {
            LogReader.Reset();
            InFlight.Reset(TIntrusivePtr<TOwnerInflight>(new TOwnerInflight));
        }
        OnQuarantine = quarantine;
    }
};

struct TChunkState {
    enum ECommitState : ui8 {
        FREE = 0,
        DATA_RESERVED_DELETE_IN_PROGRESS,
        DATA_COMMITTED_DELETE_IN_PROGRESS,
        DATA_RESERVED,
        DATA_COMMITTED,
        DATA_ON_QUARANTINE,
        DATA_COMMITTED_DELETE_ON_QUARANTINE,
        LOG_RESERVED,
        LOG_COMMITTED,
        DATA_RESERVED_DELETE_ON_QUARANTINE,
        DATA_DECOMMITTED,
        DATA_RESERVED_DECOMMIT_IN_PROGRESS,
        DATA_COMMITTED_DECOMMIT_IN_PROGRESS,
        LOCKED,
    };

    ui64 Nonce;
    ui64 CurrentNonce;
    ui64 PreviousNonce;
    std::atomic<i64> OperationsInProgress;
    TOwner OwnerId;
    ECommitState CommitState;
    ui64 CommitsInProgress;

    TChunkState()
        : Nonce(0)
        , CurrentNonce(0)
        , PreviousNonce(0)
        , OperationsInProgress(0)
        , OwnerId(OwnerUnallocated)
        , CommitState(FREE)
        , CommitsInProgress(0)
    {}

    bool HasAnyOperationsInProgress() const {
        return OperationsInProgress || CommitsInProgress;
    }

#ifdef OUT_VAR
#error "OUT_VAR already defined"
#endif
#define OUT_VAR(x) do { str << #x "# " << x << ", "; } while(false)
    TString ToString() const {
        TStringStream str;
        str << "{ ";
        OUT_VAR(Nonce);
        OUT_VAR(CurrentNonce);
        OUT_VAR(PreviousNonce);
        OUT_VAR(OperationsInProgress.load());
        OUT_VAR(OwnerId);
        OUT_VAR(CommitState);
        OUT_VAR(CommitsInProgress);
        str << "}";
        return str.Str();
    }
#undef OUT_VAR
};

struct TLogChunkInfo {
    struct TLsnRange {
        ui64 FirstLsn;
        ui64 LastLsn;
        bool IsPresent;

        TLsnRange()
            : FirstLsn(0xffffffffffffffffull)
            , LastLsn(0)
            , IsPresent(false)
        {}
    };
    ui32 ChunkIdx;
    ui64 DesiredPrevChunkLastNonce;
    ui64 FirstNonce;
    ui64 LastNonce;
    TVector<TLsnRange> OwnerLsnRange;

    ui32 CurrentUserCount;

    bool IsEndOfSplice = false;

    TLogChunkInfo(ui32 chunkIdx, ui32 ownerCount)
        : ChunkIdx(chunkIdx)
        , DesiredPrevChunkLastNonce(0)
        , FirstNonce(0)
        , LastNonce(0)
        , OwnerLsnRange(ownerCount)
        , CurrentUserCount(0)
        , IsEndOfSplice(false)
    {}

    template <bool AllowNewOwners>
    void RegisterLogSector(TOwner ownerId, ui64 ownerLsn) {
        if (ownerId >= OwnerLsnRange.size()) {
            if (AllowNewOwners) {
                OwnerLsnRange.resize(ownerId + 1);
            } else {
                return;
            }
        }
        TLogChunkInfo::TLsnRange &range = OwnerLsnRange[ownerId];
        range.FirstLsn = Min(range.FirstLsn, ownerLsn);
        range.LastLsn = Max(range.LastLsn, ownerLsn);
        if (!range.IsPresent) {
            range.IsPresent = true;
            ++CurrentUserCount;
        }
    }

    TString ToString() const {
        TStringStream str;
        str << "{ChunkIdx# " << ChunkIdx
            << " DesiredPrevChunkLastNonce# " << DesiredPrevChunkLastNonce
            << " FirstNonce# " << FirstNonce
            << " LastNonce# " << LastNonce
            << " CurrentUserCount# " << CurrentUserCount
            << " IsEndOfSplice# " << IsEndOfSplice
            << " OwnersLsnRange# {";

        for (size_t i = 0; i < OwnerLsnRange.size(); ++i) {
            const TLsnRange &range = OwnerLsnRange[i];
            if (range.IsPresent) {
                str << "{ownerId# " << i << " first# " << range.FirstLsn << " last# " << range.LastLsn << "}";
            }
        }
        str << "}}";
        return str.Str();
    }
};

} // NPDisk
} // NKikimr

template<>
inline void Out<NKikimr::NPDisk::TLogChunkInfo>(IOutputStream& os, const NKikimr::NPDisk::TLogChunkInfo& x) {
    os << x.ToString();
}
