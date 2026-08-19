#pragma once

#include "issues.h"
#include "syslog.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NPDiskTool {

struct TOwnerState {
    TVDiskID VDiskId = TVDiskID::InvalidId;
    ui32 GroupSizeInUnits = 0;
    ui64 CurrentFirstLsnToKeep = 0;
    ui64 LastWrittenCommitLsn = 0;
    ui64 FirstNonceToKeep = 0;
    ui64 LastSeenLsn = 0;
    TMap<ui8, std::pair<ui64, TString>> StartingPoints; // unmasked signature -> (lsn, payload)
};

struct TChunkSnapshot {
    TOwner OwnerId = EOwner::OwnerUnallocated;
    TChunkState::ECommitState CommitState = TChunkState::FREE;
    ui64 Nonce = 0;
};

struct TLogChunkSnapshot {
    TChunkIdx ChunkIdx = 0;
    bool IsCommitted = false;
    ui64 FirstNonce = 0;
    ui64 LastNonce = 0;
    ui32 CurrentUserCount = 0;
    struct TRange {
        bool Present = false;
        ui64 FirstLsn = 0;
        ui64 LastLsn = 0;
    };
    TRange OwnerLsnRange[256];
};

struct TParsedSysLog {
    // Both are on-disk PODs without constructors, and commands still print them when the SysLog
    // could not be read at all, so they must not start out as uninitialized memory.
    TSysLogRecord Record = {};
    TSysLogFirstNoncesToKeep FirstNoncesToKeep = {};
    ui32 FirstLogChunkToParseCommits = 0;
    TVector<TChunkSnapshot> Chunks;
    TVector<TOwnerState> Owners; // 256
    TString CompatibilityInfo;
};

TParsedSysLog ParseSysLogPayload(
    const TString& payload,
    const TDiskFormat& format,
    TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
