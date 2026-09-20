#include "state.h"

#include <cstring>
#include <util/system/unaligned_mem.h>

namespace NKikimr::NPDiskTool {

TParsedSysLog ParseSysLogPayload(
    const TString& payload,
    const TDiskFormat& format,
    TIssueLog& issues)
{
    TParsedSysLog parsed;
    parsed.Owners.resize(256);
    const ui32 chunkCount = format.DiskSizeChunks();
    parsed.Chunks.resize(chunkCount);

    if (payload.size() < sizeof(TSysLogRecord)) {
        issues.Error("syslog", TStringBuilder() << "SysLog payload smaller than TSysLogRecord size# "
            << payload.size());
        return parsed;
    }

    memcpy(&parsed.Record, payload.data(), sizeof(TSysLogRecord));
    if (parsed.Record.Version >= PDISK_SYS_LOG_RECORD_INCOMPATIBLE_VERSION_1000) {
        issues.Error("syslog", TStringBuilder() << "Incompatible SysLogRecord version# " << parsed.Record.Version);
        return parsed;
    }

    for (ui32 i = 0; i < 256; ++i) {
        TVDiskID id = parsed.Record.OwnerVDisks[i];
        id.GroupGeneration = -1;
        parsed.Owners[i].VDiskId = id;
    }

    for (ui32 i = 0; i < format.SystemChunkCount && i < chunkCount; ++i) {
        parsed.Chunks[i].OwnerId = EOwner::OwnerSystem;
        parsed.Chunks[i].CommitState = TChunkState::DATA_COMMITTED;
    }

    const char* ptr = payload.data() + sizeof(TSysLogRecord);
    const char* end = payload.data() + payload.size();
    const ui64 expectedOwners = ui64(chunkCount) * sizeof(TChunkInfo);
    if (static_cast<ui64>(end - ptr) < expectedOwners) {
        issues.Error("syslog", "SysLog payload truncated before TChunkInfo table");
        return parsed;
    }
    const auto* chunkOwners = reinterpret_cast<const TChunkInfo*>(ptr);
    ptr += expectedOwners;

    for (ui32 i = format.SystemChunkCount; i < chunkCount; ++i) {
        TOwner owner = chunkOwners[i].OwnerId;
        parsed.Chunks[i].OwnerId = owner;
        parsed.Chunks[i].Nonce = chunkOwners[i].Nonce;
        if (IsOwnerAllocated(owner)) {
            parsed.Chunks[i].CommitState = IsOwnerUser(owner)
                ? TChunkState::DATA_COMMITTED
                : TChunkState::LOG_COMMITTED;
        } else {
            parsed.Chunks[i].CommitState = TChunkState::FREE;
        }
    }

    if (parsed.Record.LogHeadChunkIdx < chunkCount) {
        parsed.Chunks[parsed.Record.LogHeadChunkIdx].OwnerId = EOwner::OwnerSystem;
        parsed.Chunks[parsed.Record.LogHeadChunkIdx].CommitState = TChunkState::DATA_COMMITTED;
    }

    if (parsed.Record.Version == PDISK_SYS_LOG_RECORD_VERSION_2) {
        parsed.FirstNoncesToKeep.Clear();
    } else if (static_cast<ui64>(end - ptr) >= sizeof(TSysLogFirstNoncesToKeep)) {
        memcpy(&parsed.FirstNoncesToKeep, ptr, sizeof(TSysLogFirstNoncesToKeep));
        ptr += sizeof(TSysLogFirstNoncesToKeep);
        for (ui32 i = 0; i < 256; ++i) {
            parsed.Owners[i].FirstNonceToKeep = parsed.FirstNoncesToKeep.FirstNonceToKeep[i];
        }
    } else {
        issues.Warning("syslog", "Missing FirstNoncesToKeep tail", true);
        parsed.FirstNoncesToKeep.Clear();
    }

    parsed.FirstLogChunkToParseCommits = parsed.Record.LogHeadChunkIdx;

    if (parsed.Record.Version >= PDISK_SYS_LOG_RECORD_VERSION_4) {
        if (static_cast<ui64>(end - ptr) < sizeof(ui64)) {
            issues.Warning("syslog", "Truncated trim-info size", true);
            return parsed;
        }
        const ui64 trimInfoBytes = ReadUnaligned<ui64>(ptr);
        ptr += sizeof(ui64);
        if (static_cast<ui64>(end - ptr) < trimInfoBytes) {
            issues.Warning("syslog", "Truncated trim-info payload", true);
            return parsed;
        }
        if (trimInfoBytes == TChunkTrimInfo::SizeForChunkCount(chunkCount) || trimInfoBytes == 0) {
            const auto* trim = reinterpret_cast<const TChunkTrimInfo*>(ptr);
            if (trimInfoBytes) {
                for (ui32 i = 0; i < chunkCount; ++i) {
                    if (trim[i / 8].TrimMask & (1u << (i % 8))) {
                        if (parsed.Chunks[i].OwnerId == EOwner::OwnerUnallocated) {
                            parsed.Chunks[i].OwnerId = EOwner::OwnerUnallocatedTrimmed;
                        }
                    }
                }
            }
        } else {
            issues.Warning("syslog", TStringBuilder() << "Unexpected trimInfoBytes# " << trimInfoBytes, true);
        }
        ptr += trimInfoBytes;
    }

    if (parsed.Record.Version >= PDISK_SYS_LOG_RECORD_VERSION_6) {
        if (static_cast<ui64>(end - ptr) < sizeof(ui32)) {
            issues.Warning("syslog", "Truncated FirstLogChunkToParseCommits", true);
            return parsed;
        }
        parsed.FirstLogChunkToParseCommits = ReadUnaligned<ui32>(ptr);
        ptr += sizeof(ui32);
    }

    if (parsed.Record.Version >= PDISK_SYS_LOG_RECORD_VERSION_7) {
        if (static_cast<ui64>(end - ptr) < sizeof(ui32)) {
            issues.Warning("syslog", "Truncated compatibility-info size", true);
            return parsed;
        }
        const ui32 protoSize = ReadUnaligned<ui32>(ptr);
        ptr += sizeof(ui32);
        if (static_cast<ui64>(end - ptr) < protoSize) {
            issues.Warning("syslog", "Truncated compatibility-info payload", true);
            return parsed;
        }
        parsed.CompatibilityInfo = TString(ptr, protoSize);
        ptr += protoSize;
    }

    if (parsed.Record.Version >= PDISK_SYS_LOG_RECORD_VERSION_8) {
        if (static_cast<ui64>(end - ptr) < sizeof(ui32)) {
            issues.Warning("syslog", "Truncated groupSizeInUnits size", true);
            return parsed;
        }
        const ui32 protoSize = ReadUnaligned<ui32>(ptr);
        ptr += sizeof(ui32);
        if (static_cast<ui64>(end - ptr) < protoSize) {
            issues.Warning("syslog", "Truncated groupSizeInUnits payload", true);
            return parsed;
        }
        for (ui32 i = 0; i < protoSize / 2; ++i) {
            const TOwner owner = ptr[i * 2 + 0];
            const ui8 units = ptr[i * 2 + 1];
            parsed.Owners[owner].GroupSizeInUnits = units;
        }
        ptr += protoSize;
    }

    return parsed;
}

} // namespace NKikimr::NPDiskTool
