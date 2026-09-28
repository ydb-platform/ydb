#include "log.h"
#include "sector.h"

#include <cstring>
#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr::NPDiskTool {

namespace {

ui64 UsableSectorsPerLogChunk(const TDiskFormat& format) {
    const ui64 sectorsPerLogChunk = format.ChunkSize / format.SectorSize;
    const ui64 nextChunkReferenceSectors = format.IsErasureEncodeNextChunkReference()
        ? NPDisk::ReplicationFactor : 1;
    return sectorsPerLogChunk - nextChunkReferenceSectors;
}

bool ParseCommitTail(TLogRecordView& rec, TIssueLog& issues) {
    if (rec.RawPayload.size() < sizeof(TCommitRecordFooter)) {
        issues.Warning("log", TStringBuilder() << "Commit record too small owner# " << (ui32)rec.OwnerId
            << " lsn# " << rec.Lsn, true);
        return false;
    }
    const auto* footer = reinterpret_cast<const TCommitRecordFooter*>(
        rec.RawPayload.data() + rec.RawPayload.size() - sizeof(TCommitRecordFooter));
#ifdef ENABLE_PDISK_SHRED
    const ui32 dirtyCount = footer->DirtyCount;
#else
    const ui32 dirtyCount = 0;
#endif
    const ui8* p = reinterpret_cast<const ui8*>(footer);
    p -= dirtyCount * sizeof(ui32);
    const ui32* deletes = reinterpret_cast<const ui32*>(p - footer->DeleteCount * sizeof(ui32));
    const ui64* commitNonces = reinterpret_cast<const ui64*>(
        reinterpret_cast<const ui8*>(deletes) - footer->CommitCount * sizeof(ui64));
    const ui32* commits = reinterpret_cast<const ui32*>(
        reinterpret_cast<const ui8*>(commitNonces) - footer->CommitCount * sizeof(ui32));

    const ui8* begin = reinterpret_cast<const ui8*>(rec.RawPayload.data());
    if (reinterpret_cast<const ui8*>(commits) < begin) {
        issues.Warning("log", TStringBuilder() << "Commit arrays overflow owner# " << (ui32)rec.OwnerId
            << " lsn# " << rec.Lsn, true);
        return false;
    }

    rec.HasCommit = true;
    rec.IsStartingPoint = footer->IsStartingPoint;
    rec.FirstLsnToKeep = ReadUnaligned<ui64>(&footer->FirstLsnToKeep);
    rec.CommitChunks.resize(footer->CommitCount);
    rec.CommitNonces.resize(footer->CommitCount);
    rec.DeleteChunks.resize(footer->DeleteCount);
    for (ui32 i = 0; i < footer->CommitCount; ++i) {
        rec.CommitChunks[i] = ReadUnaligned<ui32>(&commits[i]);
        rec.CommitNonces[i] = ReadUnaligned<ui64>(&commitNonces[i]);
    }
    for (ui32 i = 0; i < footer->DeleteCount; ++i) {
        rec.DeleteChunks[i] = ReadUnaligned<ui32>(&deletes[i]);
    }
    rec.Payload = rec.RawPayload.substr(0, footer->UserDataSize);
    return true;
}

} // namespace

TLogScanResult ScanMainLog(
    IDeviceReader& device,
    const TDiskFormat& format,
    TParsedSysLog& state,
    TIssueLog& issues)
{
    TLogScanResult result;
    ui32 chunkIdx = state.Record.LogHeadChunkIdx;
    ui64 lastNonce = state.Record.LogHeadChunkPreviousNonce;
    bool parseCommits = state.FirstLogChunkToParseCommits == state.Record.LogHeadChunkIdx;
    const ui64 usable = UsableSectorsPerLogChunk(format);
    const ui64 payloadSize = format.SectorPayloadSize();
    const ui64 maxRecordSize = usable * payloadSize;
    const ui32 chunkCount = format.DiskSizeChunks();
    THashSet<ui32> visited;
    TRepeatedIssues damaged("log", "sector offset");

    TString lastData;
    ui32 lastWrite = 0;
    TLogRecordHeader lastHeader(0, 0, 0);
    bool haveHeader = false;
    bool skipped = false;
    ui64 lastHeaderNonce = 0;
    TChunkIdx lastHeaderChunk = 0;

    auto finishRecord = [&](ui64 /*nonce*/, TChunkIdx recChunk) {
        if (!haveHeader) {
            return;
        }
        TLogRecordView rec;
        rec.OwnerId = lastHeader.OwnerId;
        rec.Signature = lastHeader.Signature;
        rec.Lsn = lastHeader.OwnerLsn;
        rec.Nonce = lastHeaderNonce;
        rec.ChunkIdx = recChunk;
        rec.RawPayload = lastData;
        if (rec.Signature.HasCommitRecord()) {
            ParseCommitTail(rec, issues);
        } else {
            rec.Payload = rec.RawPayload;
        }
        haveHeader = false;
        lastData.clear();

        auto& owner = state.Owners[rec.OwnerId];
        if (owner.VDiskId != TVDiskID::InvalidId) {
            if (rec.HasCommit && rec.FirstLsnToKeep > owner.CurrentFirstLsnToKeep) {
                owner.CurrentFirstLsnToKeep = rec.FirstLsnToKeep;
            }
            if (rec.HasCommit && rec.IsStartingPoint) {
                owner.StartingPoints[rec.Signature.GetUnmasked()] = {rec.Lsn, rec.Payload};
            }
            owner.LastSeenLsn = rec.Lsn;
            if (rec.HasCommit) {
                owner.LastWrittenCommitLsn = rec.Lsn;
            }
        }

        if (parseCommits && rec.HasCommit) {
            for (ui32 i = 0; i < rec.DeleteChunks.size(); ++i) {
                const ui32 id = rec.DeleteChunks[i];
                if (id < state.Chunks.size()) {
                    state.Chunks[id].OwnerId = EOwner::OwnerUnallocated;
                    state.Chunks[id].CommitState = TChunkState::FREE;
                }
            }
            for (ui32 i = 0; i < rec.CommitChunks.size(); ++i) {
                const ui32 id = rec.CommitChunks[i];
                if (id < state.Chunks.size()) {
                    state.Chunks[id].OwnerId = rec.OwnerId;
                    state.Chunks[id].Nonce = rec.CommitNonces[i];
                    state.Chunks[id].CommitState = TChunkState::DATA_COMMITTED;
                }
            }
        }

        if (!result.LogChunks.empty()) {
            auto& lc = result.LogChunks.back();
            auto& rng = lc.OwnerLsnRange[rec.OwnerId];
            if (!rng.Present) {
                rng.Present = true;
                rng.FirstLsn = rec.Lsn;
                rng.LastLsn = rec.Lsn;
                ++lc.CurrentUserCount;
            } else {
                rng.LastLsn = Max(rng.LastLsn, rec.Lsn);
                rng.FirstLsn = Min(rng.FirstLsn, rec.Lsn);
            }
        }

        result.Records.push_back(std::move(rec));
    };

    while (chunkIdx != 0) {
        if (chunkCount && chunkIdx >= chunkCount) {
            // Either the SysLog head or a next-chunk reference points outside the disk.
            issues.Error("log", TStringBuilder() << "Log chain leaves the disk at chunk# " << chunkIdx
                << " of " << chunkCount);
            break;
        }
        if (!visited.insert(chunkIdx).second) {
            issues.Warning("log", TStringBuilder() << "Log chunk cycle at chunk# " << chunkIdx, true);
            break;
        }
        TLogChunkSnapshot lcs;
        lcs.ChunkIdx = chunkIdx;
        lcs.IsCommitted = chunkIdx < state.Chunks.size()
            && (state.Chunks[chunkIdx].CommitState == TChunkState::DATA_COMMITTED
                || state.Chunks[chunkIdx].CommitState == TChunkState::LOG_COMMITTED);
        if (chunkIdx < state.Chunks.size()) {
            state.Chunks[chunkIdx].OwnerId = EOwner::OwnerSystem;
            state.Chunks[chunkIdx].CommitState = TChunkState::LOG_RESERVED;
        }
        if (!parseCommits && state.FirstLogChunkToParseCommits == chunkIdx) {
            parseCommits = true;
        }

        bool endOfLog = false;
        for (ui64 sectorIdx = 0; sectorIdx < usable; ++sectorIdx) {
            const ui64 offset = format.Offset(chunkIdx, sectorIdx);
            // An invalid hash is how the log tail is found: the sector was simply never written.
            auto restored = RestoreOneSector(device, format, offset, format.MagicLogChunk,
                format.LogKey, true, issues, TStringBuilder() << "log[" << chunkIdx << ":" << sectorIdx << "]",
                {}, ESectorRef::Unreferenced);
            if (!restored.Ok) {
                endOfLog = true;
                result.LastSectorIdx = sectorIdx;
                issues.Info("log", TStringBuilder() << "Log ends at chunk# " << chunkIdx
                    << " sector# " << sectorIdx);
                break;
            }
            if (lastNonce != 0 && lastNonce != Max<ui64>() && restored.Nonce != lastNonce + 1) {
                if (sectorIdx == 0) {
                    // possible nonce jump inside the sector; still process pages
                } else {
                    issues.Info("log", TStringBuilder() << "Nonce gap chunk# " << chunkIdx
                        << " sector# " << sectorIdx << " prev# " << lastNonce << " got# " << restored.Nonce);
                    endOfLog = true;
                    result.LastSectorIdx = sectorIdx;
                    break;
                }
            }
            const ui64 previousNonce = lastNonce;
            lastNonce = restored.Nonce;
            if (lcs.FirstNonce == 0) {
                lcs.FirstNonce = restored.Nonce;
            }
            lcs.LastNonce = restored.Nonce;

            // Page sizes come off the disk, so the offset is tracked in ui64: a wrapped ui32 would
            // land back inside the sector and spin here forever.
            ui64 offsetInSector = 0;
            const ui64 maxOffset = payloadSize - sizeof(TFirstLogPageHeader);
            ui8* data = restored.Payload.data();
            while (offsetInSector <= maxOffset) {
                const ui64 pageOffset = offsetInSector;
                auto* pageHeader = reinterpret_cast<TLogPageHeader*>(data + offsetInSector);
                if (pageHeader->Flags & NPDisk::LogPageTerminator) {
                    offsetInSector = payloadSize;
                    break;
                }
                if (pageHeader->Flags & NPDisk::LogPageNonceJump2) {
                    auto* jump = reinterpret_cast<TNonceJumpLogPageHeader2*>(data + offsetInSector);
                    offsetInSector += sizeof(TNonceJumpLogPageHeader2);
                    const ui64 headerPrev = ReadUnaligned<ui64>(&jump->PreviousNonce);
                    if (previousNonce > headerPrev && previousNonce != Max<ui64>() && previousNonce != 0) {
                        endOfLog = true;
                        break;
                    }
                    continue;
                }
                if (pageHeader->Flags & NPDisk::LogPageNonceJump1) {
                    offsetInSector += sizeof(TNonceJumpLogPageHeader1);
                    continue;
                }
                if (pageHeader->Flags & NPDisk::LogPageFirst) {
                    auto* first = reinterpret_cast<TFirstLogPageHeader*>(data + offsetInSector);
                    offsetInSector += sizeof(TFirstLogPageHeader);
                    const ui64 firstNonceToKeep = state.Owners[first->LogRecordHeader.OwnerId].FirstNonceToKeep;
                    const ui64 firstLsnToKeep = state.Owners[first->LogRecordHeader.OwnerId].CurrentFirstLsnToKeep;
                    if (restored.Nonce < firstNonceToKeep || first->LogRecordHeader.OwnerLsn < firstLsnToKeep) {
                        skipped = true;
                        offsetInSector += first->Size;
                        continue;
                    }
                    skipped = false;
                    const ui64 dataSize = first->DataSize;
                    if (dataSize > maxRecordSize) {
                        // Nothing legitimate declares a record longer than the whole log chunk.
                        damaged.Add("First page DataSize is implausible", pageOffset);
                        haveHeader = false;
                        offsetInSector += first->Size;
                        continue;
                    }
                    lastHeader = first->LogRecordHeader;
                    lastHeaderNonce = restored.Nonce;
                    lastHeaderChunk = chunkIdx;
                    haveHeader = true;
                    lastData = TString::Uninitialized(dataSize);
                    const ui64 take = ClampSpan(offsetInSector, first->Size, payloadSize);
                    if (take != first->Size || first->Size > dataSize) {
                        // The page claims more bytes than the sector or the record can hold; the
                        // record would be truncated, so drop it rather than parse a partial payload.
                        damaged.Add("First page size exceeds the sector or DataSize", pageOffset);
                        haveHeader = false;
                        offsetInSector += first->Size;
                        continue;
                    }
                    memcpy(lastData.Detach(), data + offsetInSector, take);
                    lastWrite = static_cast<ui32>(take);
                    offsetInSector += first->Size;
                } else {
                    offsetInSector += sizeof(TLogPageHeader);
                    if (skipped) {
                        offsetInSector += pageHeader->Size;
                        continue;
                    }
                    if (!haveHeader) {
                        damaged.Add("Orphan continuation page", pageOffset);
                        offsetInSector += pageHeader->Size;
                        continue;
                    }
                    const ui64 take = ClampSpan(offsetInSector, pageHeader->Size, payloadSize);
                    if (take != pageHeader->Size || lastWrite + take > lastData.size()) {
                        damaged.Add("Continuation page size exceeds the sector or the record", pageOffset);
                        haveHeader = false;
                        offsetInSector += pageHeader->Size;
                        continue;
                    }
                    memcpy(lastData.Detach() + lastWrite, data + offsetInSector, take);
                    lastWrite += static_cast<ui32>(take);
                    offsetInSector += pageHeader->Size;
                }
                if (offsetInSector <= pageOffset) {
                    // Every branch above advances by at least a header, so this only happens on a
                    // corrupt page; without the guard the sector would be walked forever.
                    damaged.Add("Log page makes no progress", pageOffset);
                    break;
                }
                if (haveHeader && (pageHeader->Flags & NPDisk::LogPageLast)) {
                    finishRecord(restored.Nonce, lastHeaderChunk);
                }
            }
            if (endOfLog) {
                break;
            }
            result.LastSectorIdx = sectorIdx + 1;
        }

        result.LogChunks.push_back(lcs);
        result.LastChunkIdx = chunkIdx;
        if (endOfLog) {
            break;
        }

        // The last chunk of the chain has no next-chunk reference yet, so a missing one ends the walk.
        const ui64 refOffset = format.Offset(chunkIdx, usable);
        auto next = RestoreTripleCopy(device, format, refOffset, format.MagicNextLogChunkReference,
            format.LogKey, issues, TStringBuilder() << "next-ref[" << chunkIdx << "]",
            ESectorRef::Unreferenced);
        if (!next.Ok) {
            break;
        }
        auto* ref2 = reinterpret_cast<TNextLogChunkReference2*>(next.Payload.data());
        ui32 nextChunk = ref2->NextChunk;
        if (ref2->Version >= PDISK_DATA_VERSION_3) {
            auto* ref3 = static_cast<TNextLogChunkReference3*>(ref2);
            if (ref3->NextChunkFirstNonce) {
                haveHeader = false;
                skipped = true;
                lastNonce = ref3->NextChunkFirstNonce - 1;
            } else {
                lastNonce = next.Nonce;
            }
            if (ref3->IsNotCompatible) {
                issues.Error("log", TStringBuilder() << "Incompatible next-chunk reference version# "
                    << (ui32)ref2->Version);
                break;
            }
        } else {
            lastNonce = next.Nonce;
        }
        if (nextChunk == 0) {
            break;
        }
        chunkIdx = nextChunk;
    }

    damaged.Flush(issues, "warning");
    return result;
}

bool WriteLogExport(
    const TDiskFormat& format,
    const TLogScanResult& scan,
    const TString& path,
    ui32 ownerFilter,
    TIssueLog& issues,
    ui64& bytesWritten)
{
    TFileOutput out(path);
    ui32 magic = LogExportMagic;
    ui32 version = LogExportVersion;
    ui64 guid = format.Guid;
    ui32 chunkSize = format.ChunkSize;
    ui32 sectorSize = format.SectorSize;
    out.Write(&magic, sizeof(magic));
    out.Write(&version, sizeof(version));
    out.Write(&guid, sizeof(guid));
    out.Write(&chunkSize, sizeof(chunkSize));
    out.Write(&sectorSize, sizeof(sectorSize));
    ui64 count = 0;
    for (const auto& rec : scan.Records) {
        if (ownerFilter != Max<ui32>() && rec.OwnerId != ownerFilter) {
            continue;
        }
        ++count;
    }
    out.Write(&count, sizeof(count));
    bytesWritten = sizeof(magic) + sizeof(version) + sizeof(guid) + sizeof(chunkSize) + sizeof(sectorSize) + sizeof(count);
    for (const auto& rec : scan.Records) {
        if (ownerFilter != Max<ui32>() && rec.OwnerId != ownerFilter) {
            continue;
        }
        ui8 owner = rec.OwnerId;
        ui8 signature = rec.Signature;
        ui16 reserved = 0;
        ui32 payloadSize = rec.RawPayload.size();
        out.Write(&owner, sizeof(owner));
        out.Write(&signature, sizeof(signature));
        out.Write(&reserved, sizeof(reserved));
        out.Write(&rec.Lsn, sizeof(rec.Lsn));
        out.Write(&rec.Nonce, sizeof(rec.Nonce));
        out.Write(&rec.ChunkIdx, sizeof(rec.ChunkIdx));
        out.Write(&payloadSize, sizeof(payloadSize));
        out.Write(rec.RawPayload.data(), rec.RawPayload.size());
        bytesWritten += sizeof(owner) + sizeof(signature) + sizeof(reserved) + sizeof(rec.Lsn)
            + sizeof(rec.Nonce) + sizeof(rec.ChunkIdx) + sizeof(payloadSize) + rec.RawPayload.size();
    }
    out.Flush();
    Y_UNUSED(issues);
    return true;
}

TLogScanResult ReadLogExport(const TString& path, TIssueLog& issues) {
    TLogScanResult result;
    TFile file(path, OpenExisting | RdOnly);
    const i64 fileSize = file.GetLength();
    TFileInput in(file);
    ui32 magic = 0;
    ui32 version = 0;
    if (in.Load(&magic, sizeof(magic)) != sizeof(magic) || magic != LogExportMagic) {
        issues.Error("parse-log", "Not a pdisktool log export (bad magic)");
        return result;
    }
    in.Load(&version, sizeof(version));
    if (version != LogExportVersion) {
        issues.Error("parse-log", TStringBuilder() << "Unsupported log export version# " << version);
        return result;
    }
    ui64 guid = 0;
    ui32 chunkSize = 0;
    ui32 sectorSize = 0;
    ui64 count = 0;
    in.Load(&guid, sizeof(guid));
    in.Load(&chunkSize, sizeof(chunkSize));
    in.Load(&sectorSize, sizeof(sectorSize));
    in.Load(&count, sizeof(count));
    for (ui64 i = 0; i < count; ++i) {
        TLogRecordView rec;
        ui8 owner = 0;
        ui8 signature = 0;
        ui16 reserved = 0;
        ui32 payloadSize = 0;
        if (in.Load(&owner, sizeof(owner)) != sizeof(owner)) {
            issues.Warning("parse-log", "Truncated log export", true);
            break;
        }
        in.Load(&signature, sizeof(signature));
        in.Load(&reserved, sizeof(reserved));
        in.Load(&rec.Lsn, sizeof(rec.Lsn));
        in.Load(&rec.Nonce, sizeof(rec.Nonce));
        in.Load(&rec.ChunkIdx, sizeof(rec.ChunkIdx));
        in.Load(&payloadSize, sizeof(payloadSize));
        if (fileSize > 0 && payloadSize > static_cast<ui64>(fileSize)) {
            // The size field is part of the export, so a truncated or damaged file can claim more
            // than the file holds; refuse to allocate it.
            issues.Error("parse-log", TStringBuilder() << "Log export record claims " << payloadSize
                << " payload bytes but the file is only " << fileSize << " bytes");
            break;
        }
        rec.OwnerId = owner;
        rec.Signature = signature;
        rec.RawPayload = TString::Uninitialized(payloadSize);
        if (payloadSize && in.Load(rec.RawPayload.Detach(), payloadSize) != payloadSize) {
            issues.Warning("parse-log", "Truncated log record payload", true);
            break;
        }
        if (rec.Signature.HasCommitRecord()) {
            ParseCommitTail(rec, issues);
        } else {
            rec.Payload = rec.RawPayload;
        }
        result.Records.push_back(std::move(rec));
    }
    return result;
}

} // namespace NKikimr::NPDiskTool
