#include "syslog.h"
#include "sector.h"

#include <cstring>

namespace NKikimr::NPDiskTool {

TSysLogReadResult ReadSysLog(
    IDeviceReader& device,
    const TDiskFormat& format,
    TIssueLog& issues)
{
    TSysLogReadResult result;
    const ui32 setCount = format.SysLogSectorCount;
    const ui32 beginSectorIdx = format.FirstSysLogSectorIdx();
    result.SectorSets.resize(setCount);

    // The SysLog is a ring of sector sets that is swept in full. Sets the ring has not reached yet
    // hold no valid copy, which is normal, so the sweep stays quiet and the per-set state below
    // (GoodSectorFlags, IsConsistent) carries the diagnostics instead.
    ui32 emptySets = 0;
    for (ui32 setIdx = 0; setIdx < setCount; ++setIdx) {
        const ui32 sectorIdx = beginSectorIdx + setIdx * NPDisk::ReplicationFactor;
        const ui64 offset = ui64(sectorIdx) * format.SectorSize;
        auto restored = RestoreTripleCopy(device, format, offset, format.MagicSysLogChunk,
            format.SysLogKey, issues, TStringBuilder() << "syslog[" << setIdx << "]",
            ESectorRef::Unreferenced);

        auto& info = result.SectorSets[setIdx];
        info.SetIdx = setIdx;
        info.FirstSectorIdx = sectorIdx;
        info.GoodSectorFlags = restored.GoodFlags;
        if (!restored.Ok) {
            ++emptySets;
            continue;
        }
        info.Nonce = restored.Nonce;

        auto* pageHeader = reinterpret_cast<TLogPageHeader*>(restored.Payload.data());
        if (pageHeader->Flags & NPDisk::LogPageFirst) {
            info.HasStart = true;
            auto* first = reinterpret_cast<TFirstLogPageHeader*>(restored.Payload.data());
            info.FullPayloadSize = first->DataSize;
            info.PayloadPartSize = first->Size;
            info.PayloadSignature = first->LogRecordHeader.Signature;
            info.PayloadLsn = first->LogRecordHeader.OwnerLsn;
            const ui8* src = restored.Payload.data() + sizeof(TFirstLogPageHeader);
            info.Payload.assign(src, src + first->Size);
        } else {
            const ui8* src = restored.Payload.data() + sizeof(TLogPageHeader);
            if (pageHeader->Flags & NPDisk::LogPageLast) {
                info.HasEnd = true;
                info.PayloadPartSize = pageHeader->Size;
            } else {
                info.HasMiddle = true;
                info.PayloadPartSize = pageHeader->Size;
            }
            info.Payload.assign(src, src + pageHeader->Size);
        }
        if (pageHeader->Flags & NPDisk::LogPageLast) {
            info.HasEnd = true;
            if (!info.HasStart) {
                info.PayloadPartSize = pageHeader->Size;
            }
        }
    }

    if (emptySets) {
        issues.Info("syslog", TStringBuilder() << emptySets << " of " << setCount
            << " sector sets have no valid copy (not yet reached by the SysLog ring)");
    }

    ui32 loopOffset = 0;
    for (; loopOffset < setCount; ++loopOffset) {
        if (result.SectorSets[loopOffset].HasStart) {
            break;
        }
    }
    if (loopOffset >= setCount) {
        issues.Error("syslog", "No SysLog sector has a first log page");
        return result;
    }
    bool hasAnotherStart = false;
    for (ui32 idx = loopOffset + 1; idx < setCount; ++idx) {
        if (result.SectorSets[idx].HasStart) {
            hasAnotherStart = true;
            break;
        }
    }
    if (!hasAnotherStart) {
        issues.Warning("syslog", "Only one SysLog sector has a first log page", true);
    }
    result.LoopOffset = loopOffset;

    auto markInconsistent = [&](ui32 beginSetIdx, ui32 endSetIdx, bool including) {
        ui32 idx = beginSetIdx;
        while (idx != endSetIdx) {
            result.SectorSets[idx].IsConsistent = false;
            idx = (idx + 1) % setCount;
        }
        if (including) {
            result.SectorSets[endSetIdx].IsConsistent = false;
        }
    };

    bool hasFirst = false;
    ui64 fullSize = 0;
    ui64 actualSize = 0;
    ui64 firstIdx = 0;
    ui64 prevNonce = 0;
    for (ui32 idx = 0; idx < setCount + 1; ++idx) {
        ui32 setIdx = (idx + loopOffset) % setCount;
        auto& info = result.SectorSets[setIdx];
        if (info.Payload.empty() && !info.HasStart && !info.HasEnd && !info.HasMiddle) {
            if (hasFirst) {
                markInconsistent(firstIdx, setIdx, true);
            }
            hasFirst = false;
            continue;
        }
        if (info.Nonce <= prevNonce && prevNonce != 0) {
            info.IsNonceReversal = true;
        }
        if (info.HasStart) {
            if (hasFirst) {
                markInconsistent(firstIdx, setIdx, false);
            }
            hasFirst = true;
            fullSize = info.FullPayloadSize;
            actualSize = 0;
            firstIdx = setIdx;
            prevNonce = info.Nonce;
        } else {
            if (info.Nonce != prevNonce + 1) {
                if (hasFirst) {
                    markInconsistent(firstIdx, setIdx, false);
                }
                hasFirst = false;
                info.IsConsistent = false;
                prevNonce = info.Nonce;
                continue;
            }
            prevNonce = info.Nonce;
        }
        if (!hasFirst) {
            info.IsConsistent = false;
            continue;
        }
        actualSize += info.PayloadPartSize;
        if (actualSize > fullSize) {
            markInconsistent(firstIdx, setIdx, true);
            hasFirst = false;
            continue;
        }
        if (info.HasEnd) {
            if (actualSize != fullSize) {
                markInconsistent(firstIdx, setIdx, true);
                hasFirst = false;
                continue;
            }
            hasFirst = false;
        }
    }

    ui32 bestFirst = 0;
    ui32 bestLast = 0;
    ui64 bestNonce = 0;
    for (ui32 idx = 0; idx < setCount; ++idx) {
        ui32 setIdx = (idx + loopOffset) % setCount;
        auto& info = result.SectorSets[setIdx];
        if (info.IsConsistent && !info.IsNonceReversal && info.HasStart && info.Nonce > bestNonce) {
            bestNonce = info.Nonce;
            bestFirst = idx + loopOffset;
        }
        if (info.IsConsistent && !info.IsNonceReversal && info.HasEnd && info.Nonce >= bestNonce) {
            bestLast = idx + loopOffset;
        }
    }
    result.BestNonce = bestNonce;
    if (bestNonce == 0) {
        issues.Error("syslog", "No consistent SysLog record found");
        return result;
    }

    auto& first = result.SectorSets[bestFirst % setCount];
    TString payload = TString::Uninitialized(first.FullPayloadSize);
    if (first.Payload.size() > payload.size()) {
        issues.Error("syslog", "First payload part larger than FullPayloadSize");
        return result;
    }
    memcpy(payload.Detach(), first.Payload.data(), first.Payload.size());
    ui32 writePos = first.Payload.size();
    const ui32 span = bestLast - bestFirst;
    for (ui32 idx = 1; idx <= span; ++idx) {
        auto& part = result.SectorSets[(idx + bestFirst) % setCount];
        if (writePos + part.Payload.size() > payload.size()) {
            issues.Error("syslog", "Payload part overflow while assembling SysLog record");
            return result;
        }
        memcpy(payload.Detach() + writePos, part.Payload.data(), part.Payload.size());
        writePos += part.Payload.size();
    }
    result.Payload = std::move(payload);
    result.Lsn = first.PayloadLsn;
    result.Signature = first.PayloadSignature;
    result.Ok = true;

    for (const auto& s : result.SectorSets) {
        result.MaxNonce = Max(result.MaxNonce, s.Nonce);
    }
    return result;
}

} // namespace NKikimr::NPDiskTool
