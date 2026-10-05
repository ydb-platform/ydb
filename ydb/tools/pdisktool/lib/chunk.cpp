#include "chunk.h"
#include "sector.h"

#include <cstring>
#include <util/stream/file.h>

namespace NKikimr::NPDiskTool {

namespace {

struct TSectorSpan {
    ui32 First = 0;
    ui32 Count = 0;
};

// Maps a logical byte range onto the sectors holding it. Logical offset L lives in sector L / payload.
// The range may come straight off the disk, so it is clamped to the chunk rather than trusted.
TSectorSpan LogicalSpanToSectors(const TDiskFormat& format, ui32 offset, ui32 size) {
    const ui64 payload = format.SectorPayloadSize();
    const ui64 sectors = format.ChunkSize / format.SectorSize;
    TSectorSpan span;
    if (size == 0 || offset >= sectors * payload) {
        return span;
    }
    span.First = offset / payload;
    const ui64 last = Min<ui64>(sectors - 1, (ui64(offset) + size - 1) / payload);
    span.Count = last + 1 - span.First;
    return span;
}

// Reads sectors [first, first + count) and concatenates their payloads.
TChunkReadResult ReadSectorSpan(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    TSectorSpan span,
    ESectorRef ref,
    TIssueLog& issues,
    const TString& location)
{
    const ui32 payload = format.SectorPayloadSize();
    TChunkReadResult result;
    result.Data = TString::Uninitialized(ui64(span.Count) * payload);
    if (span.Count == 0) {
        return result;
    }
    memset(result.Data.Detach(), 0, result.Data.size());

    ui64 baseNonce = 0;
    if (chunkIdx < state.Chunks.size()) {
        baseNonce = state.Chunks[chunkIdx].Nonce;
    }

    char* dst = result.Data.Detach();
    for (ui32 i = 0; i < span.Count; ++i) {
        const ui32 s = span.First + i;
        const ui64 offset = format.Offset(chunkIdx, s);
        auto restored = RestoreOneSector(device, format, offset, format.MagicDataChunk,
            format.ChunkKey, true, issues, TStringBuilder() << location << "[" << chunkIdx << ":" << s << "]",
            {}, ref);
        if (!restored.Ok) {
            ++result.GapCount;
            dst += payload;
            continue;
        }
        if (baseNonce && restored.Nonce != baseNonce + s) {
            // A valid hash with an unexpected nonce is stale data from a previous use of the chunk.
            if (ref == ESectorRef::Referenced) {
                issues.Warning(TStringBuilder() << location << "[" << chunkIdx << ":" << s << "]",
                    TStringBuilder() << "Nonce mismatch expected# " << (baseNonce + s)
                        << " got# " << restored.Nonce, true);
            }
            ++result.GapCount;
            dst += payload;
            continue;
        }
        memcpy(dst, restored.Payload.data(), Min<ui32>(payload, restored.Payload.size()));
        dst += payload;
    }
    return result;
}

} // namespace

TChunkReadResult ReadChunkLogical(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    bool raw,
    TIssueLog& issues)
{
    TChunkReadResult result;
    const ui64 chunkOffset = format.Offset(chunkIdx, 0);
    if (raw) {
        result.Data = TString::Uninitialized(format.ChunkSize);
        device.Pread(result.Data.Detach(), format.ChunkSize, chunkOffset, issues);
        return result;
    }

    if (format.IsPlainDataChunks()) {
        result.Data = TString::Uninitialized(format.ChunkSize);
        memset(result.Data.Detach(), 0, format.ChunkSize);
        device.Pread(result.Data.Detach(), format.ChunkSize, chunkOffset, issues);
        return result;
    }

    // A whole-chunk read is a scan: the tail of a chunk is normally unwritten, so report a summary
    // instead of one warning per never-written sector.
    const ui32 sectors = format.ChunkSize / format.SectorSize;
    result = ReadSectorSpan(device, format, state, chunkIdx, TSectorSpan{0, sectors},
        ESectorRef::Unreferenced, issues, "chunk");
    if (result.GapCount) {
        issues.Info(TStringBuilder() << "chunk[" << chunkIdx << "]",
            TStringBuilder() << result.GapCount << " of " << sectors
                << " sectors have no valid hash (never written or stale); exported as zeros");
    }
    return result;
}

bool WriteChunkToFile(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    const TString& path,
    bool raw,
    TIssueLog& issues,
    ui64& bytesWritten,
    ui32& gaps)
{
    auto data = ReadChunkLogical(device, format, state, chunkIdx, raw, issues);
    TFileOutput out(path);
    out.Write(data.Data.data(), data.Data.size());
    out.Flush();
    bytesWritten = data.Data.size();
    gaps = data.GapCount;
    return true;
}

TString ReadLogicalRange(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    ui32 offset,
    ui32 size,
    TIssueLog& issues,
    const TString& location)
{
    if (size == 0) {
        return {};
    }
    if (format.IsPlainDataChunks()) {
        if (offset >= format.ChunkSize) {
            issues.Warning(location, TStringBuilder() << "Read offset past chunk end offset# " << offset, true);
            return {};
        }
        const ui32 take = Min<ui32>(size, format.ChunkSize - offset);
        TString data = TString::Uninitialized(take);
        device.Pread(data.Detach(), take, format.Offset(chunkIdx, 0) + offset, issues);
        return data;
    }

    const ui32 payload = format.SectorPayloadSize();
    const ui32 sectors = format.ChunkSize / format.SectorSize;
    const ui64 logicalSize = ui64(sectors) * payload;
    if (offset >= logicalSize) {
        issues.Warning(location, TStringBuilder() << "Read offset past chunk end offset# " << offset, true);
        return {};
    }
    const ui32 take = Min<ui64>(size, logicalSize - offset);
    const TSectorSpan span = LogicalSpanToSectors(format, offset, take);
    auto covering = ReadSectorSpan(device, format, state, chunkIdx, span,
        ESectorRef::Referenced, issues, location);
    const ui32 skip = offset - span.First * payload;
    if (skip >= covering.Data.size()) {
        return {};
    }
    return covering.Data.substr(skip, Min<ui64>(take, covering.Data.size() - skip));
}

TRangeCheckResult CheckLogicalRange(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    ui32 offset,
    ui32 size,
    TIssueLog& issues,
    const TString& location)
{
    TRangeCheckResult result;
    if (size == 0 || format.IsPlainDataChunks()) {
        return result; // plain chunks carry no per-sector hash
    }
    ui64 baseNonce = 0;
    if (chunkIdx < state.Chunks.size()) {
        baseNonce = state.Chunks[chunkIdx].Nonce;
    }
    const TSectorSpan span = LogicalSpanToSectors(format, offset, size);
    for (ui32 i = 0; i < span.Count; ++i) {
        const ui32 s = span.First + i;
        const ui64 sectorOffset = format.Offset(chunkIdx, s);
        const TString where = TStringBuilder() << location << "[" << chunkIdx << ":" << s << "]";
        ++result.Checked;
        auto restored = RestoreOneSector(device, format, sectorOffset, format.MagicDataChunk,
            format.ChunkKey, false, issues, where, {}, ESectorRef::Referenced);
        if (!restored.Ok) {
            ++result.Bad;
            continue;
        }
        if (baseNonce && restored.Nonce != baseNonce + s) {
            // Same judgement as a read makes: a valid hash with the wrong nonce is data left over
            // from a previous use of the chunk, not the referenced content.
            issues.Warning(where, TStringBuilder() << "Nonce mismatch expected# " << (baseNonce + s)
                << " got# " << restored.Nonce, true);
            ++result.Bad;
        }
    }
    return result;
}

} // namespace NKikimr::NPDiskTool
