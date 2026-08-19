#include "chunk.h"
#include "sector.h"

#include <cstring>
#include <util/stream/file.h>

namespace NKikimr::NPDiskTool {

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

    const bool plain = format.IsPlainDataChunks();
    const ui32 sectors = format.ChunkSize / format.SectorSize;
    const ui32 payload = format.SectorPayloadSize();
    const ui32 logicalSize = plain ? format.ChunkSize : sectors * payload;
    result.Data = TString::Uninitialized(logicalSize);
    memset(result.Data.Detach(), 0, logicalSize);

    ui64 baseNonce = 0;
    if (chunkIdx < state.Chunks.size()) {
        baseNonce = state.Chunks[chunkIdx].Nonce;
    }

    if (plain) {
        device.Pread(result.Data.Detach(), format.ChunkSize, chunkOffset, issues);
        return result;
    }

    char* dst = result.Data.Detach();
    for (ui32 s = 0; s < sectors; ++s) {
        const ui64 offset = format.Offset(chunkIdx, s);
        auto restored = RestoreOneSector(device, format, offset, format.MagicDataChunk,
            format.ChunkKey, true, issues, TStringBuilder() << "chunk[" << chunkIdx << ":" << s << "]");
        if (!restored.Ok) {
            ++result.GapCount;
            dst += payload;
            continue;
        }
        if (baseNonce && restored.Nonce != baseNonce + s) {
            issues.Warning(TStringBuilder() << "chunk[" << chunkIdx << ":" << s << "]",
                TStringBuilder() << "Nonce mismatch expected# " << (baseNonce + s)
                    << " got# " << restored.Nonce, true);
            ++result.GapCount;
            dst += payload;
            continue;
        }
        memcpy(dst, restored.Payload.data(), Min<ui32>(payload, restored.Payload.size()));
        dst += payload;
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
    TIssueLog& issues)
{
    auto all = ReadChunkLogical(device, format, state, chunkIdx, false, issues);
    if (offset > all.Data.size()) {
        issues.Warning("chunk", TStringBuilder() << "Read offset past chunk end offset# " << offset, true);
        return {};
    }
    const ui32 take = Min<ui32>(size, all.Data.size() - offset);
    return all.Data.substr(offset, take);
}

} // namespace NKikimr::NPDiskTool
