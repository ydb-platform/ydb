#pragma once

#include "device.h"
#include "state.h"

namespace NKikimr::NPDiskTool {

struct TChunkReadResult {
    TString Data; // logical decrypted payload
    ui32 GapCount = 0;
};

TChunkReadResult ReadChunkLogical(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    bool raw,
    TIssueLog& issues);

bool WriteChunkToFile(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    const TString& path,
    bool raw,
    TIssueLog& issues,
    ui64& bytesWritten,
    ui32& gaps);

TString ReadLogicalRange(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    ui32 offset,
    ui32 size,
    TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
