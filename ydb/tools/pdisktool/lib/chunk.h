#pragma once

#include "device.h"
#include "sector.h"
#include "state.h"

namespace NKikimr::NPDiskTool {

struct TChunkReadResult {
    TString Data; // logical decrypted payload
    ui32 GapCount = 0;
};

struct TRangeCheckResult {
    ui32 Checked = 0;
    ui32 Bad = 0;
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

// Reads a referenced logical range: only the sectors covering it are touched, and a hash mismatch
// there is a real error because something on disk points at this range.
TString ReadLogicalRange(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    ui32 offset,
    ui32 size,
    TIssueLog& issues,
    const TString& location = "chunk");

// Verifies sector hashes over a referenced logical range without keeping the payload.
TRangeCheckResult CheckLogicalRange(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    TChunkIdx chunkIdx,
    ui32 offset,
    ui32 size,
    TIssueLog& issues,
    const TString& location);

} // namespace NKikimr::NPDiskTool
