#pragma once

#include "device.h"
#include "state.h"

namespace NKikimr::NPDiskTool {

struct TLogRecordView {
    TOwner OwnerId = 0;
    TLogSignature Signature = 0;
    ui64 Lsn = 0;
    ui64 Nonce = 0;
    TChunkIdx ChunkIdx = 0;
    TString Payload; // user data (commit footer stripped)
    TString RawPayload; // including commit arrays + footer if present
    bool HasCommit = false;
    bool IsStartingPoint = false;
    ui64 FirstLsnToKeep = 0;
    TVector<TChunkIdx> CommitChunks;
    TVector<ui64> CommitNonces;
    TVector<TChunkIdx> DeleteChunks;
};

struct TLogScanResult {
    TVector<TLogRecordView> Records;
    TVector<TLogChunkSnapshot> LogChunks;
    ui32 LastChunkIdx = 0;
    ui32 LastSectorIdx = 0;
};

TLogScanResult ScanMainLog(
    IDeviceReader& device,
    const TDiskFormat& format,
    TParsedSysLog& state,
    TIssueLog& issues);

constexpr ui32 LogExportMagic = 0x474C4450; // 'PDLG'
constexpr ui32 LogExportVersion = 1;

bool WriteLogExport(
    const TDiskFormat& format,
    const TLogScanResult& scan,
    const TString& path,
    ui32 ownerFilter, // Max means all
    TIssueLog& issues,
    ui64& bytesWritten);

TLogScanResult ReadLogExport(const TString& path, TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
