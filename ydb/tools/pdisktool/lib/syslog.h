#pragma once

#include "device.h"
#include "format.h"

namespace NKikimr::NPDiskTool {

struct TSysLogSectorSet {
    ui32 SetIdx = 0;
    ui32 FirstSectorIdx = 0;
    ui64 Nonce = 0;
    ui32 GoodSectorFlags = 0;
    bool HasStart = false;
    bool HasEnd = false;
    bool HasMiddle = false;
    bool IsConsistent = true;
    bool IsNonceReversal = false;
    ui64 FullPayloadSize = 0;
    ui64 PayloadPartSize = 0;
    ui64 PayloadLsn = 0;
    TLogSignature PayloadSignature = 0;
    TVector<ui8> Payload;
};

struct TSysLogReadResult {
    bool Ok = false;
    TString Payload;
    ui64 Lsn = 0;
    TLogSignature Signature = 0;
    ui32 LoopOffset = 0;
    ui64 BestNonce = 0;
    ui64 MaxNonce = 0;
    TVector<TSysLogSectorSet> SectorSets;
};

TSysLogReadResult ReadSysLog(
    IDeviceReader& device,
    const TDiskFormat& format,
    TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
