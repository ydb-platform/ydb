#pragma once

#include "device.h"

namespace NKikimr::NPDiskTool {

struct TFormatReplicaInfo {
    ui32 Index = 0;
    ui64 Nonce = 0;
    bool HashOk = false;
    bool Decrypted = false;
    TString Error;
    TDiskFormat Format;
};

struct TFormatReadResult {
    bool Ok = false;
    TDiskFormat Format;
    TVector<TFormatReplicaInfo> Replicas;
    ui32 WinningKeyIndex = Max<ui32>();
    bool UsedEncryption = true;
};

TFormatReadResult ReadDiskFormat(
    IDeviceReader& device,
    const TMainKey& mainKey,
    TIssueLog& issues,
    bool showKeys = false);

void FillFormatProto(const TFormatReadResult& result, NKikimr::NPdiskTool::TFormatResult& proto, bool showKeys);
void PrintFormatText(const NKikimr::NPdiskTool::TFormatResult& proto, IOutputStream& out);

} // namespace NKikimr::NPDiskTool
