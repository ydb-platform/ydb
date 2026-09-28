#pragma once

#include "blobs.h"
#include "device.h"
#include "format.h"
#include "keys.h"
#include "log.h"
#include "syslog.h"

namespace NKikimr::NPDiskTool {

struct TSessionOptions {
    TMainKey MainKey;
    bool Strict = false;
    bool ShowKeys = false;
    bool TryLock = true;
};

class TPDiskSession {
public:
    TIntrusivePtr<IDeviceReader> Device;
    TIssueLog Issues;
    TSessionOptions Opts;
    TFormatReadResult FormatResult;
    TDiskFormat Format;
    TSysLogReadResult SysLogRaw;
    TParsedSysLog State;
    TLogScanResult Log;
    bool Loaded = false;

    bool OpenFile(const TString& path, const TSessionOptions& opts, bool requireFormat = true);
    bool OpenSectorMap(TIntrusivePtr<NPDisk::TSectorMap> map, const TSessionOptions& opts);

    TOwner ResolveOwner(const TString& vdisk, TMaybe<ui32> ownerId, TIssueLog& issues) const;

private:
    bool Load(const TSessionOptions& opts);
};

void ReadMetadata(
    IDeviceReader& device,
    const TMainKey& mainKey,
    const TFormatReadResult& format,
    const TParsedSysLog* state,
    TIssueLog& issues,
    NKikimr::NPdiskTool::TMetadataResult& proto);

} // namespace NKikimr::NPDiskTool
