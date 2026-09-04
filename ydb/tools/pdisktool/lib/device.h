#pragma once

#include "issues.h"

#include <ydb/library/pdisk_io/sector_map.h>

#include <util/generic/ptr.h>
#include <util/system/file.h>

namespace NKikimr::NPDiskTool {

class IDeviceReader : public TThrRefBase {
public:
    virtual ~IDeviceReader() = default;
    virtual ui64 Size() const = 0;
    virtual void Pread(void* buffer, ui32 size, ui64 offset, TIssueLog& issues) = 0;
    virtual TString Description() const = 0;
};

TIntrusivePtr<IDeviceReader> OpenFileDevice(const TString& path, bool tryLock, TIssueLog& issues);
TIntrusivePtr<IDeviceReader> OpenSectorMapDevice(TIntrusivePtr<NPDisk::TSectorMap> sectorMap);

} // namespace NKikimr::NPDiskTool
