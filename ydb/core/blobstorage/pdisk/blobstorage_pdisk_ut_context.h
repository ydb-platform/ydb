#pragma once
#include "defs.h"

#include <ydb/library/pdisk_io/sector_map.h>

#include <util/folder/tempdir.h>
#include <util/folder/dirut.h>

namespace NKikimr {

class TTestContext {
public:
    ui64 PDiskGuid = 0;
    TIntrusivePtr<NPDisk::TSectorMap> SectorMap;
    THolder<TTempDir> TempDir;
    TString Path;

    using EDiskMode = NPDisk::NSectorMap::EDiskMode;
    TTestContext(bool useSectorMap, EDiskMode diskMode = EDiskMode::DM_NONE, ui64 diskSize = 0);

    const char *GetDir() {
        return TempDir ? TempDir->Name().c_str() : nullptr;
    }

    bool IsFormatedDiskExpected() {
        if (SectorMap) {
            if (SectorMap->DeviceSize) {
                return true;
            }
        }
        if (TempDir) {
            return NFs::Exists(TempDir->Name());
        }
        return false;
    }

};

} // NKikimr
