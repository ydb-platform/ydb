#pragma once
#include "defs.h"

#include <ydb/library/pdisk_io/sector_map.h>

#include <util/folder/tempdir.h>
#include <util/folder/dirut.h>

namespace NKikimr {

class TTestContext {
public:
    ui64 PDiskGuid = 0;
    const char* Dir = nullptr;
    TIntrusivePtr<NPDisk::TSectorMap> SectorMap;
    THolder<TTempDir> TempDir;

    using EDiskMode = NPDisk::NSectorMap::EDiskMode;
    TTestContext(bool makeTempDir, bool useSectorMap, EDiskMode diskMode = EDiskMode::DM_NONE, ui64 sectorMapSize = 0) {
        if (makeTempDir) {
            TempDir.Reset(new TTempDir);
            Dir = TempDir->Name().c_str();
        }
        if (useSectorMap) {
            SectorMap = new NPDisk::TSectorMap(sectorMapSize, diskMode);
        }
    }

    bool IsFormatedDiskExpected() {
        if (SectorMap) {
            if (SectorMap->DeviceSize) {
                return true;
            }
        }
        if (Dir) {
            return NFs::Exists(Dir);
        }
        return false;
    }

};

} // NKikimr
