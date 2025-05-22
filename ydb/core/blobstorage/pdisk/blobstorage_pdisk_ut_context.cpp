#include "blobstorage_pdisk_ut_context.h"
#include "blobstorage_pdisk_ut_helpers.h"

namespace NKikimr {

TTestContext::TTestContext(bool useSectorMap, EDiskMode diskMode, ui64 diskSize) {

    if (useSectorMap) {
        SectorMap = new NPDisk::TSectorMap(diskSize, diskMode);
    } else {
        TempDir.Reset(new TTempDir);
        Path = CreateFile(GetDir(), diskSize);
        Cerr << "Path# " << Path << Endl;
    }
}

} // namespace NKikimr
