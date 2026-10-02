#include "blobstorage_pdisk_ut_context.h"
#include "blobstorage_pdisk_ut_helpers.h"

namespace NKikimr {

TTestContext::TTestContext(bool useSectorMap, EDiskMode diskMode, ui64 diskSize, const TString& usePath) {

    if (useSectorMap) {
        SectorMap = new NPDisk::TSectorMap(diskSize, diskMode);
    } else {
        if (usePath) {
            Path = usePath;
            Cerr << "Using specified Path# " << Path << Endl;
        } else {
            TempDir.Reset(new TTempDir);
            Path = CreateFile(GetDir(), diskSize);
            Cerr << "Path# " << Path << Endl;
        }
    }
}

} // namespace NKikimr
