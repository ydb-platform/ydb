#include "blobstorage_pdisk_ut_context.h"
#include "blobstorage_pdisk_ut_helpers.h"

namespace NKikimr {

TTestContext::TTestContext(bool useSectorMap, EDiskMode diskMode, ui64 diskSize, const TString& usePath) {

    if (useSectorMap) {
        const auto compression = NPDisk::NSectorMap::TryGetCompressionFromPath(usePath);
        Y_ABORT_UNLESS(compression, "Invalid SectorMap compression in path: %s", usePath.c_str());
        SectorMap = new NPDisk::TSectorMap(diskSize, diskMode, *compression);
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
