#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_env.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPDiskReadOnlyRestore) {

    // When a sys log sector set has lost one of its copies, the reader copies a good one over it and writes it back.
    // A read-only PDisk must not write, so it has to start and serve its owners without that repair.
    Y_UNIT_TEST(StartWithDamagedSysLogCopy) {
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();

        auto cfg = testCtx.GetPDiskConfig();
        const ui64 sectorSize = testCtx.GetPDisk()->Format.SectorSize;
        const ui64 firstSysLogSector = testCtx.GetPDisk()->Format.FirstSysLogSectorIdx();
        const ui64 sysLogSetCount = testCtx.GetPDisk()->Format.SysLogSectorCount;

        // Damage the first copy of every sys log sector set, so that each set that holds a record has two good copies.
        const TString zeroes(sectorSize, '\0');
        for (ui64 set = 0; set < sysLogSetCount; ++set) {
            const ui64 offset = (firstSysLogSector + set * NPDisk::ReplicationFactor) * sectorSize;
            UNIT_ASSERT(testCtx.TestCtx.SectorMap->Write((const ui8*)zeroes.data(), zeroes.size(), offset));
        }

        cfg->ReadOnly = true;
        testCtx.UpdateConfigRecreatePDisk(cfg);

        vdisk.Init();
        vdisk.ReadLog();
    }
}

} // namespace NKikimr
