#include "defs.h"


#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_actions.h"
#include "blobstorage_pdisk_ut_config.h"
#include "blobstorage_pdisk_ut_run.h"

#include <util/folder/tempdir.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TYardTestRestore) {

    YARD_UNIT_TEST(TestRestore15) {
        TTestContext tc(false, true);
        ui32 chunkSize = MIN_CHUNK_SIZE;
        Run<TTestWriteChunksAndLog>(&tc, 1, chunkSize, false);
        // TODO(kruall): fix the test and remove the line below
        return;
        ui32 dataSize = 8 * chunkSize;
        NPDisk::TAlignedData dataAfter(dataSize);
        ReadPdiskFile(&tc, dataSize, dataAfter);

        for (ui32 i = 0; i < 15; ++i) {
            VERBOSE_COUT("TestRestore15 i=" << i);
            DestroySectors(&tc, dataAfter, dataSize, i, 15);

            Run<TTestCheckLog>(&tc, 1, chunkSize, false);
            //Can't use resutlts for the next test because we don't wait for the restoration before shutting down.
        }
    }

}

} // namespace NKikimr
