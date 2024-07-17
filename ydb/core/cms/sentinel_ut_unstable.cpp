#include "sentinel_ut_helpers.h"

namespace NKikimr::NCmsTest {

Y_UNIT_TEST_SUITE(TSentinelUnstableTests) {
    Y_UNIT_TEST(BSControllerCantChangeStatus) {
        TTestEnv env(8, 4);

        const TPDiskID id1 = env.RandomPDiskID();
        const TPDiskID id2 = env.RandomPDiskID();
        const TPDiskID id3 = env.RandomPDiskID();

        for (size_t i = 0; i < sizeof(ErrorStates) / sizeof(ErrorStates[0]); ++i) {
            env.AddBSCFailures(id1, {true, false, false, true, false, false});
            // will fail for all requests assuming there is only 5 retries
            env.AddBSCFailures(id2, {false, false, false, false, false, false});
            env.AddBSCFailures(id3, {false, true, false, false, true, false});
        }

        for (const EPDiskState state : ErrorStates) {
            env.SetPDiskState({id1, id2, id3}, state, EPDiskStatus::FAULTY);
            env.SetPDiskState({id1, id2, id3}, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
        }
    }
}

}
